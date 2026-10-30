package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// A vendordata document and a broker in one test server: /vendordata hands out the token the
// broker accepts, the rest records what the plugin sent.
type fakeBroker struct {
	mutex    sync.Mutex
	token    string
	requests []*http.Request
	bodies   []map[string]interface{}
	reply    func(r *http.Request) (int, interface{})
}

func (f *fakeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if r.URL.Path == "/vendordata" {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"static": map[string]string{},
			"broker": map[string]string{"instance_id": "i-1", "token": f.token},
		})
		return
	}
	f.requests = append(f.requests, r)
	var body map[string]interface{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.bodies = append(f.bodies, body)
	if r.Header.Get("X-Broker-Token") != f.token {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	status, out := f.reply(r)
	w.WriteHeader(status)
	if out != nil {
		_ = json.NewEncoder(w).Encode(out)
	}
}

func newFake(t *testing.T, reply func(r *http.Request) (int, interface{})) (*fakeBroker, *brokerBackend) {
	fake := &fakeBroker{token: "t-1", reply: reply}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	b := &brokerBackend{baseURL: server.URL + "/api/broker/v1", target: "broker",
		vendordata: server.URL + "/vendordata", http: server.Client()}
	if err := b.refreshToken(); err != nil {
		t.Fatal(err)
	}
	return fake, b
}

var dataVolume = map[string]interface{}{
	"name": "data", "volume_id": "v-1", "status": "in-use", "size_gb": 5,
	"created_at": "2026-10-28T10:00:00.000000", "attached_to": []string{"i-1", "i-2"}, "device": "/dev/vdb",
	"metadata": map[string]string{"expected_size": "5"},
}

func TestMachineIsWhoVendordataSaysAndEveryCallCarriesItsToken(t *testing.T) {
	fake, b := newFake(t, func(r *http.Request) (int, interface{}) { return 200, dataVolume })

	vol, err := b.GetByName(context.Background(), "data")
	if err != nil {
		t.Fatal(err)
	}
	if b.MachineID() != "i-1" {
		t.Fatalf("machine id %q", b.MachineID())
	}
	r := fake.requests[0]
	if r.URL.Path != "/api/broker/v1/vm/volumes/data" || r.Header.Get("X-Instance-Id") != "i-1" || r.Header.Get("X-Broker-Token") != "t-1" {
		t.Fatalf("request %s %v", r.URL.Path, r.Header)
	}
	if vol.ID != "v-1" || vol.Size != 5 || len(vol.Attachments) != 2 || vol.Attachments[0].Device != "/dev/vdb" || vol.Attachments[1].Device != "" {
		t.Fatalf("volume %+v", vol)
	}
	if vol.CreatedAt.IsZero() {
		t.Fatal("created_at not parsed")
	}
}

func TestARotatedTokenIsReadAgainOnce(t *testing.T) {
	fake, b := newFake(t, func(r *http.Request) (int, interface{}) { return 200, dataVolume })
	fake.token = "t-2" // the broker's key changed; the plugin still holds t-1

	if _, err := b.GetByName(context.Background(), "data"); err != nil {
		t.Fatal(err)
	}
	if len(fake.requests) != 2 || fake.requests[1].Header.Get("X-Broker-Token") != "t-2" {
		t.Fatalf("expected a retry with the new token, got %d requests", len(fake.requests))
	}
}

func TestMissingAndConflictAreTheBackendsErrors(t *testing.T) {
	_, missing := newFake(t, func(r *http.Request) (int, interface{}) { return 404, map[string]string{"message": "no"} })
	if _, err := missing.GetByName(context.Background(), "data"); !errors.Is(err, errNotFound) {
		t.Fatalf("want errNotFound, got %v", err)
	}

	_, busy := newFake(t, func(r *http.Request) (int, interface{}) { return 409, map[string]string{"message": "rebooting"} })
	if err := busy.Detach(context.Background(), &cinderVolume{Name: "data"}, "i-2"); !errors.Is(err, errConflict) {
		t.Fatalf("want errConflict, got %v", err)
	}
}

func TestDetachFromAnotherServerNamesIt(t *testing.T) {
	fake, b := newFake(t, func(r *http.Request) (int, interface{}) { return 200, dataVolume })

	if err := b.Detach(context.Background(), &cinderVolume{Name: "data"}, "i-2"); err != nil {
		t.Fatal(err)
	}
	if err := b.Detach(context.Background(), &cinderVolume{Name: "data"}, "i-1"); err != nil {
		t.Fatal(err)
	}
	if got := fake.requests[0].URL.RequestURI(); got != "/api/broker/v1/vm/volumes/data/attachment?server_id=i-2" {
		t.Fatalf("detach from another server: %s", got)
	}
	if got := fake.requests[1].URL.RequestURI(); got != "/api/broker/v1/vm/volumes/data/attachment" {
		t.Fatalf("detach from itself: %s", got)
	}
	if fake.requests[0].Method != http.MethodDelete {
		t.Fatalf("method %s", fake.requests[0].Method)
	}
}

func TestCreateSendsSizeAndMetadata(t *testing.T) {
	fake, b := newFake(t, func(r *http.Request) (int, interface{}) { return 200, dataVolume })

	if err := b.Create(context.Background(), "data", 5, map[string]string{"key_lock": "false"}); err != nil {
		t.Fatal(err)
	}
	if fake.requests[0].Method != http.MethodPut || fake.bodies[0]["size_gb"].(float64) != 5 {
		t.Fatalf("create %s %v", fake.requests[0].Method, fake.bodies[0])
	}
}

func TestNoVendordataEntryMeansNoGrantYet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"static": map[string]string{}})
	}))
	defer server.Close()
	b := &brokerBackend{target: "broker", vendordata: server.URL, http: server.Client()}

	if err := b.refreshToken(); err == nil {
		t.Fatal("expected an error without a broker entry")
	}
}
