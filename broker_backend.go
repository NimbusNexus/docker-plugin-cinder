package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const vendordataURL = "http://169.254.169.254/openstack/latest/vendor_data2.json"

// brokerBackend talks to the platform's VM broker instead of OpenStack: the machine holds no
// credential. It proves which server it is with the token Nova's dynamic vendordata hands it
// (`vendor_data2.json`, under the broker target's name); the broker checks what the machine may do
// with the platform's authorization rules and acts with its cluster's own credential. Volume names
// are the Docker names — the broker keeps each cluster's volumes in a namespace of its own.
type brokerBackend struct {
	baseURL    string
	target     string
	vendordata string
	http       *http.Client

	mutex      sync.Mutex
	instanceID string
	token      string
}

func newBrokerBackend(config *tConfig) (*brokerBackend, error) {
	target := config.VendordataTarget
	if target == "" {
		target = "broker"
	}
	b := &brokerBackend{
		baseURL:    strings.TrimRight(config.BrokerURL, "/"),
		target:     target,
		vendordata: vendordataURL,
		http:       &http.Client{Timeout: 30 * time.Second},
	}
	// The token is fetched now so a machine whose vendordata does not cover it fails at start, not
	// at the first volume.
	if err := b.refreshToken(); err != nil {
		return nil, err
	}
	log.Infof("VM broker %s, instance %s", b.baseURL, b.instanceID)
	return b, nil
}

func (b *brokerBackend) refreshToken() error {
	resp, err := b.http.Get(b.vendordata)
	if err != nil {
		return fmt.Errorf("vendordata unreachable: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("vendordata returned status %d", resp.StatusCode)
	}
	var doc map[string]struct {
		InstanceID string `json:"instance_id"`
		Token      string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return fmt.Errorf("vendordata unreadable: %v", err)
	}
	entry, ok := doc[b.target]
	if !ok || entry.InstanceID == "" || entry.Token == "" {
		return fmt.Errorf("vendordata has no %q entry: no grant covers this machine yet", b.target)
	}
	b.mutex.Lock()
	b.instanceID, b.token = entry.InstanceID, entry.Token
	b.mutex.Unlock()
	return nil
}

func (b *brokerBackend) credentials() (string, string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.instanceID, b.token
}

// call sends one request. A 401/403 re-reads the token once — the broker's key may have been
// rotated — before giving up.
func (b *brokerBackend) call(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	for attempt := 0; attempt < 2; attempt++ {
		status, payload, err := b.send(ctx, method, path, body)
		if err != nil {
			return err
		}
		switch {
		case status >= 200 && status < 300:
			if out != nil && len(payload) > 0 {
				return json.Unmarshal(payload, out)
			}
			return nil
		case status == http.StatusNotFound:
			return errNotFound
		case status == http.StatusConflict:
			return fmt.Errorf("%w: %s", errConflict, payload)
		case (status == http.StatusUnauthorized || status == http.StatusForbidden) && attempt == 0:
			if err := b.refreshToken(); err != nil {
				return err
			}
			continue
		default:
			return fmt.Errorf("broker %s %s: status %d: %s", method, path, status, payload)
		}
	}
	return fmt.Errorf("broker %s %s: refused", method, path)
}

func (b *brokerBackend) send(ctx context.Context, method, path string, body interface{}) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.baseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	instanceID, token := b.credentials()
	req.Header.Set("X-Instance-Id", instanceID)
	req.Header.Set("X-Broker-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	return resp.StatusCode, payload, err
}

type brokerVolume struct {
	Name       string            `json:"name"`
	VolumeID   string            `json:"volume_id"`
	Status     string            `json:"status"`
	SizeGB     int               `json:"size_gb"`
	CreatedAt  string            `json:"created_at"`
	AttachedTo []string          `json:"attached_to"`
	Device     string            `json:"device"`
	Metadata   map[string]string `json:"metadata"`
}

func (v brokerVolume) toCinderVolume(instanceID string) *cinderVolume {
	created, _ := time.Parse(time.RFC3339, v.CreatedAt)
	if created.IsZero() {
		created, _ = time.Parse("2006-01-02T15:04:05.000000", v.CreatedAt)
	}
	vol := &cinderVolume{
		ID: v.VolumeID, Name: v.Name, Status: v.Status, Size: v.SizeGB, CreatedAt: created, Metadata: v.Metadata,
	}
	for _, server := range v.AttachedTo {
		device := ""
		if server == instanceID {
			device = v.Device
		}
		vol.Attachments = append(vol.Attachments, attachment{ServerID: server, Device: device})
	}
	return vol
}

func volumePath(name string) string {
	return "/vm/volumes/" + url.PathEscape(name)
}

func (b *brokerBackend) MachineID() string {
	instanceID, _ := b.credentials()
	return instanceID
}

func (b *brokerBackend) List(ctx context.Context) ([]cinderVolume, error) {
	var listed struct {
		Items []brokerVolume `json:"items"`
	}
	if err := b.call(ctx, http.MethodGet, "/vm/volumes", nil, &listed); err != nil {
		return nil, err
	}
	vols := make([]cinderVolume, 0, len(listed.Items))
	for _, v := range listed.Items {
		vols = append(vols, *v.toCinderVolume(b.MachineID()))
	}
	return vols, nil
}

func (b *brokerBackend) read(ctx context.Context, method, path string, body interface{}) (*cinderVolume, error) {
	var v brokerVolume
	if err := b.call(ctx, method, path, body, &v); err != nil {
		return nil, err
	}
	return v.toCinderVolume(b.MachineID()), nil
}

func (b *brokerBackend) GetByName(ctx context.Context, name string) (*cinderVolume, error) {
	return b.read(ctx, http.MethodGet, volumePath(name), nil)
}

func (b *brokerBackend) Refresh(ctx context.Context, vol *cinderVolume) (*cinderVolume, error) {
	return b.GetByName(ctx, vol.Name)
}

func (b *brokerBackend) Create(ctx context.Context, name string, size int, metadata map[string]string) error {
	_, err := b.read(ctx, http.MethodPut, volumePath(name), map[string]interface{}{"size_gb": size, "metadata": metadata})
	return err
}

func (b *brokerBackend) SetMetadata(ctx context.Context, vol *cinderVolume, metadata map[string]string) error {
	_, err := b.read(ctx, http.MethodPatch, volumePath(vol.Name), map[string]interface{}{"metadata": metadata})
	return err
}

func (b *brokerBackend) Extend(ctx context.Context, vol *cinderVolume, size int) error {
	_, err := b.read(ctx, http.MethodPost, volumePath(vol.Name)+"/extend", map[string]interface{}{"size_gb": size})
	return err
}

func (b *brokerBackend) Attach(ctx context.Context, vol *cinderVolume) error {
	_, err := b.read(ctx, http.MethodPut, volumePath(vol.Name)+"/attachment", nil)
	return err
}

func (b *brokerBackend) Detach(ctx context.Context, vol *cinderVolume, serverID string) error {
	path := volumePath(vol.Name) + "/attachment"
	if serverID != "" && serverID != b.MachineID() {
		path += "?server_id=" + url.QueryEscape(serverID)
	}
	_, err := b.read(ctx, http.MethodDelete, path, nil)
	return err
}

func (b *brokerBackend) Delete(ctx context.Context, vol *cinderVolume) error {
	return b.call(ctx, http.MethodDelete, volumePath(vol.Name), nil, nil)
}
