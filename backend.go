package main

import (
	"context"
	"errors"
	"time"
)

// What the plugin needs from a block storage service. Two implementations: openstackBackend talks
// to Cinder and Nova with the credential in the config file; brokerBackend talks to the platform's
// VM broker with the machine's token from vendordata, and the machine holds no credential at all.
// The plugin logic — finding the device, formatting, mounting, resizing — is the same for both.

// errNotFound: no volume of that name.
var errNotFound = errors.New("Not Found")

// errConflict: the service refused for now — the server holding the volume is still rebooting,
// the volume is busy. Worth retrying.
var errConflict = errors.New("conflict")

type attachment struct {
	ServerID string
	Device   string
}

type cinderVolume struct {
	ID          string
	Name        string
	Status      string
	Size        int
	CreatedAt   time.Time
	Metadata    map[string]string
	Attachments []attachment
}

type backend interface {
	// The server this plugin runs on.
	MachineID() string
	List(ctx context.Context) ([]cinderVolume, error)
	// errNotFound when there is no volume of that name.
	GetByName(ctx context.Context, name string) (*cinderVolume, error)
	Refresh(ctx context.Context, vol *cinderVolume) (*cinderVolume, error)
	Create(ctx context.Context, name string, size int, metadata map[string]string) error
	SetMetadata(ctx context.Context, vol *cinderVolume, metadata map[string]string) error
	Extend(ctx context.Context, vol *cinderVolume, size int) error
	// Always to this machine.
	Attach(ctx context.Context, vol *cinderVolume) error
	// From serverID: this machine, or the one the volume is still attached to.
	Detach(ctx context.Context, vol *cinderVolume, serverID string) error
	Delete(ctx context.Context, vol *cinderVolume) error
}
