package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	"github.com/docker/go-plugins-helpers/volume"
	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/volumeattach"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

type plugin struct {
	blockClient   *gophercloud.ServiceClient
	computeClient *gophercloud.ServiceClient
	config        *tConfig
	mutex         *sync.Mutex
}

// --- Plugin constructor ---
func newPlugin(provider *gophercloud.ProviderClient, endpointOpts gophercloud.EndpointOpts, config *tConfig) (*plugin, error) {
	blockClient, err := openstack.NewBlockStorageV3(provider, endpointOpts)
	if err != nil {
		return nil, err
	}

	computeClient, err := openstack.NewComputeV2(provider, endpointOpts)
	if err != nil {
		return nil, err
	}

	if config.MachineID == "" {
		bytes, err := os.ReadFile("/etc/machine-id")
		if err != nil {
			return nil, err
		}
		id, err := uuid.FromString(strings.TrimSpace(string(bytes)))
		if err != nil {
			return nil, err
		}
		config.MachineID = id.String()
	}

	return &plugin{
		blockClient:   blockClient,
		computeClient: computeClient,
		config:        config,
		mutex:         &sync.Mutex{},
	}, nil
}

// --- Docker plugin capabilities ---
func (d plugin) Capabilities() *volume.CapabilitiesResponse {
	return &volume.CapabilitiesResponse{
		Capabilities: volume.Capability{Scope: "global"},
	}
}

// --- Create volume ---
func (d plugin) Create(r *volume.CreateRequest) error {
	logger := log.WithFields(log.Fields{"name": r.Name, "action": "create"})
	logger.Infof("Creating volume '%s' ...", r.Name)

	ctx := context.TODO()
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// DEFAULTS
	size := 10
	if s, ok := r.Options["size"]; ok {
		if v, err := strconv.Atoi(s); err == nil {
			size = v
		}
	}

	// Create volume only (do not attach yet)
	_, err := volumes.Create(ctx, d.blockClient, volumes.CreateOpts{
		Size: size,
		Name: r.Name,
	}, volumes.SchedulerHintOpts{}).Extract()
	if err != nil {
		return fmt.Errorf("Volume create failed: %v", err)
	}

	logger.Infof("Volume '%s' created (available)", r.Name)
	return nil
}

// --- Get volume ---
func (d plugin) Get(r *volume.GetRequest) (*volume.GetResponse, error) {
	vol, err := d.getByName(r.Name)
	if err != nil {
		return nil, err
	}

	return &volume.GetResponse{
		Volume: &volume.Volume{
			Name:       r.Name,
			CreatedAt:  vol.CreatedAt.Format(time.RFC3339),
			Mountpoint: filepath.Join(d.config.MountDir, r.Name),
		},
	}, nil
}

// --- List volumes ---
func (d plugin) List() (*volume.ListResponse, error) {
	ctx := context.TODO()
	var vols []*volume.Volume

	pager := volumes.List(d.blockClient, volumes.ListOpts{})
	_ = pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		vList, _ := volumes.ExtractVolumes(page)
		for _, v := range vList {
			if v.Name != "" {
				vols = append(vols, &volume.Volume{
					Name:      v.Name,
					CreatedAt: v.CreatedAt.Format(time.RFC3339),
				})
			}
		}
		return true, nil
	})

	return &volume.ListResponse{Volumes: vols}, nil
}

// --- Mount volume ---
func (d plugin) Mount(r *volume.MountRequest) (*volume.MountResponse, error) {
    logger := log.WithFields(log.Fields{"name": r.Name, "action": "mount"})
    logger.Infof("Mounting volume '%s'", r.Name)

    vol, err := d.getByName(r.Name)
    if err != nil {
        return nil, fmt.Errorf("Volume not found: %v", err)
    }

    // ⚠️ Зберігаємо список пристроїв ДО attach
    existing, _ := filepath.Glob("/dev/vd*")
    logger.Infof("Devices before attach: %v", existing)

    // Attach only if not attached
    if len(vol.Attachments) == 0 {
        logger.Infof("Attaching volume %s to instance %s", vol.ID, d.config.MachineID)
        opts := volumeattach.CreateOpts{VolumeID: vol.ID}
        _, err := volumeattach.Create(context.TODO(), d.computeClient, d.config.MachineID, opts).Extract()
        if err != nil {
            return nil, fmt.Errorf("Attach failed: %v", err)
        }

        logger.Infof("Waiting for volume to reach 'in-use' state")
        vol, err = d.waitOnVolumeState(context.TODO(), vol, "in-use")
        if err != nil {
            return nil, fmt.Errorf("Timeout waiting for volume to attach: %v", err)
        }
        logger.Infof("Volume %s is now in-use", vol.ID)
    } else {
        logger.Infof("Volume %s already attached", vol.ID)
    }

    logger.Infof("Searching for new block device...")
    dev, err := findDeviceWithTimeout(existing)
    if err != nil {
        // Додатково виведемо поточний список пристроїв
        current, _ := filepath.Glob("/dev/vd*")
        logger.Errorf("Failed to find device. Before: %v, After: %v", existing, current)
        return nil, fmt.Errorf("Block device not found for volume %s", vol.ID)
    }

    logger.Infof("Found device: %s", dev)

    fsType, err := getFilesystemType(dev)
    if err != nil {
        return nil, fmt.Errorf("Detecting filesystem failed: %v", err)
    }

    if fsType == "" {
        logger.Infof("Formatting device %s as ext4", dev)
        if err := formatFilesystem(dev, r.Name, "ext4"); err != nil {
            return nil, fmt.Errorf("Formatting failed: %v", err)
        }
    } else {
        logger.Infof("Device %s already has filesystem: %s", dev, fsType)
    }

    mountPath := filepath.Join(d.config.MountDir, r.Name)
    if err = os.MkdirAll(mountPath, 0700); err != nil {
        return nil, fmt.Errorf("Cannot create mount path: %v", err)
    }

    logger.Infof("Mounting %s to %s", dev, mountPath)
    out, err := exec.Command("mount", dev, mountPath).CombinedOutput()
    if err != nil {
        return nil, fmt.Errorf("Mount failed: %s", out)
    }

    logger.Infof("Volume '%s' mounted successfully at %s", r.Name, mountPath)
    return &volume.MountResponse{Mountpoint: mountPath}, nil
}

// --- Допоміжна функція: знайти новий диск ---
func findNewDevice(existing []string) (string, error) {
	for i := 0; i < 20; i++ {
		time.Sleep(500 * time.Millisecond)
		devices, _ := filepath.Glob("/dev/vd*")

		for _, d := range devices {
			if !contains(existing, d) && !strings.HasSuffix(d, "1") &&
				!strings.HasSuffix(d, "2") && !strings.HasSuffix(d, "3") && !strings.HasSuffix(d, "4") {
				return d, nil
			}
		}
	}
	return "", errors.New("block device not found")
}

// --- Path ---
func (d plugin) Path(r *volume.PathRequest) (*volume.PathResponse, error) {
	return &volume.PathResponse{
		Mountpoint: filepath.Join(d.config.MountDir, r.Name),
	}, nil
}

// --- Remove volume ---
func (d plugin) Remove(r *volume.RemoveRequest) error {
	ctx := context.TODO()
	vol, err := d.getByName(r.Name)
	if err != nil {
		return err
	}

	if len(vol.Attachments) > 0 {
		if _, err := d.detachVolume(ctx, vol); err != nil {
			return err
		}
	}

	return volumes.Delete(ctx, d.blockClient, vol.ID, volumes.DeleteOpts{}).ExtractErr()
}

// --- Unmount volume ---
func (d plugin) Unmount(r *volume.UnmountRequest) error {
	ctx := context.TODO()
	d.mutex.Lock()
	defer d.mutex.Unlock()

	mountPath := filepath.Join(d.config.MountDir, r.Name)
	if _, err := os.Stat(mountPath); err == nil {
		_ = syscall.Unmount(mountPath, 0)
	}

	vol, err := d.getByName(r.Name)
	if err != nil {
		return err
	}

	if len(vol.Attachments) > 0 {
		if _, err := d.detachVolume(ctx, vol); err != nil {
			return err
		}
		if _, err := d.waitOnVolumeState(ctx, vol, "available"); err != nil {
			return err
		}
	}

	return nil
}

// --- Helper: get volume by name ---
func (d plugin) getByName(name string) (*volumes.Volume, error) {
	var vol *volumes.Volume
	ctx := context.TODO()
	pager := volumes.List(d.blockClient, volumes.ListOpts{Name: name})
	err := pager.EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		vList, _ := volumes.ExtractVolumes(page)
		for _, v := range vList {
			if v.Name == name {
				vol = &v
				return false, nil
			}
		}
		return true, nil
	})

	if vol == nil || vol.ID == "" {
		return nil, errors.New("Not Found")
	}
	return vol, err
}

// --- Helper: detach volume ---
func (d plugin) detachVolume(ctx context.Context, vol *volumes.Volume) (*volumes.Volume, error) {
	for _, att := range vol.Attachments {
		if err := volumeattach.Delete(ctx, d.computeClient, att.ServerID, att.ID).ExtractErr(); err != nil {
			return nil, err
		}
	}
	return vol, nil
}

// --- Helper: wait for status ---
func (d plugin) waitOnVolumeState(ctx context.Context, vol *volumes.Volume, status string) (*volumes.Volume, error) {
	if vol.Status == status {
		return vol, nil
	}

	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return vol, fmt.Errorf("volume %s did not reach status %s (last=%s)", vol.ID, status, vol.Status)
		case <-ticker.C:
			updated, err := volumes.Get(ctx, d.blockClient, vol.ID).Extract()
			if err != nil {
				return nil, err
			}
			vol = updated
			if vol.Status == status {
				return vol, nil
			}
			if status == "in-use" && vol.Status == "available" {
				log.Warnf("Volume %s still available while waiting for in-use, continuing", vol.ID)
			}
		}
	}
}
