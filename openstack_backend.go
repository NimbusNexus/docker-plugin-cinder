package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/volumeattach"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// openstackBackend talks to Cinder and Nova directly, with the credential from the config file.
type openstackBackend struct {
	blockClient   *gophercloud.ServiceClient
	computeClient *gophercloud.ServiceClient
	config        *tConfig
}

func newOpenstackBackend(provider *gophercloud.ProviderClient, endpointOpts gophercloud.EndpointOpts, config *tConfig) (*openstackBackend, error) {
	log.Infof("Discovering OpenStack endpoints...")
	log.Infof("EndpointOpts: Region=%s, Availability=%s", endpointOpts.Region, endpointOpts.Availability)

	// Try to create block storage client
	blockClient, err := openstack.NewBlockStorageV3(provider, endpointOpts)
	if err != nil {
		log.Errorf("Failed to discover block storage endpoint using standard method: %v", err)
		identityEndpoint := provider.IdentityEndpoint

		parts := strings.SplitN(identityEndpoint, "://", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid identity endpoint format: %s", identityEndpoint)
		}
		scheme := parts[0]
		hostAndPath := parts[1]

		hostParts := strings.SplitN(hostAndPath, "/", 2)
		host := hostParts[0]

		cinderURL := fmt.Sprintf("%s://%s/volume/v3", scheme, host)

		log.Infof("Attempting manual endpoint construction: %s", cinderURL)

		blockClient = &gophercloud.ServiceClient{
			ProviderClient: provider,
			Endpoint:       cinderURL,
			Type:           "block-storage",
		}

		// ResourceBase should have the trailing slash for proper path construction
		blockClient.ResourceBase = cinderURL + "/"

		log.Infof("Manually constructed block storage client - Endpoint: %s, ResourceBase: %s",
			blockClient.Endpoint, blockClient.ResourceBase)
	} else {
		log.Infof("Block storage endpoint discovered: %s", blockClient.Endpoint)
	}

	computeClient, err := openstack.NewComputeV2(provider, endpointOpts)
	if err != nil {
		log.Errorf("Failed to discover compute endpoint: %v", err)
		return nil, fmt.Errorf("failed to create compute client: %v", err)
	}
	log.Infof("Compute endpoint discovered: %s", computeClient.Endpoint)

	if config.MachineID == "" {
		// Try to get instance UUID from OpenStack metadata first (most reliable)
		machineID, err := getInstanceIDFromMetadata()
		if err == nil && machineID != "" {
			config.MachineID = machineID
			log.Infof("Using instance ID from OpenStack metadata: %s", machineID)
		} else {
			// Fallback to /etc/machine-id
			log.Warnf("Could not get instance ID from metadata: %v, falling back to /etc/machine-id", err)
			bytes, err := os.ReadFile("/etc/machine-id")
			if err != nil {
				return nil, err
			}

			machineIDStr := strings.TrimSpace(string(bytes))

			// /etc/machine-id might be in format without dashes (32 hex chars)
			// OpenStack expects UUID format with dashes
			if len(machineIDStr) == 32 && !strings.Contains(machineIDStr, "-") {
				// Convert to UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
				machineIDStr = fmt.Sprintf("%s-%s-%s-%s-%s",
					machineIDStr[0:8],
					machineIDStr[8:12],
					machineIDStr[12:16],
					machineIDStr[16:20],
					machineIDStr[20:32])
				log.Infof("Converted machine-id to UUID format: %s", machineIDStr)
			}

			// Validate it's a proper UUID
			id, err := uuid.FromString(machineIDStr)
			if err != nil {
				return nil, fmt.Errorf("invalid machine-id format: %v", err)
			}
			config.MachineID = id.String()
			log.Infof("Using machine-id: %s", config.MachineID)
		}
	}

	return &openstackBackend{blockClient: blockClient, computeClient: computeClient, config: config}, nil
}

func (b *openstackBackend) MachineID() string {
	freshID, err := getInstanceIDFromMetadata()
	if err == nil && freshID != "" && freshID != b.config.MachineID {
		log.Warnf("MachineID mismatch! config=%s metadata=%s, updating...", b.config.MachineID, freshID)
		b.config.MachineID = freshID
	}
	return b.config.MachineID
}

func toCinderVolume(v volumes.Volume) *cinderVolume {
	vol := &cinderVolume{
		ID: v.ID, Name: v.Name, Status: v.Status, Size: v.Size, CreatedAt: v.CreatedAt, Metadata: v.Metadata,
	}
	for _, a := range v.Attachments {
		vol.Attachments = append(vol.Attachments, attachment{ServerID: a.ServerID, Device: a.Device})
	}
	return vol
}

func (b *openstackBackend) List(ctx context.Context) ([]cinderVolume, error) {
	var vols []cinderVolume
	err := volumes.List(b.blockClient, volumes.ListOpts{}).EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		vList, _ := volumes.ExtractVolumes(page)
		for _, v := range vList {
			vols = append(vols, *toCinderVolume(v))
		}
		return true, nil
	})
	return vols, err
}

func (b *openstackBackend) GetByName(ctx context.Context, name string) (*cinderVolume, error) {
	var vol *cinderVolume
	err := volumes.List(b.blockClient, volumes.ListOpts{Name: name}).EachPage(ctx, func(ctx context.Context, page pagination.Page) (bool, error) {
		vList, _ := volumes.ExtractVolumes(page)
		for _, v := range vList {
			if v.Name == name {
				vol = toCinderVolume(v)
				return false, nil
			}
		}
		return true, nil
	})
	if vol == nil || vol.ID == "" {
		return nil, errNotFound
	}
	return vol, err
}

func (b *openstackBackend) Refresh(ctx context.Context, vol *cinderVolume) (*cinderVolume, error) {
	v, err := volumes.Get(ctx, b.blockClient, vol.ID).Extract()
	if err != nil {
		return nil, err
	}
	return toCinderVolume(*v), nil
}

func (b *openstackBackend) Create(ctx context.Context, name string, size int, metadata map[string]string) error {
	_, err := volumes.Create(ctx, b.blockClient, volumes.CreateOpts{
		Size: size, Name: name, Metadata: metadata,
	}, volumes.SchedulerHintOpts{}).Extract()
	return err
}

func (b *openstackBackend) SetMetadata(ctx context.Context, vol *cinderVolume, metadata map[string]string) error {
	_, err := volumes.Update(ctx, b.blockClient, vol.ID, volumes.UpdateOpts{Metadata: metadata}).Extract()
	return err
}

func (b *openstackBackend) Extend(ctx context.Context, vol *cinderVolume, size int) error {
	return volumes.ExtendSize(ctx, b.blockClient, vol.ID, volumes.ExtendSizeOpts{NewSize: size}).ExtractErr()
}

func (b *openstackBackend) Attach(ctx context.Context, vol *cinderVolume) error {
	_, err := volumeattach.Create(ctx, b.computeClient, b.config.MachineID, volumeattach.CreateOpts{VolumeID: vol.ID}).Extract()
	return err
}

func (b *openstackBackend) Detach(ctx context.Context, vol *cinderVolume, serverID string) error {
	// Nova names an attachment by the volume's id.
	err := volumeattach.Delete(ctx, b.computeClient, serverID, vol.ID).ExtractErr()
	if err != nil && strings.Contains(err.Error(), "task_state") {
		return fmt.Errorf("%w: %v", errConflict, err)
	}
	return err
}

func (b *openstackBackend) Delete(ctx context.Context, vol *cinderVolume) error {
	return volumes.Delete(ctx, b.blockClient, vol.ID, volumes.DeleteOpts{}).ExtractErr()
}
