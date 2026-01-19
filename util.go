package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var filesystems []string = []string{
	"btrfs",
	"ext2",
	"ext3",
	"ext4",
	"fat",
	"fat32",
	"ntfs",
	"xfs",
}

func contains(slice []string, x string) bool {
	for _, s := range slice {
		if s == x {
			return true
		}
	}
	return false
}

func getFilesystemType(dev string) (string, error) {
	out, err := exec.Command("blkid", "-s", "TYPE", "-o", "value", dev).CombinedOutput()
	if err != nil {
		if len(out) == 0 {
			return "", nil
		}
		return "", errors.New(string(out))
	}
	return strings.TrimSpace(string(out)), nil
}

func formatFilesystem(dev, label, filesystem string) error {
	if !contains(filesystems, filesystem) {
		return fmt.Errorf("filesystem '%s' does not exist", filesystem)
	}

	path, err := exec.LookPath("mkfs." + filesystem)
	if err != nil {
		return fmt.Errorf("mkfs.%s not found", filesystem)
	}

	out, err := exec.Command(path, "-L", label, dev).CombinedOutput()
	if err != nil {
		return errors.New(string(out))
	}

	return nil
}

func findDeviceWithTimeout(existing []string) (string, error) {
    log.Infof("Starting device search. Existing devices: %v", existing)

    for i := 0; i < 20; i++ {
        time.Sleep(500 * time.Millisecond)
        devices, _ := filepath.Glob("/dev/vd*")
        log.Debugf("Iteration %d: found devices: %v", i, devices)

        var newDevices []string
        for _, d := range devices {
            isPartition := strings.HasSuffix(d, "1") ||
                          strings.HasSuffix(d, "2") ||
                          strings.HasSuffix(d, "3") ||
                          strings.HasSuffix(d, "4") ||
                          strings.HasSuffix(d, "5") ||
                          strings.HasSuffix(d, "6") ||
                          strings.HasSuffix(d, "7") ||
                          strings.HasSuffix(d, "8") ||
                          strings.HasSuffix(d, "9")

            if !contains(existing, d) && !isPartition {
                log.Infof("Found new device candidate: %s", d)
                newDevices = append(newDevices, d)
            }
        }

        if len(newDevices) > 0 {
            log.Infof("Returning new device: %s", newDevices[0])
            return newDevices[0], nil
        }
    }

    log.Errorf("Block device not found after 20 iterations")
    return "", fmt.Errorf("block device not found")
}

func isDirectoryPresent(path string) (bool, error) {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return stat.IsDir(), nil
	}
}
