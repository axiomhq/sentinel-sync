package monitor

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type StorageAccountMonitor struct {
	storageURL string
}

func NewStorageAccountMonitor(storageURL string) *StorageAccountMonitor {
	return &StorageAccountMonitor{
		storageURL: storageURL,
	}
}

func (c *StorageAccountMonitor) ListContainers(ctx context.Context, client *azblob.Client) (containers []*ContainerMonitor, err error) {
	pager := client.NewListContainersPager(&azblob.ListContainersOptions{
		Prefix: &amPrefix,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("can not get next page: %w", err)
		}

		for _, item := range page.ContainerItems {
			containers = append(containers, NewContainerMonitor(c.storageURL, *item.Name))
		}
	}

	return containers, nil
}
