package monitor

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/axiomhq/axiom-go/axiom"
)

type ContainerMonitor struct {
	storageURL string
	name       string
}

func NewContainerMonitor(storageURL, name string) *ContainerMonitor {
	if name == "" {
		panic("name can not be empty")
	}

	return &ContainerMonitor{
		storageURL: storageURL,
		name:       name,
	}
}

func (c *ContainerMonitor) ContainerName() string {
	return c.name
}

func (c *ContainerMonitor) TableName() string {
	return ContainerNameToTable(c.name)
}

func (c *ContainerMonitor) HasBlobs(ctx context.Context, client *azblob.Client) (bool, error) {
	_, more, err := c.GetNextBlob(ctx, client)
	return more, err
}

func (c *ContainerMonitor) GetNextBlob(ctx context.Context, client *azblob.Client) (blob *Blob, more bool, err error) {
	pager := client.NewListBlobsFlatPager(c.name, nil)
	foundBlobs := 0
	for pager.More() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			err = fmt.Errorf("can not get next page: %w", perr)
			return
		}

		for _, item := range page.Segment.BlobItems {
			foundBlobs++
			b := newBlob(c.name, *item.Name)

			if blob == nil {
				blob = b
			} else {
				older, err := b.Before(blob)
				if err != nil {
					return nil, false, err
				}
				if older {
					blob = b
				}
			}
		}
	}

	more = foundBlobs > 1
	return
}

func (c *ContainerMonitor) StreamBlob(ctx context.Context, client *azblob.Client, axiClient *axiom.Client, blob *Blob) (err error) {
	resp, err := client.DownloadStream(ctx, c.name, blob.blobName, nil)
	if err != nil {
		return fmt.Errorf("can not download blob: %w", err)
	}

	defer resp.Body.Close()
	return nil
}
