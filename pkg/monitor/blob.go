package monitor

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Blob struct {
	containerName string
	blobName      string
}

func newBlob(containerName, blobName string) *Blob {
	return &Blob{
		containerName: containerName,
		blobName:      blobName,
	}
}

func (b *Blob) ContainerName() string {
	return b.containerName
}

func (b *Blob) BlobName() string {
	return b.blobName
}

var blobNameExtract = regexp.MustCompile(`(?:y=(?P<year>\d+))/(?:m=(?P<month>\d+))/(?:d=(?P<day>\d+))/(?:h=(?P<hour>\d+))/(?:m=(?P<minute>\d+))/\w+(_\d+)?.json`)

func (b *Blob) Date() (t time.Time, err error) {
	// Blobs are stored in 5-minute folders in the following path structure:
	//  WorkspaceResourceId=/subscriptions/subscription-id/resourcegroups/<resource-group>/providers/microsoft.operationalinsights/workspaces/<workspace>/y=<four-digit numeric year>/m=<two-digit numeric month>/d=<two-digit numeric day>/h=<two-digit 24-hour clock hour>/m=<two-digit 60-minute clock minute>/PT05M.json

	matches := blobNameExtract.FindStringSubmatch(b.blobName)
	if len(matches) < 6 {
		return time.Time{}, fmt.Errorf("invalid blob name: %q", b.blobName)
	}

	year, err := strconv.Atoi(matches[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("can not parse year: %w", err)
	}

	month, err := strconv.Atoi(matches[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("can not parse month: %w", err)
	}

	day, err := strconv.Atoi(matches[3])
	if err != nil {
		return time.Time{}, fmt.Errorf("can not parse day: %w", err)
	}

	hour, err := strconv.Atoi(matches[4])
	if err != nil {
		return time.Time{}, fmt.Errorf("can not parse hour: %w", err)
	}

	minute, err := strconv.Atoi(matches[5])
	if err != nil {
		return time.Time{}, fmt.Errorf("can not parse minute: %w", err)
	}

	var nanoseconds = 0
	if len(matches) > 6 && matches[6] != "" {
		// Appends to blobs are limited to 50-K writes. More blobs will be added in the folder as PT05M_#.json*,
		// where # is the incremental blob count.
		nanoseconds, err = strconv.Atoi(matches[6])
		if err != nil {
			return time.Time{}, fmt.Errorf("can not parse blob count: %w", err)
		}
	}

	return time.Date(year, time.Month(month), day, hour, minute, 0, nanoseconds, time.UTC), nil
}

func (b *Blob) Before(test *Blob) (bool, error) {
	bTime, err := b.Date()
	if err != nil {
		return false, err
	}

	testTime, err := test.Date()
	if err != nil {
		return false, err
	}

	return bTime.Before(testTime), nil
}

func (b *Blob) Stream(ctx context.Context, client *azblob.Client) (io.ReadCloser, error) {
	resp, err := client.DownloadStream(ctx, b.containerName, b.blobName, nil)
	if err != nil {
		return nil, fmt.Errorf("can not download blob container=%q, name=%q: %w", b.containerName, b.blobName, err)
	}

	return resp.Body, nil
}

func (b *Blob) Delete(ctx context.Context, client *azblob.Client) error {
	_, err := client.DeleteBlob(ctx, b.containerName, b.blobName, nil)
	if err != nil {
		return fmt.Errorf("can not delete blob %q: %w", b.blobName, err)
	}

	return nil
}
