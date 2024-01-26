package poll

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/alitto/pond"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/sentinelexport/pkg/axm"
	"github.com/axiomhq/sentinelexport/pkg/monitor"
)

var logger = log.New(os.Stdout, "poll: ", log.LstdFlags)

// this needs to be a worker pool of containers that streams the blobs oldest to newest
// and then exists. longer worker pools that might even take hours in backfill mode
// so be careful around that. but have to syncronously process all the blobs to avoid
// queuing up all the blobs at once

type Poll struct {
	cancel  context.CancelFunc
	stopped <-chan struct{}
}

func NewPoller() *Poll {
	return &Poll{}
}

func (p *Poll) Start(ctx context.Context,
	azClient *azblob.Client, axClient *axiom.Client,
	sam *monitor.StorageAccountMonitor,
) error {
	if p.cancel != nil {
		return errors.New("already started")
	}

	ctx, p.cancel = context.WithCancel(ctx)
	stopped := make(chan struct{})
	p.stopped = stopped

	go func() {
		defer close(stopped)

		for {
			if ctx.Err() != nil {
				return
			}

			err := p.loop(ctx, azClient, axClient, sam)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Printf("error in poll loop: %v\n", err)
			}
		}
	}()
	return nil
}

func (p *Poll) Stop() error {
	if p.cancel == nil {
		return errors.New("not started")
	}

	p.cancel()
	<-p.stopped
	return nil
}

func (p *Poll) loop(ctx context.Context,
	azClient *azblob.Client, axClient *axiom.Client,
	sam *monitor.StorageAccountMonitor) error {

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// TODO: this logic will page over all the containers, create jobs to sync each one
	// then wait for all the jobs to finish before paging again.
	// This means that if it takes an hour for one container to sync, we won't pick up new
	// blobs for other containers until it's done syncing
	// This is really only an issue if we are backfilling large amounts of data
	// But we should tackle it.
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		containers, err := sam.ListContainers(ctx, azClient)
		if err != nil {
			return err
		}

		// to avoid prioritizing containers, shuffle
		rand.Shuffle(len(containers), func(i, j int) {
			containers[i], containers[j] = containers[j], containers[i]
		})

		wp := pond.New(8, 16)
		for _, container := range containers {
			streamContainer(ctx, wp, azClient, axClient, container)
		}

		// cancelling ctx should cancel the wp jobs causing them to end early
		wp.StopAndWait()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func streamContainer(ctx context.Context, wp *pond.WorkerPool,
	azClient *azblob.Client, axClient *axiom.Client,
	container *monitor.ContainerMonitor) {
	logger.Printf("syncing container=%q\n", container.ContainerName())
	wp.Submit(func() {
		for {
			if err := ctx.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				panic(err)
			}

			blob, more, err := container.GetNextBlob(ctx, azClient)
			if err != nil {
				logger.Printf("can not get next blob, container=%q: %s\n", container.ContainerName(), err)
				return
			}
			if blob == nil {
				return
			}

			dataset := container.TableName()
			if err := streamBlob(ctx, blob, dataset, azClient, axClient); err != nil {
				logger.Printf("error streaming container=%q, blob=%q: %s\n", blob.ContainerName(), blob.BlobName(), err)
				return
			}

			if !more {
				return
			}
		}
	})
}

func streamBlob(ctx context.Context, blob *monitor.Blob, datasetName string, azClient *azblob.Client, axClient *axiom.Client) error {
	blobStream, err := blob.Stream(ctx, azClient)
	if err != nil {
		return err
	}
	defer blobStream.Close()

	ds := axm.NewDataset(datasetName)
	if err := ds.Ensure(ctx, axClient); err != nil {
		return err
	}

	// TODO: would be useful to track status
	status, err := ds.Stream(ctx, axClient, blobStream)
	if err != nil {
		return err
	}

	bDate, _ := blob.Date()
	logger.Printf("%s [%s] processedBytes=%d, success=%d, failed=%d\n", datasetName, bDate.Format(time.DateTime), status.ProcessedBytes, status.Ingested, status.Failed)

	if err := blob.Delete(ctx, azClient); err != nil {
		return err
	}

	return nil

}
