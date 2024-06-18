package axm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"

	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/axiom-go/axiom/ingest"
)

var logger = log.New(os.Stdout, "axiom: ", log.LstdFlags)

type Client struct {
	*axiom.Client

	DatasetPrefix string
}

type Dataset struct {
	name string
}

func (d *Dataset) Name() string {
	return d.name
}

func NewDataset(name string) *Dataset {
	return &Dataset{
		name: name,
	}
}

func (d *Dataset) Ensure(ctx context.Context, client *Client) error {
	// ensure the dataset exists in axiom
	name := client.DatasetPrefix + d.name

	dses, err := client.Datasets.List(ctx)
	if err != nil {
		return fmt.Errorf("can not list datasets: %w", err)
	}

	exists := slices.ContainsFunc(dses, func(ds *axiom.Dataset) bool {
		return ds.Name == name
	})

	if !exists {
		_, err := client.Datasets.Create(ctx, axiom.DatasetCreateRequest{
			Name:        name,
			Description: "imported from Sentinel",
		})
		if err != nil && !errors.Is(err, axiom.ErrExists) {
			return fmt.Errorf("can not create dataset: %w", err)
		}
	}

	return nil
}

func (d *Dataset) Stream(ctx context.Context, client *Client, r io.Reader) (*ingest.Status, error) {
	r, err := axiom.GzipEncoder()(r)
	if err != nil {
		return nil, err
	}

	name := client.DatasetPrefix + d.name

	logger.Printf("streaming to axiom dataset => %q\n", name)

	return client.Ingest(ctx, name, r, axiom.NDJSON, axiom.Gzip,
		// TODO: non sentinel uses other fields, but sentinel seems to only use these fields
		ingest.SetTimestampField("TimeGenerated"), //az uses TimeGenerated, axiom uses _time
	)
}
