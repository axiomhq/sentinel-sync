package export

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/sentinelexport/pkg/monitor"
	"github.com/axiomhq/sentinelexport/pkg/poll"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use:   "export",
	Short: "exports data from azure log anaytics (via storage) to axiom",
	Long: `exports data from azure log anaytics (via storage) to axiom.

  Axiom provides a simple, scalable and fast user
  experience for machine data and real-time analytics.

  > Documentation & Support: https://docs.axiom.co
  > Source & Copyright Information: https://axiom.co`,
	Run: export,
}

func authConnectionString(ctx context.Context, connectionString string) (*azblob.Client, error) {
	return azblob.NewClientFromConnectionString(connectionString, nil)
}

func authDefualt(ctx context.Context, serviceURL string) (*azblob.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("error getting default azure credentials: %w", err)
	}

	return azblob.NewClient(serviceURL, cred, nil)
}

var (
	storageURL          string
	connectionString    string
	axiomPersonalAPIKey string
	axiomPersonalOrg    string

	axiomURL string

	workerPoolSize int
)

func init() {
	// TODO: we shouldn't be using personal tokens, API token work will allow us to use api tokens in the future
	viper.AutomaticEnv()

	flags := Cmd.Flags()
	flags.IntVar(&workerPoolSize, "worker-pool-size", 8, "the size of the worker pool used to transfer blobs to axiom (more workers == more blobs sent concurrently)")
	flags.StringVar(&axiomPersonalAPIKey, "axiom-personal-token", "", "your full axiom personal API key (or env AXIOM_PERSONAL_TOKEN)")
	if err := viper.BindPFlag("AXIOM_PERSONAL_TOKEN", flags.Lookup("axiom-personal-token")); err != nil {
		panic(err)
	}

	flags.StringVar(&axiomPersonalOrg, "axiom-personal-org", "", "your axiom personal token org")
	flags.StringVar(&axiomURL, "axiom-url", "https://api.axiom.co", "your axiom url")

	// TODO: more auth options around authing with a storage account are needed
	flags.StringVar(&connectionString, "connection-string", "", "your azure storage account connection-string (or env CONNECTION_STRING)")
	if err := viper.BindPFlag("CONNECTION_STRING", flags.Lookup("connection-string")); err != nil {
		panic(err)
	}
	flags.StringVar(&storageURL, "storage-url", "", "your azure storage account url; should be something like https://foobar.blob.core.windows.net/ (or env STORAGE_URL)")
	if err := viper.BindPFlag("STORAGE_URL", flags.Lookup("storage-url")); err != nil {
		panic(err)
	}
}

func ensureValid(ctx context.Context) (*azblob.Client, error) {
	axiomPersonalAPIKey = viper.Get("AXIOM_PERSONAL_TOKEN").(string)
	if axiomPersonalAPIKey == "" {
		return nil, fmt.Errorf("axiom personal token is required")
	}

	storageURL = viper.Get("STORAGE_URL").(string)
	if storageURL == "" {
		return nil, fmt.Errorf("storage url is required")
	}

	connectionString = viper.Get("CONNECTION_STRING").(string)
	if strings.TrimSpace(connectionString) != "" {
		azclient, err := authConnectionString(ctx, connectionString)
		if err != nil {
			return nil, fmt.Errorf("can not auth with azure via connection-string: %w", err)
		}
		return azclient, nil
	}

	azclient, err := authDefualt(ctx, storageURL)
	if err != nil {
		return nil, fmt.Errorf("can not auth with azure via default credentials: %w", err)
	}
	return azclient, nil
}

func export(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	azclient, err := ensureValid(ctx)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "error validating: %s", err)
		return
	}

	axiclient, err := axiom.NewClient(
		//axiom.SetAPITokenConfig(axiomAPIKey),
		axiom.SetPersonalTokenConfig(axiomPersonalAPIKey, axiomPersonalOrg),
		axiom.SetURL(axiomURL),
	)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "can not create axiom client: %s\n", err)
	}

	cmd.Println("exporting from azure to axiom")

	poller := poll.NewPoller(workerPoolSize)
	if err := poller.Start(ctx, azclient, axiclient, monitor.NewStorageAccountMonitor(storageURL)); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "can not start poller: %s\n", err)
	}

	sigTrap := make(chan os.Signal, 1)
	signal.Notify(sigTrap, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigTrap:
		cancel()
	case <-ctx.Done():
	}

	if err := poller.Stop(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "can not stop poller: %s\n", err)
	}

	cmd.Println("finished exporting")
}
