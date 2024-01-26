package export

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

func auth(ctx context.Context, connectionString string, opts *azidentity.DefaultAzureCredentialOptions) (*azblob.Client, error) {
	return azblob.NewClientFromConnectionString(connectionString, nil)
}

var (
	storageURL          string
	connectionString    string
	axiomPersonalAPIKey string
	axiomPersonalOrg    string

	axiomURL string
)

func init() {
	// TODO: we shouldn't be using personal tokens, API token work will allow us to use api tokens in the future
	viper.AutomaticEnv()

	flags := Cmd.Flags()
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

func export(cmd *cobra.Command, args []string) {
	axiomPersonalAPIKey = viper.Get("AXIOM_PERSONAL_TOKEN").(string)
	if axiomPersonalAPIKey == "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "axiom personal token is required")
		return
	}

	connectionString = viper.Get("CONNECTION_STRING").(string)
	if connectionString == "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "connection string is required")
		return
	}

	storageURL = viper.Get("STORAGE_URL").(string)
	if storageURL == "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "storage url is required")
		return
	}

	cmd.Println("exporting from azure to axiom")
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	azclient, err := auth(ctx, connectionString, nil)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "can not auth with azure: %s\n", err)
	}

	axiclient, err := axiom.NewClient(
		//axiom.SetAPITokenConfig(axiomAPIKey),
		axiom.SetPersonalTokenConfig(axiomPersonalAPIKey, axiomPersonalOrg),
		axiom.SetURL(axiomURL),
	)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "can not create axiom client: %s\n", err)
	}

	poller := poll.NewPoller()
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
