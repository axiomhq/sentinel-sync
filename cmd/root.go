package main

import (
	"github.com/axiomhq/sentinelexport/cmd/export"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sentinelexport",
	Short: "exports data from azure log anaytics (via storage) to axiom",
	Long: `exports data from azure log anaytics (via storage) to axiom.

  Axiom provides a simple, scalable and fast user
  experience for machine data and real-time analytics.

  > Documentation & Support: https://docs.axiom.co
  > Source & Copyright Information: https://axiom.co`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	rootCmd.AddCommand(export.Cmd)
	cobra.CheckErr(rootCmd.Execute())
}
