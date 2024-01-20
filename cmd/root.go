package cmd

import (
	"fmt"
	"os"
	"runtime"

	"github.com/nmarley/go-s3hash/app"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "s3hash",
	Short: "Download and sha256 hash s3 objects",
	Long: `The S3 hasher is designed to compute SHA256 hashes for objects
stored in Amazon S3. It is written in Go and uses Goroutines and channels to
stream the input as well as s3 objects, compute the sha256 hashes and write
them to a file. S3 objects are only kept in memory and are not stored on the
filesystem. This tool is useful for calculating the sha256 hash of large
volumes of data stored in an S3 bucket.`,
	Run: runRootCmd,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func runRootCmd(cmd *cobra.Command, _ []string) {
	bucket, err := cmd.PersistentFlags().GetString("bucket")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	keysFile, err := cmd.PersistentFlags().GetString("keys-file")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	numThreads, err := cmd.PersistentFlags().GetUint16("num-threads")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	app.Run(cmd.Name(), bucket, keysFile, numThreads)
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	var bucket string
	rootCmd.PersistentFlags().StringVar(&bucket, "bucket", "", "The s3 bucket from which to fetch objects")
	rootCmd.MarkPersistentFlagRequired("bucket")

	var keysFile string
	rootCmd.PersistentFlags().StringVar(&keysFile, "keys-file", "", "The input file from which to read s3 keys")
	rootCmd.MarkPersistentFlagRequired("keys-file")

	var numThreads uint16
	rootCmd.PersistentFlags().Uint16Var(&numThreads, "num-threads", uint16(runtime.NumCPU()*2), "The number of threads to use (defaults to NUM_CPUS * 2)")
}
