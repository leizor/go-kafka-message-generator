package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/leizor/go-kafka-message-generator/pkg/generate"
)

const version = "0.01"

func main() {
	var (
		input               []string
		packageName, output string
	)
	generateCmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate go serializers/deserializers for Kafka messages",
		Run: func(cmd *cobra.Command, args []string) {
			err := generate.Run(&packageName, &input, &output)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	generateCmd.Flags().StringVarP(&packageName, "package", "p", "", "The go package name to use in generated files")
	generateCmd.Flags().StringArrayVarP(&input, "input", "i", []string{""}, "The input directory to use")
	generateCmd.Flags().StringVarP(&output, "output", "o", "", "The output directory to create")
	for _, f := range []string{"package", "input", "output"} {
		err := generateCmd.MarkFlagRequired(f)
		if err != nil {
			panic(err)
		}
	}

	rootCmd := &cobra.Command{Use: "kmg", Version: version}
	rootCmd.AddCommand(generateCmd)
	_ = rootCmd.Execute()
}
