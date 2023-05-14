package commands

import (
	"archive/zip"
	"fmt"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/mattermost/mmetl/services/slack"
)

const attachmentsInternal = "bulk-export-attachments"

var TransformCmd = &cobra.Command{
	Use:   "transform",
	Short: "Transforms export files into Mattermost import files",
}

var TransformSlackCmd = &cobra.Command{
	Use:     "slack",
	Short:   "Transforms a Slack export.",
	Long:    "Transforms a Slack export zipfile into a Mattermost export JSONL file.",
	Example: "  transform slack --team myteam --file my_export.zip --output mm_export.json",
	Args:    cobra.NoArgs,
	RunE:    transformSlackCmdF,
}

func init() {
	TransformSlackCmd.Flags().StringP("team", "t", "", "an existing team in Mattermost to import the data into")
	if err := TransformSlackCmd.MarkFlagRequired("team"); err != nil {
		panic(err)
	}
	TransformSlackCmd.Flags().StringArrayP("file", "f", []string{}, "The Slack export file to transform. You can provide this flag multiple times to join multiple exports.")
	if err := TransformSlackCmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}
	TransformSlackCmd.Flags().StringP("output", "o", "bulk-export.jsonl", "the output path")
	TransformSlackCmd.Flags().StringP("attachments-dir", "d", "data", "the path for the attachments directory")
	TransformSlackCmd.Flags().StringP("useroverrides", "", "", "the name of a csv file used to change the MM user profiles extracted from the Slack export. The `apply_to_username` column is required. Optional columns are `username`, `first_name`, `last_name`, `position`, `email` and `password`. An empty field means no override. A single dash in the `first_name`, `last_name` or `position` field means to override with an empty string.")
	TransformSlackCmd.Flags().StringP("channeloverrides", "", "", "the name of a csv file used to change the MM channel profiles extracted from the Slack export. The `apply_to_channel` column is required. Optional columns are `name`, `display_name`, `purpose`, `header` and `topic`. In an optional field, the empty string means no override and a single dash means to override with an empty string.")
	TransformSlackCmd.Flags().BoolP("skip-convert-posts", "c", false, "Skips converting mentions and post markup. Only for testing purposes")
	TransformSlackCmd.Flags().BoolP("skip-attachments", "a", false, "Skips copying the attachments from the import file")
	TransformSlackCmd.Flags().BoolP("allow-download", "l", false, "Allows downloading the attachments for the import file")
	TransformSlackCmd.Flags().BoolP("add-json-original", "j", false, "Add the raw JSON of the Slack exported post as a prop")
	TransformSlackCmd.Flags().BoolP("discard-invalid-props", "p", false, "Skips converting posts with invalid props instead discarding the props themselves")
	TransformSlackCmd.Flags().Bool("debug", true, "Whether to show debug logs or not")

	TransformCmd.AddCommand(
		TransformSlackCmd,
	)

	RootCmd.AddCommand(
		TransformCmd,
	)
}

func transformSlackCmdF(cmd *cobra.Command, args []string) error {
	team, _ := cmd.Flags().GetString("team")
	inputFilePaths, _ := cmd.Flags().GetStringArray("file")
	outputFilePath, _ := cmd.Flags().GetString("output")
	attachmentsDir, _ := cmd.Flags().GetString("attachments-dir")
	userOverridesFilename, _ := cmd.Flags().GetString("useroverrides")
	channelOverridesFilename, _ := cmd.Flags().GetString("channeloverrides")
	skipConvertPosts, _ := cmd.Flags().GetBool("skip-convert-posts")
	skipAttachments, _ := cmd.Flags().GetBool("skip-attachments")
	allowDownload, _ := cmd.Flags().GetBool("allow-download")
	addOriginal, _ := cmd.Flags().GetBool("add-json-original")
	discardInvalidProps, _ := cmd.Flags().GetBool("discard-invalid-props")
	debug, _ := cmd.Flags().GetBool("debug")

	// output file
	if fileInfo, err := os.Stat(outputFilePath); err != nil && !os.IsNotExist(err) {
		return err
	} else if err == nil && fileInfo.IsDir() {
		return fmt.Errorf("Output file \"%s\" is a directory", outputFilePath)
	}

	// attachments dir
	attachmentsFullDir := path.Join(attachmentsDir, attachmentsInternal)

	if !skipAttachments {
		if fileInfo, err := os.Stat(attachmentsFullDir); os.IsNotExist(err) {
			if createErr := os.MkdirAll(attachmentsFullDir, 0755); createErr != nil {
				return createErr
			}
		} else if err != nil {
			return err
		} else if !fileInfo.IsDir() {
			return fmt.Errorf("File \"%s\" is not a directory", attachmentsDir)
		}
	}

	// input files
	zipReaders := make([]*zip.Reader, len(inputFilePaths))
	for i, inputFilePath := range inputFilePaths {
		fileReader, err := os.Open(inputFilePath)
		if err != nil {
			return err
		}
		defer fileReader.Close()

		zipFileInfo, err := fileReader.Stat()
		if err != nil {
			return err
		}

		zipReader, err := zip.NewReader(fileReader, zipFileInfo.Size())
		if err != nil || zipReader.File == nil {
			return err
		}

		zipReaders[i] = zipReader
	}

	// user overrides
	var userOverridesFile *os.File
	if userOverridesFilename != "" {
		var err error
		userOverridesFile, err = os.Open(userOverridesFilename)
		if err != nil {
			return err
		}
		defer userOverridesFile.Close()
	}

	// channel overrides
	var channelOverridesFile *os.File
	if channelOverridesFilename != "" {
		var err error
		channelOverridesFile, err = os.Open(channelOverridesFilename)
		if err != nil {
			return err
		}
		defer channelOverridesFile.Close()
	}

	logger := log.New()
	if debug {
		logger.Level = log.DebugLevel
	}
	slackTransformer := slack.NewTransformer(team, logger)

	slackExports := make([]*slack.SlackExport, len(zipReaders))
	for i, zipReader := range zipReaders {
		slackExport, err := slackTransformer.ParseSlackExportFile(zipReader, skipConvertPosts)
		if err != nil {
			return err
		}
		slackExports[i] = slackExport
	}

	slackExport, err := slackTransformer.MergeSlackExports(slackExports)
	if err != nil {
		return err
	}

	err = slackTransformer.ParseUserOverrides(userOverridesFile)
	if err != nil {
		return err
	}

	err = slackTransformer.ParseChannelOverrides(channelOverridesFile)
	if err != nil {
		return err
	}

	err = slackTransformer.Transform(slackExport, attachmentsDir, skipAttachments, discardInvalidProps, allowDownload, addOriginal)
	if err != nil {
		return err
	}

	if err = slackTransformer.Export(outputFilePath); err != nil {
		return err
	}

	slackTransformer.Logger.Info("Transformation succeeded!")

	return nil
}
