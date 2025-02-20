package slack

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	_ "embed"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mattermost/mattermost-server/v6/app/imports"
	"github.com/mattermost/mattermost-server/v6/model"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/unicode/norm"
)

const attachmentsInternal = "bulk-export-attachments"

type IntermediateChannel struct {
	Id               string            `json:"id"`
	OriginalName     string            `json:"original_name"`
	Name             string            `json:"name"`
	DisplayName      string            `json:"display_name"`
	Members          []string          `json:"members"`
	MembersUsernames []string          `json:"members_usernames"`
	Purpose          string            `json:"purpose"`
	Header           string            `json:"header"`
	Topic            string            `json:"topic"`
	Type             model.ChannelType `json:"type"`
}

func (c *IntermediateChannel) Sanitise(logger log.FieldLogger) {
	if c.Type == model.ChannelTypeDirect {
		return
	}

	c.Name = strings.Trim(c.Name, "_-")
	if len(c.Name) > model.ChannelNameMaxLength {
		logger.Warnf("Channel %s handle exceeds the maximum length. It will be truncated when imported.", c.DisplayName)
		c.Name = c.Name[0:model.ChannelNameMaxLength]
	}
	if len(c.Name) == 1 {
		c.Name = "slack-channel-" + c.Name
	}
	if !isValidChannelNameCharacters(c.Name) {
		c.Name = strings.ToLower(c.Id)
	}

	c.DisplayName = strings.Trim(c.DisplayName, "_-")
	if utf8.RuneCountInString(c.DisplayName) > model.ChannelDisplayNameMaxRunes {
		logger.Warnf("Channel %s display name exceeds the maximum length. It will be truncated when imported.", c.DisplayName)
		c.DisplayName = truncateRunes(c.DisplayName, model.ChannelDisplayNameMaxRunes)
	}
	if len(c.DisplayName) == 1 {
		c.DisplayName = "slack-channel-" + c.DisplayName
	}

	if utf8.RuneCountInString(c.Purpose) > model.ChannelPurposeMaxRunes {
		logger.Warnf("Channel %s purpose exceeds the maximum length. It will be truncated when imported.", c.DisplayName)
		c.Purpose = truncateRunes(c.Purpose, model.ChannelPurposeMaxRunes)
	}

	if utf8.RuneCountInString(c.Header) > model.ChannelHeaderMaxRunes {
		logger.Warnf("Channel %s header exceeds the maximum length. It will be truncated when imported.", c.DisplayName)
		c.Header = truncateRunes(c.Header, model.ChannelHeaderMaxRunes)
	}
}

type IntermediateUser struct {
	Id          string   `json:"id"`
	Username    string   `json:"username"`
	FirstName   string   `json:"first_name"`
	LastName    string   `json:"last_name"`
	Position    string   `json:"position"`
	Email       string   `json:"email"`
	Password    string   `json:"password"`
	Memberships []string `json:"memberships"`
}

func (u *IntermediateUser) Sanitise(logger log.FieldLogger) {
	if u.Email == "" {
		u.Email = u.Username + "@example.com"
		logger.Warnf("User %s does not have an email address in the Slack export. Used %s as a placeholder. The user should update their email address once logged in to the system.", u.Username, u.Email)
	}
}

type IntermediatePost struct {
	User           string                        `json:"user"`
	Channel        string                        `json:"channel"`
	Message        string                        `json:"message"`
	Props          model.StringInterface         `json:"props"`
	CreateAt       int64                         `json:"create_at"`
	Attachments    []string                      `json:"attachments"`
	Replies        []*IntermediatePost           `json:"replies"`
	IsDirect       bool                          `json:"is_direct"`
	ChannelMembers []string                      `json:"channel_members"`
	Reactions      *[]imports.ReactionImportData `json:"reactions"`
}

type Intermediate struct {
	PublicChannels   []*IntermediateChannel          `json:"public_channels"`
	PrivateChannels  []*IntermediateChannel          `json:"private_channels"`
	GroupChannels    []*IntermediateChannel          `json:"group_channels"`
	DirectChannels   []*IntermediateChannel          `json:"direct_channels"`
	UsersById        map[string]*IntermediateUser    `json:"users"`
	Posts            []*IntermediatePost             `json:"posts"`
	UserOverrides    map[string]*IntermediateUser    `json:"user_overrides"`
	ChannelOverrides map[string]*IntermediateChannel `json:"channel_overrides"`
}

func (t *Transformer) ParseUserOverrides(userOverridesFile *os.File) error {
	t.Intermediate.UserOverrides = map[string]*IntermediateUser{}
	if userOverridesFile == nil {
		return nil
	}
	t.Logger.Info("Parsing user overrides")
	reader := csv.NewReader(userOverridesFile)
	headers, err := reader.Read()
	if err != nil {
		t.Logger.Error(err.Error())
		return err
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logger.Error(err.Error())
			return err
		}
		key := ""
		overrideUser := &IntermediateUser{}
		for i, field := range record {
			switch headers[i] {
			case "apply_to_username":
				key = field
			case "username":
				overrideUser.Username = field
			case "first_name":
				overrideUser.FirstName = field
			case "last_name":
				overrideUser.LastName = field
			case "position":
				overrideUser.Position = field
			case "email":
				overrideUser.Email = field
			case "password":
				overrideUser.Password = field
			default:
				t.Logger.Warnf("Unknown field %s in user override record", headers[i])
			}
		}
		if key == "" {
			return errors.New("user override record does not have an apply_to_username value")
		}
		t.Intermediate.UserOverrides[key] = overrideUser
	}
	t.Logger.Infof("Parsed %d user overrides", len(t.Intermediate.UserOverrides))
	return nil
}

func (t *Transformer) ApplyUserOverrides(user *IntermediateUser) {
	if len(t.Intermediate.UserOverrides) == 0 {
		return
	}
	if overrideUser, ok := t.Intermediate.UserOverrides[user.Username]; ok {
		if overrideUser.Username != "" {
			user.Username = overrideUser.Username
		}
		if overrideUser.FirstName != "" {
			user.FirstName = overrideUser.FirstName
			if user.FirstName == "-" {
				user.FirstName = ""
			}
		}
		if overrideUser.LastName != "" {
			user.LastName = overrideUser.LastName
			if user.LastName == "-" {
				user.LastName = ""
			}
		}
		if overrideUser.Position != "" {
			user.Position = overrideUser.Position
			if user.Position == "-" {
				user.Position = ""
			}
		}
		if overrideUser.Email != "" {
			user.Email = overrideUser.Email
		}
		if overrideUser.Password != "" {
			user.Password = overrideUser.Password
		}
	} else {
		t.Logger.Warnf("No user override found for user %s", user.Username)
	}
}

func (t *Transformer) TransformUsers(users []SlackUser) {
	t.Logger.Info("Transforming users")

	resultUsers := map[string]*IntermediateUser{}
	for _, user := range users {
		newUser := &IntermediateUser{
			Id:        user.Id,
			Username:  user.Username,
			FirstName: user.Profile.FirstName,
			LastName:  user.Profile.LastName,
			Position:  user.Profile.Title,
			Email:     user.Profile.Email,
			Password:  model.NewId(),
		}

		if user.IsBot {
			newUser.Id = user.Profile.BotID
		}

		t.ApplyUserOverrides(newUser)

		newUser.Sanitise(t.Logger)
		resultUsers[newUser.Id] = newUser
		t.Logger.Debugf("Slack user with email %s and password %s has been imported.", newUser.Email, newUser.Password)
	}

	t.Intermediate.UsersById = resultUsers
}

func filterValidMembers(members []string, users map[string]*IntermediateUser) []string {
	validMembers := []string{}
	for _, member := range members {
		if _, ok := users[member]; ok {
			validMembers = append(validMembers, member)
		}
	}

	return validMembers
}

func getOriginalName(channel SlackChannel) string {
	if channel.Name == "" {
		return channel.Id
	} else {
		return channel.Name
	}
}

func (t *Transformer) ParseChannelOverrides(channelOverridesFile *os.File) error {
	t.Intermediate.ChannelOverrides = map[string]*IntermediateChannel{}
	if channelOverridesFile == nil {
		return nil
	}
	t.Logger.Info("Parsing channel overrides")
	reader := csv.NewReader(channelOverridesFile)
	headers, err := reader.Read()
	if err != nil {
		t.Logger.Error(err.Error())
		return err
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logger.Error(err.Error())
			return err
		}
		key := ""
		overrideChannel := &IntermediateChannel{}
		for i, field := range record {
			switch headers[i] {
			case "apply_to_channelname":
				key = field
			case "name":
				overrideChannel.Name = field
			case "display_name":
				overrideChannel.DisplayName = field
			case "purpose":
				overrideChannel.Purpose = field
			case "header":
				overrideChannel.Header = field
			case "topic":
				overrideChannel.Topic = field
			default:
				t.Logger.Warnf("Unknown field %s in channel override record", headers[i])
			}
		}
		if key == "" {
			return errors.New("channel override record does not have an apply_to_channelname value")
		}
		t.Intermediate.ChannelOverrides[key] = overrideChannel
	}
	t.Logger.Infof("Parsed %d channel overrides", len(t.Intermediate.ChannelOverrides))
	return nil
}

func (t *Transformer) ApplyChannelOverrides(channel *IntermediateChannel) {
	if len(t.Intermediate.ChannelOverrides) == 0 {
		return
	}
	if overrideChannel, ok := t.Intermediate.ChannelOverrides[channel.Name]; ok {
		if overrideChannel.Name != "" {
			channel.Name = overrideChannel.Name
			if channel.Name == "-" {
				channel.Name = ""
			}
		}
		if overrideChannel.DisplayName != "" {
			channel.DisplayName = overrideChannel.DisplayName
			if channel.DisplayName == "-" {
				channel.DisplayName = ""
			}
		}
		if overrideChannel.Purpose != "" {
			channel.Purpose = overrideChannel.Purpose
			if channel.Purpose == "-" {
				channel.Purpose = ""
			}
		}
		if overrideChannel.Header != "" {
			channel.Header = overrideChannel.Header
			if channel.Header == "-" {
				channel.Header = ""
			}
		}
		if overrideChannel.Topic != "" {
			channel.Topic = overrideChannel.Topic
			if channel.Topic == "-" {
				channel.Topic = ""
			}
		}
	} else {
		t.Logger.Warnf("No channel override found for channel %s", channel.Name)
	}
}

func (t *Transformer) TransformChannels(channels []SlackChannel, teamInternalOnly bool) []*IntermediateChannel {
	resultChannels := []*IntermediateChannel{}
	for _, channel := range channels {
		validMembers := filterValidMembers(channel.Members, t.Intermediate.UsersById)
		if (channel.Type == model.ChannelTypeDirect || channel.Type == model.ChannelTypeGroup) && len(validMembers) <= 1 {
			t.Logger.Warnf("Bulk export for direct channels containing a single member is not supported. Not importing channel %s", channel.Name)
			continue
		}

		if channel.Type == model.ChannelTypeGroup && len(validMembers) > model.ChannelGroupMaxUsers {
			channel.Name = channel.Purpose.Value
			channel.Type = model.ChannelTypePrivate
		}

		name := strings.Trim(channel.Name, "_-")

		if teamInternalOnly && (channel.Type == model.ChannelTypeDirect || channel.Type == model.ChannelTypeGroup) {
			name = strings.ToLower(channel.Id) + "-"
			if channel.Type == model.ChannelTypeDirect {
				name += "direct-"
			} else {
				name += "group-"
			}
			validMemberNames := make([]string, len(validMembers))
			for i, member := range validMembers {
				validMemberNames[i] = t.Intermediate.UsersById[member].Username
			}
			sort.Strings(validMemberNames)
			name += strings.Join(validMemberNames, "-")
		}

		if len(name) == 1 {
			name = "slack-channel-" + name
		}

		if !isValidChannelNameCharacters(name) {
			name = strings.ToLower(channel.Id)
		}

		newChannel := &IntermediateChannel{
			OriginalName: getOriginalName(channel),
			Name:         name,
			DisplayName:  name,
			Members:      validMembers,
			Purpose:      channel.Purpose.Value,
			Header:       channel.Topic.Value,
			Type:         channel.Type,
		}

		if teamInternalOnly && (newChannel.Type == model.ChannelTypeDirect || newChannel.Type == model.ChannelTypeGroup) {
			newChannel.Type = model.ChannelTypePrivate
		}

		if newChannel.Type == model.ChannelTypeOpen || newChannel.Type == model.ChannelTypePrivate {
			t.ApplyChannelOverrides(newChannel)
		}

		newChannel.Sanitise(t.Logger)
		resultChannels = append(resultChannels, newChannel)
	}

	return resultChannels
}

func (t *Transformer) PopulateUserMemberships() {
	t.Logger.Info("Populating user memberships")

	for userId, user := range t.Intermediate.UsersById {
		memberships := []string{}
		for _, channel := range t.Intermediate.PublicChannels {
			for _, memberId := range channel.Members {
				if userId == memberId {
					memberships = append(memberships, channel.Name)
					break
				}
			}
		}
		for _, channel := range t.Intermediate.PrivateChannels {
			for _, memberId := range channel.Members {
				if userId == memberId {
					memberships = append(memberships, channel.Name)
					break
				}
			}
		}
		user.Memberships = memberships
	}
}

func (t *Transformer) PopulateChannelMemberships() {
	t.Logger.Info("Populating channel memberships")

	for _, channel := range t.Intermediate.GroupChannels {
		members := []string{}
		for _, memberId := range channel.Members {
			if user, ok := t.Intermediate.UsersById[memberId]; ok {
				members = append(members, user.Username)
			}
		}

		channel.MembersUsernames = members
	}
	for _, channel := range t.Intermediate.DirectChannels {
		members := []string{}
		for _, memberId := range channel.Members {
			if user, ok := t.Intermediate.UsersById[memberId]; ok {
				members = append(members, user.Username)
			}
		}

		channel.MembersUsernames = members
	}
}

func (t *Transformer) TransformAllChannels(slackExport *SlackExport, teamInternalOnly bool) error {
	t.Logger.Info("Transforming channels")

	// transform public
	t.Intermediate.PublicChannels = t.TransformChannels(slackExport.PublicChannels, teamInternalOnly)

	// transform private
	t.Intermediate.PrivateChannels = t.TransformChannels(slackExport.PrivateChannels, teamInternalOnly)

	// transform group
	regularGroupChannels, bigGroupChannels := SplitChannelsByMemberSize(slackExport.GroupChannels, model.ChannelGroupMaxUsers)

	t.Intermediate.PrivateChannels = append(t.Intermediate.PrivateChannels, t.TransformChannels(bigGroupChannels, teamInternalOnly)...)

	if teamInternalOnly {
		t.Intermediate.PrivateChannels = append(t.Intermediate.PrivateChannels, t.TransformChannels(regularGroupChannels, teamInternalOnly)...)
	} else {
		t.Intermediate.GroupChannels = t.TransformChannels(regularGroupChannels, teamInternalOnly)
	}

	// transform direct
	if teamInternalOnly {
		t.Intermediate.PrivateChannels = append(t.Intermediate.PrivateChannels, t.TransformChannels(slackExport.DirectChannels, teamInternalOnly)...)
	} else {
		t.Intermediate.DirectChannels = t.TransformChannels(slackExport.DirectChannels, teamInternalOnly)
	}

	return nil
}

func AddPostToThreads(original SlackPost, post *IntermediatePost, threads map[string]*IntermediatePost, channel *IntermediateChannel, timestamps map[int64]bool) {
	// direct and group posts need the channel members in the import line
	if channel.Type == model.ChannelTypeDirect || channel.Type == model.ChannelTypeGroup {
		post.IsDirect = true
		post.ChannelMembers = channel.MembersUsernames
	} else {
		post.IsDirect = false
	}

	// avoid timestamp duplications
	for {
		// if the timestamp hasn't been used already, break and use
		if _, ok := timestamps[post.CreateAt]; !ok {
			break
		}
		post.CreateAt++
	}
	timestamps[post.CreateAt] = true

	// if post is part of a thread
	if original.ThreadTS != "" && original.ThreadTS != original.TimeStamp {
		rootPost, ok := threads[original.ThreadTS]
		if !ok {
			log.Printf("ERROR processing post in thread, couldn't find rootPost: %+v\n", original)
			return
		}
		rootPost.Replies = append(rootPost.Replies, post)
		return
	}

	// if post is the root of a thread
	if original.TimeStamp == original.ThreadTS {
		if threads[original.ThreadTS] != nil {
			log.Println("WARNING: overwriting root post for thread " + original.ThreadTS)
		}
		threads[original.ThreadTS] = post
		return
	}

	if threads[original.TimeStamp] != nil {
		log.Println("WARNING: overwriting root post for thread " + original.TimeStamp)
	}

	threads[original.TimeStamp] = post
}

func buildChannelsByOriginalNameMap(intermediate *Intermediate) map[string]*IntermediateChannel {
	channelsByName := map[string]*IntermediateChannel{}
	for _, channel := range intermediate.PublicChannels {
		channelsByName[channel.OriginalName] = channel
	}
	for _, channel := range intermediate.PrivateChannels {
		channelsByName[channel.OriginalName] = channel
	}
	for _, channel := range intermediate.GroupChannels {
		channelsByName[channel.OriginalName] = channel
	}
	for _, channel := range intermediate.DirectChannels {
		channelsByName[channel.OriginalName] = channel
	}
	return channelsByName
}

func getNormalisedFilePath(file *SlackFile, attachmentsDir string) string {
	n := makeAlphaNum(file.Name, '.', '-', '_')
	p := path.Join(attachmentsDir, file.Id, n)
	return norm.NFC.String(p)
}

func createDirectoryForFile(file string) error {
	dirPath := path.Dir(file)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}
	return nil
}

func addFileToPost(file *SlackFile, uploads map[string]*zip.File, post *IntermediatePost, attachmentsDir string, allowDownload bool) error {
	if _, ok := uploads[file.Id]; ok || !allowDownload {
		return addZipFileToPost(file, uploads, post, attachmentsDir)
	}

	return addDownloadToPost(file, post, attachmentsDir)
}

func addDownloadToPost(file *SlackFile, post *IntermediatePost, attachmentsDir string) error {
	destFilePath := getNormalisedFilePath(file, attachmentsInternal)
	fullFilePath := path.Join(attachmentsDir, destFilePath)
	err := createDirectoryForFile(fullFilePath)
	if err != nil {
		return err
	}

	log.Printf("Downloading %q into %q...\n", file.DownloadURL, destFilePath)

	err = downloadInto(fullFilePath, file.DownloadURL, file.Size)
	if err != nil {
		return err
	}

	log.Println("Download successful!")

	post.Attachments = append(post.Attachments, destFilePath)
	return nil
}

var sizes = []string{"KiB", "MiB", "GiB", "TiB", "PiB"}

func humanSize(size int64) string {
	if size < 0 {
		return "unknown"
	}
	if size < 1024 {
		return fmt.Sprintf("%d B", size)
	}

	limit := int64(1024 * 1024)
	for _, name := range sizes {
		if size < limit {
			return fmt.Sprintf("%.2f %s", float64(size)/float64(limit/1024), name)
		}

		limit *= 1024
	}

	return fmt.Sprintf("%.2f %s", float64(size)/float64(limit/1024), sizes[len(sizes)-1])
}

func addZipFileToPost(file *SlackFile, uploads map[string]*zip.File, post *IntermediatePost, attachmentsDir string) error {
	zipFile, ok := uploads[file.Id]
	if !ok {
		return errors.Errorf("failed to retrieve file with id %s", file.Id)
	}

	zipFileReader, err := zipFile.Open()
	if err != nil {
		return errors.Wrapf(err, "failed to open attachment from zipfile for id %s", file.Id)
	}
	defer zipFileReader.Close()

	destFilePath := getNormalisedFilePath(file, attachmentsInternal)
	fullFilePath := path.Join(attachmentsDir, destFilePath)
	err = createDirectoryForFile(fullFilePath)
	if err != nil {
		return err
	}
	destFile, err := os.Create(fullFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s in the attachments directory", file.Id)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, zipFileReader)
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s in the attachments directory", file.Id)
	}

	log.Printf("SUCCESS COPYING FILE %s TO DEST %s", file.Id, destFilePath)

	post.Attachments = append(post.Attachments, destFilePath)

	return nil
}

func (t *Transformer) CreateIntermediateUser(userID string) {
	newUser := &IntermediateUser{
		Id:        userID,
		Username:  strings.ToLower(userID),
		FirstName: "Deleted",
		LastName:  "User",
		Email:     fmt.Sprintf("%s@local", userID),
		Password:  model.NewId(),
	}
	t.ApplyUserOverrides(newUser)
	t.Intermediate.UsersById[userID] = newUser
	t.Logger.Warnf("Created a new user because the original user was missing from the import files. user=%s", userID)
}

func (t *Transformer) SlackConvertReactions(slackReactions *[]SlackReaction, postCreateAt int64) *[]imports.ReactionImportData {
	if slackReactions == nil {
		return nil
	}
	ret := make([]imports.ReactionImportData, 0, len(*slackReactions))
	for _, slackReaction := range *slackReactions {
		if slackReaction.Count != len(slackReaction.Users) {
			t.Logger.Warnf("Reaction count does not match the number of users. reaction=%s count=%d users=%d", slackReaction.Name, slackReaction.Count, len(slackReaction.Users))
		}
		emojiName := t.SlackConvertEmojiName(slackReaction.Name)
		for _, userId := range slackReaction.Users {
			user := t.Intermediate.UsersById[userId]
			if user == nil {
				t.CreateIntermediateUser(userId)
				user = t.Intermediate.UsersById[userId]
			}
			// We have no idea when the reaction was created but MM requires that the
			// reaction has a value for CreateAt and that it's greater than the post's
			// CreateAt. So we just add 1 to the post's CreateAt.
			reactionCreateAt := postCreateAt + 1
			ret = append(ret, imports.ReactionImportData{
				User:      &user.Username,
				CreateAt:  &reactionCreateAt,
				EmojiName: &emojiName,
			})
		}
	}
	return &ret
}

func (t *Transformer) SlackConvertEmojiName(slackEmojiName string) string {
	ret := slackEmojiName
	// Take care of skin tones
	for _, skinTone := range []struct {
		slack string
		mm    string
	}{
		{slack: "::skin-tone-2", mm: "_light_skin_tone"},
		{slack: "::skin-tone-3", mm: "_medium_light_skin_tone"},
		{slack: "::skin-tone-4", mm: "_medium_skin_tone"},
		{slack: "::skin-tone-5", mm: "_medium_dark_skin_tone"},
		{slack: "::skin-tone-6", mm: "_dark_skin_tone"},
	} {
		if strings.HasSuffix(ret, skinTone.slack) {
			return t.SlackConvertEmojiName(strings.TrimSuffix(ret, skinTone.slack)) + skinTone.mm
		}
	}
	// Warn about unsupported compound emoji
	if strings.Contains(ret, ":") {
		t.Logger.Warnf("Unsupported compound emoji. emoji=%s", ret)
		// Replace ":" with "_"
		ret = strings.Replace(ret, ":", "_", -1)
	}
	// Warn about unsupported emoji
	if _, ok := supportedEmojis[ret]; !ok {
		t.Logger.Warnf("Unsupported emoji. emoji=%s", ret)
	}
	// Return the emoji name if it is found
	return ret
}

//go:embed supported_emojis.txt
var supportedEmojisString string
var supportedEmojis = (func() map[string]bool {
	ret := make(map[string]bool)
	// iterate over the lines in the file
	for _, emoji := range strings.Split(supportedEmojisString, "\n") {
		if emoji != "" {
			ret[emoji] = true
		}
	}
	return ret
})()

func (t *Transformer) CreateAndAddPostToThreads(post SlackPost, threads map[string]*IntermediatePost, timestamps map[int64]bool, channel *IntermediateChannel, discardInvalidProps, addOriginal bool) {
	author := t.Intermediate.UsersById[post.User]
	if author == nil {
		t.CreateIntermediateUser(post.User)
		author = t.Intermediate.UsersById[post.User]
	}

	createAt := SlackConvertTimeStamp(post.TimeStamp)
	newPost := &IntermediatePost{
		User:      author.Username,
		Channel:   channel.Name,
		Message:   post.Text,
		CreateAt:  createAt,
		Reactions: t.SlackConvertReactions(post.Reactions, createAt),
	}

	props, propsB := t.GetPropsForPost(&post, false, addOriginal)
	if utf8.RuneCount(propsB) <= model.PostPropsMaxRunes {
		newPost.Props = props
	} else {
		if discardInvalidProps {
			t.Logger.Warn("Unable to import the post as props exceed the maximum character count. Skipping as --discard-invalid-props is enabled.")
			return
		} else {
			t.Logger.Warn("Unable to add the props to post as they exceed the maximum character count.")
		}
	}

	AddPostToThreads(post, newPost, threads, channel, timestamps)
}

func (t *Transformer) AddFilesToPost(post *SlackPost, skipAttachments bool, slackExport *SlackExport, attachmentsDir string, newPost *IntermediatePost, allowDownload bool) {
	if skipAttachments || (post.File == nil && post.Files == nil) {
		return
	}
	if post.File != nil {
		if err := addFileToPost(post.File, slackExport.Uploads, newPost, attachmentsDir, allowDownload); err != nil {
			t.Logger.WithError(err).Error("Failed to add file to post")
		}
	} else if post.Files != nil {
		for _, file := range post.Files {
			if file.Name == "" {
				t.Logger.Warnf("Not able to access the file %s as file access is denied so skipping", file.Id)
				continue
			}
			if err := addFileToPost(file, slackExport.Uploads, newPost, attachmentsDir, allowDownload); err != nil {
				t.Logger.WithError(err).Error("Failed to add file to post")
			}
		}
	}
}

func (t *Transformer) GetPropsForPost(post *SlackPost, addAttachments, addOriginal bool) (model.StringInterface, []byte) {
	props := model.StringInterface{}
	if addAttachments {
		props["attachments"] = post.Attachments
	}
	if addOriginal {
		props["slackOriginal"] = post.Original
	}
	if len(props) == 0 {
		return nil, nil
	}
	propsByteArray, _ := json.Marshal(props)
	return props, propsByteArray
}

func (t *Transformer) TransformPosts(slackExport *SlackExport, attachmentsDir string, skipAttachments, discardInvalidProps, allowDownload, addOriginal bool) error {
	t.Logger.Info("Transforming posts")

	newGroupChannels := []*IntermediateChannel{}
	newDirectChannels := []*IntermediateChannel{}
	channelsByOriginalName := buildChannelsByOriginalNameMap(t.Intermediate)

	resultPosts := []*IntermediatePost{}
	for originalChannelName, channelPosts := range slackExport.Posts {
		channel, ok := channelsByOriginalName[originalChannelName]
		if !ok {
			t.Logger.Warnf("--- Couldn't find channel %s referenced by posts", originalChannelName)
			continue
		}

		timestamps := make(map[int64]bool)
		sort.Slice(channelPosts, func(i, j int) bool {
			// Converting to milliseconds can create duplicates, so we need to use
			// microseconds to get a reproducible sort order.
			return SlackConvertTimeStampToMicroSeconds(channelPosts[i].TimeStamp) < SlackConvertTimeStampToMicroSeconds(channelPosts[j].TimeStamp)
		})
		threads := map[string]*IntermediatePost{}

		for _, post := range channelPosts {
			switch {
			// plain message that can have files attached
			case post.IsPlainMessage():
				if post.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}
				author := t.Intermediate.UsersById[post.User]
				if author == nil {
					t.CreateIntermediateUser(post.User)
					author = t.Intermediate.UsersById[post.User]
				}
				createAt := SlackConvertTimeStamp(post.TimeStamp)
				newPost := &IntermediatePost{
					User:      author.Username,
					Channel:   channel.Name,
					Message:   post.Text,
					CreateAt:  createAt,
					Reactions: t.SlackConvertReactions(post.Reactions, createAt),
				}
				t.AddFilesToPost(&post, skipAttachments, slackExport, attachmentsDir, newPost, allowDownload)

				props, propsB := t.GetPropsForPost(&post, len(post.Attachments) > 0, addOriginal)
				if utf8.RuneCount(propsB) <= model.PostPropsMaxRunes {
					newPost.Props = props
				} else {
					if discardInvalidProps {
						t.Logger.Warn("Unable import post as props exceed the maximum character count. Skipping as --discard-invalid-props is enabled.")
						continue
					} else {
						t.Logger.Warn("Unable to add props to post as they exceed the maximum character count.")
					}
				}

				AddPostToThreads(post, newPost, threads, channel, timestamps)

			// file comment
			case post.IsFileComment():
				if post.Comment == nil {
					t.Logger.Warn("Unable to import the message as it has no comments.")
					continue
				}
				if post.Comment.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}
				author := t.Intermediate.UsersById[post.Comment.User]
				if author == nil {
					t.CreateIntermediateUser(post.User)
					author = t.Intermediate.UsersById[post.User]
				}
				createAt := SlackConvertTimeStamp(post.TimeStamp)
				newPost := &IntermediatePost{
					User:      author.Username,
					Channel:   channel.Name,
					Message:   post.Comment.Comment,
					CreateAt:  createAt,
					Reactions: t.SlackConvertReactions(post.Reactions, createAt),
				}

				props, propsB := t.GetPropsForPost(&post, false, addOriginal)
				if utf8.RuneCount(propsB) <= model.PostPropsMaxRunes {
					newPost.Props = props
				} else {
					if discardInvalidProps {
						t.Logger.Warn("Unable to import the post as props exceed the maximum character count. Skipping as --discard-invalid-props is enabled.")
						continue
					} else {
						t.Logger.Warn("Unable to add the props to post as they exceed the maximum character count.")
					}
				}

				AddPostToThreads(post, newPost, threads, channel, timestamps)

			// bot message
			case post.IsBotMessage():
				if post.BotId == "" {
					if post.User == "" {
						t.Logger.Warn("Unable to import the message as the user field is missing.")
						continue
					}
					post.BotId = post.User
				}

				author := t.Intermediate.UsersById[post.BotId]
				if author == nil {
					t.CreateIntermediateUser(post.BotId)
					author = t.Intermediate.UsersById[post.BotId]
				}

				createAt := SlackConvertTimeStamp(post.TimeStamp)
				newPost := &IntermediatePost{
					User:      author.Username,
					Channel:   channel.Name,
					Message:   post.Text,
					CreateAt:  createAt,
					Reactions: t.SlackConvertReactions(post.Reactions, createAt),
				}

				t.AddFilesToPost(&post, skipAttachments, slackExport, attachmentsDir, newPost, allowDownload)

				props, propsB := t.GetPropsForPost(&post, len(post.Attachments) > 0, addOriginal)
				if utf8.RuneCount(propsB) <= model.PostPropsMaxRunes {
					newPost.Props = props
				} else {
					if discardInvalidProps {
						t.Logger.Warn("Unable to import the post as props exceed the maximum character count. Skipping as --discard-invalid-props is enabled.")
						continue
					} else {
						t.Logger.Warn("Unable to add the props to post as they exceed the maximum character count.")
					}
				}

				AddPostToThreads(post, newPost, threads, channel, timestamps)

			// channel join/leave messages
			case post.IsJoinLeaveMessage():
				if post.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}

				t.CreateAndAddPostToThreads(post, threads, timestamps, channel, discardInvalidProps, addOriginal)

			// me message
			case post.IsMeMessage():
				if post.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}
				t.CreateAndAddPostToThreads(post, threads, timestamps, channel, discardInvalidProps, addOriginal)

			// change topic message
			case post.IsChannelTopicMessage():
				if post.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}
				t.CreateAndAddPostToThreads(post, threads, timestamps, channel, discardInvalidProps, addOriginal)

			// change channel purpose message
			case post.IsChannelPurposeMessage():
				if post.User == "" {
					t.Logger.Warn("Unable to import the message as the user field is missing.")
					continue
				}
				t.CreateAndAddPostToThreads(post, threads, timestamps, channel, discardInvalidProps, addOriginal)

			// change channel name message
			case post.IsChannelNameMessage():
				if post.User == "" {
					t.Logger.Warn("Slack Import: Unable to import the message as the user field is missing.")
					continue
				}
				t.CreateAndAddPostToThreads(post, threads, timestamps, channel, discardInvalidProps, addOriginal)

			default:
				t.Logger.Warnf("Unable to import the message as its type is not supported. post_type=%s, post_subtype=%s", post.Type, post.SubType)
			}
		}

		channelPosts := []*IntermediatePost{}
		for _, post := range threads {
			channelPosts = append(channelPosts, post)
		}
		resultPosts = append(resultPosts, channelPosts...)
	}

	if addOriginal {
		// Iterate over resultPosts, check if there are any replies and if so, add them to a new prop called "slackOriginalReplies"
		for _, post := range resultPosts {
			if len(post.Replies) > 0 {
				slackOriginalReplies := make(map[string]string)
				// Populate slackOriginalReplies
				for _, reply := range post.Replies {
					slackOriginal := reply.Props["slackOriginal"]
					slackOriginalString, ok := slackOriginal.(string)
					key := fmt.Sprintf("%d", reply.CreateAt)
					if !ok {
						t.Logger.Warnf("Unable to completely compile slackOriginalReplies since the slackOriginal prop for one of the replies is not a string. reply.CreateAt=%s", key)
						continue
					}
					slackOriginalReplies[key] = slackOriginalString
				}
				// Compress and base64 encode slackOriginalReplies
				slackOriginalRepliesB, sormErr := json.Marshal(slackOriginalReplies)
				if sormErr != nil {
					t.Logger.Warnf("Unable to marshal slackOriginalReplies. sormErr=%s", sormErr.Error())
					continue
				}
				slackOriginalRepliesBCompressed := bytes.NewBuffer(nil)
				// Use zlib compression with highest compression level
				w, wErr := zlib.NewWriterLevel(slackOriginalRepliesBCompressed, zlib.BestCompression)
				if wErr != nil {
					t.Logger.Warnf("Unable to compress slackOriginalReplies. wErr=%s", wErr.Error())
					continue
				}
				_, wrErr := w.Write(slackOriginalRepliesB)
				if wrErr != nil {
					t.Logger.Warnf("Unable to compress slackOriginalReplies. wrErr=%s", wrErr.Error())
					continue
				}
				w.Close()
				slackOriginalRepliesBCompressedBase64 := base64.StdEncoding.EncodeToString(slackOriginalRepliesBCompressed.Bytes())
				// Clone props and add reply originals
				props := model.StringInterface{}
				for k, v := range post.Props {
					props[k] = v
				}
				props["slackOriginalRepliesCompressedBase64"] = slackOriginalRepliesBCompressedBase64
				// Check if props exceeds the maximum character count
				propsB, _ := json.Marshal(props)
				if utf8.RuneCount(propsB) <= model.PostPropsMaxRunes {
					if utf8.RuneCount(propsB) > model.PostPropsMaxRunes/20 {
						t.Logger.Warnf("Props exceeds 5%% of the maximum character count. Rune count=%d, Maximum rune count=%d", utf8.RuneCount(propsB), model.PostPropsMaxRunes)
					}
					post.Props = props
				} else {
					if discardInvalidProps {
						t.Logger.Warn("Unable to import the post as props exceed the maximum character count. Skipping as --discard-invalid-props is enabled.")
						continue
					} else {
						t.Logger.Warn("Unable to add the props to post as they exceed the maximum character count.")
					}
				}
			}
		}
	}

	t.Intermediate.Posts = resultPosts
	t.Intermediate.GroupChannels = append(t.Intermediate.GroupChannels, newGroupChannels...)
	t.Intermediate.DirectChannels = append(t.Intermediate.DirectChannels, newDirectChannels...)

	return nil
}

func (t *Transformer) Transform(slackExport *SlackExport, attachmentsDir string, skipAttachments, discardInvalidProps, allowDownload, addOriginal, teamInternalOnly bool) error {
	t.TransformUsers(slackExport.Users)

	if err := t.TransformAllChannels(slackExport, teamInternalOnly); err != nil {
		return err
	}

	t.PopulateUserMemberships()
	t.PopulateChannelMemberships()

	if err := t.TransformPosts(slackExport, attachmentsDir, skipAttachments, discardInvalidProps, allowDownload, addOriginal); err != nil {
		return err
	}

	return nil
}

func makeAlphaNum(str string, allowAdditional ...rune) string {
	for match, replace := range specialReplacements {
		str = strings.ReplaceAll(str, match, replace)
	}

	str = norm.NFKD.String(str)
	str = strings.Map(func(r rune) rune {
		for _, allowed := range allowAdditional {
			if r == allowed {
				return r
			}
		}

		// filter all non-ASCII runes
		if r > 127 {
			return -1
		}

		// restrict the remaining characters
		if r >= 'a' && r <= 'z' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r
		}
		if r >= '0' && r <= '9' {
			return r
		}

		return '_'
	}, str)
	return str
}

var specialReplacements = map[string]string{
	"Å": "Aa",
	"Ä": "Ae",
	"Ö": "Oe",
	"å": "aa",
	"ä": "ae",
	"ö": "oe",
	"ß": "ss",
}
