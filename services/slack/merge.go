package slack

import (
	"archive/zip"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"sort"

	"github.com/pkg/errors"
)

// MergeSlackExports merges multiple Slack exports into a single one. This can
// be useful if you are unable to do a Slack corporate export but are able to do
// one Slack workspace export from each user's perspective. It might also be
// useful if you want to merge Slack exports from different time periods, or
// different Slack workspaces.
func (t *Transformer) MergeSlackExports(slackExports []*SlackExport) (*SlackExport, error) {
	if len(slackExports) == 0 {
		return nil, errors.New("no Slack exports to merge")
	}
	if len(slackExports) == 1 {
		return slackExports[0], nil
	}
	result := &SlackExport{}
	var err error
	for i, slackExport := range slackExports {
		if i == 0 {
			result.TeamName = slackExport.TeamName
			result.Channels = cloneSlice(slackExport.Channels)
			result.PublicChannels = cloneSlice(slackExport.PublicChannels)
			result.PrivateChannels = cloneSlice(slackExport.PrivateChannels)
			result.GroupChannels = cloneSlice(slackExport.GroupChannels)
			result.DirectChannels = cloneSlice(slackExport.DirectChannels)
			result.Users = cloneSlice(slackExport.Users)
			result.Posts = cloneMap(slackExport.Posts)
			result.Uploads = cloneMap(slackExport.Uploads)
			continue
		}
		// Merge TeamName        string
		if result.TeamName != slackExport.TeamName {
			return nil, errors.Errorf("cannot merge Slack exports with different team names: %s and %s", result.TeamName, slackExport.TeamName)
		}
		// Merge Channels        []SlackChannel
		result.Channels, err = mergeChannels(result.Channels, slackExport.Channels)
		if err != nil {
			return nil, err
		}
		// Merge PublicChannels  []SlackChannel
		result.PublicChannels, err = mergeChannels(result.PublicChannels, slackExport.PublicChannels)
		if err != nil {
			return nil, err
		}
		// Merge PrivateChannels []SlackChannel
		result.PrivateChannels, err = mergeChannels(result.PrivateChannels, slackExport.PrivateChannels)
		if err != nil {
			return nil, err
		}
		// Merge GroupChannels   []SlackChannel
		result.GroupChannels, err = mergeChannels(result.GroupChannels, slackExport.GroupChannels)
		if err != nil {
			return nil, err
		}
		// Merge DirectChannels  []SlackChannel
		result.DirectChannels, err = mergeChannels(result.DirectChannels, slackExport.DirectChannels)
		if err != nil {
			return nil, err
		}
		// Merge Users           []SlackUser
		result.Users, err = mergeUsers(result.Users, slackExport.Users)
		if err != nil {
			return nil, err
		}
		// Merge Posts           map[string][]SlackPost
		result.Posts, err = mergePosts(result.Posts, slackExport.Posts)
		if err != nil {
			return nil, err
		}
		// Merge Uploads         map[string]*zip.File
		result.Uploads, err = mergeUploads(result.Uploads, slackExport.Uploads)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// cloneSlice returns a shallow copy of the given slice, that can be modified
// without affecting the original slice. The clone is not a deep copy, so
// modifying the elements of the clone will still modify the original slice.
func cloneSlice[T any](slice []T) []T {
	ret := make([]T, len(slice))
	copy(ret, slice)
	return ret
}

// cloneMap returns a shallow copy of the given map, that can be modified
// without affecting the original map. The clone is not a deep copy, so
// modifying the elements of the clone will still modify the original map.
func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	ret := map[K]V{}
	for k, v := range m {
		ret[k] = v
	}
	return ret
}

// mergeSlicesWith merges two slices of the same type, using the given id
// function to determine the identity of each element, and the given merge
// function to merge two elements with the same identity. The merge function
// should return an error if the two elements cannot be merged. Note that
// elements with the same identity will be merged even if they appear in the
// same slice.
func mergeSlicesWith[T any](a, b []T, id func(x T) string, merge func(a, b T) (T, error)) ([]T, error) {
	itemMap := map[string]T{}
	for _, source := range [][]T{a, b} {
		for _, item := range source {
			itemId := id(item)
			if existing, ok := itemMap[itemId]; ok {
				mergedItem, err := merge(existing, item)
				if err != nil {
					return nil, err
				}
				itemMap[itemId] = mergedItem
			} else {
				itemMap[itemId] = item
			}
		}
	}
	ret := []T{}
	for _, item := range itemMap {
		ret = append(ret, item)
	}
	return ret, nil
}

// mergeMapsWith merges two maps of the same type, using the given merge
// function to merge two elements with the same key. The merge function should
// return an error if the two elements cannot be merged.
func mergeMapsWith[K comparable, V any](a, b map[K]V, merge func(a, b V) (V, error)) (map[K]V, error) {
	ret := cloneMap(a)
	for k, v := range b {
		if existing, ok := ret[k]; ok {
			merged, err := merge(existing, v)
			if err != nil {
				return nil, err
			}
			ret[k] = merged
		} else {
			ret[k] = v
		}
	}
	return ret, nil
}

// mergeChannels merges two slices of SlackChannel, using the Id field to
// determine the identity of each channel.
func mergeChannels(a, b []SlackChannel) ([]SlackChannel, error) {
	return mergeSlicesWith(a, b, func(x SlackChannel) string { return x.Id }, mergeChannel)
}
func mergeChannel(a, b SlackChannel) (SlackChannel, error) {
	nothing := SlackChannel{}
	// Id      string
	if a.Id != b.Id {
		return nothing, errors.Errorf("cannot merge channels with different IDs: %s and %s", a.Id, b.Id)
	}
	// Name    string
	if a.Name != b.Name {
		return nothing, errors.Errorf("cannot merge channels with different names: %s and %s", a.Name, b.Name)
	}
	// Creator string
	if a.Creator != b.Creator {
		return nothing, errors.Errorf("cannot merge channels with different creators: %s and %s", a.Creator, b.Creator)
	}
	// Members []string
	aMembers := cloneSlice(a.Members)
	bMembers := cloneSlice(b.Members)
	sort.Strings(aMembers)
	sort.Strings(bMembers)
	if !reflect.DeepEqual(aMembers, bMembers) {
		return nothing, errors.Errorf("cannot merge channels with different members: %v and %v", a.Members, b.Members)
	}
	// Purpose SlackChannelSub
	if !reflect.DeepEqual(a.Purpose, b.Purpose) {
		return nothing, errors.Errorf("cannot merge channels with different purposes: %v and %v", a.Purpose, b.Purpose)
	}
	// Topic   SlackChannelSub
	if !reflect.DeepEqual(a.Topic, b.Topic) {
		return nothing, errors.Errorf("cannot merge channels with different topics: %v and %v", a.Topic, b.Topic)
	}
	// Type    model.ChannelType
	if a.Type != b.Type {
		return nothing, errors.Errorf("cannot merge channels with different types: %v and %v", a.Type, b.Type)
	}
	return a, nil
}

// mergeUsers merges two slices of SlackUser, using the Id field to determine
// the identity of each user.
func mergeUsers(a, b []SlackUser) ([]SlackUser, error) {
	return mergeSlicesWith(a, b, func(x SlackUser) string { return x.Id }, mergeUser)
}
func mergeUser(a, b SlackUser) (SlackUser, error) {
	if reflect.DeepEqual(a, b) {
		return a, nil
	}
	// We can modify these since the whole structure was passed by value.
	if a.Profile.Email == "" {
		a.Profile.Email = b.Profile.Email
	}
	if b.Profile.Email == "" {
		b.Profile.Email = a.Profile.Email
	}
	if reflect.DeepEqual(a, b) {
		return a, nil
	}
	return SlackUser{}, errors.Errorf("cannot merge users that differ (in other ways than email missing from one of them): %v and %v", a, b)
}

// mergePosts merges two maps of []SlackPost, using the TimeStamp field to
// determine the identity of each post in the slices.
func mergePosts(a, b map[string][]SlackPost) (map[string][]SlackPost, error) {
	return mergeMapsWith(a, b, mergePostSlice)
}
func mergePostSlice(a, b []SlackPost) ([]SlackPost, error) {
	return mergeSlicesWith(a, b, func(x SlackPost) string { return x.TimeStamp }, mergePost)
}
func mergePost(a SlackPost, b SlackPost) (SlackPost, error) {
	// Check that the posts are equal except for the Original field.
	aCopy, bCopy := a, b
	aCopy.Original = ""
	bCopy.Original = ""
	if !reflect.DeepEqual(aCopy, bCopy) {
		return SlackPost{}, errors.Errorf("cannot merge unequal posts: %v and %v", a, b)
	}
	// Check that the Original fields are equivalent, meaning equal except for
	// the last_read, subscribed, and blocks[].block_id fields.
	originalsAreEquivalent, err := postOriginalEquivalent(a.Original, b.Original)
	if err != nil {
		return SlackPost{}, err
	}
	if !originalsAreEquivalent {
		return SlackPost{}, errors.Errorf("cannot merge posts with original JSON that differs in other ways than last_read, subscribed, and blocks[].block_id: %s and %s", a.Original, b.Original)
	}
	return a, nil
}
func postOriginalEquivalent(a, b string) (bool, error) {
	var m1, m2 map[string]interface{}
	if err := json.Unmarshal([]byte(a), &m1); err != nil {
		return false, err
	}
	if err := json.Unmarshal([]byte(b), &m2); err != nil {
		return false, err
	}
	for _, m := range []map[string]interface{}{m1, m2} {
		delete(m, "last_read")
		delete(m, "subscribed")
		if blocks, ok := m["blocks"].([]interface{}); ok {
			for _, block := range blocks {
				if blockMap, ok := block.(map[string]interface{}); ok {
					delete(blockMap, "block_id")
				}
			}
		}
	}
	modifiedJSON1, err := json.Marshal(m1)
	if err != nil {
		return false, err
	}
	modifiedJSON2, err := json.Marshal(m2)
	if err != nil {
		return false, err
	}
	return string(modifiedJSON1) == string(modifiedJSON2), nil
}

// mergeUploads merges two maps of zip.File representing uploads.
func mergeUploads(a, b map[string]*zip.File) (map[string]*zip.File, error) {
	return mergeMapsWith(a, b, mergeZipFile)
}
func mergeZipFile(a, b *zip.File) (*zip.File, error) {
	if a.NonUTF8 != b.NonUTF8 {
		return nil, errors.Errorf("cannot merge zip files with different nonUTF8 flags: %v and %v", a.NonUTF8, b.NonUTF8)
	}
	if a.Name != b.Name {
		return nil, errors.Errorf("cannot merge zip files with different names: %s and %s", a.Name, b.Name)
	}
	// We allow Comment, CreatorVersion, ReaderVersion, Flags to differ
	if a.CRC32 == 0 || b.CRC32 == 0 {
		return nil, errors.Errorf("cannot merge zip files unless both CRC32 checksums are known: %d and %d", a.CRC32, b.CRC32)
	}
	if a.CRC32 != b.CRC32 {
		return nil, errors.Errorf("cannot merge zip files with different CRC32 checksums: %d and %d", a.CRC32, b.CRC32)
	}
	if a.UncompressedSize64 != b.UncompressedSize64 {
		return nil, errors.Errorf("cannot merge zip files with different uncompressed sizes: %d and %d", a.UncompressedSize64, b.UncompressedSize64)
	}
	if !sliceEquals(a.Extra, b.Extra) {
		aExtraParsed, err := parseZipExtraFields(a.Extra)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse extra fields of %s", a.Name)
		}
		bExtraParsed, err := parseZipExtraFields(b.Extra)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse extra fields of %s", b.Name)
		}
		if !zipExtraCanBeIgnored(aExtraParsed) || !zipExtraCanBeIgnored(bExtraParsed) {
			return nil, errors.Errorf("cannot merge zip files with these differing extra fields: %v and %v", a.Extra, b.Extra)
		}
	}
	if a.ExternalAttrs != b.ExternalAttrs {
		return nil, errors.Errorf("cannot merge zip files with different external attributes: %d and %d", a.ExternalAttrs, b.ExternalAttrs)
	}
	return a, nil
}

func sliceEquals[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if x != b[i] {
			return false
		}
	}
	return true
}

type zipExtraField struct {
	field uint16
	data  []byte
}

func parseZipExtraFields(extra []byte) ([]zipExtraField, error) {
	var result []zipExtraField
	for len(extra) >= 4 {
		field := binary.LittleEndian.Uint16(extra)
		size := int(binary.LittleEndian.Uint16(extra[2:]))
		if size > len(extra)-4 {
			return nil, errors.Errorf("zip extra field size %d is larger than remaining data %d", size, len(extra)-4)
		}
		result = append(result, zipExtraField{field, extra[4 : size+4]})
		extra = extra[size+4:]
	}
	if len(extra) != 0 {
		return nil, errors.Errorf("zip extra field data left over after parsing: %v", extra)
	}
	return result, nil
}

func zipExtraCanBeIgnored(extraFields []zipExtraField) bool {
	for _, field := range extraFields {
		switch field.field {
		case 0x5455:
			// Extended Timestamp (0x5455, "UT") stores additional timestamps for the
			// file, such as creation, modification, and access times. These can be
			// ignored for merging purposes.
			continue
		default:
			return false
		}
	}
	return true
}
