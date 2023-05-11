package slack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlackConvertTimeStamp(t *testing.T) {
	testCases := []struct {
		Name           string
		SlackTimeStamp string
		ExpectedResult int64
	}{
		{
			Name:           "Converting an invalid timestamp",
			SlackTimeStamp: "asd",
			ExpectedResult: 1,
		},
		{
			Name:           "Converting a valid integer timestamp",
			SlackTimeStamp: "1549307811",
			ExpectedResult: 1549307811000,
		},
		{
			Name:           "Converting a valid timestamp, rounding down",
			SlackTimeStamp: "1549307811.074100",
			ExpectedResult: 1549307811074,
		},
		{
			Name:           "Converting a valid timestamp, rounding up",
			SlackTimeStamp: "1549307811.074500",
			ExpectedResult: 1549307811075,
		},
		{
			Name:           "Converting a timestamp with 0 decimals",
			SlackTimeStamp: "1549307811.",
			ExpectedResult: 1549307811000,
		},
		{
			Name:           "Converting a timestamp with 1 decimal",
			SlackTimeStamp: "1549307811.1",
			ExpectedResult: 1549307811100,
		},
		{
			Name:           "Converting a timestamp with 2 decimals",
			SlackTimeStamp: "1549307811.12",
			ExpectedResult: 1549307811120,
		},
		{
			Name:           "Converting a timestamp with 3 decimals",
			SlackTimeStamp: "1549307811.123",
			ExpectedResult: 1549307811123,
		},
		{
			Name:           "Converting a timestamp with 4 decimals",
			SlackTimeStamp: "1549307811.1234",
			ExpectedResult: 1549307811123,
		},
		{
			Name:           "Converting a timestamp with 5 decimals",
			SlackTimeStamp: "1549307811.12345",
			ExpectedResult: 1549307811123,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			res := SlackConvertTimeStamp(tc.SlackTimeStamp)
			require.Equal(t, tc.ExpectedResult, res)
		})
	}
}

func TestSlackConvertTimeStampToMicroSeconds(t *testing.T) {
	testCases := []struct {
		Name           string
		SlackTimeStamp string
		ExpectedResult int64
	}{
		{
			Name:           "Converting an invalid timestamp",
			SlackTimeStamp: "asd",
			ExpectedResult: 1,
		},
		{
			Name:           "Converting a valid integer timestamp",
			SlackTimeStamp: "1549307811",
			ExpectedResult: 1549307811000000,
		},
		{
			Name:           "Converting a valid timestamp that would round down when converted to milliseconds",
			SlackTimeStamp: "1549307811.074100",
			ExpectedResult: 1549307811074100,
		},
		{
			Name:           "Converting a valid timestamp that would round up when converted to milliseconds",
			SlackTimeStamp: "1549307811.074500",
			ExpectedResult: 1549307811074500,
		},
		{
			Name:           "Converting a timestamp with 0 decimals",
			SlackTimeStamp: "1549307811.",
			ExpectedResult: 1549307811000000,
		},
		{
			Name:           "Converting a timestamp with 1 decimal",
			SlackTimeStamp: "1549307811.1",
			ExpectedResult: 1549307811100000,
		},
		{
			Name:           "Converting a timestamp with 2 decimals",
			SlackTimeStamp: "1549307811.12",
			ExpectedResult: 1549307811120000,
		},
		{
			Name:           "Converting a timestamp with 3 decimals",
			SlackTimeStamp: "1549307811.123",
			ExpectedResult: 1549307811123000,
		},
		{
			Name:           "Converting a timestamp with 4 decimals",
			SlackTimeStamp: "1549307811.1234",
			ExpectedResult: 1549307811123400,
		},
		{
			Name:           "Converting a timestamp with 5 decimals",
			SlackTimeStamp: "1549307811.12345",
			ExpectedResult: 1549307811123450,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			res := SlackConvertTimeStampToMicroSeconds(tc.SlackTimeStamp)
			require.Equal(t, tc.ExpectedResult, res)
		})
	}
}
