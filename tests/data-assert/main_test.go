package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fixtureReader struct {
	badKey string
	value  string
	err    error
}

func (f fixtureReader) Get(_ context.Context, key string) *redis.StringCmd {
	if key == f.badKey {
		return redis.NewStringResult(f.value, f.err)
	}
	return redis.NewStringResult(strings.Replace(key, "key-", "value-", 1), nil)
}

func TestCheckData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader fixtureReader
		want   string
	}{
		{name: "intact"},
		{name: "missing", reader: fixtureReader{badKey: "key-5", err: redis.Nil}, want: "Data count mismatch"},
		{name: "corrupt", reader: fixtureReader{badKey: "key-5", value: "private-corrupt-value"}, want: "data value mismatch for key key-5"},
		{name: "unavailable", reader: fixtureReader{badKey: "key-5", err: errors.New("connection refused")}, want: "connection refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkData(context.Background(), tc.reader)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
			if strings.Contains(fmt.Sprint(err), "private-corrupt-value") {
				t.Fatal("error exposed stored value")
			}
		})
	}
}
