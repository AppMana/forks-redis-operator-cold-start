package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestCommandFailuresReturnErrorsWithoutCredentials(t *testing.T) {
	for _, verb := range []string{"gen-redis-data", "chk-redis-data"} {
		t.Run(verb, func(t *testing.T) {
			cmd := newRootCommand()
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs([]string{verb, "--mode", "unsupported", "--password", "synthetic-redis-credential", "--password-sentinel", "synthetic-sentinel-credential"})
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			originalStdout := os.Stdout
			os.Stdout = writer
			t.Cleanup(func() { os.Stdout = originalStdout; _ = reader.Close(); _ = writer.Close() })
			commandErr := cmd.Execute()
			_ = writer.Close()
			os.Stdout = originalStdout
			printed, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			output.Write(printed)
			if commandErr == nil {
				t.Fatal("failed command reported success")
			}
			for _, forbidden := range []string{"synthetic-redis-credential", "synthetic-sentinel-credential", "[OK]"} {
				if strings.Contains(output.String(), forbidden) {
					t.Fatalf("output contained forbidden marker %q", forbidden)
				}
			}
		})
	}
}

func TestGenerationPropagatesCancelledWrite(t *testing.T) {
	cmd := newRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"gen-redis-data", "--mode", "cluster", "--host", "127.0.0.1:1"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd.SetContext(ctx)
	if err := cmd.Execute(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if strings.Contains(output.String(), "[OK]") {
		t.Fatal("failed write printed success")
	}
}

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
