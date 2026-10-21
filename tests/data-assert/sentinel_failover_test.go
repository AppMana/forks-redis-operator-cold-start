package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This opt-in probe runs inside the isolated canary namespace. The fault is
// injected separately; the client is never recreated during the observation.
func TestNativeSentinelClientAcrossFailover(t *testing.T) {
	if os.Getenv("CANARY_NATIVE_SENTINEL_TEST") != "1" {
		t.Skip("requires isolated kind canary and a separately injected failover")
	}
	const namespace = "redis-upgrade-canary-empty-disk"
	hosts := make([]string, 3)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("upgrade-sentinel-%d.upgrade-sentinel-headless.%s.svc.cluster.local:26379", i, namespace)
	}
	client, err := createRedisClient("sentinel", hosts, "", "", "upgrade", false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	lastSuccess := time.Now()
	processes := map[string]bool{}
	reads, failures := 0, 0
	writes := os.Getenv("CANARY_NATIVE_SENTINEL_WRITE_TEST") == "1"
	prefix := fmt.Sprintf("appmana:canary:ack:%d:", time.Now().UnixNano())
	acknowledged := map[string]string{}
	writeAttempts, ambiguousWrites := 0, 0
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		value, readErr := client.Get(attempt, "appmana:upgrade:sentinel-regression").Result()
		info, infoErr := client.Info(attempt, "server").Result()
		if writes && readErr == nil && infoErr == nil {
			key := fmt.Sprintf("%s%d", prefix, writeAttempts)
			want := fmt.Sprintf("value-%d", writeAttempts)
			writeAttempts++
			if err := client.Set(attempt, key, want, 0).Err(); err == nil {
				acknowledged[key] = want
			} else {
				// A timed-out write may have executed; do not count it as an
				// acknowledged success or infer that it was never applied.
				ambiguousWrites++
			}
		}
		stop()
		if ctx.Err() != nil {
			break
		}
		if readErr == redis.Nil || (readErr == nil && value != "preserved-empty-disk-20261021") {
			t.Fatal("native client observed missing or changed retained data")
		}
		if readErr == nil && infoErr == nil {
			id := serverRunID(info)
			if id == "" {
				t.Fatal("server identity missing from successful observation")
			}
			processes[id] = true
			lastSuccess = time.Now()
			reads++
			if reads == 1 {
				fmt.Println("READY: native Sentinel client connected; inject canary failover now")
			}
		} else {
			failures++
		}
		if time.Since(lastSuccess) > 30*time.Second {
			t.Fatal("native client did not recover within 30 seconds")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(processes) < 2 || time.Since(lastSuccess) > 3*time.Second {
		t.Fatal("no verified primary-process transition or client not healthy at completion")
	}
	if writes {
		if len(acknowledged) < 2 {
			t.Fatal("insufficient acknowledged writes to qualify recovery")
		}
		verify, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := verifyAcknowledgedWrites(verify, client, acknowledged); err != nil {
			t.Fatal(err)
		}
		t.Logf("PASS: retained %d acknowledged writes; failed/ambiguous write attempts=%d; prefix=%s", len(acknowledged), ambiguousWrites, prefix)
	}
	t.Logf("PASS: same native client read retained data across %d Redis processes; successful reads=%d, transient failures=%d", len(processes), reads, failures)
}

func verifyAcknowledgedWrites(ctx context.Context, reader dataReader, acknowledged map[string]string) error {
	for key, expected := range acknowledged {
		value, err := reader.Get(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("acknowledged write unavailable during verification: %w", err)
		}
		if value != expected {
			return fmt.Errorf("acknowledged write value changed")
		}
	}
	return nil
}

func TestAcknowledgedWritesRequireExactRetainedValues(t *testing.T) {
	for _, tc := range []struct {
		reader fixtureReader
		fail   bool
	}{
		{fixtureReader{}, false},
		{fixtureReader{badKey: "key-1", err: redis.Nil}, true},
		{fixtureReader{badKey: "key-1", value: "changed"}, true},
		{fixtureReader{badKey: "key-1", err: context.DeadlineExceeded}, true},
	} {
		err := verifyAcknowledgedWrites(context.Background(), tc.reader, map[string]string{"key-1": "value-1"})
		if (err != nil) != tc.fail {
			t.Fatalf("verification error=%v, expected failure=%v", err, tc.fail)
		}
	}
}

func serverRunID(info string) string {
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); ok {
			return value
		}
	}
	return ""
}

func TestServerRunID(t *testing.T) {
	if serverRunID("# Server\r\nrun_id:observed\r\n") != "observed" || serverRunID("# Server\n") != "" {
		t.Fatal("server identity parsing failed")
	}
}
