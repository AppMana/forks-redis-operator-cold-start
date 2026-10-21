package k8sutils

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestReplicationPreStopKeepsAuthenticationSeparate(t *testing.T) {
	for _, sentinelPassword := range []string{"synthetic-sentinel", ""} {
		dir := t.TempDir()
		stub := `#!/bin/sh
case "$*" in
  *"SENTINEL FAILOVER"*)
    test "${REDISCLI_AUTH:-}" = "$EXPECTED_SENTINEL" || exit 1
    touch "$TEST_DIR/sentinel-ok"
    echo OK
    ;;
  *)
    test "$REDISCLI_AUTH" = synthetic-redis || exit 1
    if test -e "$TEST_DIR/initial"; then
      touch "$TEST_DIR/demotion-ok"
      echo role:slave
    else
      touch "$TEST_DIR/initial"
      echo role:master
    fi
    ;;
esac
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "redis-cli"), []byte(stub), 0o700))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, "sh", "-c", GeneratePreStopCommand(PreStopConfig{Role: "replication", SentinelService: "sentinel", SentinelMasterName: "mymaster", WaitSeconds: 1}))
		cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "TEST_DIR="+dir, "REDIS_PORT=6379", "REDISCLI_AUTH=synthetic-redis\n", "EXPECTED_SENTINEL="+sentinelPassword, "SENTINELCLI_AUTH="+sentinelPassword)
		out, err := cmd.CombinedOutput()
		cancel()
		require.NoError(t, err, "%s", out)
		require.FileExists(t, filepath.Join(dir, "sentinel-ok"))
		require.FileExists(t, filepath.Join(dir, "demotion-ok"), "Redis authentication must survive the Sentinel subprocess")
	}
}

func TestReplicationContainerIncludesSentinelAuthReference(t *testing.T) {
	params := containerParameters{Role: "replication", SentinelService: "sentinel", SentinelMasterName: "mymaster", SentinelAuthSecret: &commonapi.ExistingPasswordSecret{Name: ptr.To("sentinel-auth"), Key: ptr.To("password")}}
	containers := generateContainerDef("redis", params, false, false, false, nil, ptr.To("v7"), nil, nil)
	require.Contains(t, containers[0].Env, corev1.EnvVar{Name: "SENTINELCLI_AUTH", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "sentinel-auth"}, Key: "password"}}})
}
