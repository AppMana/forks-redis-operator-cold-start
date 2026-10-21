package k8sutils

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPausePrimaryDoesNotHideResumeInventoryFailure(t *testing.T) {
	for _, mode := range []string{"inventory-failure", "resume-failure", "success", "task-gone"} {
		t.Run(mode, func(t *testing.T) { testPausePrimaryCleanup(t, mode) })
	}
}

func testPausePrimaryCleanup(t *testing.T, mode string) {
	t.Helper()
	dir := t.TempDir()
	kubectl := `#!/bin/sh
case "$*" in
  'config current-context') echo kind-test ;;
  *'get redisreplication'*)
    if [ -e "$TEST_STATE/observed" ]; then echo redis-replication-1; else touch "$TEST_STATE/observed"; echo redis-replication-0; fi ;;
  *'spec.nodeName'*) echo test-control-plane ;;
  *'containerID'*) printf 'containerd://%064d\n' 1 ;;
  *'INFO replication'*) echo role:master ;;
  *'get endpoints'*) echo redis-replication-1 ;;
  *) exit 2 ;;
esac
`
	docker := `#!/bin/sh
case "$*" in
  *'tasks list -q')
    case "$TEST_MODE" in inventory-failure) exit 12;; task-gone) exit 0;; esac
    printf '%064d\n' 1 ;;
  *'tasks list') printf '%064d 123 PAUSED\n' 1 ;;
  *'tasks pause'*) exit 0 ;;
  *'tasks resume'*) [ "$TEST_MODE" != resume-failure ] ;;
  'exec -d '*) exit 0 ;;
  *) exit 2 ;;
esac
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kubectl"), []byte(kubectl), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte(docker), 0o700))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "../../tests/e2e-chainsaw/v1beta2/setup/redis-replication/pause-primary.sh")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "TEST_STATE="+dir, "TEST_MODE="+mode, "NAMESPACE=chainsaw-test", "REPLICATION_NAME=redis-replication")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "the regression must fail through cleanup, not its deadline")
	if mode == "success" || mode == "task-gone" {
		require.NoError(t, err)
		require.Contains(t, string(output), "PASS:paused-primary")
	} else {
		require.Error(t, err, "unknown runtime state or failed resume must not count as success")
		require.NotContains(t, string(output), "PASS:paused-primary")
	}
}
