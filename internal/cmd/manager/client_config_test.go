package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestReconcileClientPreservesManagerSettings(t *testing.T) {
	path := t.TempDir() + "/config"
	require.NoError(t, clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: "https://127.0.0.1:1"}},
		Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test"}}, CurrentContext: "test",
	}, path))
	t.Setenv("KUBECONFIG", path)
	cfg, err := ctrl.GetConfig()
	require.NoError(t, err)
	require.Equal(t, float32(20), cfg.QPS)
	cfg.Timeout = 17 * time.Second
	c, err := createK8sClient(cfg)
	require.NoError(t, err)
	rc := c.CoreV1().RESTClient().(*rest.RESTClient)
	require.Equal(t, cfg.QPS, rc.GetRateLimiter().QPS(), "reconciliation must not silently fall back to 5 QPS")
	require.Equal(t, cfg.Timeout, rc.Client.Timeout, "reconciliation must honor configured API timeouts")
	cfg.QPS = 7
	cfg.Burst = 14
	c, err = createK8sClient(cfg)
	require.NoError(t, err)
	require.Equal(t, float32(7), c.CoreV1().RESTClient().GetRateLimiter().QPS(), "explicit limits must remain supported")
	require.Equal(t, 14, cfg.Burst, "client creation must not mutate manager config")
}
