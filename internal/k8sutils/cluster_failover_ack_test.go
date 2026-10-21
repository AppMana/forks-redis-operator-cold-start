package k8sutils

import (
	"context"
	"errors"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/stretchr/testify/require"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestFailoverAcknowledgement(t *testing.T) {
	for _, output := range []string{"OK", "OK\r\n"} {
		require.NoError(t, validateFailoverAcknowledgement(output, nil))
	}
	for _, output := range []string{"", "ERR You should send CLUSTER FAILOVER to a replica", "NOAUTH Authentication required", "OK\nERR uncertain", "private-value"} {
		err := validateFailoverAcknowledgement(output, nil)
		require.EqualError(t, err, "redis cluster failover was not acknowledged")
	}
	require.EqualError(t, validateFailoverAcknowledgement("OK", errors.New("private-value")), "redis cluster failover was not acknowledged")
}

func TestClusterFailoverRejectsUnavailableAuthentication(t *testing.T) {
	kube := k8sfake.NewSimpleClientset()
	cr := &rcvb2.RedisCluster{Spec: rcvb2.RedisClusterSpec{
		Port: ptr.To(6379), ClusterVersion: ptr.To("v7"),
		KubernetesConfig: commonapi.KubernetesConfig{ExistingPasswordSecret: &commonapi.ExistingPasswordSecret{}},
	}}
	require.EqualError(t, ClusterFailover(context.Background(), kube, cr, 0), "cannot initiate failover: authentication configuration unavailable")
	require.Empty(t, kube.Actions(), "invalid credentials must stop before pod lookup or exec")
}
