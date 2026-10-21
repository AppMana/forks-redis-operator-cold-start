package redis

import (
	"context"
	"fmt"
	"testing"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/features"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestGetPasswordMatchesInitBootstrapWhitespace(t *testing.T) {
	original := features.Enabled(features.GenerateConfigInInitContainer)
	t.Cleanup(func() {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", original)))
	})
	kube := k8sfake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"}, Data: map[string][]byte{"password": []byte(" spaced\tpassword \r\n")}})
	ref := &commonapi.ExistingPasswordSecret{Name: ptr.To("credentials"), Key: ptr.To("password")}
	for _, enabled := range []bool{true, false} {
		require.NoError(t, features.MutableFeatureGate.Set(fmt.Sprintf("GenerateConfigInInitContainer=%t", enabled)))
		value, err := NewChecker(kube).GetPassword(context.Background(), "test", ref)
		require.NoError(t, err)
		if enabled {
			require.Equal(t, " spaced\tpassword ", value)
		} else {
			require.Equal(t, "spaced\tpassword", value)
		}
	}
}

func TestGetPasswordRejectsIncompleteReference(t *testing.T) {
	for _, ref := range []*commonapi.ExistingPasswordSecret{
		{},
		{Name: ptr.To("credentials")},
		{Key: ptr.To("password")},
		{Name: ptr.To(""), Key: ptr.To("password")},
		{Name: ptr.To("credentials"), Key: ptr.To("")},
	} {
		kube := k8sfake.NewSimpleClientset()
		var err error
		require.NotPanics(t, func() { _, err = NewChecker(kube).GetPassword(context.Background(), "test", ref) })
		require.Error(t, err)
		require.Empty(t, kube.Actions(), "invalid references must fail before API access")
	}
}

func TestSentinelMonitorRejectsUnsupportedCredentialSource(t *testing.T) {
	for _, source := range []*corev1.EnvVarSource{
		{},
		{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "password"}},
		{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
		{ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.cpu"}},
		{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "password"}, FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
	} {
		rs, _, objects := newSentinelFixture("10.0.1.10")
		rs.Spec.RedisSentinelConfig.RedisReplicationPassword = source
		client := &fakeRedisClient{}
		h := &healer{redis: client, k8s: k8sfake.NewSimpleClientset(objects...)}
		require.ErrorContains(t, h.SentinelMonitor(context.Background(), rs, "10.0.0.10"), "secretKeyRef")
		require.Empty(t, client.connectHosts, "unknown credentials must never clear live auth-pass")
	}
}

func TestGetPasswordAbsentValidAndMissingKey(t *testing.T) {
	kube := k8sfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"},
		Data:       map[string][]byte{"password": []byte("synthetic-password\n")},
	})
	checker := NewChecker(kube)
	value, err := checker.GetPassword(context.Background(), "test", nil)
	require.NoError(t, err)
	require.Empty(t, value)
	require.Empty(t, kube.Actions())
	ref := &commonapi.ExistingPasswordSecret{Name: ptr.To("credentials"), Key: ptr.To("password")}
	value, err = checker.GetPassword(context.Background(), "test", ref)
	require.NoError(t, err)
	require.Equal(t, "synthetic-password", value)
	ref.Key = ptr.To("missing")
	_, err = checker.GetPassword(context.Background(), "test", ref)
	require.ErrorContains(t, err, "secret key not found")
}
