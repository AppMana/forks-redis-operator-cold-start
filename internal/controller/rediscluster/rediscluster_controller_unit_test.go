package rediscluster

import (
	"context"
	"errors"
	"testing"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

type fakeChecker struct {
	redis.Checker
	slotsAssigned bool
	err           error
	called        bool
}

func TestCurrentLeaderReplicasPreservesReadErrors(t *testing.T) {
	c := k8sfake.NewSimpleClientset()
	r := &Reconciler{K8sClient: c}
	n, err := r.currentLeaderReplicas(context.Background(), "default", "test-leader")
	assert.NoError(t, err)
	assert.Zero(t, n)
	c.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("read failed") })
	_, err = r.currentLeaderReplicas(context.Background(), "default", "test-leader")
	assert.Error(t, err, "a failed read must not masquerade as an absent StatefulSet")
	for _, replicas := range []*int32{nil, ptr.To(int32(3))} {
		r.K8sClient = k8sfake.NewSimpleClientset(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "test-leader", Namespace: "default"}, Spec: appsv1.StatefulSetSpec{Replicas: replicas}})
		n, err = r.currentLeaderReplicas(context.Background(), "default", "test-leader")
		if replicas == nil {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, int32(3), n)
		}
	}
}

func TestClusterServingStatusWithdrawsReady(t *testing.T) {
	ready := rcvb2.RedisClusterStatus{State: rcvb2.RedisClusterReady}
	next := clusterServingStatus(ready, false)
	if assert.NotNil(t, next) {
		assert.Equal(t, rcvb2.RedisClusterBootstrap, next.State)
	}
	assert.Nil(t, clusterServingStatus(ready, true))
	bootstrap := rcvb2.RedisClusterStatus{State: rcvb2.RedisClusterBootstrap}
	assert.Nil(t, clusterServingStatus(bootstrap, false))
	next = clusterServingStatus(bootstrap, true)
	if assert.NotNil(t, next) {
		assert.Equal(t, rcvb2.RedisClusterReady, next.State)
	}
}

func (f *fakeChecker) CheckClusterSlotsAssigned(ctx context.Context, cr *rcvb2.RedisCluster) (bool, error) {
	f.called = true
	return f.slotsAssigned, f.err
}

func TestShouldScaleUpExistingCluster(t *testing.T) {
	tests := []struct {
		name           string
		leaderCount    int32
		slotsAssigned  bool
		checkErr       error
		wantScaleUp    bool
		wantErr        bool
		wantCheckerHit bool
	}{
		{
			name:           "formed single-node cluster scales up via add-node (issue #1521)",
			leaderCount:    1,
			slotsAssigned:  true,
			wantScaleUp:    true,
			wantCheckerHit: true,
		},
		{
			name:           "fresh nodes without assigned slots fall back to cluster creation",
			leaderCount:    1,
			slotsAssigned:  false,
			wantScaleUp:    false,
			wantCheckerHit: true,
		},
		{
			name:           "formed two-leader cluster scales up via add-node",
			leaderCount:    2,
			slotsAssigned:  true,
			wantScaleUp:    true,
			wantCheckerHit: true,
		},
		{
			name:        "no leaders means initial creation, slot check skipped",
			leaderCount: 0,
			wantScaleUp: false,
		},
		{
			name:        "more than two leaders is always a formed cluster, slot check skipped",
			leaderCount: 3,
			wantScaleUp: true,
		},
		{
			name:           "slot check error is propagated",
			leaderCount:    1,
			checkErr:       errors.New("connection refused"),
			wantErr:        true,
			wantCheckerHit: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &fakeChecker{slotsAssigned: tt.slotsAssigned, err: tt.checkErr}
			r := &Reconciler{Checker: checker}

			scaleUp, err := r.shouldScaleUpExistingCluster(context.Background(), &rcvb2.RedisCluster{}, tt.leaderCount)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantScaleUp, scaleUp)
			}
			assert.Equal(t, tt.wantCheckerHit, checker.called)
		})
	}
}
