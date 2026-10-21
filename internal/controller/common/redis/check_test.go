package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	redisservice "github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

type deadlineKeyClient struct{ service redisservice.Service }

func (c deadlineKeyClient) Connect(*redisservice.ConnectionInfo) redisservice.Service {
	return c.service
}

type deadlineKeyService struct {
	redisservice.Service
	t *testing.T
}

func (s deadlineKeyService) GetKeyCount(ctx context.Context) (int64, error) {
	deadline, ok := ctx.Deadline()
	require.True(s.t, ok)
	remaining := time.Until(deadline)
	require.LessOrEqual(s.t, remaining, time.Second)
	require.Greater(s.t, remaining, 500*time.Millisecond, "metadata reads must not consume the network probe budget")
	return 7, nil
}

func TestKeyCountNetworkDeadlineStartsAfterMetadata(t *testing.T) {
	rr, objects := newReplicationFixture("10.0.0.10")
	kube := k8sfake.NewSimpleClientset(objects...)
	kube.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		time.Sleep(1100 * time.Millisecond)
		return false, nil, nil
	})
	c := &checker{k8s: kube, redis: deadlineKeyClient{service: deadlineKeyService{t: t}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	counts := c.GetReplicationKeyCounts(ctx, rr, []string{rr.Name + "-0"})
	require.Equal(t, int64(7), counts[rr.Name+"-0"])
}

func TestGetMasterFromReplicationSkipsUnreachableAndUnreadyPods(t *testing.T) {
	rr, objects := newReplicationFixture("10.0.0.10", "10.0.0.11")
	labels := map[string]string{"app": rr.GetStatefulSetName()}
	objects = append(objects, newLabeledRedisPod(rr.GetStatefulSetName()+"-2", labels, "10.0.0.12", corev1.PodRunning, false))
	redisClient := &fakeRedisClient{
		errByHost:      map[string]error{"10.0.0.10": errors.New("dial tcp 10.0.0.10:6379: i/o timeout")},
		isMasterByHost: map[string]bool{"10.0.0.11": true, "10.0.0.12": true},
		replicasByHost: map[string]int{"10.0.0.11": 1, "10.0.0.12": 1},
	}
	c := &checker{redis: redisClient, k8s: k8sfake.NewSimpleClientset(objects...)}

	master, err := c.GetMasterFromReplication(context.Background(), rr)
	require.NoError(t, err)

	assert.Equal(t, rr.GetStatefulSetName()+"-1", master.Name)
	assert.NotContains(t, redisClient.connectHosts, "10.0.0.12")
}

func TestGetMasterFromReplicationRejectsCompetingMasters(t *testing.T) {
	for _, counts := range []map[string]int{
		{"10.0.0.10": 2, "10.0.0.11": 0},
		{"10.0.0.10": 0, "10.0.0.11": 2},
		{"10.0.0.10": 1, "10.0.0.11": 1},
	} {
		rr, objects := newReplicationFixture("10.0.0.10", "10.0.0.11", "10.0.0.12")
		redisClient := &fakeRedisClient{
			isMasterByHost: map[string]bool{"10.0.0.10": true, "10.0.0.11": true},
			replicasByHost: counts,
		}
		c := &checker{redis: redisClient, k8s: k8sfake.NewSimpleClientset(objects...)}
		master, err := c.GetMasterFromReplication(context.Background(), rr)
		require.ErrorContains(t, err, "multiple Redis masters")
		require.Empty(t, master.Name, "replica count must not override a Sentinel election during transition")
	}
}

func TestGetReplicationKeyCountsSkipsUnprobeableAndFailedPods(t *testing.T) {
	rr, objects := newReplicationFixture("10.0.0.10", "10.0.0.11")
	labels := map[string]string{"app": rr.GetStatefulSetName()}
	objects = append(objects, newLabeledRedisPod(rr.GetStatefulSetName()+"-2", labels, "10.0.0.12", corev1.PodRunning, false))
	redisClient := &fakeRedisClient{
		keyCountByHost:    map[string]int64{"10.0.0.11": 120, "10.0.0.12": 120},
		keyCountErrByHost: map[string]error{"10.0.0.10": errors.New("dial tcp 10.0.0.10:6379: i/o timeout")},
	}
	c := &checker{redis: redisClient, k8s: k8sfake.NewSimpleClientset(objects...)}

	keyCounts := c.GetReplicationKeyCounts(context.Background(), rr, []string{
		rr.GetStatefulSetName() + "-0",
		rr.GetStatefulSetName() + "-1",
		rr.GetStatefulSetName() + "-2",
		rr.GetStatefulSetName() + "-3",
	})

	assert.Equal(t, map[string]int64{rr.GetStatefulSetName() + "-1": 120}, keyCounts)
	assert.NotContains(t, redisClient.connectHosts, "10.0.0.12")
}

func newReplicationFixture(podIPs ...string) (*rrvb2.RedisReplication, []runtime.Object) {
	rr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "example-replication", Namespace: "default"},
		Spec:       rrvb2.RedisReplicationSpec{Size: ptr.To(int32(len(podIPs)))},
	}
	labels := map[string]string{"app": rr.GetStatefulSetName()}
	objects := []runtime.Object{
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: rr.GetStatefulSetName(), Namespace: "default"},
			Spec: appsv1.StatefulSetSpec{
				Replicas: ptr.To(int32(len(podIPs))),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
			},
		},
	}
	for i, podIP := range podIPs {
		objects = append(objects, newLabeledRedisPod(fmt.Sprintf("%s-%d", rr.GetStatefulSetName(), i), labels, podIP, corev1.PodRunning, true))
	}
	return rr, objects
}
