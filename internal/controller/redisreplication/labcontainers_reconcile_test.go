package redisreplication

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/redissentinel"
	intctrlutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/controllerutil"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	redisservice "github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestLabcontainersLiveReconcileEntrypoint runs inside the labcontainers
// "operator" container network namespace against real redis-server and
// redis-sentinel containers.
func TestLabcontainersLiveReconcileEntrypoint(t *testing.T) {
	if os.Getenv("LABCONTAINERS_IN_NETNS") != "1" {
		t.Skip("set LABCONTAINERS_IN_NETNS=1 inside the labcontainers operator node")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	const (
		ns            = "spellsource"
		rrName        = "redis-master"
		rsName        = "redis-master-sentinel"
		masterGroup   = "myMaster"
		oldMasterIP   = "10.3.239.142"
		pod0IP        = "10.3.104.38"
		pod1IP        = "10.3.22.21"
		pod2NewIP     = "10.3.174.183"
		sentinel0IP   = "10.3.100.20"
		sentinel1IP   = "10.3.100.11"
		sentinel2IP   = "10.3.100.12"
		ghostSent1IP  = "10.3.100.10"
		ghostSent2IP  = "10.3.200.91"
	)

	// 1. Verify the live precondition deadlock across Redis and Sentinel containers.
	checkRedis := func(ip string) (k8sutils.ReplicationNodeInfo, int64, map[string]string) {
		c := rediscli.NewClient(&rediscli.Options{Addr: ip + ":6379", DialTimeout: 2 * time.Second})
		defer c.Close()
		raw, err := c.Info(ctx, "replication").Result()
		require.NoError(t, err, "INFO replication on %s", ip)
		info, err := k8sutils.ParseReplicationNodeInfo(raw)
		require.NoError(t, err)
		dbSize, err := c.DBSize(ctx).Result()
		require.NoError(t, err, "DBSIZE on %s", ip)
		vals := map[string]string{}
		for _, k := range []string{"spellsource:card:1", "spellsource:card:2", "spellsource:card:3"} {
			if v, getErr := c.Get(ctx, k).Result(); getErr == nil {
				vals[k] = v
			}
		}
		return info, dbSize, vals
	}

	info0, db0, vals0 := checkRedis(pod0IP)
	info1, db1, vals1 := checkRedis(pod1IP)
	info2, db2, _ := checkRedis(pod2NewIP)

	t.Logf("Pre-reconcile redis-master-0 (%s): role=%s master_host=%s link=%s offset=%d dbsize=%d",
		pod0IP, info0.Role, info0.MasterHost, info0.MasterLinkStatus, info0.EffectiveOffset(), db0)
	t.Logf("Pre-reconcile redis-master-1 (%s): role=%s master_host=%s link=%s offset=%d dbsize=%d",
		pod1IP, info1.Role, info1.MasterHost, info1.MasterLinkStatus, info1.EffectiveOffset(), db1)
	t.Logf("Pre-reconcile redis-master-2 (%s): role=%s offset=%d dbsize=%d",
		pod2NewIP, info2.Role, info2.EffectiveOffset(), db2)

	require.Equal(t, "slave", info0.Role)
	require.Equal(t, oldMasterIP, info0.MasterHost)
	require.Equal(t, "down", info0.MasterLinkStatus)
	require.Greater(t, info0.EffectiveOffset(), int64(0))
	require.GreaterOrEqual(t, db0, int64(2))
	require.Equal(t, "fireball", vals0["spellsource:card:1"])

	require.Equal(t, "slave", info1.Role)
	require.Equal(t, oldMasterIP, info1.MasterHost)
	require.Equal(t, "down", info1.MasterLinkStatus)
	require.Greater(t, info1.EffectiveOffset(), info0.EffectiveOffset(), "redis-master-1 has highest offset")
	require.Equal(t, int64(3), db1)
	require.Equal(t, "meteor", vals1["spellsource:card:3"])

	require.Equal(t, "master", info2.Role)
	require.Equal(t, int64(0), info2.EffectiveOffset())
	require.Equal(t, int64(0), db2)

	rClient := redisservice.NewClient()
	s0 := rClient.Connect(&redisservice.ConnectionInfo{Host: sentinel0IP, Port: "26379"})
	s1 := rClient.Connect(&redisservice.ConnectionInfo{Host: sentinel1IP, Port: "26379"})
	s2 := rClient.Connect(&redisservice.ConnectionInfo{Host: sentinel2IP, Port: "26379"})

	h0, _, mon0, err := s0.SentinelGetMasterAddrByName(ctx, masterGroup)
	require.NoError(t, err)
	require.True(t, mon0)
	require.Equal(t, "0.0.0.0", h0, "sentinel-0 must start stuck on placeholder 0.0.0.0")

	for name, s := range map[string]redisservice.Service{"sentinel-1": s1, "sentinel-2": s2} {
		h, _, mon, sErr := s.SentinelGetMasterAddrByName(ctx, masterGroup)
		require.NoError(t, sErr, name)
		require.True(t, mon, name)
		require.Equal(t, oldMasterIP, h, "%s must start stuck on dead master IP %s", name, oldMasterIP)
		peers, pErr := s.SentinelPeers(ctx, masterGroup)
		require.NoError(t, pErr, name)
		peerIPs := make([]string, 0, len(peers))
		for _, p := range peers {
			peerIPs = append(peerIPs, p.IP)
		}
		t.Logf("Pre-reconcile %s monitored=%s peerIPs=%v", name, h, peerIPs)
		require.Contains(t, peerIPs, ghostSent1IP, "%s retains dead sentinel-0 old IP", name)
		require.Contains(t, peerIPs, ghostSent2IP, "%s retains dead ghost sentinel IP", name)
	}

	// 2. Construct Kubernetes objects and run the fixed Reconcilers.
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	rr := &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:       rrName,
			Namespace:  ns,
			UID:        "rr-uid-1",
			Finalizers: []string{RedisReplicationFinalizer},
		},
		Spec: rrvb2.RedisReplicationSpec{
			Size: ptr.To(int32(3)),
			KubernetesConfig: commonapi.KubernetesConfig{
				Image: "quay.io/opstree/redis:v7.0.15",
			},
		},
		Status: rrvb2.RedisReplicationStatus{
			MasterNode: "redis-master-2",
		},
	}

	rs := &rsvb2.RedisSentinel{
		ObjectMeta: metav1.ObjectMeta{
			Name:       rsName,
			Namespace:  ns,
			UID:        "rs-uid-1",
			Finalizers: []string{redissentinel.RedisSentinelFinalizer},
		},
		Spec: rsvb2.RedisSentinelSpec{
			Size: ptr.To(int32(3)),
			KubernetesConfig: commonapi.KubernetesConfig{
				Image: "quay.io/opstree/redis-sentinel:v7.0.15",
			},
			RedisSentinelConfig: &rsvb2.RedisSentinelConfig{
				RedisSentinelConfig: commonapi.RedisSentinelConfig{
					RedisReplicationName: rrName,
					MasterGroupName:      masterGroup,
					SentinelConfig: commonapi.SentinelConfig{
						Quorum:                "2",
						DownAfterMilliseconds: "2000",
						FailoverTimeout:       "5000",
						ParallelSyncs:         "1",
					},
				},
			},
		},
	}

	redisLabels := common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil)
	sentinelLabels := common.GetRedisLabels(rs.GetStatefulSetName(), common.SetupTypeSentinel, "sentinel", nil)

	redisSTS := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: rrName, Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: redisLabels},
		},
		Status: appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3},
	}
	sentinelSTS := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: rs.GetStatefulSetName(), Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: sentinelLabels},
		},
		Status: appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3},
	}

	withRole := func(base map[string]string, role string) map[string]string {
		out := make(map[string]string, len(base)+1)
		for k, v := range base {
			out[k] = v
		}
		if role != "" {
			out[common.RedisRoleLabelKey] = role
		}
		return out
	}

	pod0 := newRunningPodForTest(ns, "redis-master-0", pod0IP, withRole(redisLabels, "slave"), true)
	pod1 := newRunningPodForTest(ns, "redis-master-1", pod1IP, withRole(redisLabels, "slave"), true)
	pod2 := newRunningPodForTest(ns, "redis-master-2", pod2NewIP, withRole(redisLabels, "master"), true)
	sent0 := newRunningPodForTest(ns, rs.GetStatefulSetName()+"-0", sentinel0IP, sentinelLabels, true)
	sent1 := newRunningPodForTest(ns, rs.GetStatefulSetName()+"-1", sentinel1IP, sentinelLabels, true)
	sent2 := newRunningPodForTest(ns, rs.GetStatefulSetName()+"-2", sentinel2IP, sentinelLabels, true)

	k8sClient := k8sfake.NewSimpleClientset(redisSTS, sentinelSTS, pod0, pod1, pod2, sent0, sent1, sent2)
	ctrlClient := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&rrvb2.RedisReplication{}, &rsvb2.RedisSentinel{}).
		WithObjects(rr, rs, redisSTS, sentinelSTS, pod0, pod1, pod2, sent0, sent1, sent2).
		Build()

	healer := redishealer.NewHealer(k8sClient)
	checker := redishealer.NewChecker(k8sClient)

	rrReconciler := &Reconciler{
		Client:      ctrlClient,
		StatefulSet: &fakeStatefulSetService{},
		Healer:      healer,
		Checker:     checker,
		K8sClient:   k8sClient,
	}
	rsReconciler := &redissentinel.RedisSentinelReconciler{
		Client:             ctrlClient,
		Checker:            checker,
		Healer:             healer,
		K8sClient:          k8sClient,
		ReplicationWatcher: intctrlutil.NewResourceWatcher(),
	}

	for pass := 1; pass <= 4; pass++ {
		currentRR := &rrvb2.RedisReplication{}
		require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, currentRR))
		_, err := rrReconciler.reconcileStatus(ctx, currentRR)
		require.NoError(t, err, "reconcileStatus pass %d", pass)

		require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, currentRR))
		_, err = rrReconciler.reconcileRedis(ctx, currentRR)
		require.NoError(t, err, "reconcileRedis pass %d", pass)

		_, err = rsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: rsName}})
		require.NoError(t, err, "RedisSentinel Reconcile pass %d", pass)

		time.Sleep(1500 * time.Millisecond)
	}

	// 3. Verify post-reconcile state across all Redis and Sentinel containers!
	info0After, db0After, vals0After := checkRedis(pod0IP)
	info1After, db1After, vals1After := checkRedis(pod1IP)
	info2After, db2After, vals2After := checkRedis(pod2NewIP)

	t.Logf("Post-reconcile redis-master-0 (%s): role=%s master_host=%s link=%s offset=%d dbsize=%d",
		pod0IP, info0After.Role, info0After.MasterHost, info0After.MasterLinkStatus, info0After.EffectiveOffset(), db0After)
	t.Logf("Post-reconcile redis-master-1 (%s): role=%s slaves=%d offset=%d dbsize=%d",
		pod1IP, info1After.Role, info1After.ConnectedSlaves, info1After.EffectiveOffset(), db1After)
	t.Logf("Post-reconcile redis-master-2 (%s): role=%s master_host=%s link=%s offset=%d dbsize=%d",
		pod2NewIP, info2After.Role, info2After.MasterHost, info2After.MasterLinkStatus, info2After.EffectiveOffset(), db2After)

	// Highest-offset surviving replica (redis-master-1) must be promoted to master!
	require.Equal(t, "master", info1After.Role, "highest-offset replica redis-master-1 must be promoted")
	require.Equal(t, 2, info1After.ConnectedSlaves, "both other pods must be attached to promoted master")
	require.Equal(t, int64(3), db1After)
	require.Equal(t, "meteor", vals1After["spellsource:card:3"])

	// Both redis-master-0 and the restarted redis-master-2 must be enslaved to redis-master-1 and have the full dataset!
	for name, st := range map[string]struct {
		info k8sutils.ReplicationNodeInfo
		db   int64
		vals map[string]string
	}{
		"redis-master-0": {info0After, db0After, vals0After},
		"redis-master-2": {info2After, db2After, vals2After},
	} {
		require.Equal(t, "slave", st.info.Role, "%s must be slave", name)
		require.Equal(t, pod1IP, st.info.MasterHost, "%s must replicate from promoted master %s", name, pod1IP)
		require.Equal(t, "up", st.info.MasterLinkStatus, "%s master_link_status must be up", name)
		require.Equal(t, int64(3), st.db, "%s must have all 3 keys replicated", name)
		require.Equal(t, "fireball", st.vals["spellsource:card:1"], name)
		require.Equal(t, "polymorph", st.vals["spellsource:card:2"], name)
		require.Equal(t, "meteor", st.vals["spellsource:card:3"], name)
	}

	// Verify Kubernetes pod labels and status.masterNode.
	updatedRR := &rrvb2.RedisReplication{}
	require.NoError(t, ctrlClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rrName}, updatedRR))
	require.Equal(t, "redis-master-1", updatedRR.Status.MasterNode)

	for podName, wantRole := range map[string]string{
		"redis-master-0": common.RedisRoleLabelSlave,
		"redis-master-1": common.RedisRoleLabelMaster,
		"redis-master-2": common.RedisRoleLabelSlave,
	} {
		p, pErr := k8sClient.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
		require.NoError(t, pErr)
		require.Equal(t, wantRole, p.Labels[common.RedisRoleLabelKey], "pod %s role label", podName)
	}

	// Wait briefly for the 3 Sentinels to exchange hello messages on the new master after reset.
	require.Eventually(t, func() bool {
		for _, ip := range []string{sentinel0IP, sentinel1IP, sentinel2IP} {
			svc := rClient.Connect(&redisservice.ConnectionInfo{Host: ip, Port: "26379"})
			h, _, mon, sErr := svc.SentinelGetMasterAddrByName(ctx, masterGroup)
			if sErr != nil || !mon || h != pod1IP {
				return false
			}
			info, iErr := svc.GetInfoSentinel(ctx)
			if iErr != nil {
				return false
			}
			m, ok := info.Master(masterGroup)
			if !ok || m.Status != "ok" || m.Slaves != 2 || m.Sentinels != 3 {
				return false
			}
			peers, pErr := svc.SentinelPeers(ctx, masterGroup)
			if pErr != nil || len(peers) != 2 {
				return false
			}
			for _, peer := range peers {
				if peer.IP == ghostSent1IP || peer.IP == ghostSent2IP || peer.DisconnectedOrDown() {
					return false
				}
			}
			sc := rediscli.NewClient(&rediscli.Options{Addr: ip + ":26379", DialTimeout: 2 * time.Second})
			ck, ckErr := sc.Do(ctx, "SENTINEL", "CKQUORUM", masterGroup).Text()
			sc.Close()
			if ckErr != nil || !strings.HasPrefix(ck, "OK ") {
				return false
			}
		}
		return true
	}, 12*time.Second, 500*time.Millisecond, "all 3 Sentinels must monitor promoted master %s with slaves=2, sentinels=3, zero ghost peers, and CKQUORUM OK", pod1IP)

	t.Log("SUCCESS: Live labcontainers deadlock reproduced and recovered end-to-end!")
}
