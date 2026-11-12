package redisreplication

import (
	"context"
	"errors"
	"fmt"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	placeholderGroup = "upgrade"
	placeholderAddr  = "0.0.0.0"
	deadSentinelIP1  = "10.0.9.1"
	deadSentinelIP2  = "10.0.9.2"
)

// sentinelOpinion is what one Sentinel pod answers.
type sentinelOpinion struct {
	redis.Service
	master      string
	known       int
	peers       []redis.SentinelPeerInfo
	unmonitored bool
	unreachable bool
	host        string
}

func (s sentinelOpinion) SentinelMyID(context.Context) (string, error) {
	if s.unreachable {
		return "", errors.New("i/o timeout")
	}
	return fmt.Sprintf("%040x", []byte(s.host)), nil
}

func (s sentinelOpinion) SentinelGetMasterAddrByName(context.Context, string) (string, string, bool, error) {
	if s.unmonitored {
		return "", "", false, nil
	}
	return s.master, "6379", true, nil
}

func (s sentinelOpinion) SentinelPeers(context.Context, string) ([]redis.SentinelPeerInfo, error) {
	return s.peers, nil
}

func (s sentinelOpinion) GetInfoSentinel(context.Context) (*redis.InfoSentinelResult, error) {
	return &redis.InfoSentinelResult{Masters: []redis.SentinelMasterInfo{{Name: placeholderGroup, Sentinels: s.known}}}, nil
}

type sentinelOpinions map[string]sentinelOpinion

func (c sentinelOpinions) Connect(info *redis.ConnectionInfo) redis.Service {
	opinion := c[info.Host]
	opinion.host = info.Host
	return opinion
}

// placeholderScenario is a three-pod replication watched by a three-member
// RedisSentinel. Its defaults are the state left behind when two Redis pods
// and two Sentinel pods are replaced while the master survives:
//
//   - pod 0 is a master holding the dataset, with no attached replicas,
//   - pods 1 and 2 are empty masters,
//   - Sentinel 0 monitors pod 0 and still lists both replaced Sentinels,
//   - Sentinels 1 and 2 monitor the unspecified-address placeholder.
type placeholderScenario struct {
	masters, slaves, unobserved []string
	counts                      map[string]int64
	infos                       map[string]k8sutils.ReplicationNodeInfo
	sentinels                   [3]sentinelOpinion
	lineageErr                  error
	recorded                    string
}

func podName(ordinal int) string { return fmt.Sprintf("example-replication-%d", ordinal) }
func podIP(ordinal int) string   { return fmt.Sprintf("10.0.0.%d", ordinal+1) }

func newPlaceholderScenario() *placeholderScenario {
	stalePeers := []redis.SentinelPeerInfo{
		{IP: deadSentinelIP1, Port: "26379", Flags: "s_down,sentinel,disconnected"},
		{IP: deadSentinelIP2, Port: "26379", Flags: "s_down,sentinel,disconnected"},
	}
	return &placeholderScenario{
		masters: []string{podName(0), podName(1), podName(2)},
		counts:  map[string]int64{podName(0): 4096, podName(1): 0, podName(2): 0},
		infos: map[string]k8sutils.ReplicationNodeInfo{
			podName(0): {Role: "master", ReplID: "a1", MasterReplOffset: 153000000},
			podName(1): {Role: "master", ReplID: "b2"},
			podName(2): {Role: "master", ReplID: "c3"},
		},
		sentinels: [3]sentinelOpinion{
			{master: podIP(0), known: 3, peers: stalePeers},
			{master: placeholderAddr, known: 1},
			{master: placeholderAddr, known: 1},
		},
		recorded: podName(2),
	}
}

type placeholderHarness struct {
	reconciler     *Reconciler
	instance       *rrvb2.RedisReplication
	client         client.Client
	healer         *recordingRoleHealer
	lineageChecked bool
	linked         []string
	linkedMaster   string
	promoted       string
}

func (s *placeholderScenario) build(t *testing.T) *placeholderHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	require.NoError(t, rsvb2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	rr := newReplicationInstanceForTest()
	rr.Status.MasterNode = s.recorded
	rs := &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: rr.Namespace}}
	rs.Spec.Size = ptr.To(int32(3))
	rs.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
	rs.Spec.RedisSentinelConfig.RedisReplicationName = rr.Name
	rs.Spec.RedisSentinelConfig.MasterGroupName = placeholderGroup

	redisLabels := common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil)
	sentinelLabels := common.GetRedisLabels(rs.GetStatefulSetName(), common.SetupTypeSentinel, "sentinel", nil)
	objects := []client.Object{rr, rs}
	opinions := sentinelOpinions{}
	for i := 0; i < 3; i++ {
		objects = append(objects, newRunningPodForTest(rr.Namespace, podName(i), podIP(i), redisLabels, true))
		sentinelIP := fmt.Sprintf("10.0.1.%d", i+1)
		objects = append(objects, newRunningPodForTest(rr.Namespace, fmt.Sprintf("%s-%d", rs.GetStatefulSetName(), i), sentinelIP, sentinelLabels, true))
		opinions[sentinelIP] = s.sentinels[i]
	}
	ctrlClient := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(rr).WithObjects(objects...).Build()
	instance := &rrvb2.RedisReplication{}
	require.NoError(t, ctrlClient.Get(context.Background(), client.ObjectKeyFromObject(rr), instance))

	h := &placeholderHarness{instance: instance, client: ctrlClient, healer: &recordingRoleHealer{}}
	h.reconciler = &Reconciler{
		Client:                   ctrlClient,
		StatefulSet:              &fakeStatefulSetService{},
		K8sClient:                k8sfake.NewSimpleClientset(),
		Healer:                   h.healer,
		Checker:                  keyCountsOf(s.counts),
		SentinelClient:           opinions,
		RedisReplicationTopology: topologyOf(s.masters, s.slaves, s.unobserved),
		ObserveReplicationInfo: func(_ context.Context, _ *rrvb2.RedisReplication, pods []string) map[string]k8sutils.ReplicationNodeInfo {
			observed := map[string]k8sutils.ReplicationNodeInfo{}
			for _, pod := range pods {
				if info, ok := s.infos[pod]; ok {
					observed[pod] = info
				}
			}
			return observed
		},
		RedisReplicationRealMaster: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, masters []string) string {
			for _, master := range masters {
				if s.infos[master].ConnectedSlaves > 0 {
					return master
				}
			}
			return ""
		},
		ValidateRecoveryLineage: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string, map[string]int64) error {
			h.lineageChecked = true
			return s.lineageErr
		},
		CreateRedisReplicationLink: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, pods []string, master string) error {
			h.linked, h.linkedMaster = pods, master
			return nil
		},
		PromoteRecoveryMaster: func(_ context.Context, _ kubernetes.Interface, _ *rrvb2.RedisReplication, _ []string, master string) error {
			h.promoted = master
			return nil
		},
	}
	return h
}

func (h *placeholderHarness) recordedMaster(t *testing.T) string {
	t.Helper()
	updated := &rrvb2.RedisReplication{}
	require.NoError(t, h.client.Get(context.Background(), client.ObjectKeyFromObject(h.instance), updated))
	return updated.Status.MasterNode
}

func TestUncontestedSentinelAddress(t *testing.T) {
	const real, other = "10.0.0.1:6379", "10.0.0.2:6379"
	const placeholder = "0.0.0.0:6379"
	for _, tc := range []struct {
		name    string
		answers []string
		want    string
	}{
		{"one configured Sentinel, the rest on the placeholder", []string{real, placeholder, placeholder}, real},
		{"one configured Sentinel, the rest monitor nothing", []string{"", real, ""}, real},
		{"two configured Sentinels that agree", []string{real, real, placeholder}, real},
		{"IPv6 placeholder", []string{real, "[::]:6379", placeholder}, real},
		{"two real addresses", []string{real, other, placeholder}, ""},
		{"a Sentinel that did not answer may hold another election", []string{real, placeholder}, ""},
		{"no Sentinel holds an opinion", []string{placeholder, "", placeholder}, ""},
		{"no answers", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, ok := uncontestedSentinelAddress(tc.answers, 3)
			require.Equal(t, tc.want, addr)
			require.Equal(t, tc.want != "", ok)
		})
	}
	_, ok := uncontestedSentinelAddress([]string{real}, 0)
	require.False(t, ok)
}

// The Sentinel authority decision for every state the placeholder rule must
// tell apart. wantMaster "" means recovery is refused.
func TestSentinelAuthorityWithPlaceholderSentinels(t *testing.T) {
	for _, tc := range []struct {
		name          string
		arrange       func(*placeholderScenario)
		wantMaster    string
		wantNoLineage bool
	}{
		{
			name:       "surviving data master, empty replacement masters, placeholder Sentinels",
			arrange:    func(*placeholderScenario) {},
			wantMaster: podName(0),
		},
		{
			name: "replacement Sentinels monitor nothing at all",
			arrange: func(s *placeholderScenario) {
				s.sentinels[1] = sentinelOpinion{unmonitored: true}
				s.sentinels[2] = sentinelOpinion{unmonitored: true}
			},
			wantMaster: podName(0),
		},
		{
			name: "an attached replica holding data on the same history",
			arrange: func(s *placeholderScenario) {
				s.masters, s.slaves = []string{podName(0), podName(1)}, []string{podName(2)}
				s.counts[podName(2)] = 4096
				s.infos[podName(0)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "a1", MasterReplOffset: 153000000, ConnectedSlaves: 1}
				s.infos[podName(2)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "a1", SlaveReplOffset: 153000000, MasterLinkStatus: "up"}
			},
			wantMaster: podName(0),
		},
		{
			name: "two masters both hold data",
			arrange: func(s *placeholderScenario) {
				s.counts[podName(1)] = 12
				s.infos[podName(1)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "b2", MasterReplOffset: 900}
			},
			wantNoLineage: true,
		},
		{
			name: "two masters hold data on the same replication ID",
			arrange: func(s *placeholderScenario) {
				s.counts[podName(1)] = 4096
				s.infos[podName(1)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "a1", MasterReplOffset: 152000000}
			},
			wantNoLineage: true,
		},
		{
			name: "another master has no keys but a replication offset",
			arrange: func(s *placeholderScenario) {
				s.infos[podName(1)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "b2", MasterReplOffset: 14}
			},
			wantNoLineage: true,
		},
		{
			name: "another master has an attached replica",
			arrange: func(s *placeholderScenario) {
				s.masters, s.slaves = []string{podName(0), podName(1)}, []string{podName(2)}
				s.infos[podName(1)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "b2", ConnectedSlaves: 1}
				s.infos[podName(2)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "b2"}
			},
			wantNoLineage: true,
		},
		{
			name:          "a key count is missing",
			arrange:       func(s *placeholderScenario) { delete(s.counts, podName(2)) },
			wantNoLineage: true,
		},
		{
			name:          "a replication state is missing",
			arrange:       func(s *placeholderScenario) { delete(s.infos, podName(2)) },
			wantNoLineage: true,
		},
		{
			name: "a Redis pod is unobserved",
			arrange: func(s *placeholderScenario) {
				s.masters, s.unobserved = []string{podName(0), podName(1)}, []string{podName(2)}
			},
			wantNoLineage: true,
		},
		{
			name: "the monitored pod is not serving as master",
			arrange: func(s *placeholderScenario) {
				s.masters, s.slaves = []string{podName(1), podName(2)}, []string{podName(0)}
				s.infos[podName(0)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "a1", SlaveReplOffset: 153000000, MasterLinkStatus: "down"}
			},
			wantNoLineage: true,
		},
		{
			name: "a replica holds data on a different replication ID",
			arrange: func(s *placeholderScenario) {
				s.masters, s.slaves = []string{podName(0), podName(1)}, []string{podName(2)}
				s.counts[podName(2)] = 4100
				s.infos[podName(2)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "d4", SlaveReplOffset: 153000900, MasterLinkStatus: "down"}
				s.lineageErr = errors.New("ambiguous recovery history")
			},
		},
		{
			name: "Sentinels disagree on two real addresses",
			arrange: func(s *placeholderScenario) {
				s.sentinels[1] = sentinelOpinion{master: podIP(1), known: 1}
			},
			wantNoLineage: true,
		},
		{
			name: "a Sentinel does not answer",
			arrange: func(s *placeholderScenario) {
				s.sentinels[2] = sentinelOpinion{unreachable: true}
			},
			wantNoLineage: true,
		},
		{
			name: "the monitored address belongs to no live pod",
			arrange: func(s *placeholderScenario) {
				s.sentinels[0].master = "10.0.8.8"
			},
			wantNoLineage: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scenario := newPlaceholderScenario()
			tc.arrange(scenario)
			h := scenario.build(t)
			master, authoritative := h.reconciler.sentinelElectedMaster(context.Background(), h.instance)
			require.True(t, authoritative, "configured Sentinels must keep blocking fallback election")
			require.Equal(t, tc.wantMaster, master)
			if tc.wantNoLineage {
				require.False(t, h.lineageChecked, "refused before comparing replication histories")
			}
		})
	}
}

func TestReconcileRedisReattachesEmptyMastersToSurvivingDataMaster(t *testing.T) {
	h := newPlaceholderScenario().build(t)
	_, err := h.reconciler.reconcileRedis(context.Background(), h.instance)
	require.NoError(t, err)
	require.Equal(t, podName(0), h.linkedMaster)
	require.ElementsMatch(t, []string{podName(0), podName(1), podName(2)}, h.linked)
	require.Empty(t, h.promoted, "the surviving master must not be re-promoted")
}

func TestReconcileRedisKeepsRefusingAmbiguousPlaceholderStates(t *testing.T) {
	for name, arrange := range map[string]func(*placeholderScenario){
		"two masters both hold data": func(s *placeholderScenario) {
			s.counts[podName(1)] = 12
			s.infos[podName(1)] = k8sutils.ReplicationNodeInfo{Role: "master", ReplID: "b2", MasterReplOffset: 900}
		},
		"a key count is missing": func(s *placeholderScenario) { delete(s.counts, podName(2)) },
		"a Sentinel does not answer": func(s *placeholderScenario) {
			s.sentinels[2] = sentinelOpinion{unreachable: true}
		},
		"Sentinels disagree on two real addresses": func(s *placeholderScenario) {
			s.sentinels[1] = sentinelOpinion{master: podIP(1), known: 1}
		},
		"a replica holds data on a different replication ID": func(s *placeholderScenario) {
			s.masters, s.slaves = []string{podName(0), podName(1)}, []string{podName(2)}
			s.counts[podName(2)] = 4100
			s.infos[podName(2)] = k8sutils.ReplicationNodeInfo{Role: "slave", ReplID: "d4", SlaveReplOffset: 153000900, MasterLinkStatus: "down"}
			s.lineageErr = errors.New("ambiguous recovery history")
		},
	} {
		t.Run(name, func(t *testing.T) {
			scenario := newPlaceholderScenario()
			arrange(scenario)
			h := scenario.build(t)
			result, err := h.reconciler.reconcileRedis(context.Background(), h.instance)
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			require.Empty(t, h.linkedMaster, "no pod may be reattached")
			require.Empty(t, h.promoted, "no pod may be promoted")
		})
	}
}

func TestReconcileStatusRecordsSurvivingDataMaster(t *testing.T) {
	h := newPlaceholderScenario().build(t)
	_, err := h.reconciler.reconcileStatus(context.Background(), h.instance)
	require.NoError(t, err)
	require.Equal(t, podName(0), h.recordedMaster(t))
	require.NotNil(t, h.healer.published)
	require.Equal(t, podName(0), *h.healer.published, "the data-holding master must enter the writable Service")
}
