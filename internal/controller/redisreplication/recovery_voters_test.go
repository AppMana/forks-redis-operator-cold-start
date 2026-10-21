package redisreplication

import (
	"context"
	"fmt"
	"testing"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type electorateService struct {
	redis.Service
	master string
	known  int
	id     string
}

func (s electorateService) SentinelMyID(context.Context) (string, error) { return s.id, nil }

func (s electorateService) SentinelGetMasterAddrByName(context.Context, string) (string, string, bool, error) {
	return s.master, "6379", true, nil
}

func (s electorateService) GetInfoSentinel(context.Context) (*redis.InfoSentinelResult, error) {
	return &redis.InfoSentinelResult{Masters: []redis.SentinelMasterInfo{{Name: "upgrade", Sentinels: s.known}}}, nil
}

type electorateClient map[string]electorateService

func (c electorateClient) Connect(info *redis.ConnectionInfo) redis.Service {
	s := c[info.Host]
	if s.id == "" {
		s.id = fmt.Sprintf("%040x", []byte(info.Host))
	}
	return s
}

func TestRecoveryVotersParticipateWithoutGrantingManagedMinorityAuthority(t *testing.T) {
	for _, tc := range []struct{ associated, duplicate bool }{{false, false}, {true, false}, {true, true}} {
		associated := tc.associated
		t.Run(fmt.Sprintf("associated=%v/duplicate=%v", associated, tc.duplicate), func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, rrvb2.AddToScheme(scheme))
			require.NoError(t, rsvb2.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			rr := newReplicationInstanceForTest()
			rs := &rsvb2.RedisSentinel{ObjectMeta: metav1.ObjectMeta{Name: "upgrade", Namespace: rr.Namespace}}
			rs.Spec.Size = ptr.To(int32(3))
			rs.Spec.RedisSentinelConfig = &rsvb2.RedisSentinelConfig{}
			rs.Spec.RedisSentinelConfig.RedisReplicationName = rr.Name
			rs.Spec.RedisSentinelConfig.MasterGroupName = "upgrade"
			objects := []client.Object{rr, rs}
			for i := 0; i < 2; i++ {
				objects = append(objects, newRunningPodForTest(rr.Namespace, fmt.Sprintf("%s-%d", rr.Name, i), fmt.Sprintf("10.0.0.%d", i+1), common.GetRedisLabels(rr.Name, common.SetupTypeReplication, "replication", nil), true))
			}
			probes := electorateClient{}
			for i := 0; i < 7; i++ {
				labels := map[string]string{"app": "recovery"}
				if i < 3 {
					labels = common.GetRedisLabels(rs.GetStatefulSetName(), common.SetupTypeSentinel, "sentinel", nil)
				}
				if associated {
					// Also tag managed pods: they must not be counted twice.
					labels[RecoverySentinelOwnerLabel] = rs.GetStatefulSetName()
				}
				ip := fmt.Sprintf("10.0.1.%d", i+1)
				objects = append(objects, newRunningPodForTest(rr.Namespace, fmt.Sprintf("voter-%d", i), ip, labels, true))
				master := "10.0.0.1"
				if i >= 3 {
					master = "10.0.0.2"
				}
				probes[ip] = electorateService{master: master, known: 7}
				if tc.duplicate && i >= 3 {
					probes[ip] = electorateService{master: master, known: 7, id: fmt.Sprintf("%040d", 42)}
				}
			}
			// Same association in another namespace must never join this vote.
			objects = append(objects, newRunningPodForTest("other", "foreign", "10.0.2.1", map[string]string{RecoverySentinelOwnerLabel: rs.GetStatefulSetName()}, true))
			r := &Reconciler{Client: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(), K8sClient: k8sfake.NewSimpleClientset(), SentinelClient: probes}
			groups, err := listSentinelGroups(context.Background(), r.Client, rr)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			master, authoritative := r.sentinelElectedMaster(context.Background(), rr)
			require.True(t, authoritative)
			if associated {
				require.EqualValues(t, 7, groups[0].size)
				require.Len(t, groups[0].pods, 7)
				if tc.duplicate {
					require.Empty(t, master, "cloned Sentinel identities are not independent election voters")
				} else {
					require.Equal(t, rr.Name+"-1", master)
				}
			} else {
				require.Empty(t, master, "three managed votes cannot override a seven-member electorate")
			}
		})
	}
}
