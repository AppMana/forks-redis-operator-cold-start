package redisreplication

import (
	"context"
	"sync"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	rediscli "github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// replicationForPod wakes the replication whose Redis pod changed. A drain
// sets the old primary's deletionTimestamp, which must take it out of the
// master Service at once rather than at the next resync.
func replicationForPod(_ context.Context, object client.Object) []ctrl.Request {
	labels := object.GetLabels()
	if labels["redis_setup_type"] != string(common.SetupTypeReplication) || labels["role"] != "replication" || labels["app"] == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: labels["app"]}}}
}

// sentinelEventChannels are the Sentinel announcements after which the master
// Service may have to move: a completed failover, a monitor (re)pointed by the
// RedisSentinel controller, and an old primary reconfigured as a replica.
var sentinelEventChannels = []string{"+switch-master", "+monitor", "+convert-to-slave"}

const (
	sentinelEventsResync      = 5 * time.Second
	sentinelEventsDialTimeout = 2 * time.Second
)

// SentinelEvents subscribes to every Sentinel that monitors a RedisReplication
// and enqueues that replication when the Sentinel announces a topology change,
// so the master Service follows a failover as soon as Sentinel completes it.
type SentinelEvents struct {
	client    client.Client
	k8sClient kubernetes.Interface
	events    chan event.GenericEvent
	subscribe func(ctx context.Context, info *redis.ConnectionInfo, notify func())

	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func NewSentinelEvents(c client.Client, k8sClient kubernetes.Interface) *SentinelEvents {
	return &SentinelEvents{
		client:    c,
		k8sClient: k8sClient,
		events:    make(chan event.GenericEvent, 64),
		subscribe: subscribeSentinel,
		active:    map[string]context.CancelFunc{},
	}
}

// Events is the source of replications to reconcile.
func (s *SentinelEvents) Events() <-chan event.GenericEvent {
	return s.events
}

// Start keeps one subscription per live Sentinel pod until ctx ends. It runs
// only on the elected leader, like the reconcilers it feeds.
func (s *SentinelEvents) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("sentinel-events")
	ticker := time.NewTicker(sentinelEventsResync)
	defer ticker.Stop()
	for {
		if err := s.sync(ctx); err != nil {
			logger.Error(err, "refresh Sentinel subscriptions")
		}
		select {
		case <-ctx.Done():
			s.mu.Lock()
			for key, cancel := range s.active {
				cancel()
				delete(s.active, key)
			}
			s.mu.Unlock()
			return nil
		case <-ticker.C:
		}
	}
}

func (s *SentinelEvents) sync(ctx context.Context) error {
	var replications rrvb2.RedisReplicationList
	if err := s.client.List(ctx, &replications); err != nil {
		return err
	}
	type target struct {
		replication types.NamespacedName
		info        *redis.ConnectionInfo
	}
	wanted := map[string]target{}
	checker := redishealer.NewChecker(s.k8sClient)
	for i := range replications.Items {
		rr := &replications.Items[i]
		groups, err := listSentinelGroups(ctx, s.client, rr)
		if err != nil {
			return err
		}
		for _, group := range groups {
			password, err := checker.GetPassword(ctx, rr.Namespace, group.password)
			if err != nil {
				log.FromContext(ctx).Error(err, "get Sentinel password", "replication", rr.Name, "masterGroup", group.masterGroup)
				continue
			}
			for _, pod := range group.pods {
				if pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
					continue
				}
				info := redishealer.ConnectionInfoForPod(ctx, pod, password, group.tls, s.k8sClient, rr.Namespace, "26379")
				key := rr.Namespace + "/" + rr.Name + "/" + string(pod.UID) + "/" + info.GetAddress()
				wanted[key] = target{replication: client.ObjectKeyFromObject(rr), info: info}
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, cancel := range s.active {
		if _, ok := wanted[key]; !ok {
			cancel()
			delete(s.active, key)
		}
	}
	for key, t := range wanted {
		if _, ok := s.active[key]; ok {
			continue
		}
		subCtx, cancel := context.WithCancel(ctx)
		s.active[key] = cancel
		replication := t.replication
		go s.subscribe(subCtx, t.info, func() {
			select {
			case s.events <- event.GenericEvent{Object: &rrvb2.RedisReplication{ObjectMeta: metav1.ObjectMeta{Namespace: replication.Namespace, Name: replication.Name}}}:
			case <-subCtx.Done():
			}
		})
	}
	return nil
}

// subscribeSentinel listens for sentinelEventChannels until ctx ends. go-redis
// re-establishes the subscription after connection errors.
func subscribeSentinel(ctx context.Context, info *redis.ConnectionInfo, notify func()) {
	c := rediscli.NewClient(&rediscli.Options{
		Addr:        info.GetAddress(),
		Password:    info.Password,
		TLSConfig:   info.TLSConfig,
		DialTimeout: sentinelEventsDialTimeout,
	})
	defer c.Close()
	pubsub := c.Subscribe(ctx, sentinelEventChannels...)
	defer pubsub.Close()
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			log.FromContext(ctx).Info("Sentinel announced topology change", "sentinel", info.GetAddress(), "event", msg.Channel, "payload", msg.Payload)
			notify()
		}
	}
}
