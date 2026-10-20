package redisreplication

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	redishealer "github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/redis"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/service"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common/statefulset"
	intctrlutil "github.com/OT-CONTAINER-KIT/redis-operator/internal/controllerutil"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/envs"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/monitoring"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/service/redis"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	RedisReplicationFinalizer = "redisReplicationFinalizer"
	masterGroupName           = "mymaster"
)

// Reconciler reconciles a RedisReplication object
type Reconciler struct {
	client.Client
	k8sutils.StatefulSet
	Healer                     redishealer.Healer
	Checker                    redishealer.Checker
	K8sClient                  kubernetes.Interface
	RedisReplicationTopology   func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error)
	RedisReplicationRealMaster func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string
	CreateRedisReplicationLink func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error
	ConfigureSentinel          func(context.Context, *rrvb2.RedisReplication, string) error
	SentinelMonitoredMaster    func(context.Context, *rrvb2.RedisReplication, []string) (string, error)
	ValidateRecoveryLineage    func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string, map[string]int64) error
	PromoteRecoveryMaster      func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string, string) error

	// Test seams; nil uses the live Redis and Sentinel probes.
	ObserveRoles          func(ctx context.Context, instance *rrvb2.RedisReplication) (map[string]string, error)
	RealMaster            func(ctx context.Context, instance *rrvb2.RedisReplication, masters []string) string
	SentinelElectedMaster func(ctx context.Context, instance *rrvb2.RedisReplication) (string, bool)
}

func (r *Reconciler) observeRoles(ctx context.Context, instance *rrvb2.RedisReplication) (map[string]string, error) {
	if r.ObserveRoles != nil {
		return r.ObserveRoles(ctx, instance)
	}
	if r.RedisReplicationTopology != nil {
		topology, err := r.redisReplicationTopology(ctx, instance)
		roles := map[string]string{}
		for _, pod := range topology.Masters {
			roles[pod] = "master"
		}
		for _, pod := range topology.Slaves {
			roles[pod] = "slave"
		}
		return roles, err
	}
	return k8sutils.ObserveRedisReplicationRoles(ctx, r.K8sClient, instance)
}

func (r *Reconciler) realMaster(ctx context.Context, instance *rrvb2.RedisReplication, masters []string) string {
	if r.RealMaster != nil {
		return r.RealMaster(ctx, instance, masters)
	}
	return r.redisReplicationRealMaster(ctx, instance, masters)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &rrvb2.RedisReplication{}

	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		return intctrlutil.RequeueECheck(ctx, err, "failed to get RedisReplication instance")
	}

	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisReplicationFinalizer(ctx, r.Client, instance, RedisReplicationFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}

	if common.ShouldSkipReconcile(ctx, instance) {
		return intctrlutil.Reconciled()
	}

	reconcilers := []reconciler{
		{typ: "finalizer", rec: r.reconcileFinalizer},
		// Publish the elected master before anything that can wait or fail:
		// resources and topology repair both back off while pods are missing,
		// which is exactly when the master Service has to move.
		{typ: "status", rec: r.reconcileStatus},
		{typ: "resources", rec: r.reconcileResources},
		{typ: "redis", rec: r.reconcileRedis},
	}

	requeueAfter := 30 * time.Second
	for _, reconciler := range reconcilers {
		result, err := reconciler.rec(ctx, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		if result.RequeueAfter > 0 && result.RequeueAfter < requeueAfter {
			requeueAfter = result.RequeueAfter
		}
		if result.Requeue {
			if result.RequeueAfter == 0 {
				return result, nil
			}
			return intctrlutil.RequeueAfter(ctx, requeueAfter, "")
		}
	}

	return intctrlutil.RequeueAfter(ctx, requeueAfter, "")
}

// UpdateRedisReplicationMaster records masterNode as the master of the
// replication. An empty masterNode means no master was identified in this
// reconcile, which the RedisReplicationHasMaster gauge reports either way.
//
// Whether the recorded master is then cleared depends on why none was
// identified. masterAbsent says every pod was observed and none of them is a
// master: that is conclusive, and a recorded master that is provably not the
// master must not feed the bootstrap election or the sentinel controller's
// fallback, so it is cleared. Anything else - pods that could not be probed, or
// masters that could not be told apart - is not evidence that the recorded
// master changed, so the last known master is kept for those fallbacks.
func (r *Reconciler) UpdateRedisReplicationMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterNode string, masterAbsent bool) error {
	if masterNode == "" {
		monitoring.RedisReplicationHasMaster.WithLabelValues(instance.Namespace, instance.Name).Set(0)
	} else {
		monitoring.RedisReplicationHasMaster.WithLabelValues(instance.Namespace, instance.Name).Set(1)
	}

	if masterNode == "" && instance.Status.MasterNode != "" {
		if masterAbsent {
			log.FromContext(ctx).Info("Every pod answered and none is a master, clearing the recorded master node",
				"statusMasterNode", instance.Status.MasterNode)
		} else {
			log.FromContext(ctx).Info("No master identified in a partial or ambiguous view, keeping the last known master node",
				"statusMasterNode", instance.Status.MasterNode)
			masterNode = instance.Status.MasterNode
		}
	}

	connectionInfo := instance.GetConnectionInfo(envs.GetServiceDNSDomain())

	if instance.Status.MasterNode == masterNode && connectionInfoEqual(instance.Status.ConnectionInfo, connectionInfo) {
		return nil
	}

	if instance.Status.MasterNode != masterNode {
		monitoring.RedisReplicationMasterRoleChangesTotal.WithLabelValues(instance.Namespace, instance.Name).Inc()
		logger := log.FromContext(ctx)
		logger.Info("Updating master node",
			"previous", instance.Status.MasterNode,
			"new", masterNode)
	}
	return r.updateStatus(ctx, instance, rrvb2.RedisReplicationStatus{
		MasterNode:     masterNode,
		ConnectionInfo: connectionInfo,
	})
}

func connectionInfoEqual(a, b *rrvb2.ConnectionInfo) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Host == b.Host && a.Port == b.Port && a.MasterName == b.MasterName
}

func (r *Reconciler) redisReplicationTopology(ctx context.Context, instance *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
	if r.RedisReplicationTopology != nil {
		return r.RedisReplicationTopology(ctx, r.K8sClient, instance)
	}
	return k8sutils.GetRedisReplicationTopology(ctx, r.K8sClient, instance)
}

func (r *Reconciler) redisReplicationRealMaster(ctx context.Context, instance *rrvb2.RedisReplication, masterPods []string) string {
	if r.RedisReplicationRealMaster != nil {
		return r.RedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
	}
	return k8sutils.GetRedisReplicationRealMaster(ctx, r.K8sClient, instance, masterPods)
}

func (r *Reconciler) ensureMasterHoldsData(ctx context.Context, instance *rrvb2.RedisReplication, pods []string, realMaster string) error {
	if r.Checker == nil {
		return fmt.Errorf("no checker configured, cannot verify that master %s holds data", realMaster)
	}
	keyCounts := r.Checker.GetReplicationKeyCounts(ctx, instance, pods)
	masterKeys, ok := keyCounts[realMaster]
	if !ok {
		return fmt.Errorf("key count of master %s could not be read", realMaster)
	}
	if masterKeys > 0 {
		return nil
	}
	var podsHoldingData, podsUnknown []string
	for _, pod := range pods {
		if pod == realMaster {
			continue
		}
		keys, ok := keyCounts[pod]
		switch {
		case !ok:
			podsUnknown = append(podsUnknown, pod)
		case keys > 0:
			podsHoldingData = append(podsHoldingData, pod)
		}
	}
	if len(podsHoldingData) == 0 && len(podsUnknown) == 0 {
		return nil
	}
	return fmt.Errorf("master %s holds no keys while pods %v hold data and %v could not be probed, refusing to replicate from it; "+
		"to recover, run REPLICAOF NO ONE on a pod that holds data, then REPLICAOF <its address> 6379 on %s, and the operator will attach the rest",
		realMaster, podsHoldingData, podsUnknown, realMaster)
}

func (r *Reconciler) createRedisReplicationLink(ctx context.Context, instance *rrvb2.RedisReplication, pods []string, realMaster string) error {
	if err := r.ensureMasterHoldsData(ctx, instance, pods, realMaster); err != nil {
		return err
	}
	if r.CreateRedisReplicationLink != nil {
		return r.CreateRedisReplicationLink(ctx, r.K8sClient, instance, pods, realMaster)
	}
	return k8sutils.CreateMasterSlaveReplication(ctx, r.K8sClient, instance, pods, realMaster)
}

func (r *Reconciler) configureReplicationSentinel(ctx context.Context, instance *rrvb2.RedisReplication, masterPodName string) error {
	if r.ConfigureSentinel != nil {
		return r.ConfigureSentinel(ctx, instance, masterPodName)
	}
	return r.configureSentinel(ctx, instance, masterPodName)
}

// Read PR description for context: https://github.com/OT-CONTAINER-KIT/redis-operator/pull/1843
func (r *Reconciler) observedRedisReplicationMaster(ctx context.Context, instance *rrvb2.RedisReplication, topology k8sutils.RedisReplicationTopology) (string, bool) {
	switch len(topology.Masters) {
	case 0:
		return "", false
	case 1:
		candidate := topology.Masters[0]
		if topology.Complete() {
			return candidate, true
		}
		if r.redisReplicationRealMaster(ctx, instance, topology.Masters) == candidate {
			return candidate, true
		}
		if instance.EnableSentinel() {
			monitored, err := r.sentinelMonitoredMaster(ctx, instance, topology.Masters)
			if err != nil {
				log.FromContext(ctx).Error(err, "Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and sentinel could not be asked",
					"candidate", candidate,
					"unobservedPods", topology.Unobserved)
				return "", false
			}
			if monitored == candidate {
				return candidate, true
			}
			log.FromContext(ctx).Info("Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and sentinel does not report it as master",
				"candidate", candidate,
				"statusMasterNode", instance.Status.MasterNode,
				"unobservedPods", topology.Unobserved)
			return "", false
		}
		if candidate == instance.Status.MasterNode {
			return candidate, true
		}
		log.FromContext(ctx).Info("Not trusting the only observed master: the topology is incomplete, the pod has no attached replicas and it is not the recorded master",
			"candidate", candidate,
			"statusMasterNode", instance.Status.MasterNode,
			"unobservedPods", topology.Unobserved)
		return "", false
	default:
		realMaster := r.redisReplicationRealMaster(ctx, instance, topology.Masters)
		if realMaster == "" && instance.EnableSentinel() {
			monitored, err := r.sentinelMonitoredMaster(ctx, instance, topology.Masters)
			if err != nil {
				log.FromContext(ctx).Error(err, "Could not ask sentinel which of the observed masters it monitors",
					"masters", topology.Masters)
			} else if monitored != "" {
				log.FromContext(ctx).Info("No master with attached replicas found, using the master monitored by sentinel",
					"master", monitored)
				realMaster = monitored
			}
		}
		return realMaster, realMaster != ""
	}
}

type reconciler struct {
	typ string
	rec func(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error)
}

func (r *Reconciler) reconcileFinalizer(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if k8sutils.IsDeleted(instance) {
		if err := k8sutils.HandleRedisReplicationFinalizer(ctx, r.Client, instance, RedisReplicationFinalizer); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		return intctrlutil.Reconciled()
	}
	if err := k8sutils.AddFinalizer(ctx, instance, RedisReplicationFinalizer, r.Client); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	return intctrlutil.Reconciled()
}

func (r *Reconciler) reconcileResources(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if err := k8sutils.CreateReplicationRedis(ctx, instance, r.K8sClient, r.Client); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if err := k8sutils.CreateReplicationService(ctx, instance, r.K8sClient); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if err := k8sutils.ReconcileReplicationPodDisruptionBudget(ctx, instance, instance.Spec.PodDisruptionBudget, r.K8sClient); err != nil {
		return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
	}
	if instance.EnableSentinel() {
		svc := newSentinelService(instance)
		_, err := service.Reconcile(ctx, r.Client, svc, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
		sts := newSentinelStatefulSet(instance, svc.Name)
		_, err = statefulset.Reconcile(ctx, r.Client, sts, instance)
		if err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
	}
	return intctrlutil.Reconciled()
}

func (r *Reconciler) configureSentinel(ctx context.Context, inst *rrvb2.RedisReplication, masterPodName string) error {
	if masterPodName == "" {
		return nil
	}
	masterPod, err := r.K8sClient.CoreV1().Pods(inst.Namespace).Get(ctx, masterPodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get master pod: %w", err)
	}
	var monitorAddr string
	if inst.Spec.Sentinel.ResolveHostnames == "yes" {
		monitorAddr = replicationPodHostname(inst, masterPodName)
	} else {
		monitorAddr = masterPod.Status.PodIP
	}

	if monitorAddr == "" {
		return fmt.Errorf("master pod IP not ready")
	}

	masterPassword, err := r.masterPassword(ctx, inst)
	if err != nil {
		return fmt.Errorf("get master password secret: %w", err)
	}

	sentinelPods, err := r.getSentinelPods(ctx, inst)
	if err != nil {
		return fmt.Errorf("get sentinel pods: %w", err)
	}

	if len(sentinelPods.Items) == 0 {
		return nil
	}

	redisClient := redis.NewClient()
	for _, pod := range sentinelPods.Items {
		if err := r.configureSentinelPod(ctx, redisClient, inst, pod, monitorAddr, masterPassword); err != nil {
			log.FromContext(ctx).Error(err, "failed to configure sentinel pod", "pod", pod.Name)
			continue
		}
	}

	return nil
}

func (r *Reconciler) getSentinelPods(ctx context.Context, inst *rrvb2.RedisReplication) (*corev1.PodList, error) {
	labels := common.GetRedisLabels(
		inst.SentinelStatefulSet(),
		common.SetupTypeSentinel,
		"sentinel",
		inst.GetLabels(),
	)

	var selector []string
	for k, v := range labels {
		selector = append(selector, fmt.Sprintf("%s=%s", k, v))
	}

	return r.K8sClient.CoreV1().Pods(inst.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: strings.Join(selector, ","),
	})
}

func (r *Reconciler) configureSentinelPod(
	ctx context.Context,
	redisClient redis.Client,
	inst *rrvb2.RedisReplication,
	sentinelPod corev1.Pod,
	masterAddr string,
	masterPassword string,
) error {
	sentinelPassword, err := r.sentinelPassword(ctx, inst)
	if err != nil {
		return err
	}

	sentinelConnInfo := &redis.ConnectionInfo{
		Host:     sentinelPod.Status.PodIP,
		Port:     "26379",
		Password: sentinelPassword,
	}

	sentinelService := redisClient.Connect(sentinelConnInfo)

	masterConnInfo := &redis.ConnectionInfo{
		Host:     masterAddr,
		Port:     "6379",
		Password: masterPassword,
	}

	quorum := int(inst.Spec.Sentinel.Size/2) + 1
	if err := sentinelService.SentinelMonitor(
		ctx,
		masterConnInfo,
		masterGroupName,
		fmt.Sprintf("%d", quorum),
	); err != nil {
		return err
	}

	for k, v := range map[string]string{
		"down-after-milliseconds": inst.Spec.Sentinel.DownAfterMilliseconds,
		"parallel-syncs":          inst.Spec.Sentinel.ParallelSyncs,
		"failover-timeout":        inst.Spec.Sentinel.FailoverTimeout,
	} {
		if v == "" {
			continue
		}
		if err := sentinelService.SentinelSet(ctx, masterGroupName, k, v); err != nil {
			return err
		}
	}

	if err := r.sentinelResetIfNeed(ctx, inst, sentinelService); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) sentinelResetIfNeed(ctx context.Context, inst *rrvb2.RedisReplication, redisService redis.Service) error {
	logger := log.FromContext(ctx)

	sentinelInfo, err := redisService.GetInfoSentinel(ctx)
	if err != nil {
		return fmt.Errorf("get sentinel info: %w", err)
	}

	masterInfo, ok := sentinelInfo.Master(masterGroupName)
	if !ok {
		return fmt.Errorf("master group %s not found in sentinel info", masterGroupName)
	}

	expectedSlaves := int(inst.Spec.GetReplicationCounts("replication")) - 1
	expectedSentinels := int(inst.Spec.Sentinel.Size)

	if !masterInfo.HasStaleEntries(expectedSlaves, expectedSentinels) {
		return nil
	}
	logger.Info("Sentinel has stale topology entries, reset needed",
		"expectedSlaves", expectedSlaves,
		"slaves", masterInfo.Slaves,
		"expectedSentinels", expectedSentinels,
		"sentinels", masterInfo.Sentinels)
	if err := redisService.SentinelReset(ctx, masterGroupName); err != nil {
		return fmt.Errorf("reset sentinel: %w", err)
	}

	return nil
}

func (r *Reconciler) reconcileRedis(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	if instance.EnableSentinel() {
		if !r.IsStatefulSetReady(ctx, instance.Namespace, instance.SentinelStatefulSet()) {
			return intctrlutil.RequeueAfter(ctx, time.Second*30, "waiting for sentinel statefulset to be ready")
		}
		if !r.IsStatefulSetReady(ctx, instance.Namespace, instance.RedisStatefulSet()) {
			return intctrlutil.RequeueAfter(ctx, time.Second*30, "waiting for redis statefulset to be ready")
		}
	}

	if len(instance.Spec.GetRedisDynamicConfig()) > 0 && r.IsStatefulSetReady(ctx, instance.Namespace, instance.RedisStatefulSet()) {
		if err := k8sutils.SetRedisReplicationDynamicConfig(ctx, r.K8sClient, instance); err != nil {
			return intctrlutil.RequeueE(ctx, err, "failed to set dynamic config")
		}
	}

	topology, err := r.redisReplicationTopology(ctx, instance)
	if err != nil {
		// Missing replicas are an expected degraded state. Keep observing
		// elected roles every 30s instead of exponential error backoff.
		log.FromContext(ctx).Error(err, "topology observation incomplete; keeping current replication")
		return intctrlutil.RequeueAfter(ctx, 30*time.Second, "waiting for complete topology observation")
	}
	masterNodes, slaveNodes := topology.Masters, topology.Slaves
	observedPods := topology.Observed()
	incompleteTopology := !topology.Complete()
	realMaster, masterPositivelyIdentified := r.observedRedisReplicationMaster(ctx, instance, topology)
	if !incompleteTopology && (len(masterNodes) == 0 || (len(masterNodes) > 1 && realMaster == "")) {
		candidates := masterNodes
		if len(candidates) == 0 {
			candidates = slaveNodes
		}
		realMaster = k8sutils.PickPreferredMaster(instance, candidates)
		if r.Checker == nil {
			return intctrlutil.RequeueAfter(ctx, 30*time.Second, "recovery checker unavailable")
		}
		counts := r.Checker.GetReplicationKeyCounts(ctx, instance, append(append([]string{}, masterNodes...), slaveNodes...))
		validate := r.ValidateRecoveryLineage
		if validate == nil {
			validate = k8sutils.ValidateRecoveryLineage
		}
		allPods := append(append([]string{}, masterNodes...), slaveNodes...)
		if err := validate(ctx, r.K8sClient, instance, allPods, realMaster, counts); err != nil {
			log.FromContext(ctx).Error(err, "Cold-start recovery refused: data authority is ambiguous")
			return intctrlutil.RequeueAfter(ctx, 30*time.Second, "ambiguous recovery data")
		}
		if len(masterNodes) == 0 {
			promote := r.PromoteRecoveryMaster
			if promote == nil {
				promote = k8sutils.PromoteMasterAndReslave
			}
			if err := promote(ctx, r.K8sClient, instance, allPods, realMaster); err != nil {
				return intctrlutil.RequeueE(ctx, err, "cold-start recovery failed")
			}
			return intctrlutil.RequeueAfter(ctx, noMasterRequeue, "observe recovered primary")
		}
		// Include ghost-linked replicas in the repair, not just the masters.
		if err := r.createRedisReplicationLink(ctx, instance, allPods, realMaster); err != nil {
			return intctrlutil.RequeueE(ctx, err, "cold-start replication repair failed")
		}
		return intctrlutil.RequeueAfter(ctx, noMasterRequeue, "observe recovered primary")
	}
	if len(masterNodes) > 1 {
		log.FromContext(ctx).Info("Creating redis replication by executing replication creation commands")

		if incompleteTopology {
			log.FromContext(ctx).Info("Skipping replication reconfiguration because the observed topology is incomplete",
				"observedPods", observedPods,
				"unobservedPods", topology.Unobserved)
		} else if realMaster == "" {
			log.FromContext(ctx).Info("Skipping replication reconfiguration because the current master could not be identified")
		} else if err := r.createRedisReplicationLink(ctx, instance, masterNodes, realMaster); err != nil {
			log.FromContext(ctx).Error(err, "Failed to create redis replication link",
				"master", realMaster, "masters", masterNodes)
			return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
		}
	} else if len(masterNodes) == 1 && len(slaveNodes) > 0 {
		currentRealMaster := r.redisReplicationRealMaster(ctx, instance, masterNodes)

		if currentRealMaster == "" && !instance.EnableSentinel() {
			log.FromContext(ctx).Info("Detected disconnected slaves, reconfiguring replication",
				"master", realMaster, "slaves", slaveNodes)

			if incompleteTopology {
				log.FromContext(ctx).Info("Skipping master-slave reconfiguration because the observed topology is incomplete",
					"observedPods", observedPods,
					"unobservedPods", topology.Unobserved)
			} else {
				allPods := append(masterNodes, slaveNodes...)
				if err := r.createRedisReplicationLink(ctx, instance, allPods, realMaster); err != nil {
					log.FromContext(ctx).Error(err, "Failed to reconfigure master-slave replication",
						"master", realMaster, "slaves", slaveNodes)
					return intctrlutil.RequeueAfter(ctx, time.Second*60, "")
				}
				log.FromContext(ctx).Info("Successfully reconfigured slave replication")
			}
		}
	}

	monitoring.RedisReplicationReplicasSizeMismatch.WithLabelValues(instance.Namespace, instance.Name).Set(0)
	if instance.Spec.Size != nil && int(*instance.Spec.Size) != observedPods {
		monitoring.RedisReplicationReplicasSizeMismatch.WithLabelValues(instance.Namespace, instance.Name).Set(1)
	}

	monitoring.RedisReplicationReplicasSizeCurrent.WithLabelValues(instance.Namespace, instance.Name).Set(float64(observedPods))
	monitoring.RedisReplicationReplicasSizeDesired.WithLabelValues(instance.Namespace, instance.Name).Set(float64(*instance.Spec.Size))

	if instance.EnableSentinel() {
		if incompleteTopology && !masterPositivelyIdentified {
			log.FromContext(ctx).Info("Skipping sentinel reconfiguration because topology is incomplete and the master is ambiguous",
				"observedPods", observedPods,
				"unobservedPods", topology.Unobserved)
		} else if err := r.configureReplicationSentinel(ctx, instance, realMaster); err != nil {
			log.FromContext(ctx).Error(err, "failed to configure sentinel")
		}
	}

	return intctrlutil.Reconciled()
}

// noMasterRequeue is how soon status is re-observed while no pod can be put
// into the master Service, i.e. while writes are failing. Pod and Sentinel
// events normally wake the reconciler before it elapses.
const noMasterRequeue = 5 * time.Second

// reconcileStatus records the elected master and makes it the only pod the
// master Service selects. It must keep working while pods are missing or
// terminating: that is exactly when the master moves.
func (r *Reconciler) reconcileStatus(ctx context.Context, instance *rrvb2.RedisReplication) (ctrl.Result, error) {
	// One concurrent, bounded probe per pod feeds both the election and the
	// labels, so an unresponsive old primary costs one RoleProbeTimeout.
	roles, err := r.observeRoles(ctx, instance)
	if apierrors.IsNotFound(err) {
		// The StatefulSet is created by the resources step.
		return intctrlutil.Reconciled()
	}
	if err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}
	var masterNodes, slaveNodes []string
	for pod, role := range roles {
		switch role {
		case "master":
			masterNodes = append(masterNodes, pod)
		case "slave":
			slaveNodes = append(slaveNodes, pod)
		}
	}
	slices.Sort(masterNodes)
	slices.Sort(slaveNodes)

	realMaster := r.electedMaster(ctx, instance, masterNodes, slaveNodes)
	// A partial or cold-start view cannot disprove the last recorded primary.
	// Preserve it for the existing conservative bootstrap recovery policy.
	if realMaster != "" {
		if err = r.UpdateRedisReplicationMaster(ctx, instance, realMaster, false); err != nil {
			return intctrlutil.RequeueE(ctx, err, "")
		}
	}
	labels := common.GetRedisLabels(instance.GetName(), common.SetupTypeReplication, "replication", instance.GetLabels())
	if err = r.Healer.PublishReplicationRoleLabels(ctx, instance.GetNamespace(), labels, roles, realMaster); err != nil {
		return intctrlutil.RequeueE(ctx, err, "")
	}

	if realMaster != "" {
		monitoring.RedisReplicationConnectedSlavesTotal.WithLabelValues(instance.Namespace, instance.Name).Set(float64(len(slaveNodes)))
		return intctrlutil.Reconciled()
	}
	monitoring.RedisReplicationConnectedSlavesTotal.WithLabelValues(instance.Namespace, instance.Name).Set(float64(0))
	return ctrl.Result{RequeueAfter: noMasterRequeue}, nil
}

// electedMaster returns the pod that may be put into the master Service, or
// "" when no pod can be positively identified as the primary.
//
// Sentinel is authoritative when it monitors the replication: it is what
// promotes replicas, and its answer is available while pods are missing or
// while an empty replacement also reports role:master. The Sentinel choice is
// only published once that pod reports role:master itself.
//
// Without a Sentinel answer, a lone observed master is only trusted when the
// view is complete, when it has attached replicas, or when it is the recorded
// master (the same guard upstream applies in observedRedisReplicationMaster);
// otherwise an empty replacement of a dead primary would become writable.
func (r *Reconciler) electedMaster(ctx context.Context, instance *rrvb2.RedisReplication, masters, slaves []string) string {
	logger := log.FromContext(ctx)
	if r.SentinelMonitoredMaster != nil && instance.EnableSentinel() {
		master, err := r.SentinelMonitoredMaster(ctx, instance, masters)
		if err == nil {
			return master
		}
	}
	if elected, ok := r.sentinelElectedMaster(ctx, instance); ok {
		if slices.Contains(masters, elected) {
			return elected
		}
		logger.Info("Sentinel-elected master is not serving as master",
			"sentinelMaster", elected, "observedMasters", masters)
		return ""
	}
	switch len(masters) {
	case 0:
		return ""
	case 1:
		candidate := masters[0]
		if instance.Spec.Size != nil && len(masters)+len(slaves) == int(*instance.Spec.Size) {
			return candidate
		}
		if r.realMaster(ctx, instance, masters) == candidate || candidate == instance.Status.MasterNode {
			return candidate
		}
		logger.Info("Not trusting the only observed master of a partial view",
			"candidate", candidate, "statusMasterNode", instance.Status.MasterNode, "slaves", slaves)
		return ""
	default:
		return r.realMaster(ctx, instance, masters)
	}
}

func (r *Reconciler) updateStatus(ctx context.Context, rr *rrvb2.RedisReplication, status rrvb2.RedisReplicationStatus) error {
	copy := rr.DeepCopy()
	copy.Spec = rrvb2.RedisReplicationSpec{}
	copy.Status = status
	return common.UpdateStatus(ctx, r.Client, copy)
}

// SetupWithManager sets up the controller with the Manager. Besides the CR,
// Redis pod changes and Sentinel failover announcements trigger reconciles, so
// the master Service follows the elected master without waiting for a resync.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	sentinelEvents := NewSentinelEvents(mgr.GetClient(), r.K8sClient)
	if err := mgr.Add(sentinelEvents); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&rrvb2.RedisReplication{}).
		WithOptions(opts).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(replicationForPod)).
		WatchesRawSource(&source.Channel{Source: sentinelEvents.Events()}, &handler.EnqueueRequestForObject{}).
		Complete(r)
}
