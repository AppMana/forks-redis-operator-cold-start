package k8sutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	rcvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/rediscluster/v1beta2"
	redis "github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ClusterHasEmptyMasters returns true if the cluster contains at least one master
// with no slot ranges assigned.
// This is useful to decide if `redis-cli --cluster rebalance --cluster-use-empty-masters`
// should be attempted.
func ClusterHasEmptyMasters(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) (bool, error) {
	seedPod := fmt.Sprintf("%s-leader-0", cr.Name)
	redisClient := configureRedisClient(ctx, client, cr, seedPod)
	defer redisClient.Close()

	nodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		return false, err
	}

	return clusterHasEmptyMasters(nodes), nil
}

func clusterHasEmptyMasters(nodes []clusterNodesResponse) bool {
	for _, fields := range nodes {
		// CLUSTER NODES format:
		// 0:id 1:addr 2:flags 3:master 4:ping 5:pong 6:epoch 7:link [8+:slots...]
		if len(fields) < 8 {
			// malformed line; ignore rather than failing hard
			continue
		}

		flags := fields[2]
		linkState := fields[7]

		if !hasFlag(flags, "master") {
			continue
		}

		// Ignore masters that are clearly not healthy participants
		if hasAnyFlag(flags, "fail", "handshake", "noaddr") || linkState != "connected" {
			continue
		}

		// Slots are everything after index 7.
		// If none exist, this master is "empty".
		if len(fields) == 8 {
			return true
		}

		hasSlotToken := false
		for _, tok := range fields[8:] {
			if looksLikeSlotToken(tok) {
				hasSlotToken = true
				break
			}
		}
		if !hasSlotToken {
			return true
		}
	}

	return false
}

func hasAnyFlag(flags string, wanted ...string) bool {
	for _, w := range wanted {
		if hasFlag(flags, w) {
			return true
		}
	}
	return false
}

func hasFlag(flags string, flag string) bool {
	// flags are comma-separated: "master", "myself,master", "slave", ...
	for _, f := range strings.Split(flags, ",") {
		if f == flag {
			return true
		}
	}
	return false
}

func looksLikeSlotToken(tok string) bool {
	// Very permissive on purpose.
	// We only need to distinguish "some slot-related token exists" from "none".
	if tok == "" {
		return false
	}
	// migration markers are also slot-related
	if strings.Contains(tok, "->-") || strings.Contains(tok, "-<-") {
		return true
	}
	// common cases: "0-5460" or "5461"
	if tok[0] >= '0' && tok[0] <= '9' {
		return true
	}
	// bracketed forms: "[0-5460]" or "[5461->-...]"
	if strings.HasPrefix(tok, "[") && len(tok) > 1 && tok[1] >= '0' && tok[1] <= '9' {
		return true
	}
	return false
}

// ClusterStableNoOpenSlots returns true if the cluster appears safe for
// disruptive operations like rebalance/reshard:
//   - cluster_state == ok
//   - no migrating/importing slot markers in CLUSTER NODES ("->-" / "-<-")
//   - no nodes in handshake/fail/noaddr
//   - (optional) cluster_slot_migration_active_tasks == 0 if present in CLUSTER INFO
func ClusterStableNoOpenSlots(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) (bool, error) {
	seedPod := fmt.Sprintf("%s-leader-0", cr.Name)
	redisClient := configureRedisClient(ctx, client, cr, seedPod)
	defer redisClient.Close()

	info, err := redisClient.ClusterInfo(ctx).Result()
	if err != nil {
		return false, err
	}

	nodes, err := clusterNodes(ctx, redisClient)
	if err != nil {
		return false, err
	}

	return clusterStableNoOpenSlots(info, nodes), nil
}

func clusterStableNoOpenSlots(clusterInfo string, nodes []clusterNodesResponse) bool {
	// 1) cluster_state gate
	kv := parseClusterInfo(clusterInfo)
	if kv["cluster_state"] != "ok" {
		return false
	}

	// 2) migration-task gate (Redis 7 exposes these fields in your output)
	if v, ok := kv["cluster_slot_migration_active_tasks"]; ok {
		n, convErr := strconv.Atoi(strings.TrimSpace(v))
		if convErr == nil && n > 0 {
			return false
		}
	}

	// 3) open slot + node health gate from CLUSTER NODES
	for _, fields := range nodes {
		if len(fields) < 8 {
			// malformed line -> treat as not stable (conservative)
			return false
		}

		flags := fields[2]
		linkState := fields[7]

		// If nodes are not fully established, don't rebalance.
		if hasAnyFlag(flags, "handshake", "fail", "noaddr") || linkState != "connected" {
			return false
		}
	}

	return !clusterHasOpenSlots(nodes)
}

func parseClusterInfo(info string) map[string]string {
	out := make(map[string]string)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		out[parts[0]] = strings.TrimSpace(parts[1])
	}
	return out
}

func clusterHasOpenSlots(nodes []clusterNodesResponse) bool {
	for _, fields := range nodes {
		// Open slot markers appear in the slot tokens:
		// "[5491->-...]" migrating / "[5491-<-...]" importing
		for _, tok := range fields[8:] {
			if strings.Contains(tok, "->-") || strings.Contains(tok, "-<-") {
				return true
			}
		}
	}
	return false
}

func waitForClusterNoOpenSlots(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, timeout time.Duration) error {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := redisClient.ClusterInfo(ctx).Result()
		if err != nil || !strings.Contains(info, "cluster_state:ok") {
			time.Sleep(2 * time.Second)
			continue
		}

		nodes, err := clusterNodes(ctx, redisClient)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		if clusterHasOpenSlots(nodes) {
			time.Sleep(2 * time.Second)
			continue
		}

		return nil
	}
	return fmt.Errorf("cluster still has open slots or is not converged after %s", timeout)
}

// ReshardRedisCluster transfer the slots from the last node to the provided transfer node.
//
// NOTE: when all slot been transferred, the node become slave of the transfer node.
func ReshardRedisCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, shardIdx int32, transferNodeIdx int32, remove bool) error {
	transferNodeName := fmt.Sprintf("%s-leader-%d", cr.Name, transferNodeIdx)
	redisClient := configureRedisClient(ctx, client, cr, transferNodeName)
	defer redisClient.Close()

	var cmd []string

	// Transfer Pod details
	transferPOD := RedisDetails{
		PodName:   transferNodeName,
		Namespace: cr.Namespace,
	}
	// Remove POD details
	removePOD := RedisDetails{
		PodName:   cr.Name + "-leader-" + strconv.Itoa(int(shardIdx)),
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "reshard"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, transferPOD))
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, transferNodeName)
	if err != nil {
		return fmt.Errorf("cannot reshard: authentication configuration unavailable")
	}
	cmd = append(cmd, authArgs...)

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, transferNodeName)...)

	//--cluster-from <node-id> --cluster-to <node-id> --cluster-slots <number of slots> --cluster-yes

	// Remove Node
	removeNodeID := getRedisNodeID(ctx, client, cr, removePOD)
	cmd = append(cmd, "--cluster-from")
	cmd = append(cmd, removeNodeID)

	// Transfer Node
	transferNodeID := getRedisNodeID(ctx, client, cr, transferPOD)
	if removeNodeID == "" || transferNodeID == "" || removeNodeID == transferNodeID {
		return fmt.Errorf("cannot reshard: source and target must have distinct known identities")
	}
	cmd = append(cmd, "--cluster-to")
	cmd = append(cmd, transferNodeID)

	var removeNode func() error
	if remove {
		followers, err := retiringFollowerIDs(ctx, client, cr, redisClient, removeNodeID)
		if err != nil {
			return err
		}
		removeNode = func() error {
			if err := RemoveRedisFollowerNodesFromCluster(ctx, client, cr, followers); err != nil {
				return err
			}
			return RemoveRedisNodeFromCluster(ctx, client, cr, removeNodeID)
		}
	}
	return reshardBeforeRemoval(func() (int, error) {
		return ownedRedisClusterSlots(ctx, redisClient, removeNodeID)
	}, func(slots int) error {
		transferCmd := append(cmd, "--cluster-slots", strconv.Itoa(slots), "--cluster-yes")
		if _, err := executeCommand1(ctx, client, cr, transferCmd, transferNodeName); err != nil {
			return fmt.Errorf("reshard execution failed; retaining source node")
		}
		return nil
	}, removeNode)
}

// A successful CLI exit alone does not prove that it is safe to remove a shard.
// Observe primary slot ownership again, including when retrying an empty source.
func reshardBeforeRemoval(observe func() (int, error), transfer func(int) error, remove func() error) error {
	slots, err := observe()
	if err != nil {
		return err
	}
	if slots < 0 || slots > 16384 {
		return fmt.Errorf("invalid source slot count")
	}
	if slots > 0 {
		if err := transfer(slots); err != nil {
			return err
		}
	}
	remaining, err := observe()
	if err != nil {
		return err
	}
	if remaining != 0 {
		return fmt.Errorf("source still owns slots after reshard; retaining source node")
	}
	if remove != nil {
		return remove()
	}
	return nil
}

func ownedRedisClusterSlots(ctx context.Context, client *redis.Client, nodeID string) (int, error) {
	if nodeID == "" {
		return 0, fmt.Errorf("source identity is unavailable")
	}
	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("source slot inventory is unavailable")
	}
	if len(slots) == 0 {
		return 0, fmt.Errorf("cluster slot inventory is empty")
	}
	total := 0
	var covered [16384]bool
	coverage := 0
	for _, slot := range slots {
		if slot.Start < 0 || slot.End < slot.Start || slot.End >= 16384 || len(slot.Nodes) == 0 || slot.Nodes[0].ID == "" {
			return 0, fmt.Errorf("cluster slot inventory is invalid")
		}
		for i := slot.Start; i <= slot.End; i++ {
			if covered[i] {
				return 0, fmt.Errorf("cluster slot inventory overlaps")
			}
			covered[i] = true
			coverage++
		}
		// Only the first node owns the slots; following nodes are replicas.
		if slot.Nodes[0].ID == nodeID {
			total += slot.End - slot.Start + 1
		}
	}
	if coverage != len(covered) {
		return 0, fmt.Errorf("cluster slot inventory is incomplete")
	}
	return total, nil
}

func getRedisClusterSlots(ctx context.Context, redisClient *redis.Client, nodeID string) string {
	totalSlots := 0

	redisSlots, err := redisClient.ClusterSlots(ctx).Result()
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to get cluster slots")
		return ""
	}
	for _, slot := range redisSlots {
		for _, node := range slot.Nodes {
			if node.ID == nodeID {
				// Each slot range is a continuous block managed by the node
				totalSlots += slot.End - slot.Start + 1
				break
			}
		}
	}

	return strconv.Itoa(totalSlots)
}

// getRedisNodeID would return nodeID of a redis node by passing pod
func getRedisNodeID(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, pod RedisDetails) string {
	redisClient := configureRedisClient(ctx, client, cr, pod.PodName)
	defer redisClient.Close()

	pong, err := redisClient.Ping(ctx).Result()
	if err != nil || pong != "PONG" {
		log.FromContext(ctx).Error(err, "Failed to ping Redis server")
		return ""
	}

	cmd := redis.NewStringCmd(ctx, "cluster", "myid")
	err = redisClient.Process(ctx, cmd)
	if err != nil {
		log.FromContext(ctx).Error(err, "Redis command failed with this error")
		return ""
	}

	output, err := cmd.Result()
	if err != nil {
		log.FromContext(ctx).Error(err, "Redis command failed with this error")
		return ""
	}
	log.FromContext(ctx).V(1).Info("Redis node ID ", "is", output)
	return output
}

// FixRedisCluster runs `redis-cli --cluster fix` to resolve any open/stuck slots
// (e.g., slots left in migrating/importing state from a previous interrupted rebalance).
// This must be called before add-node or rebalance when the cluster may have open slots.
func FixRedisCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	pod := RedisDetails{
		PodName:   cr.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	cmd := []string{"redis-cli", "--cluster", "fix"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, pod))
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, cr.Name+"-leader-0")
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get password authentication arguments")
		return err
	}
	cmd = append(cmd, authArgs...)
	cmd = append(cmd, "--cluster-yes")
	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)

	_, err = executeCommand1(ctx, client, cr, cmd, cr.Name+"-leader-0")
	return err
}

// Rebalance the Redis CLuster using the Empty Master Nodes
func RebalanceRedisClusterEmptyMasters(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) {
	if err := waitForClusterNoOpenSlots(ctx, client, cr, 2*time.Minute); err != nil {
		log.FromContext(ctx).Info("Skipping rebalance: cluster not ready", "reason", err.Error())
		return
	}

	// cmd = redis-cli --cluster rebalance <redis>:<port> --cluster-use-empty-masters
	var cmd []string
	pod := RedisDetails{
		PodName:   cr.Name + "-leader-1",
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "rebalance"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, pod))
	cmd = append(cmd, "--cluster-use-empty-masters")
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, cr.Name+"-leader-0")
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get password authentication arguments")
	}
	cmd = append(cmd, authArgs...)

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)

	executeCommand(ctx, client, cr, cmd, cr.Name+"-leader-1")
}

func CheckIfEmptyMasters(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) error {
	totalRedisLeaderNodes := CheckRedisNodeCount(ctx, client, cr, "leader")
	if totalRedisLeaderNodes != cr.Spec.GetReplicaCounts("leader") {
		return fmt.Errorf("cannot inspect empty masters: observed leader count does not match desired count")
	}
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()

	empty, err := observeEmptyMasters(totalRedisLeaderNodes, func(i int32) (string, string) {
		pod := RedisDetails{
			PodName:   cr.Name + "-leader-" + strconv.Itoa(int(i)),
			Namespace: cr.Namespace,
		}
		podNodeID := getRedisNodeID(ctx, client, cr, pod)
		if podNodeID == "" {
			return "", ""
		}
		podSlots := getRedisClusterSlots(ctx, redisClient, podNodeID)
		return podNodeID, podSlots
	})
	if err != nil {
		return err
	}
	if empty {
		RebalanceRedisClusterEmptyMasters(ctx, client, cr)
	}
	return nil
}

// Complete the inventory before permitting a rebalance. Empty strings from the
// legacy observation helpers mean lookup failure, not a node with zero slots.
func observeEmptyMasters(count int32, observe func(int32) (string, string)) (bool, error) {
	if count <= 0 {
		return false, fmt.Errorf("cannot inspect empty masters: no leaders observed")
	}
	seen := make(map[string]bool, count)
	empty := false
	for i := int32(0); i < count; i++ {
		nodeID, slots := observe(i)
		if nodeID == "" || seen[nodeID] {
			return false, fmt.Errorf("cannot inspect empty masters: missing or duplicate node identity at ordinal %d", i)
		}
		seen[nodeID] = true
		n, err := strconv.ParseUint(slots, 10, 15)
		if err != nil || n > 16384 {
			return false, fmt.Errorf("cannot inspect empty masters: missing or invalid slot count at ordinal %d", i)
		}
		empty = empty || n == 0
	}
	return empty, nil
}

// Rebalance Redis Cluster Would Rebalance the Redis Cluster without using the empty masters
func RebalanceRedisCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) {
	// cmd = redis-cli --cluster rebalance <redis>:<port>
	var cmd []string
	pod := RedisDetails{
		PodName:   cr.Name + "-leader-1",
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "rebalance"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, pod))
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, cr.Name+"-leader-0")
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get password authentication arguments")
	}
	cmd = append(cmd, authArgs...)

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)

	executeCommand(ctx, client, cr, cmd, cr.Name+"-leader-1")
}

func waitForNodePresence(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, nodeIP string, timeout time.Duration) error {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nodes, err := clusterNodes(ctx, redisClient)
		if err == nil && checkRedisNodePresence(ctx, nodes, nodeIP) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("node %s did not appear in cluster nodes after %s", nodeIP, timeout)
}

// Add redis cluster node would add a node to the existing redis cluster using redis-cli
func AddRedisNodeToCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster) {
	cmd := []string{"redis-cli", "--cluster", "add-node"}
	activeRedisNode := CheckRedisNodeCount(ctx, client, cr, "leader")
	newPod := RedisDetails{
		PodName:   cr.Name + "-leader-" + strconv.Itoa(int(activeRedisNode)),
		Namespace: cr.Namespace,
	}
	existingPod := RedisDetails{
		PodName:   cr.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	cmd = append(cmd, getEndpoint(ctx, client, cr, newPod))
	cmd = append(cmd, getEndpoint(ctx, client, cr, existingPod))
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, cr.Name+"-leader-0")
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get password authentication arguments")
	}
	cmd = append(cmd, authArgs...)

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)

	executeCommand(ctx, client, cr, cmd, cr.Name+"-leader-0")

	if err := waitForNodePresence(ctx, client, cr, getRedisServerIP(ctx, client, newPod), 90*time.Second); err != nil {
		log.FromContext(ctx).Error(err, "node added but not converged yet: %w")
	}
}

// getAttachedFollowerNodeIDs would return a slice of redis followers attached to a redis leader
func getAttachedFollowerNodeIDs(ctx context.Context, redisClient *redis.Client, masterNodeID string) ([]string, error) {
	// 3acb029fead40752f432c84f9bed2e639119a573 192.168.84.239:6379@16379,redis-cluster-v1beta2-follower-5 slave e3299968586dd457a8dba04fc6c747cecd38510f 0 1713595736542 6 connected
	slaveNodes, err := redisClient.ClusterSlaves(ctx, masterNodeID).Result()
	if err != nil {
		return nil, fmt.Errorf("attached follower inventory is unavailable")
	}
	slaveIDs := make([]string, 0, len(slaveNodes))
	seen := map[string]bool{}
	for _, slave := range slaveNodes {
		fields := strings.Fields(slave)
		if len(fields) < 8 || !hasFlag(fields[2], "slave") || fields[3] != masterNodeID || seen[fields[0]] {
			return nil, fmt.Errorf("attached follower inventory is malformed or ambiguous")
		}
		seen[fields[0]] = true
		slaveIDs = append(slaveIDs, fields[0])
	}
	return slaveIDs, nil
}

// Retire only previously identified obsolete followers, after slot transfer.
func RemoveRedisFollowerNodesFromCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, ids []string) error {
	redisClient := configureRedisClient(ctx, client, cr, cr.Name+"-leader-0")
	defer redisClient.Close()
	return retireEmptyFollowers(ids, func(id string) (int, error) {
		return ownedRedisClusterSlots(ctx, redisClient, id)
	}, func(id string) error { return RemoveRedisNodeFromCluster(ctx, client, cr, id) })
}

// Remove redis cluster node would remove last node to the existing redis cluster using redis-cli
func RemoveRedisNodeFromCluster(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("cannot remove node: identity is unavailable")
	}
	existingPod := RedisDetails{
		PodName:   cr.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	cmd := []string{"redis-cli", "--cluster", "del-node"}
	cmd = append(cmd, getEndpoint(ctx, client, cr, existingPod))
	cmd = append(cmd, nodeID)
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, cr.Name+"-leader-0")
	if err != nil {
		return fmt.Errorf("cannot remove node: authentication configuration unavailable")
	}
	cmd = append(cmd, authArgs...)
	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.Name+"-leader-0")...)
	if _, err := executeCommand1(ctx, client, cr, cmd, cr.Name+"-leader-0"); err != nil {
		return fmt.Errorf("cluster node removal failed")
	}
	return nil
}

// verifyLeaderPod return true if the pod is leader/master
func VerifyLeaderPod(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, leadIndex int32) bool {
	podName := cr.Name + "-leader-" + strconv.Itoa(int(leadIndex))

	redisClient := configureRedisClient(ctx, client, cr, podName)
	defer redisClient.Close()
	return verifyLeaderPodInfo(ctx, redisClient, podName)
}

func verifyLeaderPodInfo(ctx context.Context, redisClient *redis.Client, podName string) bool {
	info, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to Get the role Info of the", "redis pod", podName)
		return false
	}

	lines := strings.Split(info, "\r\n")
	var role string
	for _, line := range lines {
		if strings.HasPrefix(line, "role:") {
			role = strings.TrimPrefix(line, "role:")
			return role == "master"
		}
	}
	return false
}

func ClusterFailover(ctx context.Context, client kubernetes.Interface, cr *rcvb2.RedisCluster, shardIdx int32) error {
	slavePodName := cr.Name + "-leader-" + strconv.Itoa(int(shardIdx))
	// cmd = redis-cli cluster failover
	var cmd []string
	pod := RedisDetails{
		PodName:   slavePodName,
		Namespace: cr.Namespace,
	}
	endpoint := getEndpoint(ctx, client, cr, pod)
	lastColon := strings.LastIndex(endpoint, ":")
	if lastColon < 0 {
		return fmt.Errorf("invalid endpoint format: %s", endpoint)
	}
	host, port := endpoint[:lastColon], endpoint[lastColon+1:]
	cmd = []string{"redis-cli", "-h", host, "-p", port}
	authArgs, err := getRedisClusterAuthArgs(ctx, client, cr, slavePodName)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get password authentication arguments")
	}
	cmd = append(cmd, authArgs...)

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, slavePodName)...)
	cmd = append(cmd, "cluster", "failover")

	log.FromContext(ctx).V(1).Info("Redis cluster failover command is", "Command", cmd)
	execOut, err := executeCommand1(ctx, client, cr, cmd, slavePodName)
	if err != nil {
		log.FromContext(ctx).Error(err, "Could not execute command", "Command", cmd, "Output", execOut)
		return err
	}
	return nil
}
