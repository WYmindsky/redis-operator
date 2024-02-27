package k8sutils

import (
	"strconv"
	"strings"

	redisv1beta2 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta2"
	"github.com/go-redis/redis"
)

//	{
//		"172.16.20.1": "redis-cluster-leader-0",
//		"172.16.20.2": "redis-cluster-leader-1",
//		"172.16.20.1": "redis-cluster-leader-2",
//		...
//	}
var StsPodIPNameMap = map[string]string{}

// Reshard the redis Cluster
func ReshardRedisCluster(cr *redisv1beta2.RedisCluster) bool {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	var cmd []string
	currentRedisCount := CheckRedisNodeCount(cr, "leader")
	// currentRedisCount := *cr.Spec.Size

	// Transfer Pod details

	transferPOD := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	// Remove POD details
	removePOD := RedisDetails{
		PodName:   cr.Name + "-leader-" + strconv.Itoa(int(currentRedisCount)-1),
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "reshard"}

	logger.Info("version", "version", *cr.Spec.ClusterVersion, "is v7?", *cr.Spec.ClusterVersion == "v7")
	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(transferPOD, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(transferPOD)+":6379")
	}

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.ObjectMeta.Name+"-leader-0")...)

	//--cluster-from <node-id> --cluster-to <node-id> --cluster-slots <number of slots> --cluster-yes

	// Remove Node
	removeNodeID := getRedisNodeID(cr, removePOD)
	cmd = append(cmd, "--cluster-from")
	cmd = append(cmd, removeNodeID)

	// Transfer Node
	transferNodeID := getRedisNodeID(cr, transferPOD)
	cmd = append(cmd, "--cluster-to")
	cmd = append(cmd, transferNodeID)

	// Cluster Slots
	slot := getRedisClusterSlots(cr, removeNodeID)
	cmd = append(cmd, "--cluster-slots")
	cmd = append(cmd, slot)

	cmd = append(cmd, "--cluster-yes")

	logger.Info("Redis cluster reshard command is", "Command", cmd)

	if slot == "0" {
		logger.Info("Skipped the execution of", "Cmd", cmd)
		return true
	}
	return executeCommand(cr, cmd, cr.ObjectMeta.Name+"-leader-0")
}

func getRedisClusterSlots(cr *redisv1beta2.RedisCluster, nodeID string) string {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	totalSlots := 0

	redisClient := configureRedisClient(cr, cr.ObjectMeta.Name+"-leader-0")
	defer redisClient.Close()
	redisClusterInfo, err := redisClient.ClusterNodes().Result()
	if err != nil {
		logger.Error(err, "Failed to Get Cluster Info")
		return ""
	}

	// Split the Redis cluster info into lines
	lines := strings.Split(redisClusterInfo, "\n")

	// Iterate through all lines
	for _, line := range lines {
		if strings.Contains(line, "master") && strings.Contains(line, "connected") && !strings.Contains(line, ",fail") { // Check if this line is a master node
			parts := strings.Fields(line)
			if parts[0] == nodeID { // Check if this is the node we're interested in
				for _, conn := range parts[8:] {
					slotRange := strings.Split(conn, "-")
					if len(slotRange) < 2 {
						totalSlots = totalSlots + 1
					} else {
						start, _ := strconv.Atoi(slotRange[0])
						end, _ := strconv.Atoi(slotRange[1])
						totalSlots = totalSlots + end - start + 1
					}
				}
				break
			}
		}
	}
	// logger.Info("slot", "nodeID", nodeID, "slot", totalSlots)
	return strconv.Itoa(totalSlots)
}

// getRedisNodeID would return nodeID of a redis node by passing pod
func getRedisNodeID(cr *redisv1beta2.RedisCluster, pod RedisDetails) string {
	var client *redis.Client
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	client = configureRedisClient(cr, pod.PodName)
	defer client.Close()

	pong, err := client.Ping().Result()
	if err != nil || pong != "PONG" {
		logger.Error(err, "Failed to ping Redis server")
		return ""
	}

	cmd := redis.NewStringCmd("cluster", "myid")
	err = client.Process(cmd)
	if err != nil {
		logger.Error(err, "Redis command failed with this error")
		return ""
	}

	output, err := cmd.Result()
	if err != nil {
		logger.Error(err, "Redis command failed with this error")
		return ""
	}
	return output
}

// Rebalance the Redis CLuster using the Empty Master Nodes
func RebalanceRedisClusterEmptyMasters(cr *redisv1beta2.RedisCluster) {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	// cmd = redis-cli --cluster rebalance <redis>:<port> --cluster-use-empty-masters -a <pass>
	var cmd []string

	// get ready pod
	_, readyIP := GetAllFailedPods(cr)
	readyPodName := StsPodIPNameMap[readyIP]
	pod := RedisDetails{
		PodName:   readyPodName,
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "rebalance"}

	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(pod, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(pod)+":6379")
	}

	cmd = append(cmd, "--cluster-use-empty-masters")

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, readyPodName)...)

	logger.Info("Redis cluster rebalance command is", "Command", cmd)
	// if execute fail, maybe all nodes cluster config inconsistent
	// needs to forget new added master and execute "cluster meet ip port" to rejoin cluster
	if !executeCommand(cr, cmd, readyPodName) {
		RejoinToCluster(cr, readyPodName)
	}
}

func RejoinToCluster(cr *redisv1beta2.RedisCluster, readyPodName string) {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	// forget new master
	logger.Info("[ERR] Nodes don't agree about configuration! forget it and meet it")
	podNameList, podIP := getLeader(cr)

	for _, podName := range podNameList {
		cmd := []string{"redis-cli"}
		pod := RedisDetails{
			PodName:   podName,
			Namespace: cr.Namespace,
		}

		if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
			pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
			if err != nil {
				logger.Error(err, "Error in getting redis password")
			}
			cmd = append(cmd, "-a")
			cmd = append(cmd, pass)
		}

		cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, readyPodName)...)
		cmd = append(cmd, "cluster")
		podNodeID := getRedisNodeID(cr, pod)
		podSlots := getRedisClusterSlots(cr, podNodeID)

		if podSlots == "0" || podSlots == "" {
			logger.Info("Found Inconsistent Leader Node", "NodeID", podNodeID, "ip", podIP)
			cmd = append(cmd, "reset")
			if executeCommand(cr, cmd, StsPodIPNameMap[podIP]) {
				logger.Info("cluster reset ok")
				cmd = cmd[:len(cmd)-1]
				cmd = append(cmd, "forget", podNodeID)
				if executeCommand(cr, cmd, readyPodName) {
					cmd = cmd[:len(cmd)-2]
					cmd = append(cmd, "meet", podIP, "6379")
					executeCommand(cr, cmd, readyPodName)
				}
			} else {
				logger.Info("cluster reset failed")
				continue
			}
		}
	}
}

func CheckIfEmptyMasters(cr *redisv1beta2.RedisCluster) {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)

	podNameList, _ := getLeader(cr)
	// logger.Info("leaders", "pod leaders", podNameList)
	for _, podName := range podNameList {
		pod := RedisDetails{
			PodName:   podName,
			Namespace: cr.Namespace,
		}
		podNodeID := getRedisNodeID(cr, pod)
		podSlots := getRedisClusterSlots(cr, podNodeID)

		if podSlots == "0" || podSlots == "" {
			logger.Info("Found Empty Redis Leader Node", "pod", pod)
			RebalanceRedisClusterEmptyMasters(cr)
			break
		}
	}
}

// getLeader get all masters with slot field
func getLeader(cr *redisv1beta2.RedisCluster) ([]string, string) {
	clusterNodes := checkRedisCluster(cr)

	podNameList := []string{}
	podIP := ""
	for _, node := range clusterNodes {
		if node[2] == "master" || node[2] == "myself,master" {
			podIP := strings.Split(node[1], ":")[0]
			podName := StsPodIPNameMap[podIP]
			if podName != "" {
				podNameList = append(podNameList, podName)
			}
			if len(node) == 8 {
				podIP = strings.Split(node[1], ":")[0]
			}
		}
	}
	return podNameList, podIP
}

// Rebalance Redis Cluster Would Rebalance the Redis Cluster without using the empty masters
func RebalanceRedisCluster(cr *redisv1beta2.RedisCluster) bool {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	// cmd = redis-cli --cluster rebalance <redis>:<port> -a <pass>
	var cmd []string
	pod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-1",
		Namespace: cr.Namespace,
	}
	cmd = []string{"redis-cli", "--cluster", "rebalance"}

	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(pod, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(pod)+":6379")
	}

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.ObjectMeta.Name+"-leader-0")...)

	logger.Info("Redis cluster rebalance command is", "Command", cmd)
	return executeCommand(cr, cmd, cr.ObjectMeta.Name+"-leader-1")
}

// Add redis cluster node would add a node to the existing redis cluster using redis-cli
func AddRedisNodeToCluster(cr *redisv1beta2.RedisCluster) {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	var cmd []string
	activeRedisNode := CheckRedisNodeCount(cr, "leader")

	newPod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-" + strconv.Itoa(int(activeRedisNode)),
		Namespace: cr.Namespace,
	}
	existingPod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-0",
		Namespace: cr.Namespace,
	}

	cmd = []string{"redis-cli", "--cluster", "add-node"}

	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(newPod, cr, "leader")+":6379")
		cmd = append(cmd, getRedisHostname(existingPod, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(newPod)+":6379")
		cmd = append(cmd, getRedisServerIP(existingPod)+":6379")
	}

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.ObjectMeta.Name+"-leader-0")...)

	logger.Info("Redis cluster add-node command is", "Command", cmd)
	executeCommand(cr, cmd, cr.ObjectMeta.Name+"-leader-0")
}

// getAttachedFollowerNodeIDs would return a slice of redis followers attached to a redis leader
func getAttachedFollowerNodeIDs(cr *redisv1beta2.RedisCluster, masterNodeID string) []string {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)

	redisClient := configureRedisClient(cr, cr.ObjectMeta.Name+"-leader-0")
	defer redisClient.Close()
	redisClusterInfo, err := redisClient.ClusterNodes().Result()
	if err != nil {
		logger.Error(err, "Failed to Get Cluster Info")
		return nil
	}

	slaveIDs := []string{}
	// Split the Redis cluster info into lines
	lines := strings.Split(redisClusterInfo, "\n")

	for _, line := range lines {
		if strings.Contains(line, "slave") && strings.Contains(line, "connected") {
			parts := strings.Fields(line)
			if len(parts) >= 3 && parts[3] == masterNodeID {
				slaveIDs = append(slaveIDs, parts[0])
			}
		}

	}

	logger.Info("Slaves Nodes attached to", "node", masterNodeID, "are", slaveIDs)
	return slaveIDs
}

// Remove redis follower node would remove all follower nodes of last leader node using redis-cli
func RemoveRedisFollowerNodesFromCluster(cr *redisv1beta2.RedisCluster) bool {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	var cmd []string
	currentRedisCount := CheckRedisNodeCount(cr, "leader")

	existingPod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	lastLeaderPod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-" + strconv.Itoa(int(currentRedisCount)-1),
		Namespace: cr.Namespace,
	}

	cmd = []string{"redis-cli"}

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}
	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.ObjectMeta.Name+"-leader-0")...)

	lastLeaderPodNodeID := getRedisNodeID(cr, lastLeaderPod)
	followerNodeIDs := getAttachedFollowerNodeIDs(cr, lastLeaderPodNodeID)

	cmd = append(cmd, "--cluster", "del-node")
	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(existingPod, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(existingPod)+":6379")
	}

	for _, followerNodeID := range followerNodeIDs {
		cmd = append(cmd, followerNodeID)
		logger.Info("Redis cluster follower remove command is", "Command", cmd)
		if !executeCommand(cr, cmd, cr.ObjectMeta.Name+"-leader-0") {
			return false
		}
		cmd = cmd[:len(cmd)-1]
	}
	return true
}

// Remove redis cluster node would remove last node to the existing redis cluster using redis-cli
func RemoveRedisNodeFromCluster(cr *redisv1beta2.RedisCluster) {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	var cmd []string
	currentRedisCount := CheckRedisNodeCount(cr, "leader")

	existingPod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-0",
		Namespace: cr.Namespace,
	}
	removePod := RedisDetails{
		PodName:   cr.ObjectMeta.Name + "-leader-" + strconv.Itoa(int(currentRedisCount)-1),
		Namespace: cr.Namespace,
	}

	cmd = []string{"redis-cli", "--cluster", "del-node"}

	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(existingPod, cr, "leader")+":6379")
	} else {
		cmd = append(cmd, getRedisServerIP(existingPod)+":6379")
	}

	removePodNodeID := getRedisNodeID(cr, removePod)
	cmd = append(cmd, removePodNodeID)

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, cr.ObjectMeta.Name+"-leader-0")...)

	logger.Info("Redis cluster leader remove command is", "Command", cmd)
	if getRedisClusterSlots(cr, removePodNodeID) != "0" {
		logger.Info("Skipping execution remove leader not empty", "cmd", cmd)
	}
	executeCommand(cr, cmd, cr.ObjectMeta.Name+"-leader-0")
}

// verifyLeaderPod return true if the pod is leader/master
func VerifyLeaderPod(cr *redisv1beta2.RedisCluster, leadFlag bool) bool {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	podName := cr.Name + "-leader-"
	if leadFlag {
		podName += "0"
	} else {
		podName += strconv.Itoa(int(CheckRedisNodeCount(cr, "leader")) - 1)
	}

	redisClient := configureRedisClient(cr, podName)
	defer redisClient.Close()
	info, err := redisClient.Info("replication").Result()
	if err != nil {
		logger.Error(err, "Failed to Get the role Info of the", "redis pod", podName)
		return false
	}

	lines := strings.Split(info, "\r\n")
	role := ""
	for _, line := range lines {
		if strings.HasPrefix(line, "role:") {
			role = strings.TrimPrefix(line, "role:")
			return role == "master"
		}
	}
	return false
}

func ClusterFailover(cr *redisv1beta2.RedisCluster, leadFlag bool) bool {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	slavePodName := cr.Name + "-leader-"
	if leadFlag {
		slavePodName += "0"
	} else {
		slavePodName += strconv.Itoa(int(CheckRedisNodeCount(cr, "leader")) - 1)
	}
	// cmd = redis-cli cluster failover  -a <pass>

	var cmd []string
	pod := RedisDetails{
		PodName:   slavePodName,
		Namespace: cr.Namespace,
	}

	cmd = []string{"redis-cli", "-h"}

	if *cr.Spec.ClusterVersion == "v7" {
		cmd = append(cmd, getRedisHostname(pod, cr, "leader"))
	} else {
		cmd = append(cmd, getRedisServerIP(pod))
	}
	cmd = append(cmd, "-p", "6379")

	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
		cmd = append(cmd, "-a")
		cmd = append(cmd, pass)
	}
	cmd = append(cmd, "cluster", "failover")

	cmd = append(cmd, getRedisTLSArgs(cr.Spec.TLS, slavePodName)...)
	logger.Info("ready for cluster failover", "command", cmd, "pod", slavePodName)
	logger.Info("Redis cluster failover command is", "Command", cmd)

	return executeCommand(cr, cmd, slavePodName)
}
