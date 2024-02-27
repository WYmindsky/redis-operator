package k8sutils

import (
	"os"
	"strconv"

	redisv1beta2 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta2"
)

// UpdatePodIPNameMap
// save new_ip:podName to stsPodIPNameMap = map[string]string and /data/${instance.Name}.txt, dealing with pod was deleted, podIP changed, new ip not in cluster nodes
func UpdatePodIPNameMap(instance *redisv1beta2.RedisCluster, fMap *os.File) {
	logger := generateRedisManagerLogger(instance.Namespace, instance.ObjectMeta.Name)
	leaderCount := *instance.Spec.Size

	for i := 0; i < int(leaderCount); i++ {
		leaderPodName := instance.ObjectMeta.Name + "-leader-" + strconv.Itoa(i)
		leadPodIP := GetPodIP(instance.Namespace, leaderPodName)
		followerPodName := instance.ObjectMeta.Name + "-follower-" + strconv.Itoa(i)
		followerPodIP := GetPodIP(instance.Namespace, followerPodName)

		StsPodIPNameMap[leadPodIP] = leaderPodName
		StsPodIPNameMap[followerPodIP] = followerPodName
	}
	_, err := fMap.Seek(0, 0)
	if err != nil {
		logger.Error(err, "failed clean %s", fMap.Name())
	}
	err = fMap.Truncate(0)
	if err != nil {
		logger.Error(err, "failed truncate %s", fMap.Name())
	}
	for k, v := range StsPodIPNameMap {
		fMap.WriteString(k + "=" + v + "\n")
	}
}
