/*
Copyright 2020 Opstree Solutions.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/OT-CONTAINER-KIT/redis-operator/api/status"
	redisv1beta2 "github.com/OT-CONTAINER-KIT/redis-operator/api/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/k8sutils"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

var (
	fMap *os.File
	err  error
)

// Reconcile is part of the main kubernetes reconciliation loop
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := r.Log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)
	reqLogger.Info("Reconciling opstree redis Cluster controller")
	instance := &redisv1beta2.RedisCluster{}

	err = r.Client.Get(context.TODO(), req.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// make sure pod podName write to StsPodIPNameMap and /data/map.txt
	// save podNameIPMap record
	fname := fmt.Sprintf("/data/%s.txt", instance.Name)
	fMap, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		reqLogger.Error(err, fmt.Sprintf("error when open %s", fname))
	}
	defer fMap.Close()

	scanner := bufio.NewScanner(fMap)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			splitStr := strings.Split(line, "=")
			k8sutils.StsPodIPNameMap[splitStr[0]] = splitStr[1]
		}
	}
	keys := make([]string, 0, len(k8sutils.StsPodIPNameMap))
	for k := range k8sutils.StsPodIPNameMap {
		keys = append(keys, k)
	}
	// reqLogger.Info(fmt.Sprintf("stspodipnamemap keys: %v", keys))
	if len(keys) == 0 {
		reqLogger.Info("no map in StsPodIPNameMap, add it")
		k8sutils.UpdatePodIPNameMap(instance, fMap)
		reqLogger.Info("added StsPodIPNameMap ok", "fMap", fMap)
	}

	if _, found := instance.ObjectMeta.GetAnnotations()["rediscluster.opstreelabs.in/skip-reconcile"]; found {
		reqLogger.Info("Found annotations rediscluster.opstreelabs.in/skip-reconcile, so skipping reconcile")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	leaderReplicas := instance.Spec.GetReplicaCounts("leader")
	followerReplicas := instance.Spec.GetReplicaCounts("follower")
	totalReplicas := leaderReplicas + followerReplicas

	if err := k8sutils.HandleRedisClusterFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	if err := k8sutils.AddRedisClusterFinalizer(instance, r.Client); err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	// Check if the cluster is downscaled
	// 5 > 4
	if leaderReplicas < instance.Status.ReadyLeaderReplicas {
		reqLogger.Info("ready to downscale cluster", "leaderReplicas", leaderReplicas, "readyLeader", instance.Status.ReadyLeaderReplicas)
		//  Imp if the last index of leader sts is not leader make it then
		// check whether the redis is leader or not ?
		// if not true then make it leader pod
		for i := int32(0); i < instance.Status.ReadyLeaderReplicas-leaderReplicas; i++ {
			reqLogger.Info("start loop", "time", i, "total", instance.Status.ReadyLeaderReplicas-leaderReplicas)
			// check leader-0, if not master, failover it
			if !(k8sutils.VerifyLeaderPod(instance, true)) {
				// lastLeaderPod is slaving right now Make it the master Pod
				// We have to bring a manual failover here to make it a leaderPod
				// clusterFailover should also include the clusterReplicate since we have to map the followers to new leader
				k8sutils.ClusterFailover(instance, true)
			}
			// check leader-last, if not master, failover it, wait cluster nodes update
			time.Sleep(10 * time.Second)
			if !(k8sutils.VerifyLeaderPod(instance, false)) {
				if !k8sutils.ClusterFailover(instance, false) {
					reqLogger.Error(fmt.Errorf("%s", "failover failed"), "leader-max failover failed, retry after 10 seconds")
					return ctrl.Result{RequeueAfter: time.Second * 10}, nil
				}
			}
			// sleep 5s, wait cluster nodes topology update finished
			time.Sleep(5 * time.Second)
			// Step 1 Rehard the Cluster
			k8sutils.ReshardRedisCluster(instance)
			reqLogger.Info("reshard ok", "time", i)
			// Step 2 Remove the Follower Node
			k8sutils.RemoveRedisFollowerNodesFromCluster(instance)
			reqLogger.Info("remove follower ok", "time", i)
			// Step 3 Remove the Leader Node
			k8sutils.RemoveRedisNodeFromCluster(instance)
			reqLogger.Info("remove leader ok", "time", i)
			// Step 4 Rebalance the cluster
			if k8sutils.RebalanceRedisCluster(instance) {
				reqLogger.Info("rebalance ok", "time", i)
				// update ready status ?
				// Step 5 Update Cluster Status: ReadyLeaderReplicas, ReadyFollowerReplicas
				k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterDownScale, status.DownScaleClusterReason, instance.Status.ReadyLeaderReplicas-1, instance.Status.ReadyFollowerReplicas-1)
				time.Sleep(time.Second * 5)
				err = r.Client.Get(context.TODO(), req.NamespacedName, instance)
				if err != nil {
					if errors.IsNotFound(err) {
						return ctrl.Result{}, nil
					}
					return ctrl.Result{}, err
				}
				reqLogger.Info("update status ok", "time", i, "readyLeader", instance.Status.ReadyLeaderReplicas, "readyFollower", instance.Status.ReadyFollowerReplicas)
			}
		}
		return ctrl.Result{RequeueAfter: time.Second * 100}, nil
	}

	// Mark the cluster status as initializing if there are no leader or follower nodes
	if instance.Status.ReadyLeaderReplicas == 0 && instance.Status.ReadyFollowerReplicas == 0 {
		err = k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterInitializing, status.InitializingClusterLeaderReason, 0, 0)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 10}, err
		}
	}

	err = k8sutils.CreateRedisLeader(instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}
	if leaderReplicas != 0 {
		err = k8sutils.CreateRedisLeaderService(instance)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
	}

	err = k8sutils.ReconcileRedisPodDisruptionBudget(instance, "leader", instance.Spec.RedisLeader.PodDisruptionBudget)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	redisLeaderInfo, err := k8sutils.GetStatefulSet(instance.Namespace, instance.ObjectMeta.Name+"-leader")
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	if int32(redisLeaderInfo.Status.ReadyReplicas) == leaderReplicas {

		// Mark the cluster status as initializing if there are no follower nodes
		if instance.Status.ReadyLeaderReplicas == 0 && instance.Status.ReadyFollowerReplicas == 0 {
			err = k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterInitializing, status.InitializingClusterFollowerReason, leaderReplicas, 0)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 10}, err
			}
		}

		err = k8sutils.CreateRedisFollower(instance)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
		// if we have followers create their service.
		if followerReplicas != 0 {
			err = k8sutils.CreateRedisFollowerService(instance)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 60}, err
			}
		}
		err = k8sutils.ReconcileRedisPodDisruptionBudget(instance, "follower", instance.Spec.RedisFollower.PodDisruptionBudget)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 60}, err
		}
	}
	redisFollowerInfo, err := k8sutils.GetStatefulSet(instance.Namespace, instance.ObjectMeta.Name+"-follower")
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Second * 60}, err
	}

	if leaderReplicas == 0 {
		reqLogger.Info("Redis leaders Cannot be 0", "Ready.Replicas", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Expected.Replicas", leaderReplicas)
		return ctrl.Result{RequeueAfter: time.Second * 60}, nil
	}

	if int32(redisLeaderInfo.Status.ReadyReplicas) != leaderReplicas && int32(redisFollowerInfo.Status.ReadyReplicas) != followerReplicas {
		reqLogger.Info("Redis leader and follower nodes are not ready yet", "Ready.Replicas", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Expected.Replicas", leaderReplicas)
		return ctrl.Result{RequeueAfter: time.Second * 60}, nil
	}

	// Mark the cluster status as bootstrapping if all the leader and follower nodes are ready
	if int32(redisLeaderInfo.Status.ReadyReplicas) == leaderReplicas && int32(redisFollowerInfo.Status.ReadyReplicas) == followerReplicas {
		if instance.Status.ReadyLeaderReplicas == leaderReplicas && instance.Status.ReadyFollowerReplicas == 0 {
			err = k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterBootstrap, status.BootstrapClusterReason, leaderReplicas, followerReplicas)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 10}, err
			}
		}
	}

	reqLogger.Info("Creating redis cluster by executing cluster creation commands", "Leaders.Ready", strconv.Itoa(int(redisLeaderInfo.Status.ReadyReplicas)), "Followers.Ready", strconv.Itoa(int(redisFollowerInfo.Status.ReadyReplicas)))
	reqLogger.Info(fmt.Sprintf("stspodipnamemap keys: %v", keys))

	// some pod was deleted, so the podIP changed, remove old podIP from cluster and add new podIP to cluster
	// 1.get all fail podIP, then itearate the list
	failPods, readyIP := k8sutils.GetAllFailedPods(instance)
	if len(failPods) != 0 {
		// 2.get podName from stsPodIPNameMap
		for _, v := range failPods {
			if k8sutils.StsPodIPNameMap[v.PodIP] == "" {
				reqLogger.Error(fmt.Errorf("%s", "podIP not exist in stsPodIPNameMap"), "podIP", v.PodIP, "nodeID", v.NodeID, "failPods", failPods)
			} else {
				// wait cluster failover automatically
				time.Sleep(20 * time.Second)
				// 3.get podIP now
				newPodIP := k8sutils.GetPodIP(instance.Namespace, k8sutils.StsPodIPNameMap[v.PodIP])
				// 4.remove old podIP
				if k8sutils.RemoveOldPodIP(v.NodeID, readyIP, instance) {
					time.Sleep(5 * time.Second)
					// 5.add new podIP as (leader/follower)'s replica
					k8sutils.AddNewPodIP(k8sutils.StsPodIPNameMap[v.PodIP], newPodIP, readyIP, instance)
					k8sutils.UpdatePodIPNameMap(instance, fMap)
				}
			}
		}
	}

	if k8sutils.CheckRedisNodeCount(instance, "") != totalReplicas {
		leaderCount := k8sutils.CheckRedisNodeCount(instance, "leader")
		if leaderCount != leaderReplicas {
			reqLogger.Info("Not all leader are part of the cluster...", "Leaders.Count", leaderCount, "Instance.Size", leaderReplicas)
			if leaderCount <= 2 {
				k8sutils.ExecuteRedisClusterCommand(instance)
			} else {
				if leaderCount < leaderReplicas {
					reqLogger.Info("Needs to scale up cluster", "current leader", leaderCount, "desire leader", leaderReplicas)
					// Scale up the cluster
					// Step 2 : Add Redis Node
					k8sutils.AddRedisNodeToCluster(instance)
					// Step 3 Rebalance the cluster using the empty masters
					k8sutils.RebalanceRedisClusterEmptyMasters(instance)
					k8sutils.UpdatePodIPNameMap(instance, fMap)
					// Update ready status
					k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterScaling, status.ScalingClusterReason, leaderCount+1, instance.Status.ReadyFollowerReplicas)
					// Check If there is No Empty Master Node
					k8sutils.CheckIfEmptyMasters(instance)
				}
			}
		} else {
			if followerReplicas > 0 && redisFollowerInfo.Status.ReadyReplicas == followerReplicas {
				reqLogger.Info("All leader are part of the cluster, adding follower/replicas", "Leaders.Count", leaderCount, "Instance.Size", leaderReplicas, "Follower.Replicas", followerReplicas)
				// Check If there is No Empty Master Node
				k8sutils.CheckIfEmptyMasters(instance)
				k8sutils.ExecuteRedisReplicationCommand(instance)
				k8sutils.UpdatePodIPNameMap(instance, fMap)
				// Update ready status
				k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterScaling, status.ScalingClusterReason, instance.Status.ReadyLeaderReplicas, followerReplicas)
			} else {
				reqLogger.Info("no follower/replicas configured, skipping replication configuration", "Leaders.Count", leaderCount, "Leader.Size", leaderReplicas, "Follower.Replicas", followerReplicas)
			}
		}
	} else {
		reqLogger.Info("Redis leader count is desired")
		if int(totalReplicas) > 1 && k8sutils.CheckRedisClusterState(instance) >= int(totalReplicas)-1 {
			reqLogger.Info("Redis leader is not desired, executing failover operation")
			err = k8sutils.ExecuteFailoverOperation(instance)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Second * 10}, err
			}
		}
	}

	// Check If there is No Empty Master Node
	if k8sutils.CheckRedisNodeCount(instance, "") == totalReplicas {
		k8sutils.CheckIfEmptyMasters(instance)
	}
	reqLogger.Info("Will reconcile redis cluster operator in again 10 seconds")

	// Mark the cluster status as ready if all the leader and follower nodes are ready
	if instance.Status.ReadyLeaderReplicas == leaderReplicas && instance.Status.ReadyFollowerReplicas == followerReplicas {
		err = k8sutils.UpdateRedisClusterStatus(instance, status.RedisClusterReady, status.ReadyClusterReason, leaderReplicas, followerReplicas)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Second * 10}, err
		}
	}

	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta2.RedisCluster{}).
		Complete(r)
}
