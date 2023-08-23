/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.ucx.BlockId
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

object UcxRpcMessages {
  /**
   * Called from executor to request Nvkv lock.
   */
  case class NvkvRequestLock(executorId: Long, execEp: RpcEndpointRef)
  
  /**
   * Called from executor to release Nvkv lock.
   */
  case class NvkvReleaseLock(executorId: Long)

  /**
   * Reply from driver when lock is given to pending executer.
   */
  case class NvkvLock(executerLocalId: Int)
  
  /**
   * Called from executor to driver, to introduce dpu address.
   */
  case class ExecutorAdded(executorId: Long, endpoint: RpcEndpointRef,
                           dpuSockAddress: SerializableDirectBuffer)

  /**
   * Reply from driver with all executors in the cluster with their worker addresses.
   */
  case class IntroduceAllExecutors(executorIdToAddress: Map[Long, SerializableDirectBuffer])
}
