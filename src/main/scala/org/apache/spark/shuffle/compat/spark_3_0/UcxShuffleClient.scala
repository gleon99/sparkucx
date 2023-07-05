/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.nio.ByteBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBlockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport, mapId2PartitionId: Map[Long, Int]) extends BlockStoreClient with Logging {

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    // if (blockIds.length > 32) {
    //   val (b1, b2) = blockIds.splitAt(blockIds.length / 2)
    //   fetchBlocks(host, port, execId, b1, listener, downloadFileManager)
    //   fetchBlocks(host, port, execId, b2, listener, downloadFileManager)
    //   return
    // }

    val ucxBlockIds = Array.ofDim[UcxShuffleBlockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    var send = 0
    var receive = 0
    for (i <- blockIds.indices) {
      send = send + 1
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, mapId2PartitionId(blockId.mapId), blockId.reduceId)
      callbacks(i) = (result: OperationResult) => {
        val memBlock = result.getData
        receive = receive + 1
        val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
        listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
          override def release: ManagedBuffer = {
            memBlock.close()
            this
          }
        })
      }
      val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
      transport.fetchBlocksByBlockIds(execId.toLong, Array(ucxBlockIds(i)), resultBufferAllocator, Array(callbacks(i)))
    }

    while (send != receive) {
      transport.progress()
    }
  }

  override def close(): Unit = {

  }
}
