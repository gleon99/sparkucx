/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer

import org.apache.spark.TaskContext
import org.apache.spark.storage._
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.shuffle.ucx.{UcxShuffleTransport, CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}


/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  // TODO: Init UCX worker here; Is this a singleton?
  extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  val shuffleManager: CommonUcxShuffleManager = ucxShuffleManager


  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    // In Spark-3.0 MapId is long and unique among all jobs in spark. We need to use partitionId as offset
    // in metadata buffer
    val partitionId = TaskContext.getPartitionId()
    val dataFile = getDataFile(shuffleId, mapId)
    if (!dataFile.exists() || dataFile.length() == 0) {
      return
    }
    writeIndexFileAndCommitCommon(shuffleId, partitionId, lengths, new RandomAccessFile(dataFile, "r"))
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    // var expected: ManagedBuffer = super.getBlockData(blockId, dirs)

    logDebug("LEO UcxShuffleBlockResolver getBlockData")
    val ucxTransport: UcxShuffleTransport = shuffleManager.ucxTransport
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
     }

    var length = ucxTransport.getNvkvHandler.getPartitonLength(shuffleId, mapId, startReduceId).toInt
    var offset = ucxTransport.getNvkvHandler.getPartitonOffset(shuffleId, mapId, startReduceId)
    logDebug(s"LEO UcxShuffleBlockResolver shuffleId $shuffleId mapId $mapId startReduceId $startReduceId endReduceId $endReduceId offset $offset length $length")
    val resultBuffer: ByteBuffer = ucxTransport.getNvkvHandler.read(length, offset)

    new NioManagedBuffer(resultBuffer)
  }
}
