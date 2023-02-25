/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx;


import java.util.Map;
import java.util.Optional;

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;
// import org.apache.spark.shuffle.ucx.DpuShuffleMapOutputWriter;
import org.apache.spark.shuffle.ucx;

// temp
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter};

class DpuShuffleExecutorComponents(val sparkConf: SparkConf) extends ShuffleExecutorComponents with Logging {
    logDebug("LEO DpuShuffleExecutorComponents constructor");
//   private final SparkConf sparkConf;
//   private var blockManager = null;
  private var blockResolver: IndexShuffleBlockResolver = null;

//   DpuShuffleExecutorComponents {
//     this.sparkConf = sparkConf;
//   }

//   public DpuShuffleExecutorComponents(
//       SparkConf sparkConf,
//       BlockManager blockManager,
//       IndexShuffleBlockResolver blockResolver) {
//     this.sparkConf = sparkConf;
//     this.blockManager = blockManager;
//     this.blockResolver = blockResolver;
//   }

  override def initializeExecutor(appId: String, execId: String, extraConfigs: Map[String, String]) = {
    logDebug("LEO DpuShuffleExecutorComponents initializeExecutor");
    // String appId, String execId, Map<String, String> extraConfigs) = {
    val blockManager = SparkEnv.get.blockManager;
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int) = {
    // throw new UnsupportedOperationException("Not implemented for DPU");
    logDebug("LEO DpuShuffleExecutorComponents createMapOutputWriter");
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    new DpuShuffleMapOutputWriter(shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long) = {
    Optional.empty();
    // None
//     logDebug("LEO DpuShuffleExecutorComponents createSingleFileMapOutputWriter");
//     if (blockResolver == null) {
//       throw new IllegalStateException(
//           "Executor components must be initialized before getting writers.");
//     }
//     Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
   }
}