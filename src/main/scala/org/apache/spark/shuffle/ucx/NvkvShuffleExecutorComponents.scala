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
import org.apache.spark.shuffle.ucx;
import org.apache.spark.shuffle.sort.io.{LocalDiskShuffleMapOutputWriter, LocalDiskSingleSpillMapOutputWriter};

import org.openucx.jnvkv.Nvkv;

class NvkvShuffleExecutorComponents(val sparkConf: SparkConf) extends ShuffleExecutorComponents with Logging {
    logDebug("LEO NvkvShuffleExecutorComponents constructor");
    
    private var blockResolver: IndexShuffleBlockResolver = null;
    private var nvkvWrapper: NvkvWrapper = null;
    
    override def initializeExecutor(appId: String, execId: String, extraConfigs: Map[String, String]) = {

   

    nvkvWrapper = new NvkvWrapper(execId, sparkConf)
    
    // .get("spark.shuffle.ucx.ib_devices", "mlx5_0"),
    //                               sparkConf.get("spark.shuffle.ucx.core_mask", "0x1"),
    //                               sparkConf.get("spark.shuffle.ucx.nvme_devices", "41:00.0"));

    val blockManager = SparkEnv.get.blockManager;
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int) = {
    logDebug("LEO NvkvShuffleExecutorComponents createMapOutputWriter");
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    new NvkvShuffleMapOutputWriter(shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long) = {
    Optional.empty();

   }
}