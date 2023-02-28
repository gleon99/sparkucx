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
    
    override def initializeExecutor(appId: String, execId: String, extraConfigs: Map[String, String]) = {
    logDebug("LEO NvkvShuffleExecutorComponents initializeExecutor; execId: " + execId);

    val ib_devices_str = sparkConf.get("spark.shuffle.ucx.ib_devices", "mlx5_0");
    val ib_devices_arr = ib_devices_str.split(",");
    val ib_device = ib_devices_arr(execId.toInt % ib_devices_arr.length)

    val coreMask = sparkConf.get("spark.shuffle.ucx.core_mask", "0x1");

    val nmve_devices_str = sparkConf.get("spark.shuffle.ucx.nvme_devices", "41:00.0");
    val nmve_devices_arr = nmve_devices_str.split(",");
    val nvme_device = nmve_devices_arr(execId.toInt % nmve_devices_arr.length)

    logDebug("LEO ib_device: " + ib_device + " coreMask: " + coreMask + " nvme_device: " + nvme_device);
    Nvkv.init(ib_device, 1, coreMask);

    var  ds = Array ( new Nvkv.DataSet(nvme_device, 1) );

    var nvkvContext = Nvkv.open(ds , Nvkv.LOCAL);

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