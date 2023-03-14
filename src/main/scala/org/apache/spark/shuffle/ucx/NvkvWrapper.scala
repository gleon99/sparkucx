/*
 * Copyright (C) 2023, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

package org.apache.spark.shuffle.ucx;

import org.openucx.jnvkv.Nvkv;
import org.apache.spark.internal.Logging;
import org.apache.spark.SparkConf;


 class NvkvWrapper(execId: String, sparkConf: SparkConf) extends Logging {
    val execIdInt = execId.toInt
    val ibDevicesStr = sparkConf.get("spark.shuffle.ucx.ib_devices", "mlx5_0");
    val ibDevicesArr = ibDevicesStr.split(",");
    val ibDevice = ibDevicesArr(execIdInt % ibDevicesArr.length)

    val coreMask = sparkConf.get("spark.shuffle.ucx.core_mask", "0x1");

    val nmveDevicesStr = sparkConf.get("spark.shuffle.ucx.nvme_devices", "41:00.0");
    val nmveDevicesArr = nmveDevicesStr.split(",");
    val nvmeDevice = nmveDevicesArr(execIdInt % nmveDevicesArr.length)
    logDebug("LEO NvkvWrapper ctor; execId: " + execIdInt +
                  " ib_device: " + ibDevice + " coreMask: " + coreMask + " nvme_device: " + nvmeDevice);

    Nvkv.init(ibDevice, execIdInt, coreMask);
    logDebug("Nvkv.init done");

    var ds = Array ( new Nvkv.DataSet(nvmeDevice, execIdInt) );
    var nvkvContext = Nvkv.open(ds , Nvkv.LOCAL);
    logDebug("Nvkv.open done");

 }