/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.blaze.plan

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.sql.blaze._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.util.SerializableConfiguration
import org.blaze.{protobuf => pb}

import java.net.URI
import java.security.PrivilegedExceptionAction
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

abstract class NativeBatchScanBase(batchScanExec: BatchScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq :+
      ("predicate_evaluation_errors", SQLMetrics
        .createMetric(sparkContext, "Native.predicate_evaluation_errors")) :+
      ("row_groups_pruned", SQLMetrics
        .createMetric(sparkContext, "Native.row_groups_pruned")) :+
      ("bytes_scanned", SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_scanned")) :+
      ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time")) :+
      ("io_time_getfs", SQLMetrics
        .createNanoTimingMetric(sparkContext, "Native.io_time_getfs")): _*)
  override val output: Seq[Attribute] = batchScanExec.output
  override val outputPartitioning: Partitioning = batchScanExec.outputPartitioning

  protected val inputFileScanRDD: FileScanRDD = {
    MethodUtils.invokeMethod(batchScanExec, true, "prepare")
    MethodUtils.invokeMethod(batchScanExec, true, "waitForSubqueries")
    batchScanExec.inputRDDs().head match {
      case rdd: FileScanRDD => rdd
      case rdd: MapPartitionsRDD[_, _] => rdd.prev.asInstanceOf[FileScanRDD]
    }
  }

  private val partitionSchema = batchScanExec.scan match {
    case fileScan: FileScan => fileScan.readPartitionSchema
    case _ => new StructType()
  }

  protected def getDataSchema: StructType = batchScanExec.scan match {
    case fileScan: FileScan => fileScan.readDataSchema
    case _ => new StructType()
  }


  private val fileSizes = inputFileScanRDD.filePartitions
    .flatMap(_.files)
    .groupBy(_.filePath)
    .mapValues(_.map(_.length).sum)
    .map(identity) // make this map serializable

  protected def nativePruningPredicateFilters: Seq[pb.PhysicalExprNode] =
    batchScanExec.runtimeFilters
      .map(expr => NativeConverters.convertScanPruningExpr(expr))

  protected def nativeFileSchema: pb.Schema =
    NativeConverters.convertSchema(getDataSchema)

  protected def nativePartitionSchema: pb.Schema =
    NativeConverters.convertSchema(partitionSchema)

  protected def nativeFileGroups: FilePartition => pb.FileGroup = (partition: FilePartition) => {
    // list input file statuses
    val nativePartitionedFile = (file: PartitionedFile) => {
      val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
        NativeConverters.convertValue(
          file.partitionValues.get(index, field.dataType),
          field.dataType)
      }
      pb.PartitionedFile
        .newBuilder()
        .setPath(s"${file.filePath}")
        .setSize(fileSizes(file.filePath))
        .addAllPartitionValues(nativePartitionValues.asJava)
        .setLastModifiedNs(0)
        .setRange(
          pb.FileRange
            .newBuilder()
            .setStart(file.start)
            .setEnd(file.start + file.length)
            .build())
        .build()
    }
    pb.FileGroup
      .newBuilder()
      .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
      .build()
  }

  // check whether native converting is supported
  nativePruningPredicateFilters
  nativeFileSchema
  nativePartitionSchema
  nativeFileGroups

  protected def putJniBridgeResource(
      resourceId: String,
      broadcastedHadoopConf: Broadcast[SerializableConfiguration]): Unit = {
    val sharedConf = broadcastedHadoopConf.value.value
    JniBridge.resourcesMap.put(
      resourceId,
      (location: String) => {
        val getFsTimeMetric = metrics("io_time_getfs")
        val currentTimeMillis = System.currentTimeMillis()
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = {
            FileSystem.get(new URI(location), sharedConf)
          }
        })
        getFsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
        fs
      })
  }

  protected def broadcastedHadoopConf: Broadcast[SerializableConfiguration] = {
    val sparkSession = Shims.get.getSqlContext(batchScanExec).sparkSession
    val hadoopConf =
      sparkSession.sessionState.newHadoopConf()
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override protected def doCanonicalize(): SparkPlan = batchScanExec.canonicalized
}
