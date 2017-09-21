package edu.utah.cs.partitioner

import edu.utah.cs.spatial.{LineSegment, MBR}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.collection.mutable

/**
  * Created by dongx on 1/16/2017.
  */
object STRTrajPartition {
  def apply(origin: RDD[(MBR, (Int, Array[LineSegment]))], est_partition: Int,
            sample_rate: Double, max_entries_per_node: Int)
  : RDD[(MBR, (Int, Array[LineSegment]))] = {
    val part = new STRMBRPartitioner(est_partition, sample_rate, max_entries_per_node, origin)
    val shuffled = new ShuffledRDD[MBR, (Int, Array[LineSegment]), (Int, Array[LineSegment])](origin, part)
    shuffled
  }
}