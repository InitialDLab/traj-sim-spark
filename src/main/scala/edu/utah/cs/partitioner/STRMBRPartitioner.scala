package edu.utah.cs.partitioner

import edu.utah.cs.index.RTree
import edu.utah.cs.spatial.{MBR, Point}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.collection.mutable

/**
  * Created by Dong Xie on 10/24/2016.
  */
object STRMBRPartition {
  def apply(origin: RDD[(MBR, Int)], est_partition: Int,
            sample_rate: Double, max_entries_per_node: Int): RDD[(MBR, Int)] = {
    val part = new STRMBRPartitioner(est_partition, sample_rate, max_entries_per_node, origin)
    val shuffled = new ShuffledRDD[MBR, Int, Int](origin, part)
    shuffled
  }
}


class STRMBRPartitioner(est_partition: Int,
                        sample_rate: Double,
                        max_entries_per_node: Int,
                        rdd: RDD[_ <: Product2[MBR, Any]])
  extends Partitioner {

  def numPartitions: Int = partitions

  private case class Bounds(min: Array[Double], max: Array[Double])

  var (partBound, partitions) = {
    val data_bounds = {
      rdd.aggregate[Bounds](null)((bound, data) => {
        if (bound == null) {
          Bounds(data._1.low.coord, data._1.high.coord)
        } else {
          Bounds(bound.min.zip(data._1.low.coord).map(x => Math.min(x._1, x._2)),
            bound.max.zip(data._1.high.coord).map(x => Math.max(x._1, x._2)))
        }
      }, (left, right) => {
        if (left == null) right
        else if (right == null) left
        else {
          Bounds(left.min.zip(right.min).map(x => Math.min(x._1, x._2)),
            left.max.zip(right.max).map(x => Math.max(x._1, x._2)))
        }
      })
    }

    val seed = System.currentTimeMillis()
    val sampled = rdd.sample(withReplacement = false, sample_rate, seed).map(_._1).collect()

    val dim = new Array[Int](2)
    var remaining = est_partition.toDouble
    for (i <- 0 until 2) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (2 - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupSegment(entries: Array[MBR], now_min: Array[Double],
                              now_max: Array[Double], cur_dim: Int, until_dim: Int): Array[MBR] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_.centroid.coord(cur_dim) < _.centroid.coord(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      val flag = 1 << cur_dim
      var ans = mutable.ArrayBuffer[MBR]()
      if (cur_dim < until_dim) {
        for (i <- grouped.indices) {
          val cur_min = now_min
          val cur_max = now_max
          if (i == 0 && i == grouped.length - 1) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            cur_min(cur_dim) = data_bounds.min(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.centroid.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            cur_min(cur_dim) = grouped(i).head.centroid.coord(cur_dim)
            cur_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            cur_min(cur_dim) = grouped(i).head.centroid.coord(cur_dim)
            cur_max(cur_dim) = grouped(i + 1).head.centroid.coord(cur_dim)
          }
          ans ++= recursiveGroupSegment(grouped(i), cur_min, cur_max, cur_dim + 1, until_dim)
        }
        ans.toArray
      } else {
        for (i <- grouped.indices) {
          if (i == 0 && i == grouped.length - 1) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else if (i == 0) {
            now_min(cur_dim) = data_bounds.min(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.centroid.coord(cur_dim)
          } else if (i == grouped.length - 1) {
            now_min(cur_dim) = grouped(i).head.centroid.coord(cur_dim)
            now_max(cur_dim) = data_bounds.max(cur_dim)
          } else {
            now_min(cur_dim) = grouped(i).head.centroid.coord(cur_dim)
            now_max(cur_dim) = grouped(i + 1).head.centroid.coord(cur_dim)
          }
          ans += MBR(Point(now_min.clone()), Point(now_max.clone()))
        }
        ans.toArray
      }
    }

    val cur_min = new Array[Double](2)
    val cur_max = new Array[Double](2)
    val mbrs = recursiveGroupSegment(sampled, cur_min, cur_max, 0, 1)

    (mbrs.zipWithIndex, mbrs.length)
  }

  private val rt = RTree.applyMBR(partBound.map(x => (x._1, x._2, 1)), max_entries_per_node)

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[MBR]

    rt.circleRange(k.centroid, 0.0).head._2
  }
}
