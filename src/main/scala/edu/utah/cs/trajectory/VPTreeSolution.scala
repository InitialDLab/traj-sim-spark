package edu.utah.cs.trajectory

import edu.utah.cs.index.VPTree
import edu.utah.cs.partitioner.IDPartition
import edu.utah.cs.spatial.{LineSegment, Point}
import edu.utah.cs.util.MetricObject
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.Random

/**
  * Created by dongx on 2/1/17.
  */
object VPTreeSolution {
  final val max_entries_per_node = 25
  //final val k_values = Array(10, 30, 50, 70, 100)
  final val k = 10
  final val c = 5

  private case class VPTraj(id: Int, data: Array[LineSegment]) extends MetricObject {
    override def distance(o: MetricObject): Double = {
      Trajectory.hausdorffDistance(data, o.asInstanceOf[VPTraj].data)
    }
  }

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  private def shuffle[T](data: Array[T]) = {
    var i = 0
    val n = data.length
    while (i < n - 1) {
      val tmp = i + Random.nextInt(n - i)
      var t = data(i)
      data(i) = data(tmp)
      data(tmp) = data(i)
      i += 1
    }
  }

  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("VPTreeSolution")
      .set("spark.locality.wait", "0").set("spark.driver.maxResultSize", "4g")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: VPTreeSolution <query_traj_filename> <traj_data_filename>")
      System.exit(1)
    }

    val query_traj_filename = args(0)
    val traj_data_filename = args(1)

    val query_traj_file = Source.fromFile(query_traj_filename)
    val queries = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      (splitted(0).toInt, LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
    }.toArray.groupBy(_._1).map(x => x._2.map(_._2))

    Thread.sleep(6000)

    val start1 = System.currentTimeMillis()

    val trajs = sc.textFile(traj_data_filename).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
      }).toArray.groupBy(_._1).map(now => VPTraj(now._1, now._2.map(_._2))).iterator
    })

    val pivots = trajs.takeSample(withReplacement = false, trajs.partitions.length, System.currentTimeMillis()).map(_.data)
    val bc_pivots = sc.broadcast(pivots)
    val bc_pivots_vptree = sc.broadcast(VPTree(pivots.zipWithIndex.map(x => VPTraj(x._2, x._1))))
    val traj_with_pivot = trajs.mapPartitions(iter => {
      iter.map(x => {
        (bc_pivots_vptree.value.knn(x, 1)._1.head._1.id, x)
      })
    })
    val parted_by_pivot = IDPartition(traj_with_pivot, pivots.length)
    val indexed = parted_by_pivot.mapPartitionsWithIndex((id, iter) => {
      val data = iter.map(_._2.asInstanceOf[VPTraj]).toArray
      val pivot = bc_pivots.value(id)
      val cover_radius = data.map(x => Trajectory.hausdorffDistance(x.data, pivot)).max
      val vp_tree = VPTree(data)
      Array((pivot, cover_radius, data.length, vp_tree)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stats = indexed.map(x => (x._1, x._2, x._3)).collect()
      .zipWithIndex.map(x => (x._1._1, x._1._2, x._1._3, x._2))

    val end1 = System.currentTimeMillis()
    println("Time to build index: " + ((end1 - start1) / 1000.0))


    var tot_time = 0.0
    queries.foreach(query => {
      val start2 = System.currentTimeMillis()
      println("----------------------------------------------")
      val sorted_pivots = stats.map(x => (Trajectory.hausdorffDistance(x._1, query), x._2, x._3, x._4)).sortBy(_._1)
      var i = 0
      var sum = 0
      while (sum < k) {
        sum +=  sorted_pivots(i)._3
        i += 1
      }

      val prune_set = sorted_pivots.slice(0, i).map(_._4).toSet
      val bc_query = sc.broadcast(query)
      val bc_k = sc.broadcast(k)
//      val first_filter = new PartitionPruningRDD(indexed, prune_set.contains)
//        .flatMap(i_part => {
//          i_part._4.knn(VPTraj(0, bc_query.value), bc_k.value)._1.map(x => (x._2, x._1.id))
//        }).takeOrdered(k)(new ResultOrdering)

      val first_filter = new PartitionPruningRDD(indexed, prune_set.contains)
        .aggregate((Array[(Double, Int)](), 0))((now, part) => {
          val knn_res = part._4.knn(VPTraj(0, bc_query.value), bc_k.value)
          ((knn_res._1.map(x => (x._2, x._1.id)) ++ now._1).sortBy(_._1).take(bc_k.value), now._2 + knn_res._2)
        }, (left, right) => {
          ((left._1 ++ right._1).sortBy(_._1).take(bc_k.value), left._2 + right._2)
        })

      val tick1 = System.currentTimeMillis()
      println("Time for first filter: " + ((tick1 - start2) / 1000.0))

      val pruning_bound = first_filter._1.last._1
      val global_prune_set =
        sorted_pivots.filter(x => x._1 - x._2 <= pruning_bound).map(_._4).toSet -- prune_set
      val bc_pruning_bound = sc.broadcast(pruning_bound)

//      val second_filter = new PartitionPruningRDD(indexed, global_prune_set.contains)
//        .flatMap(i_part => {
//          i_part._4.knn(VPTraj(0, bc_query.value), k, bc_pruning_bound.value)._1.map(x => (x._2, x._1.id))
//        }).takeOrdered(k)(new ResultOrdering)

      val second_filter = new PartitionPruningRDD(indexed, global_prune_set.contains)
        .aggregate((Array[(Double, Int)](), 0))((now, part) => {
          val knn_res = part._4.knn(VPTraj(0, bc_query.value), bc_k.value, bc_pruning_bound.value)
          ((knn_res._1.map(x => (x._2, x._1.id)) ++ now._1).sortBy(_._1).take(bc_k.value), now._2 + knn_res._2)
        }, (left, right) => {
          ((left._1 ++ right._1).sortBy(_._1).take(bc_k.value), left._2 + right._2)
        })

      val final_res = (first_filter._1 ++ second_filter._1).sortBy(_._1).take(k)

      val end2 = System.currentTimeMillis()
      println("Time for second filter and final merge: " + ((end2 - tick1) / 1000.0))
      println("# of trajs checked distance:" + (first_filter._2 + second_filter._2 + pivots.length))
      println("Total Latency: " + ((end2 - start2) / 1000.0))
      final_res.foreach(println)
      tot_time += (end2 - start2) / 1000.0
      println("----------------------------------------------")
      bc_k.destroy()
      bc_query.destroy()
      bc_pruning_bound.destroy()
    })

    println("Average Latency: " + (tot_time / 100.0))

    bc_pivots.destroy()
    bc_pivots_vptree.destroy()
    sc.stop()
  }
}
