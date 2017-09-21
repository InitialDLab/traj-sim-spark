package edu.utah.cs.trajectory

import edu.utah.cs.index.RTree
import edu.utah.cs.partitioner.STRTrajPartition
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by dongx on 1/16/2017.
  */
object TrajIndexing {
  final val max_entries_per_node = 25
  //final val k_values = Array(1, 10, 30, 50, 70, 100)
  final val k_values = Array(10)
  //final val k = 10
  final val N = 1401138 
  final val c = 5
  //final val c_values = Array(1, 3, 5, 7, 10)
  //final val c_values = Array(5)

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def getMBR(x: (Int, Array[(Int, LineSegment)])): MBR = {
    val pts = x._2.flatMap(p => Array(p._2.start, p._2.end))
    var maxx = Double.MinValue
    var maxy = Double.MinValue
    var minx = Double.MaxValue
    var miny = Double.MaxValue
    pts.foreach(x => {
      maxx = Math.max(x.coord(0), maxx)
      maxy = Math.max(x.coord(1), maxy)
      minx = Math.min(x.coord(0), minx)
      miny = Math.min(x.coord(1), miny)
    })
    MBR(Point(Array(minx, miny)), Point(Array(maxx, maxy)))
  }

  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("TrajIndexing").set("spark.locality.wait", "0")
      .set("spark.driver.maxResultSize", "4g")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: TrajIndexing <query_traj_filename> <traj_data_filename>")
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
      }).toArray.groupBy(_._1).map(now => (getMBR(now), (now._1, now._2.sortBy(_._1).map(_._2)))).iterator
    })

    val partitioned_traj = STRTrajPartition(trajs, trajs.partitions.length, 0.01, max_entries_per_node)
    //val partitioned_traj = STRTrajPartition(trajs, trajs.partitions.length, 0.01, max_entries_per_node)

    val indexed_traj = partitioned_traj.mapPartitions(iter => {
      val data = iter.toArray
      var index: RTree = null
      if (data.length > 0) {
        index = RTree(data.zipWithIndex.map(x => (x._1._1, x._2, x._1._2._1)), 25)
      }
      Array((data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stat = indexed_traj.mapPartitions(iter => iter.map(x => (x._2.root.m_mbr, x._1.length))).collect()
    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2)), max_entries_per_node)

    val end1 = System.currentTimeMillis()
    println("------------------------------------------------------------")
    println("Time to build indexes: " + (end1 - start1) / 1000.0)
    println("------------------------------------------------------------")

    k_values.foreach(k => {
      var tot_time = 0.0
      queries.foreach(query_traj => {
        val start2 = System.currentTimeMillis()
        println("------------------------------------------------------------")
        val bc_query = sc.broadcast(query_traj)
        val global_intersect = global_rtree.circleRange(query_traj, 0.0).map(_._2).toSet
        //val c = global_intersect.size
        println("Going to Sample:" + (c * k))
        val sample_set = new PartitionPruningRDD(indexed_traj, global_intersect.contains).flatMap(_._1)
          .takeSample(withReplacement = false, c * k, System.currentTimeMillis())

        val pruning_bound = sc.parallelize(sample_set, Math.min(c * k, sc.defaultParallelism))
          .map(x => Trajectory.discreteFrechetDistance(x._2, bc_query.value)).collect().sorted.take(k).last
          //.map(x => Trajectory.hausdorffDistance(x._2, bc_query.value)).collect().sorted.take(k).last
        val end2 = System.currentTimeMillis()

        println("Time to calculate pruning bound: " + (end2 - start2) / 1000.0)
        println("The pruning bound is: " + pruning_bound)

        val start3 = System.currentTimeMillis()
        val bc_pruning_bound = sc.broadcast(pruning_bound)
        val global_prune_set = global_rtree.circleRange(query_traj, pruning_bound).map(_._2).toSet

        val pruned_rdd = new PartitionPruningRDD(indexed_traj, global_prune_set.contains)
        val filtered = pruned_rdd.flatMap(part => part._2.circleRange(bc_query.value, bc_pruning_bound.value)
          .map(x => part._1(x._2)))
        val res = filtered.repartition(Math.max(sc.defaultParallelism, filtered.partitions.length))
          .map(x => (Trajectory.discreteFrechetDistance(bc_query.value, x._2), x._1))
          //.map(x => (Trajectory.hausdorffDistance(bc_query.value, x._2), x._1))
          .takeOrdered(k)(new ResultOrdering)

        val end3 = System.currentTimeMillis()
        println("# distance calculated: " + (filtered.count() + c * k))
        println("Time to calculate Finalize Result: " + (end3 - start3) / 1000.0)
        println("Total Latency: " + ((end3 - start2) / 1000.0))
        println("The results show as below:")
        res.foreach(println)
        println("------------------------------------------------------------")
        tot_time += (end3 - start2) / 1000.0
        bc_query.destroy()
        bc_pruning_bound.destroy()
      })

      println("Average Latency for k = " + k + " is : " + (tot_time / 100.0))
      println("===================================================")
    })

    sc.stop()
  }
}
