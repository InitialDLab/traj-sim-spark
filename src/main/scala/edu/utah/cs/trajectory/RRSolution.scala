package edu.utah.cs.trajectory

import edu.utah.cs.index.RTree
import edu.utah.cs.index_rr.RTreeWithRR
import edu.utah.cs.partitioner.STRSegPartition
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable
import scala.io.Source

/**
  * Created by dongx on 12/19/2016.
  */
object RRSolution {
  final val max_entries_per_node = 25
  final val k = 10
  final val c = 5
  //final val max_spatial_span = 2.550598
  //final val max_spatial_span = 0.46757
  final val max_spatial_span = 0.5080

  def getMBR(x: Array[LineSegment]): MBR = {
    val pts = x.flatMap(p => Array(p.start, p.end))
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

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RRSolution")
      .set("spark.locality.wait", "0").set("spark.driver.maxResultSize", "4g")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: RRSolution <query_traj_filename> <traj_data_filename>")
      System.exit(1)
    }

    Thread.sleep(6000)

    val query_traj_filename = args(0)
    val traj_data_filename = args(1)

    val query_traj_file = Source.fromFile(query_traj_filename)
    val queries = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      (splitted(0).toInt, LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
    }.toArray.groupBy(_._1).map(x => x._2.map(_._2))

    val start1 = System.currentTimeMillis()

    val dataRDD = sc.textFile(traj_data_filename)
      .map(x => x.split('\t'))
      .map(x => (LineSegment(Point(Array(x(1).toDouble, x(2).toDouble)),
        Point(Array(x(3).toDouble, x(4).toDouble))),
        TrajMeta(x(0).toInt, x(5).toInt)))

    val (partitioned_rdd, _) = STRSegPartition(dataRDD, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val indexed_seg_rdd = partitioned_rdd.mapPartitions(iter => {
      val data = iter.toArray
      var index: RTreeWithRR = null
      if (data.length > 0) {
        index = RTreeWithRR(data.zipWithIndex.map(x => (x._1._1, x._2, x._1._2.traj_id)), max_entries_per_node)
      }
      Array((data, index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stat = indexed_seg_rdd.mapPartitions(iter => iter.map(x => (x._2.root.m_mbr, x._1.length, x._2.root.rr))).collect()

    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2.toInt)), max_entries_per_node)

    val end1 = System.currentTimeMillis()
    println("------------------------------------------------------------")
    println("Time to build indexes: " + (end1 - start1) / 1000.0)
    println("------------------------------------------------------------")

    var tot_time = 0.0
    queries.foreach(query_traj => {
      val start2 = System.currentTimeMillis()
      val bc_query = sc.broadcast(query_traj)
      val global_intersect = global_rtree.circleRange(query_traj, 0.0)
      val global_intersect_mbrs = global_intersect.map(_._1.asInstanceOf[MBR])

      val sample_base = global_intersect.aggregate(new RoaringBitmap())((a, b) => RoaringBitmap.or(a, stat(b._2)._3),
        (a, b) => RoaringBitmap.or(a, b))

      val cards = sample_base.getCardinality
      println("Cardinality of intersected Partitions: " + cards)
      val n_samples = c * k
      println("Going to sample: " + n_samples)
      assert(cards >= k)

      val set = mutable.HashSet[Int]()
      val rnd = scala.util.Random

      for (i <- 0 until n_samples) {
        var x = rnd.nextInt(cards)
        while (set.contains(x)) x = rnd.nextInt(cards)
        set += x
      }

      var i = 0
      val samples = mutable.HashSet[Int]()
      val iter = sample_base.iterator()
      while (iter.hasNext) {
        val x = iter.next()
        if (set.contains(i)) samples += x
        i = i + 1
      }

      val bc_samples = sc.broadcast(samples.toSet)

      val pruning_bound_filter = global_rtree.circleRange(global_intersect_mbrs, max_spatial_span).map(_._2).toSet
      val pruning_bound = new PartitionPruningRDD(indexed_seg_rdd, pruning_bound_filter.contains)
        .flatMap(x => x._1.filter(now => samples.contains(now._2.traj_id)).map(x => x._2.traj_id -> x._1))
        .groupByKey(Math.min(samples.size, sc.defaultParallelism))
        .map(x => Trajectory.hausdorffDistance(bc_query.value, x._2.toArray)).takeOrdered(k).last

      val end2 = System.currentTimeMillis()

      println("------------------------------------------------------------")
      println("Time to calculate pruning bound: " + (end2 - start2) / 1000.0)
      println("The pruning bound is: " + pruning_bound)

      val start3 = System.currentTimeMillis()
      val global_prune = global_rtree.circleRange(query_traj, pruning_bound)
      val global_prune_set = global_prune.map(_._2).toSet

      val pruned_rdd = new PartitionPruningRDD(indexed_seg_rdd, global_prune_set.contains)
      val pruned_traj_id1 = stat.zipWithIndex.filter(x => !global_prune_set.contains(x._2)).map(_._1._3)
        .aggregate(new RoaringBitmap())((a, b) => RoaringBitmap.or(a, b), (a, b) => RoaringBitmap.or(a, b))

      val bc_pruning_bound = sc.broadcast(pruning_bound)
      val saved_traj_local = pruned_rdd.map(part => {
        RoaringBitmap.andNot(part._2.root.rr, part._2.antiCircleRangeBF(bc_query.value, bc_pruning_bound.value))
      }).reduce((a, b) => RoaringBitmap.or(a, b))

      val saved_traj = RoaringBitmap.andNot(saved_traj_local, pruned_traj_id1)

      val end3 = System.currentTimeMillis()

      println("Time to calculate all saved traj_ids: " + (end3 - start3) / 1000.0)

      val start4 = System.currentTimeMillis()
      val bc_saved_traj = sc.broadcast(saved_traj_local.toArray)

      val final_filter_set = global_rtree.circleRange(global_prune.map(_._1.asInstanceOf[MBR]), max_spatial_span)
        .map(_._2).toSet

      val final_filtered = new PartitionPruningRDD(indexed_seg_rdd, final_filter_set.contains)
        .flatMap(x => {
          x._1.filter(now => bc_saved_traj.value.contains(now._2.traj_id)).map(x => x._2.traj_id -> x._1)
        })

      val res = final_filtered.groupByKey(sc.defaultParallelism)
        .map(x => (Trajectory.hausdorffDistance(bc_query.value, x._2.toArray), x._1))
        .takeOrdered(k)(new ResultOrdering)

      val end4 = System.currentTimeMillis()
      tot_time += (end4 - start2) / 1000.0
      println("Time to finish the final filter: " + (end4 - start4) / 1000.0)
      println("# of distance calculated: " + (c * k + saved_traj.getCardinality))
      println("Total Latency: " + ((end4 - start2) / 1000.0))
      println("The results show as below:")
      res.foreach(println)
      println("------------------------------------------------------------")
      tot_time += (end4 - start2) / 1000.0
    })

    printf("Average Latency: " + (tot_time / 100.0))

    sc.stop()
  }
}
