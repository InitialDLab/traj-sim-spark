package edu.utah.cs.trajectory
import edu.utah.cs.index.RTree
import edu.utah.cs.index_bf.RTreeWithBF
import edu.utah.cs.partitioner.{STRSegPartition, STRTrajPartition}
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import edu.utah.cs.util._
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by dongx on 9/6/16.
  * Line Segment Trajectory Storage
  */
object BFDISolution {
  final val max_entries_per_node = 25
  final val k = 10
  final val c = 5

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

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

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BFDISolution").set("spark.locality.wait", "0")
      .set("spark.driver.maxResultSize", "4g")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: BloomFilterSolution <query_traj_filename> <traj_data_filename>")
      System.exit(1)
    }

    Thread.sleep(3000)

    val query_traj_filename = args(0)
    val traj_data_filename = args(1)

    val start1 = System.currentTimeMillis()

    val dataRDD = sc.textFile(traj_data_filename)
      .map(x => x.split('\t'))
      .map(x => (LineSegment(Point(Array(x(1).toDouble, x(2).toDouble)),
        Point(Array(x(3).toDouble, x(4).toDouble))),
        TrajMeta(x(0).toInt, x(5).toInt)))

    val trajs = sc.textFile(traj_data_filename).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
      }).toArray.groupBy(_._1).map(now => {
        val cur_traj = now._2.sortBy(_._1).map(_._2)
        (getMBR(cur_traj), (now._1, cur_traj))
      }).iterator
    })

    val part_traj = STRTrajPartition(trajs, dataRDD.partitions.length, 0.01, max_entries_per_node)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(part_traj.partitions.length)

    val traj_stat = part_traj.mapPartitions(iter => {
      Array(iter.aggregate[(MBR, Int)]((null, 0))((res, now) => {
        if (res._1 == null) (now._1, 1)
        else (res._1.union(now._1), res._2 + 1)
      }, (left, right) => {
        if (left._1 == null) right
        else if (left._1 == null) left
        else (left._1.union(right._1), left._2 + right._2)
      })).iterator
    }).collect()
    val traj_global_rtree =
      RTree.applyMBR(traj_stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2)), max_entries_per_node)

    val optimal_num_bits = BloomFilter.optimalNumBits(10000, 0.1)
    val optimal_num_hashes = BloomFilter.optimalNumHashes(10000, optimal_num_bits)
    val bf_meta = BloomFilterMeta(optimal_num_bits, optimal_num_hashes)
    val bc_bf_meta = sc.broadcast(bf_meta)
    BloomFilter.meta = bf_meta

    val (partitioned_rdd, part_mbrs) = STRSegPartition(dataRDD, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val indexed_seg_rdd_with_traj_id = partitioned_rdd.mapPartitions(iter => {
      BloomFilter.meta = bc_bf_meta.value
      val data = iter.toArray
      var index: RTreeWithBF = null
      if (data.length > 0) {
        index = RTreeWithBF(data.map(x => (x._1, x._2.traj_id)).zipWithIndex.map(x => (x._1._1, x._2, x._1._2)),
          max_entries_per_node, bc_bf_meta.value)
      }
      Iterator((data.map(_._2.traj_id).distinct, index))
    })
    val indexed_seg_rdd = indexed_seg_rdd_with_traj_id.map(_._2).persist(StorageLevel.MEMORY_AND_DISK_SER)
	indexed_seg_rdd.count()
    val stat = indexed_seg_rdd_with_traj_id
      .mapPartitions(iter => iter.map(x => (x._2.root.m_mbr, x._2.root.size, x._1))).collect()

    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2.toInt)), max_entries_per_node)

    val end1 = System.currentTimeMillis()
    println("------------------------------------------------------------")
    println("Time to build indexes: " + (end1 - start1) / 1000.0)
    println("------------------------------------------------------------")


    val query_traj_file = Source.fromFile(query_traj_filename)
    val queries = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      (splitted(0).toInt, LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
    }.toArray.groupBy(_._1).map(x => x._2.map(_._2))

    var tot_time = 0.0
    queries.foreach(query_traj => {
      val start2 = System.currentTimeMillis()
      val bc_query = sc.broadcast(query_traj)
      val global_intersect = global_rtree.circleRange(query_traj, 0.0)
      val global_intersect_mbrs = global_intersect.map(_._1.asInstanceOf[MBR])
      val global_intersect_set = global_intersect.map(_._2).toSet

      val sample_base = stat.zipWithIndex.filter(x => global_intersect_set.contains(x._2)).flatMap(_._1._3)

      val cards = sample_base.length
      val rnd = scala.util.Random
      val set = mutable.HashSet[Int]()
      val samples = mutable.HashSet[Int]()
      val n_samples = c * k
      for (i <- 0 until n_samples) {
        var x = rnd.nextInt(cards)
        while (set.contains(x)) x = rnd.nextInt(cards)
        set += x
        samples += sample_base(x)
      }

      val bc_samples = sc.broadcast(samples.toSet)
      val pruning_bound_filter = traj_global_rtree.circleRange(global_intersect_mbrs, 0.0).map(_._2).toSet
      val pruning_bound = new PartitionPruningRDD(part_traj, pruning_bound_filter.contains)
        .filter(x => bc_samples.value.contains(x._2._1))
        .repartition(Math.min(samples.size, sc.defaultParallelism))
        .map(x => Trajectory.discreteFrechetDistance(bc_query.value, x._2._2))
        .takeOrdered(k).last
      val end2 = System.currentTimeMillis()
      println("Time to calculate pruning bound: " + (end2 - start2) / 1000.0)
      println("The pruning bound is: " + pruning_bound)

      val start3 = System.currentTimeMillis()
      val global_prune = global_rtree.circleRange(query_traj, pruning_bound)
      val global_prune_mbrs = global_prune.map(_._1.asInstanceOf[MBR])
      val global_prune_set = global_prune.map(_._2).toSet

      val pruned_rdd = new PartitionPruningRDD(indexed_seg_rdd, global_prune_set.contains)

      val bc_prunbound = sc.broadcast(pruning_bound)
      val saved_trajs = pruned_rdd.map(part => {
        BloomFilter.meta = bc_bf_meta.value
        part.circleRangeBF(bc_query.value, bc_prunbound.value)
      }).reduce((a, b) => BitArray.or(a, b))

      val end3 = System.currentTimeMillis()

      println("Time to calculate all saved traj_ids: " + (end3 - start3) / 1000.0)

      val start4 = System.currentTimeMillis()
      val bc_saved_traj = sc.broadcast(saved_trajs)
      val final_prune_set = traj_global_rtree.circleRange(global_prune.map(_._1.asInstanceOf[MBR]), 0.0).map(_._2).toSet
      val final_filtered = new PartitionPruningRDD(part_traj, final_prune_set.contains)
        .mapPartitions(iter => {
          BloomFilter.meta = bc_bf_meta.value
          iter.filter(now => BloomFilter.mayContains(bc_saved_traj.value, now._2._1))
        })

      val res = final_filtered.repartition(sc.defaultParallelism)
        .mapPartitions(iter => iter.map(x =>(Trajectory.discreteFrechetDistance(x._2._2, bc_query.value), x._2._1)))
        .takeOrdered(k)(new ResultOrdering)

      val end4 = System.currentTimeMillis()
      tot_time += (end4 - start2) / 1000.0
      println("Time to finish the final filter: " + (end4 - start4) / 1000.0)
      println("# of distance calculated: " + (c * k + final_filtered.count()))
      println("Total Latency: " + ((end4 - start2) / 1000.0))
      println("The results show as below:")
      res.foreach(println)
      println("------------------------------------------------------------")
    })
    println("Average Latency for c = " + c + " is : " + (tot_time / 100.0))
    println("===================================================")

    sc.stop()
  }
}

