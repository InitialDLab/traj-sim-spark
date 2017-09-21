package edu.utah.cs.trajectory

import edu.utah.cs.index.RTree
import edu.utah.cs.index_bf.RTreeWithBF
import edu.utah.cs.partitioner.STRSegPartition
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import edu.utah.cs.util._
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by dongx on 9/6/16.
  * Line Segment Trajectory Storage
  */
object BloomFilterSolution {
  final val max_entries_per_node = 25
  final val k = 10
  final val c = 5
  final val max_spatial_span = 0.46757
  //final val max_spatial_span = 2.550598

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BloomFilterSolution").set("spark.locality.wait", "0")
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

    val optimal_num_bits = BloomFilter.optimalNumBits(5000, 0.1)
    val optimal_num_hashes = BloomFilter.optimalNumHashes(5000, optimal_num_bits)
    val bf_meta = BloomFilterMeta(optimal_num_bits, optimal_num_hashes)
    val bc_bf_meta = sc.broadcast(bf_meta)
    BloomFilter.meta = bf_meta

    val (partitioned_rdd, part_mbrs) = STRSegPartition(dataRDD, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val indexed_seg_rdd = partitioned_rdd.mapPartitions(iter => {
      BloomFilter.meta = bc_bf_meta.value
      val data = iter.toArray
      var index: RTreeWithBF = null
      if (data.length > 0) {
        index = RTreeWithBF(data.map(x => (x._1, x._2.traj_id)).zipWithIndex.map(x => (x._1._1, x._2, x._1._2)),
          max_entries_per_node, bc_bf_meta.value)
      }
      Array((data, index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stat = indexed_seg_rdd.mapPartitions(iter => iter.map(x => (x._2.root.m_mbr, x._1.length, x._2.root.bf))).collect()

    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2)), max_entries_per_node)

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

      val sample_set = new PartitionPruningRDD(indexed_seg_rdd, global_intersect_set.contains).flatMap(_._1)
        .takeSample(withReplacement = false, c * k, System.currentTimeMillis()).map(_._2.traj_id).toSet

      assert(sample_set.size >= k)

      val pruning_bound_filter = global_rtree.circleRange(global_intersect_mbrs, max_spatial_span).map(_._2).toSet
      val pruning_bound = new PartitionPruningRDD(indexed_seg_rdd, pruning_bound_filter.contains)
        .flatMap(x => x._1.filter(now => sample_set.contains(now._2.traj_id)))
        .groupBy(_._2.traj_id).repartition(Math.min(sample_set.size, sc.defaultParallelism))
        .map(x => Trajectory.hausdorffDistance(x._2.toArray.map(_._1), bc_query.value)).takeOrdered(k).last

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
        part._2.circleRangeBF(bc_query.value, bc_prunbound.value)
      }).reduce((a, b) => BitArray.or(a, b))

      val end3 = System.currentTimeMillis()

      println("Time to calculate all saved traj_ids: " + (end3 - start3) / 1000.0)

      val start4 = System.currentTimeMillis()
      val bc_saved_traj = sc.broadcast(saved_trajs)
      val final_filter_set = global_rtree.circleRange(global_prune_mbrs, max_spatial_span).map(_._2).toSet

      val fianl_filter = new PartitionPruningRDD(indexed_seg_rdd, final_filter_set.contains)
        .flatMap(x => {
          BloomFilter.meta = bc_bf_meta.value
          x._1.filter(now => BloomFilter.mayContains(bc_saved_traj.value, now._2.traj_id))
        }).groupBy(_._2.traj_id).repartition(sc.defaultParallelism)

      val res = fianl_filter.map(x => (Trajectory.hausdorffDistance(x._2.map(_._1).toArray, bc_query.value), x._1))
        .takeOrdered(k)(new ResultOrdering)

      val end4 = System.currentTimeMillis()
      tot_time += (end4 - start2) / 1000.0
      println("Time to finish the final filter: " + (end4 - start4) / 1000.0)
      println("# of distance calculated: " + (c * k + fianl_filter.count()))
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

