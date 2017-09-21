package edu.utah.cs.trajectory

import edu.utah.cs.index.RTree
import edu.utah.cs.index_bm.RTreeWithBM
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
object BitMapSolution {
  final val max_entries_per_node = 25
  final val k = 10
  final val c = 5
  final val N = 940698
  //final val max_spatial_span = 0.46757
  final val max_spatial_span = 2.550598

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BitMapSolution")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: BitMapSolution <query_traj_filename> <traj_data_filename>")
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
        TrajMeta(x(0).toInt, x(5).toInt)))//.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //val optimal_num_bits = BloomFilter.optimalNumBits(N, 0.1)
    //val optimal_num_hashes = BloomFilter.optimalNumHashes(N, optimal_num_bits)
    //println(optimal_num_bits + "\t" + optimal_num_hashes)
    val bm_meta = BitMapMeta(N)
    val bc_bm_meta = sc.broadcast(bm_meta)
    BitMap.meta = bm_meta

    val (partitioned_rdd, part_mbrs) = STRSegPartition(dataRDD, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val indexed_seg_rdd = partitioned_rdd.mapPartitions(iter => {
      BitMap.meta = bc_bm_meta.value
      val data = iter.toArray
      var index: RTreeWithBM = null
      //var traj_ids: Array[Int] = null
      if (data.length > 0) {
        index = RTreeWithBM(data.map(x => (x._1, x._2.traj_id)).zipWithIndex.map(x => (x._1._1, x._2, x._1._2)),
          max_entries_per_node, bc_bm_meta.value)
        //traj_ids = data.map(_._2.traj_id).distinct
      }
      Array((data, index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stat = indexed_seg_rdd.mapPartitions(iter => iter.map(x => (x._2.root.m_mbr, x._1.length, x._2.root.bf))).collect()

    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2)), max_entries_per_node)

    val end1 = System.currentTimeMillis()
    println("------------------------------------------------------------")
    println("Time to build indexes: " + (end1 - start1) / 1000.0)
    println("------------------------------------------------------------")

    val start2 = System.currentTimeMillis()
    val query_traj_file = Source.fromFile(query_traj_filename)
    val query_traj = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble)))
    }.toArray

    val bc_query = sc.broadcast(query_traj)

//    val sample_set = dataRDD.takeSample(withReplacement = false, c * k, System.currentTimeMillis()).map(_._2.traj_id).toSet
//
//    assert(sample_set.size >= k)
//
//    val pruning_bound = dataRDD.filter(x => sample_set.contains(x._2.traj_id)).groupBy(_._2.traj_id)
//      .map(x => minmaxtraj(x._2.toArray.map(_._1), bc_query.value)).takeOrdered(k).last
    val global_intersect = global_rtree.circleRange(query_traj, 0.0)
    val global_intersect_mbrs = global_intersect.map(_._1.asInstanceOf[MBR])
    val global_intersect_set = global_intersect.map(_._2).toSet

    val sample_set = new PartitionPruningRDD(indexed_seg_rdd, global_intersect_set.contains).flatMap(_._1)
      .takeSample(withReplacement = false, c * k, System.currentTimeMillis()).map(_._2.traj_id).toSet

    assert(sample_set.size >= k)

    val pruning_bound_filter = global_rtree.circleRange(global_intersect_mbrs, max_spatial_span).map(_._2).toSet
    val pruning_bound = new PartitionPruningRDD(indexed_seg_rdd, pruning_bound_filter.contains)
      .flatMap(x => x._1.filter(now => sample_set.contains(now._2.traj_id))).groupBy(_._2.traj_id)
      .map(x => Trajectory.hausdorffDistance(x._2.toArray.map(_._1), bc_query.value)).takeOrdered(k).last
//    val pruning_bound = dataRDD.filter(x => sample_set.contains(x._2.traj_id)).groupBy(_._2.traj_id)
//      .map(x => minmaxtraj(x._2.toArray.map(_._1), bc_query.value)).takeOrdered(k).last

    //val pruning_bound = 8.65080562241333

    val end2 = System.currentTimeMillis()

    println("------------------------------------------------------------")
    println("Time to calculate pruning bound: " + (end2 - start2) / 1000.0)
    println("The pruning bound is: " + pruning_bound)
    println("------------------------------------------------------------")

    val start3 = System.currentTimeMillis()
    //val global_prune_set = query_traj.map(x => {
    //  global_rtree.circleRange(x, pruning_bound).map(_._2)
    //}).flatMap(list => list).toSet
    val global_prune = global_rtree.circleRange(query_traj, pruning_bound)
    val global_prune_mbrs = global_prune.map(_._1.asInstanceOf[MBR])
    val global_prune_set = global_prune.map(_._2).toSet

    val pruned_rdd = new PartitionPruningRDD(indexed_seg_rdd, global_prune_set.contains)
    val pruned_traj_id1 = stat.zipWithIndex.filter(x => !global_prune_set.contains(x._2)).map(_._1._3)
      .aggregate(BitArray.create(bm_meta.num_bits))((a, b) => BitArray.or(a, b), (a, b) => BitArray.or(a, b))

    val bc_prunbound = sc.broadcast(pruning_bound)

    val pruned_traj_id2 = pruned_rdd.map(part => {
      BitMap.meta = bc_bm_meta.value
      part._2.antiCircleRangeBF(bc_query.value, bc_prunbound.value)
    }).reduce((a, b) => BitArray.or(a, b))

    val saved_trajs = BitArray.flip(BitArray.or(pruned_traj_id1, pruned_traj_id2))

//    val saved_trajs = pruned_rdd.map(part => {
//      BloomFilter.meta = bc_bm_meta.value
//      part._2.circleRangeBF(bc_query.value, bc_prunbound.value)
//    }).reduce((a, b) => BitArray.or(a, b))

    val end3 = System.currentTimeMillis()

    println("------------------------------------------------------------")
    println("Time to calculate all saved traj_ids: " + (end3 - start3) / 1000.0)
    println("Pruned trajs after global pruning:" + BitArray.count(pruned_traj_id1))
    println("Pruned trajs after local pruning:" + BitArray.count(BitArray.or(pruned_traj_id1, pruned_traj_id2)))
    println("# of saved trajs: " + BitArray.count(saved_trajs))
    println("------------------------------------------------------------")

    val start4 = System.currentTimeMillis()
    val bc_saved_traj = sc.broadcast(saved_trajs)
    val final_filter_set = global_rtree.circleRange(global_prune_mbrs, max_spatial_span).map(_._2).toSet

    val res = new PartitionPruningRDD(indexed_seg_rdd, final_filter_set.contains)
      .flatMap(x => {
        BitMap.meta = bc_bm_meta.value
        x._1.filter(now => BitMap.contains(bc_saved_traj.value, now._2.traj_id))
      }).groupBy(_._2.traj_id).map(x => (Trajectory.hausdorffDistance(x._2.map(_._1).toArray, bc_query.value), x._1))
      .takeOrdered(k)(new ResultOrdering)

//    val res = dataRDD.mapPartitions(iter => {
//      BloomFilter.meta = bc_bm_meta.value
//      val cur_traj = mutable.ListBuffer[LineSegment]()
//      val ans = mutable.ListBuffer[(Double, Int)]()
//      var last_traj_id = -1
//      val new_iter = iter.filter(x => BloomFilter.mayContains(bc_saved_traj.value, x._2.traj_id))
//      while (new_iter.hasNext) {
//        val now = new_iter.next
//        if (now._2.traj_id != last_traj_id) {
//          if (cur_traj.nonEmpty) ans += ((minmaxtraj(cur_traj.toArray, bc_query.value), last_traj_id))
//          last_traj_id = now._2.traj_id
//          cur_traj.clear()
//        }
//        cur_traj += now._1
//      }
//      if (cur_traj.nonEmpty) ans += ((minmaxtraj(cur_traj.toArray, bc_query.value), last_traj_id))
//      ans.iterator
//      //iter.toArray.groupBy(_._2.traj_id).filter(x => BloomFilter.mayContains(bc_saved_traj.value, x._1))
//      //    .map(x => (minmaxtraj(x._2.map(_._1), bc_query.value), x._1)).iterator
//    }).takeOrdered(k)(new ResultOrdering)

    val end4 = System.currentTimeMillis()

    println("------------------------------------------------------------")
    println("Time to finish the final filter: " + (end4 - start4) / 1000.0)
    println("------------------------------------------------------------")

    println("------------------------------------------------------------")
    println("The results show as below:")
    res.foreach(println)
    println("------------------------------------------------------------")

    sc.stop()
  }
}
