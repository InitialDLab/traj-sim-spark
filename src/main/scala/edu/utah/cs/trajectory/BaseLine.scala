package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{LineSegment, Point}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by dongx on 8/22/16.
  */
object BaseLine {
  //final val k_values = Array(1, 10, 30, 50, 70, 100)
  final val k_values = Array(10)

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BaseLine")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: BaseLine <query_traj_filename> <traj_data_filename>")
      System.exit(1)
    }

    Thread.sleep(3000)

    val query_traj_filename = args(0)
    val traj_data_filename = args(1)

    val query_traj_file = Source.fromFile(query_traj_filename)
    val queries = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      (splitted(0).toInt, LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
    }.toArray.groupBy(_._1).map(x => x._2.map(_._2)).slice(0, 20)

    k_values.foreach(k => {
      var tot_time = 0.0
      queries.foreach(query_traj => {
        println("-------------------------------------------------")

        val start = System.currentTimeMillis()
        val bc_query = sc.broadcast(query_traj)

        val res = sc.textFile(traj_data_filename).map{ line =>
          val splitted = line.split('\t')
          (splitted(0).toInt,
            LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
              Point(Array(splitted(3).toDouble, splitted(4).toDouble))))}.mapPartitions(iter => {
          val cur_traj = mutable.ListBuffer[LineSegment]()
          val ans = mutable.ListBuffer[(Double, Int)]()
          var last_traj_id = -1
          while (iter.hasNext) {
            val now = iter.next
            if (now._1 != last_traj_id) {
              if (cur_traj.nonEmpty) ans += ((Trajectory.hausdorffDistance(cur_traj.toArray, bc_query.value), last_traj_id))
              //if (cur_traj.nonEmpty) ans += ((Trajectory.discreteFrechetDistance(cur_traj.toArray, bc_query.value), last_traj_id))
              last_traj_id = now._1
              cur_traj.clear()
            }
            cur_traj += now._2
          }
          if (cur_traj.nonEmpty) ans += ((Trajectory.hausdorffDistance(cur_traj.toArray, bc_query.value), last_traj_id))
          //if (cur_traj.nonEmpty) ans += ((Trajectory.discreteFrechetDistance(cur_traj.toArray, bc_query.value), last_traj_id))
          ans.iterator
        }).takeOrdered(k)(new ResultOrdering)

        val end = System.currentTimeMillis()
        res.foreach(println)
        println("Latency: " + ((end - start) / 1000.0))
        println("-------------------------------------------------")
        tot_time += (end - start) / 1000.0
      })

      println("Average Latency for k = " + k + " is : " + (tot_time / 20.0))
      println("===================================================")
    })


    sc.stop()
  }
}
