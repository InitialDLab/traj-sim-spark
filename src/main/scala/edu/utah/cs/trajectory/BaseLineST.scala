package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{Point, LineSegment}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by Dong Xie on 10/23/2016.
  */
object BaseLineST {
  final val k = 10
  final val N = 34085

  def minmaxtraj(x: Array[LineSegment], y: Array[LineSegment]) = {
    x.map(now_x => y.map(now_y => now_x.minDist(now_y)).min).max
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: BaseLine <query_traj_filename> <traj_data_filename>")
      System.exit(1)
    }

    val query_traj_filename = args(0)
    val traj_data_filename = args(1)

    val query_traj_file = Source.fromFile(query_traj_filename)
    val query_traj = query_traj_file.getLines().map { line =>
      val splitted = line.split('\t')
      LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
        Point(Array(splitted(3).toDouble, splitted(4).toDouble)))
    }.toArray

    val traj_data_file = Source.fromFile(traj_data_filename)
    val cur_traj = mutable.ListBuffer[LineSegment]()
    val ans = mutable.ListBuffer[(Double, Int)]()
    var last_traj_id = -1
    val new_iter = traj_data_file.getLines().map(cur => {
      val x = cur.split("\t")
      (LineSegment(Point(Array(x(1).toDouble, x(2).toDouble)), Point(Array(x(3).toDouble, x(4).toDouble))),
        TrajMeta(x(0).toInt, 1))
    })
    var i = 0
    while (new_iter.hasNext) {
      val now = new_iter.next
      if (now._2.traj_id != last_traj_id) {
        if (cur_traj.nonEmpty) ans += ((Trajectory.hausdorffDistance(cur_traj.toArray, query_traj), last_traj_id))
        last_traj_id = now._2.traj_id
        i += 1
        println("checking " + i + " trajectory....")
        cur_traj.clear()
      }
      cur_traj += now._1
    }
    if (cur_traj.nonEmpty) ans += ((Trajectory.hausdorffDistance(cur_traj.toArray, query_traj), last_traj_id))
    //assert(ans.size == N)
    ans.sortBy(_._1).take(k).foreach(println)
  }
}
