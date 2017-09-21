package edu.utah.cs.trajectory

import edu.utah.cs.index.VPTree
import edu.utah.cs.spatial.{LineSegment, Point}
import edu.utah.cs.util.MetricObject

import scala.io.Source
import scala.collection.mutable

/**
  * Created by dongx on 2/1/17.
  */
object VPTreeST {
  //final val k_values = Array(10, 30, 50, 70, 100)
  final val k = 10

  private case class VPTraj(id: Int, data: Array[LineSegment]) extends MetricObject {
    override def distance(o: MetricObject): Double = {
      Trajectory.hausdorffDistance(data, o.asInstanceOf[VPTraj].data)
    }
  }

  def main(args: Array[String]) : Unit = {
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
    val trajs = mutable.ListBuffer[VPTraj]()
    val ans = mutable.ListBuffer[(Double, Int)]()
    var last_traj_id = -1
    val new_iter = traj_data_file.getLines().map(cur => {
      val x = cur.split("\t")
      (LineSegment(Point(Array(x(1).toDouble, x(2).toDouble)), Point(Array(x(3).toDouble, x(4).toDouble))), x(0).toInt)
    })
    var i = 0
    while (new_iter.hasNext) {
      val now = new_iter.next
      if (now._2 != last_traj_id) {
        if (cur_traj.nonEmpty) trajs += VPTraj(last_traj_id, cur_traj.toArray)
        last_traj_id = now._2
        i += 1
        //println("checking " + i + " trajectory....")
        cur_traj.clear()
      }
      cur_traj += now._1
    }
    if (cur_traj.nonEmpty) trajs += VPTraj(last_traj_id, cur_traj.toArray)
    //assert(ans.size == N)
    val tree = VPTree(trajs.toArray)
    tree.knn(VPTraj(-1, query_traj), k)._1.map(x => (x._1.id, x._2)).foreach(println)
  }
}
