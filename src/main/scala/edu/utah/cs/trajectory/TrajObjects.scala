package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{LineSegment, Point}

case class TrajMeta(traj_id: Int, seg_id: Int)

case class Trajectory(id: Int, segments: Array[Point]) {
  def distanceFrom(otherTraj: Trajectory): Double = {
    Math.min(Trajectory.hDistance(this, otherTraj), Trajectory.hDistance(otherTraj, this))
  }
}

object Trajectory {
  def RDPCompress(traj: Array[Point], epsilon: Double): Array[Point] = {
    val baseLineSeg = LineSegment(traj.head, traj.last)
    val dmax = traj.map(x => x.minDist(baseLineSeg)).zipWithIndex.maxBy(_._1)
    if (dmax._1 > epsilon) {
      RDPCompress(traj.slice(0, dmax._2 + 1), epsilon) ++ RDPCompress(traj.slice(dmax._2, traj.length), epsilon)
    } else {
      Array(traj.head, traj.last)
    }
  }

  def parseLine(line: String): Trajectory = {
    val splitted = line.split(" ")
    Trajectory(splitted(0).toInt, splitted.iterator.drop(1).map(_.toDouble).grouped(2).map(seq => Point(Array(seq(0), seq(1)))).toArray)
  }

  def hDistance(traj1: Trajectory, traj2: Trajectory): Double = {
    traj1.segments.iterator.take(traj1.segments.length - 1).zip(traj1.segments.iterator.drop(1)).map {
      case (q0, q1) =>
        val qSegment = LineSegment(q0, q1)
        traj2.segments.iterator.take(traj2.segments.length - 1).zip(traj2.segments.iterator.drop(1)).map {
          case (p0, p1) => qSegment.minDist(LineSegment(p0, p1))
        }.min
    }.max
  }

  def distanceFrom(seg_iter: Iterable[Tuple2[Int, LineSegment]],
      traj2: Array[LineSegment]): Double = {
    seg_iter.map { case (_, seg1) =>
        traj2.iterator.map { seg2 =>
            seg1.minDist(seg2)
        }.min
    }.max
  }

  def hausdorffDistance(x: Array[LineSegment], y: Array[LineSegment]): Double = {
    Math.max(x.map(seg_1 => y.map(seg_2 => seg_1.matchDist(seg_2)).min).max,
             y.map(seg_1 => x.map(seg_2 => seg_1.matchDist(seg_2)).min).max)
  }

  def discreteFrechetDistance(x: Array[LineSegment], y: Array[LineSegment]): Double = {
    val n = x.length
    val m = y.length
    val ca: Array[Array[Double]] = Array.fill[Double](n, m)(-1.0)
    var i = 0
    while (i < n) {
      var j = 0
      while (j < m) {
        if (i == 0 && j == 0) ca(i)(j) = x(i).matchDist(y(j))
        else if (i == 0) ca(i)(j) = Math.max(ca(i)(j - 1), x(i).matchDist(y(j)))
        else if (j == 0) ca(i)(j) = Math.max(ca(i - 1)(j), x(i).matchDist(y(j)))
        else ca(i)(j) = Math.max(Math.min(Math.min(ca(i - 1)(j), ca(i)(j - 1)), ca(i - 1)(j - 1)), x(i).matchDist(y(j)))
        j += 1
      }
      i += 1
    }
    ca.last.last
  }
}
