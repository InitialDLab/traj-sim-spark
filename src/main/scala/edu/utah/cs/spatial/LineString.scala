package edu.utah.cs.spatial

/**
  * Created by dongx on 1/16/2017.
  */
case class LineString(segs: Array[LineSegment]) extends Shape {
  private val mbr: MBR = segs.foldLeft(segs(0).getMBR)((now , seg) => now.union(seg.getMBR))

  override def minDist(other: Shape): Double = segs.map(x => x.minDist(other)).min

  override def intersects(other: Shape): Boolean = segs.exists(x => x.intersects(other))

  def hausdorff(other: LineString): Double =
    Math.max(segs.map(now_x => other.segs.map(now_y => now_x.minDist(now_y)).min).max,
             other.segs.map(now_x => segs.map(now_y => now_x.minDist(now_y)).min).max)

  override def getMBR: MBR = mbr
}
