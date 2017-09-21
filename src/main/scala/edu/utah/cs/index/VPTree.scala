package edu.utah.cs.index

import edu.utah.cs.util.MetricObject

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by dongx on 2/3/17.
  */
abstract class VPTreeNode[T <: MetricObject: ClassTag]

case class VPTreeInternalNode[T <: MetricObject: ClassTag](vp: T, threshold: Double,
                                 left: VPTreeNode[T], right: VPTreeNode[T]) extends VPTreeNode[T]

case class VPTreeLeafNode[T <: MetricObject: ClassTag](points: Array[T]) extends VPTreeNode[T]

case class VPTree[T <: MetricObject: ClassTag](root: VPTreeNode[T]) extends Index with Serializable {
  private[cs] case class HeapItem(point: T, dis: Double) extends Ordered[HeapItem] {
    override def compare(that: HeapItem): Int = dis.compare(that.dis)
  }

  def knn(query: T, k: Int, dis_threshold: Double = Double.MaxValue): (Array[(T, Double)], Int) = {
    val pq = mutable.PriorityQueue[HeapItem]()
    var tau = dis_threshold
    var checked = 0

    def offer(x: HeapItem) = {
      if (pq.size == k) pq.dequeue()
      pq.enqueue(x)
      if (pq.size == k) tau = pq.head.dis
    }

    def recursive_knn(node: VPTreeNode[T]) : Unit = {
      if (node != null) {
        node match {
          case VPTreeLeafNode(ps) =>
            checked += ps.length
            ps.foreach(x => {
              val dis = query.distance(x)
              if (dis < tau) offer(HeapItem(x, dis))
            })
          case VPTreeInternalNode(vp, th, left, right) =>
            val vp_dis = query.distance(vp)
            checked += 1
            if (vp_dis < tau) offer(HeapItem(vp, vp_dis))
            if (vp_dis < th) {
              if (vp_dis - tau <= th) recursive_knn(left)
              if (vp_dis + tau >= th) recursive_knn(right)
            } else {
              if (vp_dis + tau >= th) recursive_knn(right)
              if (vp_dis - tau <= th) recursive_knn(left)
            }
        }
      }
    }
    recursive_knn(root)

    (pq.dequeueAll.map(x => (x.point, x.dis)).toArray.reverse, checked)
  }

}

object VPTree {
  def buildNode[T <: MetricObject: ClassTag](points: Array[T], leaf_capacity: Int): VPTreeNode[T] = {
    if (points.isEmpty) {
      null
    } else if (points.length < leaf_capacity) {
      VPTreeLeafNode(points)
    } else {
      val n = points.length
      val vp_id = Random.nextInt(n)
      val t = points(vp_id)
      points(vp_id) = points(0)
      points(0) = t
      val vp = points.head
      val ps_with_dis = points.slice(1, n).map(x => (vp.distance(x), x)).sortBy(_._1)
      val median = Math.ceil((n - 1) / 2.0).toInt - 1
      val threshold = ps_with_dis(median)._1
      VPTreeInternalNode(vp, threshold,
        buildNode(ps_with_dis.slice(0, median + 1).map(_._2), leaf_capacity),
        buildNode(ps_with_dis.slice(median + 1, n).map(_._2), leaf_capacity))
    }
  }

  def apply[T <: MetricObject: ClassTag](points: Array[T], leaf_capacity: Int = 25): VPTree[T] = {
    VPTree(buildNode(points, leaf_capacity))
  }
}
