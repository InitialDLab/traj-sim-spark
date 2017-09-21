package edu.utah.cs.index

import edu.utah.cs.spatial.Point
import edu.utah.cs.util.{BloomFilter, MetricObject}

/**
  * Created by dongx on 2/3/17.
  */
object VPTreeTest {
  private case class VPPoint(data: Point, id: Int) extends MetricObject {
    override def distance(o: MetricObject): Double = {
      data.minDist(o.asInstanceOf[VPPoint].data)
    }
  }


  def main(args: Array[String]): Unit = {
//    val tree = VPTree((0 until 1000).map(x => VPPoint(Point(Array(x - 1, x + 1)), x + 1)).toArray)
//    tree.knn(VPPoint(Point(Array(3, 3)), 0), 10, 5)._1.foreach(println)
    val optimal_num_bits = BloomFilter.optimalNumBits(10000, 0.1)
    val optimal_num_hashes = BloomFilter.optimalNumHashes(10000, optimal_num_bits)
    println(optimal_num_bits, optimal_num_hashes)
  }
}
