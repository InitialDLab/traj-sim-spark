package edu.utah.cs.index_bm

import edu.utah.cs.spatial._
import edu.utah.cs.util._
import org.roaringbitmap.RoaringBitmap

/**
  * Created by dongx on 10/7/16.
  */
object RTreeWithBMTest {
  def main(args: Array[String]) = {
//    val bm_meta = BitMapMeta(100)
//    BitMap.meta = bm_meta
//    val data = (0 until 100).map(x => (LineSegment(Point(Array(x - 1, x)), Point(Array(x, x))), x, x)).toArray
//    val rt = RTreeWithBM(data, 10, bm_meta)
//    val res = rt.circleRangeBF(LineSegment(Point(Array(2, 2)), Point(Array(1, 2))), 1000)
//    println(BitArray.count(res))
//    println(BitArray.count(rt.root.bf))
//    rt.root.bf.foreach(x => println(x.toBinaryString))
    val bitmap1 = RoaringBitmap.bitmapOf(1, 2, 3, 4)
    val bitmap2 = RoaringBitmap.bitmapOf(2, 3, 6, 7)
    println(RoaringBitmap.andNot(bitmap1, bitmap2))
    println(RoaringBitmap.andNot(bitmap2, bitmap1))
  }
}
