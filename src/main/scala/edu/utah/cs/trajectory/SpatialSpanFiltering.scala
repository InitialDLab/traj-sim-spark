package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{Point, LineSegment}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Dong Xie on 10/23/2016.
  */
object SpatialSpanFiltering {
  def getStats(x: (Int, Array[(Int, LineSegment, Int)])) = {
    val num_segs = x._2.length
    val tot_dis = x._2.map(p => p._2.length).sum
    val pts = x._2.flatMap(p => Array(p._2.start, p._2.end))
    var maxx = Double.MinValue
    var maxy = Double.MinValue
    var minx = Double.MaxValue
    var miny = Double.MaxValue
    pts.foreach(x => {
      maxx = Math.max(x.coord(0), maxx)
      maxy = Math.max(x.coord(1), maxy)
      minx = Math.min(x.coord(0), minx)
      miny = Math.min(x.coord(1), miny)
    })
    (x._1, num_segs, tot_dis, Point(Array(minx, miny)).minDist(Point(Array(maxx, maxy))))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SpatialSpanFiltering")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    Thread.sleep(3000)

    if (args.length < 2) {
      println("usage: SpatialSpanFiltering <input_file_path> <output_file_path>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)

    val stats = sc.textFile(input_file_path).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))), splitted(5).toInt)
      }).toArray.groupBy(_._1).filter(now => {
        val stat = getStats(now)
        stat._4 > 0.001 && stat._2 > 20 && stat._4 < 0.5080
      }).iterator
    }).repartition(800)
	  .flatMap(x => x._2.map(now => now._1 + "\t" + now._2.toTSV + "\t" + now._3))
      .saveAsTextFile(output_file_path)

    sc.stop()
  }
}
