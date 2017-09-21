package edu.utah.cs.trajectory

import java.io._

import edu.utah.cs.spatial.{LineSegment, Point}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongx on 10/5/16.
  */
object SpatialSpanStat {
  def getStats(x: (Int, Array[(Int, LineSegment)])) = {
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
    val sparkConf = new SparkConf().setAppName("SpatialSpanStat")//.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    Thread.sleep(3000)

    if (args.length < 2) {
      println("usage: SpatialSpanStat <input_file_path> <output_file_path>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)

    val stats = sc.textFile(input_file_path).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
      }).toArray.groupBy(_._1).map(now => getStats(now)).iterator
    }).collect().sortBy(_._1)

    val file = new File(output_file_path)
    val bw = new BufferedWriter(new FileWriter(file))

    stats.foreach(x => bw.write(x._1 + "\t" + x._2 + "\t" + "%.6f".format(x._3)
      + "\t" + "%.6f".format(x._4) + "\n"))

    bw.close()

    sc.stop()
  }
}
