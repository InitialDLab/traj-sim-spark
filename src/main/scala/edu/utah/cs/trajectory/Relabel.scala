package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{LineSegment, Point}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongx on 10/5/16.
  */
object Relabel {
  case class TrajMeta(traj_id: String, seg_id: Int)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Relabel")
    val sc = new SparkContext(sparkConf)

    if (args.length != 2) {
      println("usage: Relabel <input_path> <output_path>")
      System.exit(1)
    }

    Thread.sleep(3000)

    val input_file_name = args(0)
    val output_file_name = args(1)

    sc.textFile(input_file_name, 900).map(x => {
      val splitted = x.split('\t')
      (splitted(0),
        LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
          Point(Array(splitted(3).toDouble, splitted(4).toDouble))), splitted(5))
    }).groupBy(_._1)
      .zipWithIndex()
      .flatMap(x => x._1._2.map(now => x._2.toString + "\t" + now._2.toTSV + "\t" + now._3))
      .saveAsTextFile(output_file_name)

    sc.stop()
  }
}
