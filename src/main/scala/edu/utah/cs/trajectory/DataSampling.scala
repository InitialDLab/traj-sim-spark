package edu.utah.cs.trajectory

import edu.utah.cs.spatial.{LineSegment, Point}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongx on 1/27/2017.
  */
object DataSampling {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataSampling")
    val sc = new SparkContext(sparkConf)

    Thread.sleep(3000)

    if (args.length < 2) {
      println("usage: DataSampling <input_file_path> <output_file_path> <sample_rate>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)
    val sample_rate = args(2).toDouble

    sc.textFile(input_file_path).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
      }).toArray.groupBy(_._1).map(now => (now._1, now._2.sortBy(_._1).map(_._2))).iterator
    }).sample(withReplacement = false, sample_rate, System.currentTimeMillis()).repartition(4096)
      .flatMap(x => x._2.zipWithIndex.map(now => x._1 + "\t" + now._1.toTSV + "\t" + now._2))
      .saveAsTextFile(output_file_path)

    sc.stop()
  }
}
