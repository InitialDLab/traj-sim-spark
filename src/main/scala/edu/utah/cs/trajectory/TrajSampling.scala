package edu.utah.cs.trajectory

import java.io.{BufferedWriter, File, FileWriter}

import edu.utah.cs.spatial.{LineSegment, Point}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongx on 1/17/17.
  */
object TrajSampling {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TrajSampling")
    val sc = new SparkContext(sparkConf)

    Thread.sleep(3000)

    if (args.length < 2) {
      println("usage: TrajSampling <input_file_path> <output_file_path> <sample_count>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)
    val cnt = args(2).toInt

    val sampled_trajs = sc.textFile(input_file_path).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(1).toDouble, splitted(2).toDouble)),
            Point(Array(splitted(3).toDouble, splitted(4).toDouble))))
      }).toArray.groupBy(_._1).map(now => (now._1, now._2.sortBy(_._1).map(_._2))).iterator
    }).takeSample(withReplacement = false, cnt, System.currentTimeMillis())

    val file = new File(output_file_path)
    val bw = new BufferedWriter(new FileWriter(file))

    for (i <- sampled_trajs.indices) {
      val cur_traj = sampled_trajs(i)._2
      cur_traj.foreach(x => bw.write(i + "\t" + x.toTSV + "\n"))
    }

    bw.close()

    sc.stop()
  }
}
