package edu.utah.cs.generator

import edu.utah.cs.spatial.Point
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by dongx on 1/18/17.
  * This generator should work in such manner:
  * - Generate a uniform random point within the starting scope defined by (<low_x>, <low_y>) -- (<high_x>, <high_y>)
  * - Generate the number of steps <num_step> following a normal distribution with parameters (<step_avg> and <step_dev>)
  * - Iterate <num_step> times:
  *   + generate two random numbers (<step_x>, <step_y>) with normal distribution defined by <range_dev> (mean is 0)
  *   + move the object to (<cur_x> - <step_x>, (<cur_y> - <step_y>))
  */
object RandomTrajGenerator {
  def rnd(low: Double, high: Double): Double = Random.nextDouble() * (high - low) + low
  def flip_coin(p: Double): Int = if (Random.nextDouble() > 0.9) 1 else -1
  def gaussianRnd(mean: Double, scale: Double) = Random.nextGaussian() * scale + mean
  def gaussianRnd(scale: Double) = Random.nextGaussian() * scale

  def main(args: Array[String]): Unit = {
//    if (args.length != 9) {
//      println("Usage: RandomTrajGenerator <n> <low_x> <low_y> <high_x> <high_y> <steps_avg> <steps_dev> <basic_step> <range_dev> <output_file_path>")
//      System.exit(1)
//    }

    //val sparkConf = new SparkConf().setAppName("TrajSampling")
    //val sc = new SparkContext(sparkConf)

//    Thread.sleep(3000)

//    val n = args(0).toInt
//    val low_x = args(1).toDouble
//    val low_y = args(2).toDouble
//    val high_x = args(3).toDouble
//    val high_y = args(4).toDouble
//    val steps_avg = args(5).toInt
//    val steps_dev = args(6).toDouble
//    val range_dev = args(7).toDouble
//    val output_file_path = args(8)
//    val n = args(0).toInt
    val low_x = 0.0
    val low_y = 0.0
    val high_x = 100.0
    val high_y = 100.0
    val steps_avg = 20
    val steps_dev = 10
    val range_dev = 8.0
//    val output_file_path = args(8)

//    sc.parallelize(0 until n, sc.defaultParallelism)
//      .flatMap(x => {
//        val ans = mutable.ListBuffer[String]()
//        val last_x = rnd(low_x, high_x)
//        val last_y = rnd(low_y, high_y)
//        val steps = gaussianRnd(steps_avg, steps_dev).toInt
//        for (i <- 0 until steps) {
//          val cur_x = last_x + gaussianRnd(range_dev)
//          val cur_y = last_y + gaussianRnd(range_dev)
//          ans += s"$x\t$last_x\t$last_y\t$cur_x\t$cur_y\t$i"
//        }
//        ans.iterator
//      }).saveAsTextFile(output_file_path)

    val res = mutable.ListBuffer[Point]()
    var x1 = rnd(low_x, high_x)
    var y1 = rnd(low_y, high_y)
    val basic_step_x = gaussianRnd(5.0)
    val basic_step_y = gaussianRnd(5.0)
    var x2 = x1 + gaussianRnd(basic_step_x, basic_step_x * 0.5)
    var y2 = y1 + gaussianRnd(basic_step_y, basic_step_y * 0.5)
    res += Point(Array(x1, y1))
    res += Point(Array(x2, y2))
    val steps = gaussianRnd(steps_avg, steps_dev).toInt
    assert(steps > 10)
    for (i <- 0 until steps) {
      val cur_x = ((x1 + x2) / 2.0) + flip_coin(0.95) * gaussianRnd(basic_step_x, basic_step_x * 0.3)
      val cur_y = ((y1 + y2) / 2.0) + flip_coin(0.95) * gaussianRnd(basic_step_y, basic_step_y * 0.3)
      res += Point(Array(cur_x, cur_y))
      x1 = x2
      y1 = y2
      x2 = cur_x
      y2 = cur_y
    }

    println("X = [")
    res.foreach(x => println(s"${x.coord(0)},"))
    println("]")
    println()
    println("Y = [")
    res.foreach(x => println(s"${x.coord(1)},"))
    println("]")

    //sc.stop()
  }

}
