package edu.utah.cs.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}


/**
  * Created by dongx on 2/1/17.
  */
object IDPartition {
  def apply(origin: RDD[_ <: Product2[Int, Any]], n_part: Int)
  : RDD[_ <: Product2[Int, Any]] = {
    val part = new IDPartitioner(n_part)
    val shuffled = new ShuffledRDD[Int, Any, Any](origin, part)
    shuffled
  }
}

class IDPartitioner(n_part: Int) extends Partitioner {
  override def numPartitions: Int = n_part

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}
