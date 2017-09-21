package edu.utah.cs.util

import scala.util.Random

/**
  * Created by dongx on 10/4/16.
  */
case class BloomFilterMeta(num_bits: Int, num_hashs: Int) {
  val seeds = (1 to num_hashs).map(x => (Random.nextInt(Integer.MAX_VALUE), Random.nextInt(Integer.MAX_VALUE)))
}

object BloomFilter {
  var meta: BloomFilterMeta = null

  private def calcHash(seed: (Int, Int), key: Int) =
    (((seed._1 % meta.num_bits) * (key & meta.num_bits) + seed._2 % meta.num_bits) % meta.num_bits + meta.num_bits) % meta.num_bits

  def put(bf: Array[Int], key: Int): Unit = {
    meta.seeds.foreach(seed => {
      BitArray.set(bf, calcHash(seed, key))
    })
  }

  def mayContains(bf: Array[Int], key: Int): Boolean = {
    meta.seeds.foreach(seed => {
      if (!BitArray.get(bf, calcHash(seed, key))) return false
    })
    true
  }

  def optimalNumBits(num_items: Long, fp_rate: Double): Int = {
    math.ceil(-1 * num_items * math.log(fp_rate) / math.log(2) / math.log(2)).toInt
  }

  def optimalNumHashes(num_items: Long, num_bits: Long): Int = {
    math.ceil(num_bits / num_items * math.log(2)).toInt
  }
}
