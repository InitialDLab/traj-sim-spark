package edu.utah.cs.util

/**
  * Created by dongx on 1/19/17.
  */
case class BitMapMeta(num_bits: Int)

object BitMap {
  var meta: BitMapMeta = null

  def put(bf: Array[Int], key: Int): Unit = BitArray.set(bf, key)

  def contains(bf: Array[Int], key: Int): Boolean = BitArray.get(bf, key)
}
