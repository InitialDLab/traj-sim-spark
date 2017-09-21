package edu.utah.cs.util

/**
  * Created by dongx on 10/4/16.
  */
object BitArray {
  def create(length: Int): Array[Int] = {
    Array.fill[Int](math.ceil(length / 32.0).toInt){0}
  }

  def get(bytes: Array[Int], id: Int) = {
    (bytes(id / 32) & (1 << (id % 32))) != 0
  }

  def set(bytes: Array[Int], id: Int) = {
    bytes(id / 32) = bytes(id / 32) | (1 << (id % 32))
  }

  def or(a: Array[Int], b: Array[Int]) = {
    a.zip(b).map(x => x._1 | x._2)
  }

  def and(a: Array[Int], b: Array[Int]) = {
    a.zip(b).map(x => x._1 & x._2)
  }

  def flip(a: Array[Int]) = a.map(~_)

  def count(a: Array[Int]) = {
    a.map(x => x.toBinaryString.count(_ == '1')).sum
  }

}
