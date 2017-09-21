package edu.utah.cs.util

/**
  * Created by dongx on 2/3/17.
  */
abstract class MetricObject {
  def distance(o: MetricObject): Double
}
