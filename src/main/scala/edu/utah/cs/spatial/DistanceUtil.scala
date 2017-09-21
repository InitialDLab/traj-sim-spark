package edu.utah.cs.spatial

import org.apache.commons.math3.util.FastMath

object DistanceUtil {
  def computeGPSCoordDis(lat0: Double, lon0: Double, lat1: Double, lon1: Double): Double = {
    val R = 6371e3
    val phi0 = lat0.toRadians
    val phi1 = lat1.toRadians
    val delta_phi = (lat1 - lat0).toRadians
    val delta_lambda = (lon1 - lon0).toRadians

    val a = FastMath.sin(delta_phi / 2) * FastMath.sin(delta_phi / 2) +
      FastMath.cos(phi0) * FastMath.cos(phi1) *
      FastMath.sin(delta_lambda / 2) * Math.sin(delta_lambda / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))

    R * c
  }
}