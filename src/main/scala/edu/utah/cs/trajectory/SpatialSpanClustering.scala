package edu.utah.cs.trajectory

import java.io.{BufferedWriter, File, FileWriter}

import com.vividsolutions.jts.geom.{GeometryCollection, GeometryFactory}
import edu.utah.cs.partitioner.STRMBRPartition
import edu.utah.cs.spatial.{LineSegment, MBR, Point, Polygon}
import edu.utah.cs.util._
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geojson.geom.GeometryJSON

/**
  * Created by Dong Xie on 10/24/2016.
  */
object SpatialSpanClustering {
  final val max_entries_per_node = 25

  def getMBR(x: (Int, Array[(Int, LineSegment)])): (MBR, Int) = {
    val pts = x._2.flatMap(p => Array(p._2.start, p._2.end))
    var maxx = Double.MinValue
    var maxy = Double.MinValue
    var minx = Double.MaxValue
    var miny = Double.MaxValue
    pts.foreach(x => {
      maxx = Math.max(x.coord(0), maxx)
      maxy = Math.max(x.coord(1), maxy)
      minx = Math.min(x.coord(0), minx)
      miny = Math.min(x.coord(1), miny)
    })
    (MBR(Point(Array(minx, miny)), Point(Array(maxx, maxy))), x._1)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SpatialSpanClustering"))

    if (args.length < 2) {
      println("usage: SpatialSpanClustering <input_file_path> <output_file_path>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)

    val bf_meta = BloomFilterMeta(10000, 1)
    val bc_bf_meta = sc.broadcast(bf_meta)
    BloomFilter.meta = bf_meta

    val mbrs = sc.textFile(input_file_path).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split("\t")
        (splitted(0).toInt,
          LineSegment(Point(Array(splitted(2).toDouble, splitted(1).toDouble)),
            Point(Array(splitted(4).toDouble, splitted(3).toDouble))))
      }).toArray.groupBy(_._1).map(now => getMBR(now)).iterator
    })

    val num_partitions = mbrs.getNumPartitions * 4

    val partitioned_rdd = STRMBRPartition(mbrs, num_partitions, 0.01, max_entries_per_node)

    val part_bounds = partitioned_rdd.mapPartitions(iter => {
      if (iter.nonEmpty) {
        var maxx = Double.MinValue
        var maxy = Double.MinValue
        var minx = Double.MaxValue
        var miny = Double.MaxValue
        iter.map(_._1).foreach(x => {
          maxx = Math.max(x.high.coord(0), maxx)
          maxy = Math.max(x.high.coord(1), maxy)
          minx = Math.min(x.low.coord(0), minx)
          miny = Math.min(x.low.coord(1), miny)
        })
        Array(MBR(Point(Array(minx, miny)), Point(Array(maxx, maxy)))).iterator
      } else Array().iterator
    }).collect()

    val file = new File(output_file_path)
    val bw = new BufferedWriter(new FileWriter(file))

    val collection = new GeometryCollection(part_bounds.map(x =>
      Polygon(Array(x.low, Point(Array(x.low.coord(0), x.high.coord(1))),
        x.high, Point(Array(x.high.coord(0), x.low.coord(1))), x.low)).content), new GeometryFactory)

    new GeometryJSON().writeGeometryCollection(collection, bw)

    bw.close()

    sc.stop()
  }
}
