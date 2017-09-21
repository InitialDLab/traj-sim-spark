package edu.utah.cs.trajectory

import java.io.{BufferedWriter, File, FileWriter}

import com.vividsolutions.jts.geom.{GeometryCollection, GeometryFactory}
import edu.utah.cs.partitioner.STRSegPartition
import edu.utah.cs.spatial.{LineSegment, MBR, Point, Polygon}
import edu.utah.cs.util.{BloomFilter, BloomFilterMeta}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geojson.geom.GeometryJSON

/**
  * Created by dongx on 10/24/16.
  */
object LineSegmentClustering {
  final val max_entries_per_node = 25
  final val k = 10
  final val N = 34085

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("LineSegmentClustering"))

    if (args.length < 2) {
      println("usage: SpatialSpanClustering <input_file_path> <output_file_path>")
      System.exit(1)
    }

    val input_file_path = args(0)
    val output_file_path = args(1)

    val dataRDD = sc.textFile(input_file_path)
      .map(x => x.split('\t'))
      .map(x => (LineSegment(Point(Array(x(2).toDouble, x(1).toDouble)),
        Point(Array(x(4).toDouble, x(3).toDouble))),
        TrajMeta(x(0).toInt, x(5).toInt)))

    val bf_meta = BloomFilterMeta(N, 1)
    val bc_bf_meta = sc.broadcast(bf_meta)
    BloomFilter.meta = bf_meta

    val num_partitions = dataRDD.getNumPartitions
    val (partitioned_rdd, part_mbrs) = STRSegPartition(dataRDD, num_partitions, 0.01, max_entries_per_node)

    val part_bounds = partitioned_rdd.mapPartitions(iter => {
      if (iter.nonEmpty) {
        var maxx = Double.MinValue
        var maxy = Double.MinValue
        var minx = Double.MaxValue
        var miny = Double.MaxValue
        iter.map(_._1).foreach(x => {
          maxx = Math.max(Math.max(x.start.coord(0), x.end.coord(0)), maxx)
          maxy = Math.max(Math.max(x.start.coord(1), x.end.coord(1)), maxy)
          minx = Math.min(Math.min(x.start.coord(0), x.end.coord(0)), minx)
          miny = Math.min(Math.min(x.start.coord(1), x.end.coord(1)), miny)
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
