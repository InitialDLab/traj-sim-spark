name := "traj-sim"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.6.28"

libraryDependencies ++= Seq(
  "org.geotools" % "gt-geojson" % "15.2"
)

resolvers ++= Seq(
  "geosolutions" at "http://maven.geo-solutions.it/",
  "osgeo" at "http://download.osgeo.org/webdav/geotools/"
)
