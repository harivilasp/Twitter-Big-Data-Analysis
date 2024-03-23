package wc

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object TwitterTriangleRedRDDJoin {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("TwitterFollowerTriangleRedRDDCount")
    conf.set("spark.logLineage", "true")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)
    val max = args(2).toInt
    // Load the data
    val edges = sc.textFile(inputPath)
      .map(line => {
        val tokens = line.split(",")
        (tokens(0).toInt, tokens(1).toInt)
      })
      .filter { case (follower, followee) => follower < max && followee < max }

    // Reverse the edges for joining with paths
    val reversedEdges = edges.map { case (follower, followee) => (followee, follower) }

    // Generate paths: (intermediate, (follower, followee))
    // two-step paths
    val paths = edges.join(reversedEdges)
      .map { case (intermediate, (followee, follower)) => (follower, (intermediate, followee)) }

    // Join paths with original edges to find triangles: (follower, ((intermediate, followee), follower2))
    // Filter only valid triangles: follower == follower2
    val potentialTriangles = paths.join(reversedEdges)
      .filter { case (follower, ((intermediate, followee), follower2)) => followee == follower2 }
      .map { case (follower, ((intermediate, followee), _)) => (follower, intermediate, followee) }

    val triangles = potentialTriangles.count() / 3;

//    potentialTriangles.saveAsTextFile(outputPath)
    println(s"Total triangles: $triangles")
    logger.info(potentialTriangles.toDebugString)
    logger.info(s"Total triangles: $triangles")
    sc.parallelize(Seq(triangles)).saveAsTextFile(outputPath)
    sc.stop()
  }
}
