package wc

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object TwitterTriangleRepRDDJoin {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("TwitterFollowerTriangleRepRDDCount")
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
      .filter { case (followee, follower) => followee < max && follower < max }

    // Create a broadcast variable for efficient lookup
    val smallMap = sc.broadcast(edges.groupBy { case (from, to) => to }.collectAsMap())

    // Define a long accumulator to count triangles
    val counter = sc.longAccumulator("Triangle Accumulator")

    // Traverse edges to find triangles
    edges.foreach { case (followee, follower) =>
      smallMap.value.get(followee).foreach { z =>
        z.foreach { case (mapFrom, mapTo) =>
          smallMap.value.get(mapFrom).foreach { x =>
            x.foreach { case (f, t) =>
              if (follower == f) {
                counter.add(1)
              }
            }
          }
        }
      }
    }

    println(s"Total triangles: ${counter.value / 3}")
    logger.info(s"Total triangles: ${counter.value / 3}")
    sc.parallelize(Seq(counter.value / 3)).saveAsTextFile(outputPath)
    sc.stop()
  }
}
