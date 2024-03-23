package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object RDDG {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterFollowerCount")
    conf.set("spark.logLineage", "true")
    val sc = new SparkContext(conf)

    val twitterDataRDD = sc.textFile(args(0))
      .map(line => line.split(","))
      .filter { case (x) => x(1).toInt % 100 == 0 }

    val RDDGfollowerCount = twitterDataRDD
      .map(x => (x(1), 1))
      .groupByKey()
      .mapValues(_.size)

    RDDGfollowerCount.saveAsTextFile(args(1))

    println(RDDGfollowerCount.toDebugString)

    sc.stop()
  }
}
