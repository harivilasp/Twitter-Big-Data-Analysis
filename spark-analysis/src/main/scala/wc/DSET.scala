package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DSET {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val inputPath = args(0)
    val outputPath = args(1)

    val followersDF = spark.read
      .option("header", "false")
      .csv(inputPath)
      .toDF("followee", "follower")
      .select($"followee".cast("int"), $"follower".cast("int"))
      .filter($"followee" % 100 === 0)

    val groupedDF = followersDF.groupBy("followee").count()

    groupedDF
      .rdd
      .map(row => row.mkString(","))
      .saveAsTextFile(outputPath)

    groupedDF.explain(true)
    spark.stop()
  }
}
