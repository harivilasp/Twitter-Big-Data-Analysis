package wc

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.broadcast

object TwitterTriangleRepDSJoin {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val spark = SparkSession.builder()
      .appName("TwitterFollowerTriangleRepDSCount")
      .config("spark.logLineage", "true")
      .getOrCreate()

    import spark.implicits._

    val inputPath = args(0)
    val outputPath = args(1)
    val max = args(2).toInt

    val edges = spark.read
      .option("header", "false")
      .csv(inputPath)
      .toDF("followee", "follower")
      .select($"followee".cast("int"), $"follower".cast("int"))
      .filter($"followee" < max && $"follower" < max)

    val smallDF = broadcast(edges)
    val pathTwoDF = edges.as("dataframe1").join(smallDF.as("dataframe2"))
      .where($"dataframe1.followee" === $"dataframe2.follower")
      .select($"dataframe1.follower", $"dataframe2.followee").toDF("follower", "followee")

    val triangles = pathTwoDF.as("dataframe1").join(smallDF.as("dataframe2"))
      .where($"dataframe1.followee" === $"dataframe2.follower" && $"dataframe1.follower" === $"dataframe2.followee")
      .select($"dataframe1.follower")

    logger.info(triangles.explain(extended = true))
    println("The number of triangles is: " + triangles.count() / 3)
    logger.info("The number of triangles is: " + triangles.count() / 3)
    val result = triangles.count() / 3
    spark.sparkContext.parallelize(Seq(result)).saveAsTextFile(outputPath)
    spark.stop()
  }
}
