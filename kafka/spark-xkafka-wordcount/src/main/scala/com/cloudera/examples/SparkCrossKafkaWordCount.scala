package com.cloudera.examples

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, explode, split, window}

import scala.collection.JavaConversions.dictionaryAsScalaMap

object SparkCrossKafkaWordCount {
  val properties = new Properties()

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Syntax: spark-submit <properties_file>")
      System.exit(1)
    }
    loadProperties(args(0))

    val spark = SparkSession
      .builder
      .appName("KafkaExample")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .options(kafkaProps("input"))
      .option("subscribe", inputTopic())
      .load()

    // Split and explode words
    val words = df
      .select(
        current_timestamp().alias("processingTime"),
        explode(split($"value".cast("STRING"), " +")).alias("word")
      )

    // Generate running word count
    val wordCounts = words
      .withWatermark("processingTime", "10 seconds")
      .groupBy(
        window($"processingTime", "10 seconds", "10 seconds"),
        $"word")
      .count()

    // Write results to console
    val console = wordCounts
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    // Convert each line of result to a message string and write it to Kafka on another cluster
    val messages = wordCounts
      .selectExpr("concat(CAST(window AS STRING), ': word=', word, ', count=', CAST(count AS STRING)) as value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .options(kafkaProps("output"))
      .option("topic", outputTopic())
      .start()

    console.awaitTermination()
    messages.awaitTermination()
  }

  def loadProperties(path: String): Unit = {
    val is = new FileInputStream(path)
    properties.load(is)
  }

  def kafkaProps(prefix: String): Map[String, String] = {
    val regex = (prefix + raw"\.(.*)").r
    val partFunc: PartialFunction[(String, String), (String, String)] = {
      case (regex(k), v) => (k, v)
    }
    (for (p <- properties.map(p => (p._1.toString, p._2.toString)) if partFunc isDefinedAt p) yield partFunc(p)).toMap
  }

  def topics(key: String): String = {
    properties.getOrDefault(key, "").toString
  }

  def inputTopic(): String = {
    topics("inputTopic")
  }

  def outputTopic(): String = {
    topics("outputTopic")
  }
}
