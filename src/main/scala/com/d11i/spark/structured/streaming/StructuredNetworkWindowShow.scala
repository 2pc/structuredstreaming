package com.d11i.spark.structured.streaming

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StructuredNetworkWindowShow {

  def main(args: Array[String]) {

    //    if (args.length < 3) {
    //
    //      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
    //
    //        " <window duration in seconds> [<slide duration in seconds>]")
    //
    //      System.exit(1)
    //
    //    }

    val host = "10.9.21.57"
    val port = "6666"
    val windowSize = 3
    val slideSize = 2
    val triggerTime = 1
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredNetworkWindowShow"+String.valueOf(System.currentTimeMillis()))
      .getOrCreate()

    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()
    val wordCounts:DataFrame = lines.select(window($"timestamp",windowDuration,slideDuration),$"value")
    // Start running the query that prints the windowed word counts to the console

    val query = wordCounts.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .trigger(ProcessingTime(s"$triggerTime seconds"))
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }

}