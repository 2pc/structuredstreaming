package com.d11i.spark.structured.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

object StructuredStreaming2ES {


  case class Customer(id: String,
                      name: String,
                      age: Int,
                      isAdult: Boolean,
                      nowEpoch: Long)
  

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // run app locally utilizing all cores
      .master("local[*]")
      .appName(getClass.getName)
//      .config("es.nodes", "10.9.21.57")
//      .config("es.port", "9200")
      .config("es.index.auto.create", "true") // this will ensure that index is also created on first POST
      .config("es.nodes.wan.only", "true") // needed to run against dockerized ES for local tests
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "changeme")
      .getOrCreate()

    import spark.implicits._

    val customerEvents: Dataset[Customer] = spark
      .readStream
      .format("socket")
      .option("host", "10.9.21.57")
      .option("port", "7777")
      // maximum number of lines processed per trigger interval
      .option("maxFilesPerTrigger", 5)
      .load()
      .as[String]
      .map(line => {
        val cols = line.split(",")
        val age = cols(2).toInt
        Customer(cols(0), cols(1), age, age > 18, System.currentTimeMillis())
      })

    customerEvents
      .writeStream
      .outputMode(OutputMode.Append())
     // .format("es")
      .format("org.elasticsearch.spark.sql")// when "es" cannot found class "es.DefaultSource"
      .option("es.nodes", "localhost")
      .option("es.port", "9200")
      .option("checkpointLocation", "/tmp/checkpointLocation")
     // .option("es.mapping.id", "id")//how about no mapping.id?
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start("a/profile")
      .awaitTermination()
  }

}
