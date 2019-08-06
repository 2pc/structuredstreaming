package com.d11i.spark.structured.streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

object ReadFromHiveByThriftJdbc{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName(getClass.getName)
      .getOrCreate()

    import spark.implicits._
    val thriftJdbcUrl = "jdbc:hive2://127.0.0.1:10020"
    val dialect = JdbcDialects
    /**
      * fix: when spark2.x no data return,only  schema when
      * link:https://issues.apache.org/jira/browse/SPARK-21063
      * link:https://stackoverflow.com/questions/44434304/read-data-from-remote-hive-on-spark-over-jdbc-returns-empty-result
      */
    JdbcDialects.unregisterDialect(dialect.get(thriftJdbcUrl))
    JdbcDialects.registerDialect(HiveDialect)

    val df = spark.read.format("jdbc")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
      .option("url", thriftJdbcUrl)
      .option("dbtable", "(select * from t limit 10) tmp_view")
      .option("user", "admin")
      .option("password", "123456")
      .option("fetchsize", "20")
      .load().limit(10)
   // LOG.info("##################before show##############################")
    df.show(2)

  }
}

case object HiveDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = url.startsWith("jdbc:hive2")
  override def quoteIdentifier(colName: String): String = {
    colName.split('.').map(part => s"`$part`").mkString(".")
  }
}

