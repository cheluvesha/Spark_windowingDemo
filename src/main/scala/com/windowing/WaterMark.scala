package com.windowing

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  col,
  expr,
  from_json,
  sum,
  to_timestamp,
  window
}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WaterMark {
  val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    // Creating SparkSession Object
    val spark = SparkSession
      .builder()
      .appName("WaterMark")
      .master("local[*]")
      .config("spark.streaming.stopGracefullyOnShutdown", value = true)
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    logger.info("SparkSession is created")

    // Schema for DataFrame which is created from kafka source
    val schema = StructType(
      List(
        StructField("CreatedTime", StringType),
        StructField("Type", StringType),
        StructField("Amount", StringType),
        StructField("BrokerCode", StringType)
      )
    )
    // Reading DataFrom Kafka topic to create DataFrame
    val kafkaStockDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "trade")
      .option("startingOffsets", "earliest")
      .load()
    logger.info("DataFrame is created, reading from kafka")

    // json to string
    val valueDF = kafkaStockDF
      .select(from_json(col("value").cast("String"), schema).alias("values"))
    // casting to timestamp type
    val tradeDF = valueDF
      .select("values.*")
      .withColumn(
        "CreatedTime",
        to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("Buy", expr("case When Type='BUY' then Amount else 0 end"))
      .withColumn("Sell", expr("case When Type='SELL' then Amount else 0 end"))
    // performing tumbling window with watermarking
    val windowAggDF = tradeDF
      .withWatermark("CreatedTime", "30 minutes")
      .groupBy(window(col("CreatedTime"), "15 minutes"))
      .agg(sum("Buy").alias("TotalBuy"), sum("Sell").alias("TotalSell"))
    // selecting required columns
    val outputDF = windowAggDF.select(
      "window.start",
      "window.end",
      "TotalBuy",
      "TotalSell"
    )
    // Writing data into console
    val windowQuery = outputDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
    logger.info("Counting Invoices")

    windowQuery.awaitTermination()

  }
}
