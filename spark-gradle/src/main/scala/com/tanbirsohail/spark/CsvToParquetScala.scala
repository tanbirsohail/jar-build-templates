package com.tanbirsohail.spark

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.SparkConf



object CsvToParquetScala {
  val sparkConf:SparkConf = new SparkConf().setAppName("CsvToParquetScala").setMaster("local")
  val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") // To avoid too many INFO log messages

  def main(args: Array[String]): Unit = {
    val inputDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("src/main/resources/california_housing.csv")
    inputDf.show(10, truncate = false)
    inputDf.printSchema()
    inputDf.write.parquet("output/csvtoparquetscala/out.parquet")
  }



}
