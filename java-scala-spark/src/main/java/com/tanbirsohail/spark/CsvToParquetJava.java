package com.tanbirsohail.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToParquetJava {
    private static final SparkConf sparkConf = new SparkConf().setAppName("CsvToParquetJava").setMaster("local");
    private static final SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
     // To avoid too many INFO log messages

    public static void main(String[] args) {
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> inputDf = spark.read().format("csv").option("inferSchema", "true").option("header", "true").load("src/main/resources/california_housing.csv");
        inputDf.show(10, false);
        inputDf.printSchema();
        inputDf.write().parquet("output/csvtoparquetjava/out.parquet");
    }
}
