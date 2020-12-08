package com.tanbirsohail.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Main {
    private static final SparkConf sparkConf = new SparkConf().setAppName("CsvToParquetJava").setMaster("local");
    private static final SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
    public static void main(String[] args) {
        System.out.println("Spark Version is : " + spark.version());
    }
}
