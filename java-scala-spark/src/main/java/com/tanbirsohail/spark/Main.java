package com.tanbirsohail.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.util.Arrays;

public class Main {
    private static final SparkConf sparkConf = new SparkConf().setAppName("Main").setMaster("local");
    private static final SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
    public static void main(String[] args) {
        System.out.println("Spark Version is : " + spark.version());

        System.out.println("Printing all spark configurations : ");
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Tuple2<String, String>[] allConf = jsc.getConf().getAll();
        Arrays.stream(allConf).iterator().forEachRemaining( (t) -> System.out.println(t._1 + " : "+ t._2));
    }
}
