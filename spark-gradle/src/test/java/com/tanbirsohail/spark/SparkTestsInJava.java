package com.tanbirsohail.spark;


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.CollectionUtils;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class SparkTestsinJava {

    private static SparkSession spark;

    @BeforeAll
    static void setupAllTests() {
        spark = SparkSession.builder()
                .appName("unit-testing")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void teardownAllTests() {
       spark.stop();
    }

    @Test
    void testDataframeJoin() {
        String[] expectedColumns = {"Pk", "Name", "Age", "Gender", "Occupation"};

        // df1
        List<Row> df1Rows = new ArrayList<>();
        df1Rows.add(RowFactory.create("id12", "Mark", 23));
        df1Rows.add(RowFactory.create("id13", "Steve", 24));
        df1Rows.add(RowFactory.create("id14", "Amy", 22));

        // More verbose way of creating schema
        StructType df1Schema = new StructType()
                .add(new StructField("Pk", DataTypes.StringType, false, Metadata.empty()))
                .add(new StructField("Name", DataTypes.StringType, false, Metadata.empty()))
                .add(new StructField("Age", DataTypes.IntegerType, false, Metadata.empty()));

        Dataset<Row> df1 = spark.createDataFrame(df1Rows, df1Schema);

        // df2
        List<Row> df2Rows = new ArrayList<>();
        df2Rows.add(RowFactory.create("id12", "Male"));
        df2Rows.add(RowFactory.create("id13", "Male"));
        df2Rows.add(RowFactory.create("id14", "Female"));

        // Easier way of creating schema
        StructType df2Schema = new StructType()
        .add("Pk", "String", false)
        .add("Gender", "String", false);

        Dataset<Row> df2 = spark.createDataFrame(df2Rows, df2Schema);

        // df3
        List<Row> df3Rows = new ArrayList<>();
        df3Rows.add(RowFactory.create("id12", "Teacher"));
        df3Rows.add(RowFactory.create("id13", "Fireman"));
        df3Rows.add(RowFactory.create("id14", "Nurse"));

        StructType df3Schema = new StructType()
        .add("Pk", "String", false)
        .add("Occupation", "String", false);

        Dataset<Row> df3 = spark.createDataFrame(df3Rows, df3Schema);

        // Join
        Dataset<Row> actual = df1.join(df2,"Pk").join(df3, "Pk");

        actual.show(false);

        assert actual.count() != 0;
        assert Arrays.equals(expectedColumns, actual.columns());

        Row amy = actual.where("Pk == 'id14'").select("Name", "Age", "Occupation").head();
        assert 22 == (int) amy.getAs("Age");


    }
}
