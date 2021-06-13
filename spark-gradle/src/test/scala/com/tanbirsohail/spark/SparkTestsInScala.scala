package com.tanbirsohail.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Test, _}


@TestInstance(Lifecycle.PER_CLASS)
class SparkTestsInScala extends Assertions {

  private var spark: SparkSession = _

  @BeforeAll
  def setupAllTests(): Unit = {
    spark =
      SparkSession.builder
        .appName("unit-testing")
        .master("local[*]")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  @AfterAll
  def teardownAllTests(): Unit = {
    spark.stop()
  }


  @Test
  def testDataframeJoin(): Unit = {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val expectedColumns = Array[String]("Pk", "Name", "Age", "Gender", "Occupation")
    val df1 = Seq(
      ("id12", "Mark", 23),
      ("id13", "Steve", 24),
      ("id14", "Amy", 22)
    ).toDF("Pk", "Name", "Age")

    val df2 = Seq(
      ("id12", "Male"),
      ("id13", "Male"),
      ("id14", "Female")
    ).toDF("Pk", "Gender")

    val df3 = Seq(
      ("id12", "Teacher"),
      ("id13", "Fireman"),
      ("id14", "Nurse")
    ).toDF("Pk", "Occupation")

    val actual:DataFrame = df1.join(df2, usingColumn = "Pk").join(df3, usingColumn = "Pk")

    // Assert Joined Values
    assert(actual.count() != 0L)
    assert(expectedColumns sameElements actual.columns)
    val amy: Row = actual.where($"Pk" === "id14").select($"Name", $"Age", $"Occupation").head()
    assert(22 == amy.getAs[Int]("Age"))

  }

}