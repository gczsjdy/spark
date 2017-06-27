package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/**
  * Created by gcz on 17-5-9.
  */
class VectorizedExpressionSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  test("Vectorized add") {
    disableWholeStageCodegen {

      val longTest = spark.range(0, 10000).select($"id" + $"id" + $"id")
      val doubleTest = spark.range(0, 10000).map(0.1 * _).select($"value" + $"value")
      checkAnswer(
        longTest,
        (0 until 10000).map(i => Row(3 * i))
      )
      checkAnswer(
        doubleTest,
        (0 until 10000).map(i => Row(0.2 * i))
      )
      println(longTest.explain(true))
      println(doubleTest.explain(true))
    }
  }

  test("Vectorized substring") {
    disableWholeStageCodegen {

      val value = "abcdefgh"
      val schema = StructType(List(StructField("value", StringType)))

      val substring = s"substring(value, ${value.length/2})"
      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select $substring from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row("defgh"))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized upper") {
    disableWholeStageCodegen {
      val value = "abcdefgh"
      val schema = StructType(List(StructField("value", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select upper(value) from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row(value.toUpperCase))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized lower") {
    disableWholeStageCodegen {
      val value = "ABCDEFGH"
      val schema = StructType(List(StructField("value", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select lower(value) from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row(value.toLowerCase))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized concat") {
    disableWholeStageCodegen {
      val value = "abcdefgh"
      val value2 = "ijkl"
      val schema = StructType(List(StructField("value", StringType), StructField("value2", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value, value2))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select concat(value, value2) from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row("abcdefghijkl"))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized trim") {
    disableWholeStageCodegen {
      val value = " abcdefgh "
      val schema = StructType(List(StructField("value", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select trim(value) from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row(value.trim))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized length") {
    disableWholeStageCodegen {
      val value = "abcdefgh"
      val schema = StructType(List(StructField("value", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select length(value) from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row(value.length))
      )
      println(test.explain(true))
    }
  }

  test("Vectorized abs") {
    disableWholeStageCodegen {
      val doubleTest = spark.range(-10000, 0).selectExpr("abs(id)")
      checkAnswer(
        doubleTest,
        (Range(10000, 0, -1)).map(i => Row(i))
      )
      println(doubleTest.explain(true))
    }
  }

  test("Vectorized sqrt") {
    disableWholeStageCodegen {
      val doubleTest = spark.range(0, 10000).map(0.1 * _).selectExpr("sqrt(value)")
      checkAnswer(
        doubleTest,
        (0 until 10000).map(i => Row(Math.sqrt(0.1 * i)))
      )
      println(doubleTest.explain(true))
    }
  }

  test("Vectorized toUnixTimeStamp") {
    disableWholeStageCodegen {
      val value = "2016-04-08"
      val schema = StructType(List(StructField("value", StringType)))

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(1000)(Row(value))), schema).createOrReplaceTempView("string_table")

      val test = spark.sql(s"select to_unix_timestamp(value, 'yyyy-MM-dd') from string_table")

      checkAnswer(
        test,
        (0 until 1000).map(i => Row(1460041200))
      )
      println(test.explain(true))
    }
  }

  def disableWholeStageCodegen(f: => Unit): Unit = {
    withSQLConf("spark.sql.codegen.wholeStage" -> "false"){
      f
    }
  }
}
