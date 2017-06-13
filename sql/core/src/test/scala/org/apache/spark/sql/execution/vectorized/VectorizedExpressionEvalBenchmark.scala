package org.apache.spark.sql.execution.vectorized

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.Benchmark

import scala.util.Try

object VectorizedExpressionEvalBenchmark {

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("test-expression-vectorization")
    .getOrCreate()

  import spark.implicits._

  def add(iters: Int) = {

    val count = 1000000

    // Set default configs. Individual cases will change them if necessary.
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    def prepareAndExecAdd(numAdd: Int) = {
      val column = (0 until numAdd).foldLeft[Column]($"id"){
        (tot, col) => tot + $"id"
      }
      val range = spark.range(0, count)
      (0 until iters).foreach { _ =>
        range.select($"id", column).collect()
      }
    }

    def getRowBasedAdd(numAdd: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        prepareAndExecAdd(numAdd)
      }
    }

    def getVectorizedAdd(numAdd: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        prepareAndExecAdd(numAdd)
      }
    }

    def explain(numAdd: Int) = {
      val column = (0 until numAdd).foldLeft[Column]($"id"){
        (tot, col) => tot + $"id"
      }
      spark.range(0, count).select(column).explain()
    }

    // test 350-400 to see the turning point of significant performance drop of row-based add
    // on my computer with 32k l1i cache, it degrades significantly at 360 columns add case
    val testAddColumnNumber = (List(2, 200, 400) ++ Range.inclusive(350, 380, 10)).sorted

    val benchmark = new Benchmark(s"Add expresion evaluation", count*iters)
    explain(testAddColumnNumber.head)

    testAddColumnNumber.foreach {
      num =>
        benchmark.addCase(s"Row-based add $num columns")(getRowBasedAdd(num))
        benchmark.addCase(s"Vectorized add $num columns")(getVectorizedAdd(num))
    }
    benchmark.run()
//    Java HotSpot(TM) 64-Bit Server VM 1.8.0_111-b14 on Linux 4.4.0-78-generic
//    Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
//      Add expresion evaluation:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
//      ------------------------------------------------------------------------------------------------
//    Row-based add 2 columns                       2092 / 2171          4.8         209.2       1.0X
//      Vectorized add 2 columns                      3134 / 3146          3.2         313.4       0.7X
//      Row-based add 350 columns                     5436 / 5538          1.8         543.6       0.4X
//      Vectorized add 350 columns                    7798 / 7929          1.3         779.8       0.3X
//      Row-based add 360 columns                   37448 / 38106          0.3        3744.8       0.1X
//      Vectorized add 360 columns                    7858 / 7874          1.3         785.8       0.3X
//      Row-based add 370 columns                   38924 / 39023          0.3        3892.4       0.1X
//      Vectorized add 370 columns                    8282 / 8286          1.2         828.2       0.3X
  }

  def substring(iters: Int) = {

    val count = 1000

    val value = "abcdefgh"
    val schema = StructType(List(StructField("value", StringType)))

    // Set default configs. Individual cases will change them if necessary.
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    def prepareAndExecSubstring(numSubstring: Int) = {
      val substring = s"substring(value, ${value.length/2})"
      val column = (0 until numSubstring - 1).foldLeft[String](substring){
        (tot, col) => tot + "," + substring
      }
      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(count)(Row(value))), schema).createOrReplaceTempView("string_table")

      (0 until iters).foreach { _ =>
        spark.sql(s"select $column from string_table").collect()
      }
    }

    def getRowBasedSubstring(numSubstring: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true", SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "500") {
        prepareAndExecSubstring(numSubstring)
      }
    }

    def getVectorizedSubstring(numSubstring: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false", SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "500") {
        prepareAndExecSubstring(numSubstring)
      }
    }

    def explain(numSubstring: Int) = {
      val substring = s"substring(value, ${value.length/2})"
      val column = (0 until numSubstring).foldLeft[String](substring){
        (tot, col) => tot + "," + substring
      }
      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(count)(Row(value))), schema).createOrReplaceTempView("string_table")
      spark.sql(s"select $column from string_table").explain()
    }

    val testAddColumnNumber = (List(2, 100, 200, 300, 400)).sorted

    val benchmark = new Benchmark(s"Substring expresion evaluation", count*iters)
    explain(testAddColumnNumber.head)

    testAddColumnNumber.foreach {
      num =>
        benchmark.addCase(s"Vectorized $num substrings")(getVectorizedSubstring(num))
        benchmark.addCase(s"Row-based $num substrings")(getRowBasedSubstring(num))

    }
    benchmark.run()
//      Java HotSpot(TM) 64-Bit Server VM 1.8.0_111-b14 on Linux 4.4.0-78-generic
//      Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
//      Substring expresion evaluation:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
//      ------------------------------------------------------------------------------------------------
//      Vectorized 2 substrings                       1294 / 1334          0.1       12942.8       1.0X
//      Row-based 2 substrings                         923 /  948          0.1        9226.3       1.4X
//      Vectorized 100 substrings                     4113 / 4117          0.0       41126.9       0.3X
//      Row-based 100 substrings                      4074 / 4117          0.0       40744.0       0.3X
//      Vectorized 200 substrings                     7396 / 7425          0.0       73955.5       0.2X
//      Row-based 200 substrings                      8247 / 8271          0.0       82474.5       0.2X
//      Vectorized 300 substrings                   11143 / 11159          0.0      111434.1       0.1X
//      Row-based 300 substrings                    12261 / 12268          0.0      122606.6       0.1X
//      Vectorized 400 substrings                   15138 / 15146          0.0      151377.4       0.1X
//      Row-based 400 substrings                    16481 / 16492          0.0      164807.4       0.1
  }

  def stringOpsTest(iters: Int) = {
    val count = 1000

    val value = "abcd"
    val value2 = "ef"
    val schema = StructType(List(StructField("value", StringType), StructField("value2", StringType)))

    // Set default configs. Individual cases will change them if necessary.
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    def prepareAndExecNestedStringOps(num: Int) = {
        val sql = s"upper(value)"
        val column = (0 until num - 1).foldLeft[String](sql) {
          (tot, col) => s"concat(upper(trim(substring($tot, 3))), value2)"
        }
        spark.createDataFrame(spark.sparkContext.parallelize(
          Seq.fill(count)(Row(value, value2))), schema).createOrReplaceTempView("string_table")

        (0 until iters).foreach { _ =>
          spark.sql(s"select $column from string_table").collect()
        }
    }

    def prepareAndExecMultipleStringOps(num: Int) = {
            val sql = s"substring(value, ${value.length/2}), upper(value), concat(value, value2), length(value), trim(value2), lower(value)"
      val column = (0 until num - 1).foldLeft[String](sql) {
        (tot, col) => s"$tot, $sql"
      }
      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(count)(Row(value, value2))), schema).createOrReplaceTempView("string_table")

      (0 until iters).foreach { _ =>
        spark.sql(s"select $column from string_table").collect()
      }
    }

    def getRowBasedStringOps(num: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true", SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> "500") {
        prepareAndExecNestedStringOps(num)
      }
    }

    def getVectorizedStringOps(num: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false", SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> "500") {
        prepareAndExecNestedStringOps(num)
      }
    }

    def explain(num: Int) = {
          val sql = s"value"
      val column = (0 until num - 1).foldLeft[String](sql) {
        (tot, col) => s"concat(upper(trim(substring($tot, 3))), value2)"
      }

      spark.createDataFrame(spark.sparkContext.parallelize(
        Seq.fill(count)(Row(value, value2))), schema).createOrReplaceTempView("string_table")

      spark.sql(s"select $column from string_table").explain()
      spark.sql(s"select $column from string_table").show()

    }

    val testAddColumnNumber = (List(2, 50, 75, 100, 120)).sorted

    val benchmark = new Benchmark(s"Multiple StringOps expresion evaluation", count*iters)
    explain(testAddColumnNumber.head)

    testAddColumnNumber.foreach {
      num =>
        benchmark.addCase(s"Vectorized $num stringOps")(getVectorizedStringOps(num))
        benchmark.addCase(s"Row-based $num stringOps")(getRowBasedStringOps(num))
    }
    benchmark.run()

//      Java HotSpot(TM) 64-Bit Server VM 1.8.0_111-b14 on Linux 4.4.0-78-generic
//      Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
//      Multiple StringOps expresion evaluation: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
//      ------------------------------------------------------------------------------------------------
//      Vectorized 2 stringOps                         144 /  158          0.1       14370.6       1.0X
//      Row-based 2 stringOps                          104 /  111          0.1       10366.2       1.4X
//      Vectorized 50 stringOps                       6550 / 6552          0.0      654955.5       0.0X
//      Row-based 50 stringOps                        6563 / 6589          0.0      656313.2       0.0X
//      Vectorized 75 stringOps                     16432 / 16482          0.0     1643201.4       0.0X
//      Row-based 75 stringOps                      16604 / 16706          0.0     1660386.1       0.0X
//      Vectorized 100 stringOps                    33082 / 33264          0.0     3308190.5       0.0X
//      Row-based 100 stringOps                     33253 / 33518          0.0     3325294.3       0.0X
//      Vectorized 120 stringOps                    51726 / 51973          0.0     5172563.5       0.0X
//      Row-based 120 stringOps                     52864 / 53080          0.0     5286419.9       0.0X
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  def main(args: Array[String]) = {
    add(10)
    substring(100)
    stringOpsTest(10)
  }
}
