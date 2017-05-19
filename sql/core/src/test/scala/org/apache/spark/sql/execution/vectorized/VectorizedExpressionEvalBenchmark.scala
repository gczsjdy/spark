package org.apache.spark.sql.execution.vectorized

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Benchmark

import scala.util.Try

object VectorizedExpressionEvalBenchmark {

  def add(iters: Int) = {

    val count = 1000000000

    val spark = SparkSession.builder
      .master("local[1]")
      .appName("test-expression-vectorization")
      .getOrCreate()

    import spark.implicits._

    // Set default configs. Individual cases will change them if necessary.
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")


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

    def getRowBasedAdd(numAdd: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        val column = (0 until numAdd).foldLeft[Column]($"id"){
          (tot, col) => tot + $"id"
        }
        val range = spark.range(0, count)
        (0 until iters).foreach { _ =>
          range.select(column)
        }
      }
    }

    def getVectorizedAdd(numAdd: Int): Int => Unit = { i: Int =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val column = (0 until numAdd).foldLeft[Column]($"id"){
          (tot, col) => tot + $"id"
        }
        val range = spark.range(0, count)
        (0 until iters).foreach { _ =>
          range.select(column)
        }
      }
    }

    val rowBasedAdd = getRowBasedAdd(2)

    val rowBasedAdd100 = getRowBasedAdd(100)

    val rowBasedAdd500 = getRowBasedAdd(500)

    val vectorizedAdd = getVectorizedAdd(2)

    val vectorizedAdd100 = getVectorizedAdd(100)

    val vectorizedAdd500 = getVectorizedAdd(500)

    val benchmark = new Benchmark(s"Add expresion evaluation with rows: $count, iters: $iters", count*iters)
    benchmark.addCase("Row-based add 2 columns")(rowBasedAdd)
    benchmark.addCase("Vectorized add 2 columns")(vectorizedAdd)
    benchmark.addCase("Row-based add 100 columns")(rowBasedAdd100)
    benchmark.addCase("Vectorized add 100 columns")(vectorizedAdd100)
    benchmark.addCase("Row-based add 2000 columns")(rowBasedAdd500)
    benchmark.addCase("Vectorized add 2000 columns")(vectorizedAdd500)
    benchmark.run()

  }
  def main(args: Array[String]) = {
    add(10)
  }
}
