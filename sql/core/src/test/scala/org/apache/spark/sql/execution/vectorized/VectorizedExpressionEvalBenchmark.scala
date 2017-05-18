package org.apache.spark.sql.execution.vectorized

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Benchmark

import scala.util.Try

/**
  * Created by gcz on 17-5-18.
  */
object VectorizedExpressionEvalBenchmark {

  val count = 10000

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

  val rowBasedAdd = {i: Int =>
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      spark.range(0, count).select($"id" + $"id" + $"id")
    }
  }

  val vectorizedAdd = {i: Int =>
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      spark.range(0, count).select($"id" + $"id" + $"id")
    }
  }

  val benchmark = new Benchmark("Add expresion evaluation", 1)
    benchmark.addCase("Row-based eval")(rowBasedAdd)
    benchmark.addCase("Vectorized eval")(vectorizedAdd)
    benchmark.run()

  def main(args: Array[String]) = {

  }
}
