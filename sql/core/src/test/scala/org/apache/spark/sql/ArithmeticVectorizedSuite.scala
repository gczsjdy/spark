package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

/**
  * Created by gcz on 17-5-9.
  */
class ArithmeticVectorizedSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  test("Vectorized add local table") {
    val table = "test"
    withSQLConf("spark.sql.codegen.wholeStage" -> "false"){

      val longTest = spark.range(0, 10000).select($"id" + $"id")
      val doubleTest = spark.range(0, 10000).map(0.1 * _).select($"value" + $"value")
      checkAnswer(
        longTest,
        (0 until 10000).map(i => Row(2 * i))
      )
      checkAnswer(
        doubleTest,
        (0 until 10000).map(i => Row(0.2 * i))
      )
      println(longTest.explain(true))
      println(doubleTest.explain(true))
    }
  }
}
