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

      checkAnswer(
        spark.range(0, 10000).select($"id" + $"id"),
        (0 until 10000).map(i => Row(2 * i))
      )
      println(spark.range(1, 10000).select($"id" + $"id").explain(true))
    }

  }
  test("Vectorized add external table") {
//    withTable("tmp") {
//      spark.sql("create table tmp (value string)")
//      spark.sql("load data local inpath './sql/hive/src/test/resources/data/files/kv1.txt' into table tmp")
//
//    }

  }
}
