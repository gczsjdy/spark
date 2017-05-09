package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

/**
  * Created by gcz on 17-5-9.
  */
class ArithmeticVectorizedSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  test("Vectorized add") {
    val table = "test"
    withTempView(table) {
//      val text = (0 until 1000).foldLeft[String](""){
//        (str, i) => str + s",($i, ${i+1})"
//      }.substring(1)
//
//      spark.sql(s"create table $table (col1 int, col2 int)")
//      spark.sql(s"insert into $table values text")
//spark.sql("create external table test(col1 int, col2 int) location '/home/gcz/test.json'")

      Seq((1, 2)).toDF("col1", "col2").createOrReplaceTempView(table)
//      checkAnswer(
//        spark.sql(
//          s"""
//             |SELECT
//             |  col1 + col2
//             |FROM $table
//           """.stripMargin),
//        (0 until 1000).map(i => Row(2 * i + 1))
//      )
      println(sql(s"select col1 + col2, col1, col1* col2 from $table").explain())
    }

  }
}
