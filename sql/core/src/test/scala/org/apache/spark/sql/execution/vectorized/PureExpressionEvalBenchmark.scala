package org.apache.spark.sql.execution.vectorized

import org.apache.spark.memory.MemoryMode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, RowNumber}
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.Benchmark

import scala.collection.JavaConverters._

/**
  * Created by gcz on 17-6-24.
  */
object PureExpressionEvalBenchmark {

  val numIters = 1

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.driver.memory", "10g")
    .config("spark.executor.memory", "10g")
    .getOrCreate()
  import spark.implicits._

  def prepareColumnarBatch(rowNumber: Int): RDD[ColumnarBatchBase] = {
    val internalRows = prepareInteralRows(rowNumber)
    internalRows.mapPartitions[ColumnarBatchBase]{
      (iter) => ColumnVectorUtils.fromInternalRowToBatch(
        new StructType().add("value", LongType), MemoryMode.ON_HEAP, iter.asJava
      ).asScala
    }
  }

  def prepareInteralRows(rowNumber: Int): RDD[InternalRow] = {
    spark.sparkContext.range(0, rowNumber, 1).map(i => InternalRow(i))
  }

  def prepareAddExpression = Add(Add(Add(Add(
    Add(BoundReference(0, LongType, true), BoundReference(0, LongType, true)),
    BoundReference(0, LongType, true)), BoundReference(0, LongType, true)), BoundReference(0, LongType, true)), BoundReference(0, LongType, true))

  val prepareSource =
    """
      |public Object generate(Object[] references) {
      |return new GeneratedIterator(references);
      |}
      |
      |final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
      |private Object[] references;
      |private scala.collection.Iterator[] inputs;
      |private scala.collection.Iterator inputadapter_input;
      |private UnsafeRow project_result;
      |private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder;
      |private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter;
      |
      |public GeneratedIterator(Object[] references) {
      |this.references = references;
      |}
      |
      |public void init(int index, scala.collection.Iterator[] inputs) {
      |partitionIndex = index;
      |this.inputs = inputs;
      |inputadapter_input = inputs[0];
      |project_result = new UnsafeRow(1);
      |this.project_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result, 0);
      |this.project_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder, 1);
      |
      |}
      |
      |protected void processNext() throws java.io.IOException {
      |while (inputadapter_input.hasNext() && !stopEarly()) {
      |InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
      |long inputadapter_value = inputadapter_row.getLong(0);
      |
      |boolean project_isNull = false;
      |
      |long project_value = -1L;
      |project_value = inputadapter_value + inputadapter_value + inputadapter_value+ inputadapter_value + inputadapter_value;
      |project_rowWriter.write(0, project_value);
      |append(project_result);
      |if (shouldStop()) return;
      |}
      |}
      |}
      |
    """.stripMargin


  def addMe(row: InternalRow) = {
    val value = row.getLong(0)
    value + value
  }

  def getSource(num: Int) = {
    val ds = spark.createDataFrame(spark.sparkContext.parallelize(
      Seq.fill(num)(Row(1))), new StructType().add("value", LongType)).select($"value" + $"value"* $"value")
    val plan = ds.queryExecution.executedPlan
    println(plan.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2.body)
    plan
  }
  
  def addExpressionBenchmark(rowNumber: Int) = {

    val vectorizedData = prepareColumnarBatch(rowNumber)
    val data = prepareInteralRows(rowNumber)

    val addExpression = prepareAddExpression

    val cleanedSource = new CodeAndComment(prepareSource, collection.Map.empty)
    val ctx = new CodegenContext
    val code = addExpression.genCode(ctx)

    val references = ctx.references.toArray

    val benchmark = new Benchmark("Parquet Reader Single Int Column Scan", rowNumber)
    benchmark.addCase("Vectorized Eval", numIters) { iter =>
      vectorizedData.map(addExpression.vectorizedEval(_))
    }
    benchmark.addCase("Whole-stage codegen Eval", numIters) { iter =>
      val c = data.mapPartitionsWithIndex { (index, iter) =>
        val clazz = CodeGenerator.compile(cleanedSource)
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        buffer.init(index, Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            buffer.hasNext
          }
          override def next: InternalRow = buffer.next()
        }
      }.count()//To verify foreach(row => println(s"${row.numFields} ${row.getLong(0)} ${row.getLong(1)}"))
    }
//    benchmark.addCase("Whole-stage codegen Eval2", numIters) { iter =>
//      val c = data.map { row =>
//        addMe(row)
//      }.count()//To verify foreach(row => println(s"${row.numFields} ${row.getLong(0)} ${row.getLong(1)}"))
//    }
//    benchmark.addCase("Row-based Eval", numIters) { iter =>
//      data.map { iter =>
//        InternalRow(addExpression.eval(iter))
//      }.collect()
//    }

    benchmark.run()
  }

  def main(args: Array[String]) = {
    val rowNumber = 100000000
    addExpressionBenchmark(rowNumber)
  }
}
