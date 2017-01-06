/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.",
  extended = """
    Examples:
      > SELECT _FUNC_(1 < 2, 'a', 'b');
       a
  """)
// scalastyle:on line.size.limit
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (!trueValue.dataType.sameType(falseValue.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueValue.dataType.simpleString} and ${falseValue.dataType.simpleString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = trueValue.dataType

  override def eval(input: InternalRow): Any = {
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = predicate.genCode(ctx)
    val trueEval = trueValue.genCode(ctx)
    val falseEval = falseValue.genCode(ctx)

    // place generated code of condition, true value and false value in separate methods if
    // their code combined is large
    val combinedLength = condEval.code.length + trueEval.code.length + falseEval.code.length
    val generatedCode = if (combinedLength > 1024 &&
      // Split these expressions only if they are created from a row object
      (ctx.INPUT_ROW != null && ctx.currentVars == null)) {

      val (condFuncName, condGlobalIsNull, condGlobalValue) =
        createAndAddFunction(ctx, condEval, predicate.dataType, "evalIfCondExpr")
      val (trueFuncName, trueGlobalIsNull, trueGlobalValue) =
        createAndAddFunction(ctx, trueEval, trueValue.dataType, "evalIfTrueExpr")
      val (falseFuncName, falseGlobalIsNull, falseGlobalValue) =
        createAndAddFunction(ctx, falseEval, falseValue.dataType, "evalIfFalseExpr")
      s"""
        $condFuncName(${ctx.INPUT_ROW});
        boolean ${ev.isNull} = false;
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        if (!$condGlobalIsNull && $condGlobalValue) {
          $trueFuncName(${ctx.INPUT_ROW});
          ${ev.isNull} = $trueGlobalIsNull;
          ${ev.value} = $trueGlobalValue;
        } else {
          $falseFuncName(${ctx.INPUT_ROW});
          ${ev.isNull} = $falseGlobalIsNull;
          ${ev.value} = $falseGlobalValue;
        }
      """
    }
    else {
      s"""
        ${condEval.code}
        boolean ${ev.isNull} = false;
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        if (!${condEval.isNull} && ${condEval.value}) {
          ${trueEval.code}
          ${ev.isNull} = ${trueEval.isNull};
          ${ev.value} = ${trueEval.value};
        } else {
          ${falseEval.code}
          ${ev.isNull} = ${falseEval.isNull};
          ${ev.value} = ${falseEval.value};
        }
      """
    }

    ev.copy(code = generatedCode)
  }

  private def createAndAddFunction(
      ctx: CodegenContext,
      ev: ExprCode,
      dataType: DataType,
      baseFuncName: String): (String, String, String) = {
    val globalIsNull = ctx.freshName("isNull")
    ctx.addMutableState("boolean", globalIsNull, s"$globalIsNull = false;")
    val globalValue = ctx.freshName("value")
    ctx.addMutableState(ctx.javaType(dataType), globalValue,
      s"$globalValue = ${ctx.defaultValue(dataType)};")
    val funcName = ctx.freshName(baseFuncName)
    val funcBody =
      s"""
         |private void $funcName(InternalRow ${ctx.INPUT_ROW}) {
         |  ${ev.code.trim}
         |  $globalIsNull = ${ev.isNull};
         |  $globalValue = ${ev.value};
         |}
         """.stripMargin
    ctx.addNewFunction(funcName, funcBody)
    (funcName, globalIsNull, globalValue)
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}

/**
 * Abstract parent class for common logic in CaseWhen and CaseWhenCodegen.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
abstract class CaseWhenBase(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression])
  extends Expression with Serializable {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  def valueTypes: Seq[DataType] = branches.map(_._2.dataType) ++ elseValue.map(_.dataType)

  def valueTypesEqual: Boolean = valueTypes.size <= 1 || valueTypes.sliding(2, 1).forall {
    case Seq(dt1, dt2) => dt1.sameType(dt2)
  }

  override def dataType: DataType = branches.head._2.dataType

  override def nullable: Boolean = {
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.map(_.nullable).getOrElse(true)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    // Make sure all branch conditions are boolean types.
    if (valueTypesEqual) {
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
  }

  override def eval(input: InternalRow): Any = {
    var i = 0
    val size = branches.size
    while (i < size) {
      if (java.lang.Boolean.TRUE.equals(branches(i)._1.eval(input))) {
        return branches(i)._2.eval(input)
      }
      i += 1
    }
    if (elseValue.isDefined) {
      return elseValue.get.eval(input)
    } else {
      return null
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }
}


/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END - When `expr1` = true, returns `expr2`; when `expr3` = true, return `expr4`; else return `expr5`.")
// scalastyle:on line.size.limit
case class CaseWhen(
    val branches: Seq[(Expression, Expression)],
    val elseValue: Option[Expression] = None)
  extends CaseWhenBase(branches, elseValue) with CodegenFallback with Serializable {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    super[CodegenFallback].doGenCode(ctx, ev)
  }

  def toCodegen(): CaseWhenCodegen = {
    CaseWhenCodegen(branches, elseValue)
  }
}

/**
 * CaseWhen expression used when code generation condition is satisfied.
 * OptimizeCodegen optimizer replaces CaseWhen into CaseWhenCodegen.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
case class CaseWhenCodegen(
    val branches: Seq[(Expression, Expression)],
    val elseValue: Option[Expression] = None)
  extends CaseWhenBase(branches, elseValue) with Serializable {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Generate code that looks like:
    //
    // condA = ...
    // if (condA) {
    //   valueA
    // } else {
    //   condB = ...
    //   if (condB) {
    //     valueB
    //   } else {
    //     condC = ...
    //     if (condC) {
    //       valueC
    //     } else {
    //       elseValue
    //     }
    //   }
    // }
    val cases = branches.map { case (condExpr, valueExpr) =>
      val cond = condExpr.genCode(ctx)
      val res = valueExpr.genCode(ctx)
      s"""
        ${cond.code}
        if (!${cond.isNull} && ${cond.value}) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
      """
    }

    var generatedCode = cases.mkString("", "\nelse {\n", "\nelse {\n")

    elseValue.foreach { elseExpr =>
      val res = elseExpr.genCode(ctx)
      generatedCode +=
        s"""
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        """
    }

    generatedCode += "}\n" * cases.size

    ev.copy(code = s"""
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      $generatedCode""")
  }
}

/** Factory methods for CaseWhen. */
object CaseWhen {
  def apply(branches: Seq[(Expression, Expression)], elseValue: Expression): CaseWhen = {
    CaseWhen(branches, Option(elseValue))
  }

  /**
   * A factory method to facilitate the creation of this expression when used in parsers.
   *
   * @param branches Expressions at even position are the branch conditions, and expressions at odd
   *                 position are branch values.
   */
  def createFromParser(branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case cond :: value :: Nil => Some((cond, value))
      case value :: Nil => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 == 1) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}

/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * When a = b, returns c; when a = d, returns e; else returns f.
 */
object CaseKeyWhen {
  def apply(key: Expression, branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case cond :: value :: Nil => Some((EqualTo(key, cond), value))
      case value :: Nil => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 == 1) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}

/**
 * A function that returns the index of str in (str1, str2, ...) list or 0 if not found.
 * It takes at least 2 parameters, and all parameters' types should be subtypes of AtomicType.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, str1, str2, ...) - Returns the index of str in the str1,str2,... or 0 if not found.",
  extended = """
    Examples:
      > SELECT _FUNC_(10, 9, 3, 10, 4);
       3
  """)
case class Field(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(children(0).dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires at least 2 arguments")
    } else if (!children.forall(_.dataType.isInstanceOf[AtomicType])) {
      TypeCheckResult.TypeCheckFailure(s"FIELD requires all arguments to be of AtomicType")
    } else
      TypeCheckResult.TypeCheckSuccess
  }

  override def dataType: DataType = IntegerType

  override def eval(input: InternalRow): Any = {
    val target = children.head.eval(input)
    val targetDataType = children.head.dataType
    def findEqual(target: Any, params: Seq[Expression], index: Int): Int = {
      params.toList match {
        case Nil => 0
        case head::tail if targetDataType == head.dataType
          && head.eval(input) != null && ordering.equiv(target, head.eval(input)) => index
        case _ => findEqual(target, params.tail, index + 1)
      }
    }
    if(target == null)
      0
    else
      findEqual(target, children.tail, 1)
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val target = evalChildren(0)
    val targetDataType = children(0).dataType
    val rest = evalChildren.drop(1)
    val restDataType = children.drop(1).map(_.dataType)

    def updateEval(evalWithIndex: ((ExprCode, DataType), Int)): String = {
      val ((eval, dataType), index) = evalWithIndex
      s"""
        ${eval.code}
        if (${dataType.equals(targetDataType)}
          && ${ctx.genEqual(targetDataType, eval.value, target.value)}) {
          ${ev.value} = ${index};
        }
      """
    }

    def genIfElseStructure(code1: String, code2: String): String = {
      s"""
         ${code1}
         else {
          ${code2}
         }
       """
    }

    def dataTypeEqualsTarget(evalWithIndex: ((ExprCode, DataType), Int)): Boolean = {
      val ((eval, dataType), index) = evalWithIndex
      dataType.equals(targetDataType)
    }

    ev.copy(code = s"""
      ${target.code}
      boolean ${ev.isNull} = false;
      int ${ev.value} = 0;
      ${rest.zip(restDataType).zipWithIndex.map(x => (x._1, x._2 + 1)).filter(
        dataTypeEqualsTarget).map(updateEval).reduceRight(genIfElseStructure)}
      """)
  }
}
