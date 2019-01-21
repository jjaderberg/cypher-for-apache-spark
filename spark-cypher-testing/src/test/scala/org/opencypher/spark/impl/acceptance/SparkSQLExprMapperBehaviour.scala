/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.acceptance

import java.util.Collections

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.opencypher.okapi.api.types.{CTListOrNull, CTNull}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{NullLit, _}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.testing.CAPSTestSuite

import scala.language.implicitConversions

class SparkSQLExprMapperBehaviour extends CAPSTestSuite with DefaultGraphInit {
  val header: RecordHeader = RecordHeader.empty

  describe("index out of bounds list access should be null") {

    it("one") {
      val result = caps.cypher("RETURN [1][1] AS res")
      result.records.toMaps should equal(
        Bag(CypherMap("res" -> null))
      )
    }

    it("two") {
      val result = caps.cypher("RETURN [1, 'apa'][2] AS res")
      result.records.toMaps should equal(
        Bag(CypherMap("res" -> null))
      )
    }

    it("three") {
      val result = caps.cypher("RETURN ['a', [1]][2] as res")
      result.records.toMaps should equal(
        Bag(CypherMap("res" -> null))
      )
    }

  }

  describe("lists with mixed types") {

    it("should work in a Cypher query") {
      val result = caps.cypher("RETURN ['a', [1]] AS res")
      result.records.toMaps should equal(
        Bag(CypherMap("res" -> ???))
      )
    }

    it("should work as nested expressions") {
      val expr = ListLit(StringLit("a")(), ListLit(IntegerLit(1)()))
      convert(expr, RecordHeader.empty) should equal(
        functions.array(functions.lit("a"), functions.array(functions.lit(1)))
      )
    }

  }

  private def convert(expr: Expr, header: RecordHeader = header): Column = {
    expr.asSparkSQLExpr(header, df, CypherMap.empty)
  }
  val df: DataFrame = sparkSession.createDataFrame(
    Collections.emptyList[Row](),
    StructType(List()))
}
