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
package org.opencypher.okapi.neo4j.io

import scala.collection.JavaConverters._
import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.exceptions.ClientException
import org.opencypher.okapi.impl.exception.IllegalStateException
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object EntityWriter2 extends Logging {

  private val ROW_IDENTIFIER = "row"

  def createNodes[T](
      nodes: Iterator[T],
      rowMapping: Array[String],
      config: Neo4jConfig,
      labels: Set[String],
      batchSize: Int = 1000
    )(rowToListValue: T => Value): Iterator[Long] = {
    val labelString = labels.cypherLabelPredicate

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map { case (key, i) => s"SET n.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |CREATE (n$labelString)
         |$setStatements
         |RETURN collect(id(n)) AS neo4jIds
         """.stripMargin

    writeNodes(nodes, createQ, config, batchSize)(rowToListValue)
  }

  def createRelationships(
      relationships: Iterator[Array[AnyRef]],
      startNodeIndex: Int,
      endNodeIndex: Int,
      rowMapping: Array[String],
      config: Neo4jConfig,
      relType: String,
      nodeLabel: Option[String],
      idMap: mutable.Map[Long, Long],
      batchSize: Int = 1000
    )(rowToListValue: Array[AnyRef] => Value): Unit = {

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map { case (key, i) => s"SET rel.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val nodeLabelString = nodeLabel.toSet[String].cypherLabelPredicate

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |MATCH (from) WHERE id(from) = $ROW_IDENTIFIER[$startNodeIndex]
         |MATCH (to) WHERE id(to) = $ROW_IDENTIFIER[$endNodeIndex]
         |CREATE (from)-[rel:$relType]->(to)
         |$setStatements
         """.stripMargin

//    val createQ =
//      s"""
//         |UNWIND $$batch AS $ROW_IDENTIFIER
//         |MATCH (from$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$startNodeIndex]})
//         |MATCH (to$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$endNodeIndex]})
//         |CREATE (from)-[rel:$relType]->(to)
//         |$setStatements
//         """.stripMargin

    val relationshipsWithPhysicalIds = relationships.map {
      row => {
        row(startNodeIndex) = idMap(row(startNodeIndex).asInstanceOf[Int].toLong).asInstanceOf[AnyRef]
        row(endNodeIndex) = idMap(row(endNodeIndex).asInstanceOf[Int].toLong).asInstanceOf[AnyRef]
        row
      }
    }

    writeRelationships(relationshipsWithPhysicalIds, createQ, config, idMap, batchSize)(rowToListValue)
  }

//  private def resultMapper(value: Value): Long = ???


  private def parseResult(result: StatementResult): Iterator[Long] = {
//    val neo4jIds: Array[Long] = result.single().get(0).asList(Values.ofLong()).toArray().asInstanceOf[Array[Long]]
//    result.consume()
//    neo4jIds.iterator

    result.single().get(0).asList[Long] {
      l: Value => l.asLong()
    }.iterator.asScala
    //      new Function[Value, Long] {
    //        override def apply(value: Value) = value.asLong()
    //      }

//    result.single().get(0).asList(Values.ofLong()).toA
//    val parsedResult: util.List[lang.Long] = result.single().get(0)
//      .asList(Values.ofLong())
//      .map(Long2long)
////      .iterator()
//    result.consume()
//    parsedResult
  }

  private def writeNodes[T](
     entities: Iterator[T],
     query: String,
     config: Neo4jConfig,
     batchSize: Int = 1000
   )(rowToListValue: T => Value): Iterator[Long] = {

    var result = Iterator[Long]()

    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)
    val reuseStatement = new Statement(query, reuseParameters)

    config.withSession { session =>
      val batches = entities.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val rowParameters = new Array[Value](batch.size)

        batch.zipWithIndex.foreach { case (row, i) => rowParameters(i) = rowToListValue(row) }

        reuseMap.put("batch", Values.value(rowParameters: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)
        Try {
          session.writeTransaction {
            new TransactionWork[Iterator[Long]] {
              override def execute(transaction: Transaction): Iterator[Long] = {
//                logger.debug(s"Executing query: $reuseStatement")
                parseResult(transaction.run(reuseStatement))
              }
            }
          }
        } match {
          case Success(queryResult) => result = result ++ queryResult
//          case Success(_) => ()

          case Failure(exception: ClientException) if exception.getMessage.contains("already exists") =>
            val originalMessage = exception.getMessage

            val entityType = if (originalMessage.contains("Node(")) "nodes" else "relationships"

            val duplicateIdRegex = """.+('[0-9a-fA-F]+')$""".r
            val duplicateId = originalMessage match {
              case duplicateIdRegex(idString) => idString
              case _ => "UNKNOWN"
            }

            val message = s"Could not write the graph to Neo4j. The graph you are attempting to write contains at least two $entityType with CAPS id $duplicateId"
            throw IllegalStateException(message, Some(exception))

          case Failure(e) => throw e
        }
      }
    }
    result
  }

  private def writeRelationships(
      relationships: Iterator[Array[AnyRef]],
      query: String,
      config: Neo4jConfig,
      idMap: mutable.Map[Long, Long],
      batchSize: Int = 1000
    )(rowToListValue: Array[AnyRef] => Value): Unit = {
    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)
    val reuseStatement = new Statement(query, reuseParameters)

    config.withSession { session =>
      val batches = relationships.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val rowParameters = new Array[Value](batch.size)

        batch.zipWithIndex.foreach { case (row, i) => rowParameters(i) = rowToListValue(row) }

        reuseMap.put("batch", Values.value(rowParameters: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)
        Try {
          session.writeTransaction {
            new TransactionWork[Unit] {
              override def execute(transaction: Transaction): Unit = {
//                logger.debug(s"Executing query: $reuseStatement")
                transaction.run(reuseStatement).consume()
              }
            }
          }
        } match {
          case Success(_) => ()

          case Failure(exception: ClientException) if exception.getMessage.contains("already exists") =>
            val originalMessage = exception.getMessage

            val entityType = if (originalMessage.contains("Node(")) "nodes" else "relationships"

            val duplicateIdRegex = """.+('[0-9a-fA-F]+')$""".r
            val duplicateId = originalMessage match {
              case duplicateIdRegex(idString) => idString
              case _ => "UNKNOWN"
            }

            val message = s"Could not write the graph to Neo4j. The graph you are attempting to write contains at least two $entityType with CAPS id $duplicateId"
            throw IllegalStateException(message, Some(exception))

          case Failure(e) => throw e
        }
      }
    }
  }
}
