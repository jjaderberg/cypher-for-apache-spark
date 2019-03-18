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
package org.opencypher.spark.testing

import java.io.{File, PrintWriter}

import org.apache.commons.lang.StringEscapeUtils
import org.opencypher.okapi.api.value.CypherValue.{CypherList => OKAPICypherList, CypherMap => OKAPICypherMap, CypherNull => OKAPICypherNull, CypherString => OKAPICypherString, CypherValue => OKAPICypherValue}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.tck.test.ScenariosFor
import org.opencypher.okapi.tck.test.TckToCypherConverter.tckValueToCypherValue
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.{Backward => TCKBackward, CypherBoolean => TCKCypherBoolean, CypherFloat => TCKCypherFloat, CypherInteger => TCKCypherInteger, CypherList => TCKCypherList, CypherNode => TCKCypherNode, CypherNull => TCKCypherNull, CypherOrderedList => TCKCypherOrderedList, CypherPath => TCKCypherPath, CypherProperty => TCKCypherProperty, CypherPropertyMap => TCKCypherPropertyMap, CypherRelationship => TCKCypherRelationship, CypherString => TCKCypherString, CypherUnorderedList => TCKUnorderedList, CypherValue => TCKCypherValue, Forward => TCKForward}
import org.scalatest.prop.TableFor1

import scala.collection.mutable


object AcceptanceTestGenerator extends App {
  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios: ScenariosFor = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)
  private val escapeStringMarks = "\"\"\""
  private val tabs = "\t\t"
  private val packageNames = Map("white" -> "whiteList", "black" -> "blackList")

  case class ResultRows(queryResult: String, expected: List[Map[String, TCKCypherValue]])

  private def generateClassFile(featureName: String, scenarios: TableFor1[Scenario], black: Boolean, path: String) = {
    val packageName = if (black) packageNames.get("black") else packageNames.get("white")
    val className = featureName
    val classHeader =
      s"""|/*
          | * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
          | *
          | * Licensed under the Apache License, Version 2.0 (the "License");
          | * you may not use this file except in compliance with the License.
          | * You may obtain a copy of the License at
          | *
          | *     http://www.apache.org/licenses/LICENSE-2.0
          | *
          | * Unless required by applicable law or agreed to in writing, software
          | * distributed under the License is distributed on an "AS IS" BASIS,
          | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
          | * See the License for the specific language governing permissions and
          | * limitations under the License.
          | *
          | * Attribution Notice under the terms of the Apache License 2.0
          | *
          | * This work was created by the collective efforts of the openCypher community.
          | * Without limiting the terms of Section 6, any Derivative Work that is not
          | * approved by the public consensus process of the openCypher Implementers Group
          | * should not be described as “Cypher” (and Cypher® is a registered trademark of
          | * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
          | * proposals for change that have been documented or implemented should only be
          | * described as "implementation extensions to Cypher" or as "proposed changes to
          | * Cypher that are not yet approved by the openCypher community".
          | */
          |package org.opencypher.spark.testing.${packageName.get}
          |
          |import org.scalatest.junit.JUnitRunner
          |import org.junit.runner.RunWith
          |import org.opencypher.okapi.tck.test.CypherToTCKConverter
          |import org.opencypher.spark.testing.CAPSTestSuite
          |import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory
          |import org.opencypher.spark.impl.graph.CAPSGraphFactory
          |import org.opencypher.okapi.api.value.CypherValue._
          |import scala.util.{Failure, Success, Try}
          |import org.opencypher.tools.tck.api.CypherValueRecords
          |import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, CypherString => TCKCypherString, CypherOrderedList => TCKCypherOrderedList,
          |   CypherNode => TCKCypherNode, CypherRelationship => TCKCypherRelationship, Connection, Forward => TCKForward, Backward => TCKBackward,
          |   CypherInteger => TCKCypherInteger, CypherFloat => TCKCypherFloat, CypherBoolean => TCKCypherBoolean, CypherProperty => TCKCypherProperty,
          |   CypherPropertyMap => TCKCypherPropertyMap, CypherNull => TCKCypherNull, CypherPath => TCKCypherPath}
          |
          |
          |@RunWith(classOf[JUnitRunner])
          |class $className extends CAPSTestSuite{""".stripMargin


    val testCases = scenarios.map(scenario =>
      if (scenario.name.equals("Failing on incorrect unicode literal")) "" //this fails at scala-compilation
      else
        generateTest(scenario, black)).mkString("\n")

    val file = new File(s"$path${packageName.get}/$className.scala")

    val fileString =
      s"""$classHeader
         |
         |$testCases
         |}""".stripMargin
    val out = new PrintWriter(file)
    out.print(fileString)
    out.close()
    file.createNewFile()
  }

  private def alignQuery(query: String): String = {
    query.replaceAllLiterally("\n", s"\n      ")
  }

  private def escapeString(s: String): String = {
    s""" "${StringEscapeUtils.escapeJava(s)}" """
  }

  private def tckCypherValueToCreateString(value: TCKCypherValue): String = {
    value match {
      case TCKCypherString(v) => s"TCKCypherString(${escapeString(v)})"
      case TCKCypherInteger(v) => s"TCKCypherInteger(${v}L)"
      case TCKCypherFloat(v) => s"TCKCypherFloat($v)"
      case TCKCypherBoolean(v) => s"TCKCypherBoolean($v)"
      case TCKCypherProperty(key, v) => s"""TCKCypherProperty("${escapeString(key)}",${tckCypherValueToCreateString(v)})"""
      case TCKCypherPropertyMap(properties) =>
        val propertiesCreateString = properties.map { case (key, v) => s"(${escapeString(key)}, ${tckCypherValueToCreateString(v)})" }.mkString(",")
        s"TCKCypherPropertyMap(Map($propertiesCreateString))"
      case l: TCKCypherList =>
        l match {
          case TCKCypherOrderedList(elems) => s"TCKCypherOrderedList(List(${elems.map(tckCypherValueToCreateString).mkString(",")}))"
          case _ => s"TCKCypherValue.apply(${escapeString(l.toString)}, false)"
        }
      case TCKCypherNull => "TCKCypherNull"
      case TCKCypherNode(labels, properties) => s"TCKCypherNode(Set(${labels.map(escapeString).mkString(",")}), ${tckCypherValueToCreateString(properties)})"
      case TCKCypherRelationship(typ, properties) => s"TCKCypherRelationship(${escapeString(typ)}, ${tckCypherValueToCreateString(properties)})"
      case TCKCypherPath(start, connections) =>
        val connectionsCreateString = connections.map {
          case TCKForward(r, n) => s"TCKForward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
          case TCKBackward(r, n) => s"TCKBackward(${tckCypherValueToCreateString(r)},${tckCypherValueToCreateString(n)})"
        }.mkString(",")

        s"TCKCypherPath(${tckCypherValueToCreateString(start)},List($connectionsCreateString))"
      case other =>
        throw NotImplementedException(s"Converting Cypher value $value of type `${other.getClass.getSimpleName}`")
    }
  }

  private def cypherValueToCreateString(value: OKAPICypherValue): String = {
    value match {
      case OKAPICypherList(l) => s"List(${l.map(cypherValueToCreateString).mkString(",")})"
      case OKAPICypherMap(m) =>
        val mapElementsString = m.map {
          case (key, cv) => s"(${escapeString(key)},${cypherValueToCreateString(cv)})"
        }.mkString(",")
        s"CypherMap($mapElementsString)"
      case OKAPICypherString(s) => escapeString(s)
      case OKAPICypherNull => "CypherNull"
      case _ => s"${value.getClass.getSimpleName}(${value.unwrap})"
    }
  }

  private def tckCypherMapToTCKCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, cypherValue) => escapeString(key) -> tckCypherValueToCreateString(cypherValue)
    }
    s"Map(${mapElementsString.mkString(",")})"
  }

  private def tckCypherMapToOkapiCreateString(cypherMap: Map[String, TCKCypherValue]): String = {
    val mapElementsString = cypherMap.map {
      case (key, tckCypherValue) => escapeString(key) -> cypherValueToCreateString(tckValueToCypherValue(tckCypherValue))
    }
    s"CypherMap(${mapElementsString.mkString(",")})"
  }

  private def stepsToString(steps: List[(Step, Int)]): String = {
    val contextExecQueryStack = mutable.Stack[Int]()
    val contextParameterStepNrs = mutable.Stack[Int]()

    steps.map {
      case (Parameters(p, _), stepNr) => contextParameterStepNrs.push(stepNr)
        s"val parameter$stepNr = ${tckCypherMapToOkapiCreateString(p)}"
      case (Execute(query, querytype, _), nr) =>
        querytype match {
          case ExecQuery =>
            contextExecQueryStack.push(nr)
            val parameters = if (contextParameterStepNrs.nonEmpty) s", parameter${contextParameterStepNrs.head}" else ""
            s"""
               |    lazy val result$nr = graph.cypher($escapeStringMarks${alignQuery(query)}$escapeStringMarks$parameters)
           """
          case _ =>
            //currently no TCK-Tests with side effect queries
            throw NotImplementedException("Side Effect Queries not supported yet")
        }
      case (ExpectResult(expectedResult: CypherValueRecords, _, sorted), _) =>
        val resultNumber = contextExecQueryStack.head
        val equalMethod = if (sorted) "equals" else "equalsUnordered"

        s"""
           |    val result${resultNumber}ValueRecords = CypherToTCKConverter.convertToTckStrings(result$resultNumber.records).asValueRecords
           |    val expected${resultNumber}ValueRecords = CypherValueRecords(List(${expectedResult.header.map(escapeString).mkString(",")}),
           |      List(${expectedResult.rows.map(tckCypherMapToTCKCreateString).mkString(s", \n$tabs$tabs$tabs")}))
           |    result${resultNumber}ValueRecords.$equalMethod(expected${resultNumber}ValueRecords)
           """.stripMargin
      case (ExpectError(errorType, errorPhase, detail, _), _) =>
        val stepNumber = contextExecQueryStack.head
        //todo: check for errorType and detail (when coresponding errors exist like SyntaxError, TypeError, ParameterMissing, ...)
        s"""
           |$tabs val errorMessage$stepNumber  = an[Exception] shouldBe thrownBy{result$stepNumber}
           """.stripMargin

      case (SideEffects(expected, _), _) =>
        val relevantEffects = expected.v.filter(_._2 > 0) //check if relevant Side-Effects exist
        if (relevantEffects.nonEmpty)
        //SideEffects not relevant in CAPS
          s"$tabs fail() //TODO: side effects not handled yet"
        else
          ""
      case _ => ""
    }.filter(_.nonEmpty).mkString(s"\n$tabs")
  }


  private def generateTest(scenario: Scenario, black: Boolean): String = {
    val (initSteps, execSteps) = scenario.steps.partition {
      case Execute(_, InitQuery, _) => true
      case _ => false
    }

    val initQuery = escapeStringMarks + initSteps.foldLeft("")((combined, x) => x match {
      case Execute(query, InitQuery, _) => combined + s"\n$tabs$tabs  ${alignQuery(query)}"
      case _ => combined
    }) + escapeStringMarks

    val execString = stepsToString(execSteps.zipWithIndex)
    val testString =
      s"""
         |    val graph = ${if (initSteps.nonEmpty) s"CAPSScanGraphFactory.initGraph($initQuery)" else "CAPSGraphFactory.apply().empty"}
         |    $execString
       """.stripMargin

    if (black)
      s"""  it("${scenario.name}") {
         |    Try({
         |     ${testString.replaceAll("\n    ", "\n     ")}
         |    }) match{
         |      case Success(_) =>
         |        throw new RuntimeException(s"A blacklisted scenario may work (cases with expected error probably are false-positives)")
         |      case Failure(_) =>
         |    }
         |   }
      """.stripMargin
    else
      s"""  it("${scenario.name}") {
         |    $testString
         |  }
      """.stripMargin
  }

  //checks if package directories exists clears them or creates new
  private def setUpDirectories(path: String): Unit = {
    packageNames.values.map(packageName => {
      val directory = new File(path + packageName)
      if (directory.exists()) {
        val files = directory.listFiles()
        files.map(_.delete())
      }
      else {
        directory.mkdir()
      }
      val gitIgnoreFile = new File(path + packageName + "/.gitignore")
      val writer = new PrintWriter(gitIgnoreFile)
      writer.println("*")
      writer.close()
      gitIgnoreFile.createNewFile()
    }
    )
  }

  //generates test-cases for given scenario names
  def generateGivenScenarios(path: String, keyWords: Array[String] = Array.empty): Unit = {
    setUpDirectories(path)
    val wantedWhiteScenarios = scenarios.whiteList.filter(scen => keyWords.map(keyWord => scen.name.contains(keyWord)).reduce(_ || _))
    val wantedBlackScenarios = scenarios.blackList.filter(scen => keyWords.map(keyWord => scen.name.contains(keyWord)).reduce(_ || _))
    generateClassFile("specialWhiteCases", wantedWhiteScenarios, black = false, path)
    generateClassFile("specialBlackCases", wantedBlackScenarios, black = true, path)
  }

  def generateAllScenarios(path: String): Unit = {
    setUpDirectories(path)
    val blackFeatures = scenarios.blackList.groupBy(_.featureName)
    val whiteFeatures = scenarios.whiteList.groupBy(_.featureName)
    whiteFeatures.map { feature => {
      generateClassFile(feature._1, feature._2, black = false, path)
    }
    }

    blackFeatures.map { feature => {
      generateClassFile(feature._1, feature._2, black = true, path)
    }
    }
  }

  //in gradle only "src/test/scala/org/opencypher/spark/testing/" is needed as a path
  if (args.isEmpty) {
    val defaultPath = s"spark-cypher-tck/src/test/scala/org/opencypher/spark/testing/"
    generateAllScenarios(defaultPath)
  }
  else {
    val (path, scenarioNames) = (args(0), args(1))
    if (scenarioNames.nonEmpty)
      generateGivenScenarios(path, scenarioNames.split('|'))
    else
      generateAllScenarios(path)
  }

}
