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

import java.net.URI

import org.neo4j.driver.v1.Values
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults.metaPropertyKey
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

import scala.collection.mutable

object Neo4jWriteBenchmark2 extends App {

  val config = Neo4jConfig(
    new URI("bolt://localhost"),
    "neo4j",
    Some("passwd")
  )

  def rowToListValue(data: Array[AnyRef]) = Values.value(data.map(Values.value): _*)

  private val numberOfNodes = 10000
  val inputNodes = (1 to numberOfNodes).map { i =>
    Array[AnyRef](i.asInstanceOf[AnyRef], i.asInstanceOf[AnyRef], i.toString.asInstanceOf[AnyRef], (i % 2 == 0).asInstanceOf[AnyRef])
  }

  val inputRels = (2 to numberOfNodes).map { i =>
    Array[AnyRef](i.asInstanceOf[AnyRef], (i - 1).asInstanceOf[AnyRef], i.asInstanceOf[AnyRef], (i % 2 == 0).asInstanceOf[AnyRef])
  }

  config.withSession { session =>
    session.run(s"CREATE CONSTRAINT ON (n:Foo) ASSERT n.$metaPropertyKey IS UNIQUE").consume()
  }

  val timings: Seq[Long] = (1 to 10).map { _ =>
    config.withSession { session =>
      session.run("MATCH (n) DETACH DELETE n").consume()
    }

    Measurement.time {
      val neo4jIds = EntityWriter2.createNodes(
        inputNodes.toIterator,
        Array(metaPropertyKey, "val1", "val2", "val3"),
        config,
        Set("Foo", "Bar", "Baz")
      )(rowToListValue)
      val idMap = mutable.Map[Long, Long]()
      var i = 0
      while (neo4jIds.hasNext) {
        val capsId = inputNodes(i)(0).asInstanceOf[Int].toLong
        idMap.put(capsId, neo4jIds.next())
        i += 1
      }
//      println(neo4jIds.take(20).toList)

      EntityWriter2.createRelationships(
        inputRels.toIterator,
        1,
        2,
        Array(metaPropertyKey, null, null, "val3"),
        config,
        "REL",
        Some("Foo"),
        idMap
      )(rowToListValue)
    }._2

  }

  config.withSession { session =>
    val result = session.run(s"MATCH (n)-[r]->() RETURN count(r) AS relCount")
    println(s"Number of rels: ${result.single().get(0).asLong()}")
    result.consume()
  }

  println(s"MIN: ${timings.min}")
  println(s"MAX: ${timings.max}")
  println(s"AVG: ${timings.sum / timings.size}")
}
