/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9387 and #9388: the country-partitioned triangle count push-down (LSQB Q3) walked the KNOWS hops as undirected whatever
 * direction the query wrote, and ignored the labels of the three persons (and of the chain nodes). The oracle is the same pattern
 * behind a WITH (the row pipeline), which is the Cypher answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9387TriangleLabelsAndDirectionTest extends TestHelper {
  private static final String CHAINS = "MATCH (country:Country) MATCH (person1:Person)-[:IS_LOCATED_IN]->(city1:City)-[:IS_PART_OF]->(country) "
      + "MATCH (person2:Person)-[:IS_LOCATED_IN]->(city2:City)-[:IS_PART_OF]->(country) MATCH (person3:Person)-[:IS_LOCATED_IN]->(city3:City)-[:IS_PART_OF]->(country) ";
  private static final String UNDIRECTED = CHAINS + "MATCH (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3)-[:KNOWS]-(person1)";
  private static final String DIRECTED = CHAINS + "MATCH (person1)-[:KNOWS]->(person2)-[:KNOWS]->(person3)-[:KNOWS]->(person1)";
  private static final String VARS = "country, person1, person2, person3, city1, city2, city3";

  @Override
  protected void beginTest() {
    for (final String t : new String[] { "Person", "Company", "City", "Country" })
      database.command("sql", "CREATE VERTEX TYPE " + t);
    for (final String e : new String[] { "KNOWS", "IS_LOCATED_IN", "IS_PART_OF" })
      database.command("sql", "CREATE EDGE TYPE " + e);
  }

  private RID[] graph(final String thirdType) {
    final RID[] p = new RID[3];
    database.transaction(() -> {
      final RID country = database.newVertex("Country").save().getIdentity();
      final RID city = database.newVertex("City").save().getIdentity();
      city.asVertex().newEdge("IS_PART_OF", country);
      p[0] = database.newVertex("Person").save().getIdentity();
      p[1] = database.newVertex("Person").save().getIdentity();
      p[2] = database.newVertex(thirdType).save().getIdentity();
      for (final RID x : p)
        x.asVertex().newEdge("IS_LOCATED_IN", city);
    });
    return p;
  }

  private void knows(final RID from, final RID to) {
    database.transaction(() -> from.asVertex().newEdge("KNOWS", to));
  }

  @Test
  void triangleThroughNonPersonIsNotCounted() throws InterruptedException {
    final RID[] p = graph("Company");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(UNDIRECTED, 0L);
  }

  @Test
  void triangleOfPersonsIsCounted() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(UNDIRECTED, 6L);
  }

  @Test
  void directedPatternDoesNotCountUndirectedTriangle() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[0], p[2]);
    assertAllAgree(DIRECTED, 0L);
  }

  @Test
  void directedCycleIsCounted() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(DIRECTED, 3L);
  }

  @Test
  void chainLabelsAreEnforced() throws InterruptedException {
    // the cities are not Company vertices: the partition chain has no match
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    final String wrongCity = UNDIRECTED.replace("(city1:City)", "(city1:Company)").replace("(city2:City)", "(city2:Company)")
        .replace("(city3:City)", "(city3:Company)");
    assertAllAgree(wrongCity, 0L);
  }

  @Test
  void anchorLabelIsEnforced() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(UNDIRECTED.replace("(country:Country)", "(country:Company)"), 0L);
  }

  @Test
  void subTypeOfTheLabelIsAccepted() throws InterruptedException {
    database.command("sql", "CREATE VERTEX TYPE Employee EXTENDS Person");
    final RID[] p = graph("Employee");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(UNDIRECTED, 6L);
  }

  @Test
  void ambiguousChainWithLabelsIsCountedExactly() throws InterruptedException {
    // a second city in the same country for one person: two paths to the country, one of them through a rejected label
    final RID[] p = graph("Person");
    database.transaction(() -> {
      final RID country = database.newVertex("Country").save().getIdentity();
      final RID other = database.newVertex("Company").save().getIdentity();
      other.asVertex().newEdge("IS_PART_OF", country);
      p[0].asVertex().newEdge("IS_LOCATED_IN", other);
    });
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    assertAllAgree(UNDIRECTED, 6L);
  }

  @Test
  void conflictingLabelsOnOneVariableDeclineThePushDown() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    // person1 is a Person in its chain and a Company in the cycle: nothing is both
    final String conflicting = UNDIRECTED.replace("MATCH (person1)-[:KNOWS]-", "MATCH (person1:Company)-[:KNOWS]-");
    assertAllAgree(conflicting, 0L);
  }

  @Test
  void labelOnTheChainEndOfOneChainOnlyIsEnforced() throws InterruptedException {
    final RID[] p = graph("Person");
    knows(p[0], p[1]);
    knows(p[1], p[2]);
    knows(p[2], p[0]);
    // only the second chain labels its end as a Company, which the country is not: the three chains share the anchor
    final String mixed = UNDIRECTED.replaceFirst("\\(city2:City\\)-\\[:IS_PART_OF\\]->\\(country\\)",
        "(city2:City)-[:IS_PART_OF]->(country:Company)");
    assertAllAgree(mixed, 0L);
  }

  private void assertAllAgree(final String match, final long expected) throws InterruptedException {
    assertThat(count(match + " WITH " + VARS + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
    final String written = match + " RETURN count(*) AS n";
    assertThat(count(written)).as("no view").isEqualTo(expected);

    createView("narrow", "VERTEX TYPES (Person) EDGE TYPES (KNOWS) UPDATE MODE OFF");
    assertThat(count(written)).as("narrow view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

    createView("wide", "VERTEX TYPES (Person, Company, City, Country) EDGE TYPES (KNOWS, IS_LOCATED_IN, IS_PART_OF) UPDATE MODE OFF");
    assertThat(count(written)).as("wide view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
  }

  private void createView(final String name, final String definition) throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
