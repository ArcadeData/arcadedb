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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the COUNT ANTI-JOIN CHAIN push-down: issue #9277 (hop and anti-join neighbour maps reused across hops whose
 * target labels differ) and issue #9278 (a negated pattern carrying an edge property map, an edge variable, an inline WHERE or
 * end node labels/properties was pushed down with those parts dropped). Each query runs as written and with the same WHERE after a
 * WITH (row pipeline); the two must agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9277And9278AntiJoinChainTest {
  private static final String DB_PATH = "./target/databases/issue9277-9278";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void crossLabelChainReusesNoMap() {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Employee", "CREATE VERTEX TYPE Tag",
        "CREATE EDGE TYPE KNOWS", "CREATE EDGE TYPE HAS_INTEREST" })
      database.command("sql", ddl);
    database.begin();
    final RID x0 = save(database.newVertex("Person").set("id", 0L)), x1 = save(database.newVertex("Person").set("id", 1L)),
        y0 = save(database.newVertex("Employee").set("id", 0L)), t = save(database.newVertex("Tag"));
    x0.asVertex().newEdge("KNOWS", x1);
    x1.asVertex().newEdge("KNOWS", y0);
    y0.asVertex().newEdge("HAS_INTEREST", t);
    database.commit();

    final String chain = "MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Employee)-[:HAS_INTEREST]->(t:Tag) ";
    assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3)", 1);
    assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 1);
    assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND id(p1) <> id(p3)", 1);
    assertSameCount(chain, "p1, p2, p3, t", "NOT (p1)-[:KNOWS]-(p3) AND p1.id <> p3.id", 0);
    // no HAS_INTEREST hop
    assertSameCount("MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Employee) ", "p1, p2, p3", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 1);
  }

  @Test
  void edgePropertyMapOfNegatedPatternIsHonoured() {
    for (final String ddl : new String[] { "CREATE VERTEX TYPE P", "CREATE VERTEX TYPE Q", "CREATE EDGE TYPE K", "CREATE EDGE TYPE L" })
      database.command("sql", ddl);
    database.begin();
    final RID x = save(database.newVertex("P").set("id", 0L)), y = save(database.newVertex("P").set("id", 1L)),
        z = save(database.newVertex("P").set("id", 2L)), t = save(database.newVertex("Q").set("id", 0L));
    x.asVertex().newEdge("K", y).save();
    y.asVertex().newEdge("K", z).save();
    z.asVertex().newEdge("L", t).save();
    x.asVertex().newEdge("K", z).set("w", 0L).save();
    database.commit();

    final String chain = "MATCH (x:P)-[:K]->(y:P)-[:K]->(z:P)-[:L]->(t:Q) ";
    final String vars = "x, y, z, t";
    assertSameCount(chain, vars, "NOT (x)-[:K {w: 1}]->(z)", 1);
    assertSameCount(chain, vars, "NOT (x)-[:K {w: 0}]->(z)", 0);
    assertSameCount(chain, vars, "NOT (x)-[:K]->(z)", 0);
    assertSameCount(chain, vars, "NOT (x)-[:K {w: 1}]->(z) AND x <> z", 1);
    assertSameCount(chain, vars, "NOT (x)-[:K]->(z:Foo)", 1);
    assertSameCount(chain, vars, "NOT (x:P)-[:K]->(z:P)", 0);
    assertSameCount(chain, vars, "NOT (x:P)-[:K]->(z:Q)", 1);
    assertSameCount(chain, vars, "NOT (x)-[:K]->(z {id: 99})", 1);
    assertSameCount(chain, vars, "NOT (x)-[:K]->(z {id: 2})", 0);
  }

  private void assertSameCount(final String chain, final String vars, final String where, final long expected) {
    final long asWritten = count(chain + "WHERE " + where + " RETURN count(*) AS n");
    final long afterWith = count(chain + "WITH " + vars + " WHERE " + where + " RETURN count(*) AS n");
    assertThat(afterWith).as("row pipeline: " + where).isEqualTo(expected);
    assertThat(asWritten).as("as written: " + where).isEqualTo(expected);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private static RID save(final MutableVertex v) {
    v.save();
    return v.getIdentity();
  }
}
