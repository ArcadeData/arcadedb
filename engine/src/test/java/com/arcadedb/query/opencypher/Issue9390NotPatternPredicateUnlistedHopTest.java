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
 * Issue #9390: a NOT pattern predicate over an edge type the Graph Analytical View lists answered as if the pair were connected when the
 * same MATCH also had a hop over an edge type the view does not list. The oracle is the same query without a view.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9390NotPatternPredicateUnlistedHopTest extends TestHelper {
  private static final String HOPS = "MATCH (t1:Tag)-[:LIKES]->(m:Message)<-[:REPLY_OF]-(c:Comment)<-[:HAS_TAG]-(t2:Tag) ";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE VERTEX TYPE Message");
    database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
    for (final String e : new String[] { "LIKES", "REPLY_OF", "HAS_TAG" })
      database.command("sql", "CREATE EDGE TYPE " + e);
    database.transaction(() -> {
      final RID t1 = database.newVertex("Tag").save().getIdentity();
      final RID t2 = database.newVertex("Tag").save().getIdentity();
      final RID m = database.newVertex("Message").save().getIdentity();
      final RID c = database.newVertex("Comment").save().getIdentity();
      t1.asVertex().newEdge("LIKES", m);
      c.asVertex().newEdge("REPLY_OF", m);
      t2.asVertex().newEdge("HAS_TAG", c);
    });
  }

  @Test
  void notPredicateOnListedTypeWithUnlistedHop() throws InterruptedException {
    assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t1) AND t1 <> t2 RETURN count(*) AS n", 1L);
    assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t1) RETURN count(*) AS n", 1L);
    assertAllAgree(HOPS + "WHERE NOT (t1)-[:HAS_TAG]->(c) AND t1 <> t2 RETURN count(*) AS n", 1L);
    assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t2) RETURN count(*) AS n", 0L);
    assertAllAgree(HOPS + "WHERE (c)<-[:HAS_TAG]-(t2) RETURN count(*) AS n", 1L);
    assertAllAgree(HOPS + "WHERE (c)<-[:HAS_TAG]-(t1) RETURN count(*) AS n", 0L);
  }

  private void assertAllAgree(final String query, final long expected) throws InterruptedException {
    assertThat(count(query)).as("no view").isEqualTo(expected);

    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (Tag, Message, Comment) EDGE TYPES (HAS_TAG, REPLY_OF) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      assertThat(count(query)).as("view without LIKES").isEqualTo(expected);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
