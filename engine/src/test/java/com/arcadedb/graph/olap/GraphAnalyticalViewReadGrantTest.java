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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Graph Analytical View answers a query only for a user who may read every record it holds: the vertices and edges of every
 * type it covers. Any other user is answered from the records, so a query gives that user the answer it gives without the view.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewReadGrantTest {
  private static final String PATH = "target/databases/GraphAnalyticalViewReadGrantTest";

  // planned with the view for a user who may read everything: the expansion is a step of the plan the cache keeps
  private static final String READABLE_EXPANSION = "MATCH (a:Person)-[:Knows]->(b:Person) RETURN a.name AS a, b.name AS b ORDER BY a";

  private static final String[][] QUERIES = {
      { "opencypher", READABLE_EXPANSION },
      { "opencypher", "MATCH (p:Person)<-[:Owns]-(s:Secret) RETURN p.name AS name, s.name AS sname ORDER BY name, sname" },
      { "opencypher", "MATCH (p:Person)<-[:Owns]-(s:Secret) RETURN p.name AS name ORDER BY name" },
      { "opencypher", "CALL algo.pageRank() YIELD node, score RETURN count(node) AS c" },
      { "opencypher", "MATCH ()-[r:Owns]->() RETURN count(*) AS c" },
      { "opencypher", "MATCH (p:Person)<-[:Owns]-(s) RETURN count(s) AS c" },
      { "opencypher", "MATCH (p:Person)<-[:Owns]-(s) RETURN p.name AS name, count(s) AS c ORDER BY name" },
      { "sql", "SELECT name, in('Owns').size() AS c FROM Person ORDER BY name" },
      { "sql", "MATCH {type: Person, as: p}.in('Owns'){as: s} RETURN count(*) AS c" } };

  private DatabaseFactory factory;
  private Database        database;
  private Set<Integer>    refusedFiles;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Secret");
    database.command("sql", "CREATE EDGE TYPE Knows");
    database.command("sql", "CREATE EDGE TYPE Owns");

    database.transaction(() -> {
      final List<MutableVertex> people = new ArrayList<>();
      for (int i = 0; i < 5; i++)
        people.add(database.newVertex("Person").set("name", "p" + i).save());
      for (int i = 0; i < people.size() - 1; i++)
        people.get(i).newEdge("Knows", people.get(i + 1)).save();
      // more of them than people, so that a plan walks from the people to them
      for (int i = 0; i < 12; i++) {
        final MutableVertex secret = database.newVertex("Secret").set("name", "s" + i).save();
        secret.newEdge("Owns", people.get(i % 3)).save();
      }
    });

    refusedFiles = new HashSet<>();
    for (final String typeName : new String[] { "Secret", "Owns" })
      for (final Bucket bucket : database.getSchema().getType(typeName).getBuckets(true))
        refusedFiles.add(bucket.getFileId());
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  @Test
  void restrictedUserGetsTheSameAnswersWithAndWithoutTheView() {
    final Map<String, String> withoutView = restrictedOutcomes();

    createViewOverEveryType();
    ((DatabaseInternal) database).getCypherPlanCache().invalidate();
    ((DatabaseInternal) database).getExecutionPlanCache().invalidate();

    assertThat(restrictedOutcomes()).isEqualTo(withoutView);
  }

  @Test
  void restrictedUserDoesNotReuseAPlanCachedForAnotherUser() {
    final Map<String, String> withoutView = restrictedOutcomes();

    createViewOverEveryType();
    ((DatabaseInternal) database).getCypherPlanCache().invalidate();
    ((DatabaseInternal) database).getExecutionPlanCache().invalidate();

    // planned and cached for a user that may read everything, with the view in the plans
    final Map<String, String> unrestricted = outcomes();
    assertThat(unrestricted.values()).noneMatch(o -> o.startsWith("refused"));
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + READABLE_EXPANSION)) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("GAVExpandAll");
    }

    assertThat(restrictedOutcomes()).isEqualTo(withoutView);

    // the cached plans still answer the unrestricted user
    assertThat(outcomes()).isEqualTo(unrestricted);
  }

  @Test
  void viewIsOfferedOnlyToAUserWhoMayReadEverythingItHolds() {
    final GraphAnalyticalView everyType = createViewOverEveryType();

    assertThat(GraphTraversalProviderRegistry.findProvider(database)).isSameAs(everyType);

    bindUser(Set.of());
    assertThat(GraphTraversalProviderRegistry.findProvider(database)).as("a user who may read every type").isSameAs(everyType);

    bindUser(refusedFiles);
    assertThat(GraphTraversalProviderRegistry.findProvider(database)).isNull();
    assertThat(GraphTraversalProviderRegistry.findProvider(database, "Owns")).isNull();
    assertThat(GraphTraversalProviderRegistry.findProvider(database, "Knows")).isNull();
  }

  @Test
  void viewOverReadableTypesStillServesTheRestrictedUser() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW readableOnly VERTEX TYPES (Person) EDGE TYPES (Knows) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "readableOnly");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

    bindUser(refusedFiles);
    final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProviderAllowingPartialVertexCoverage(database, "Knows");
    assertThat(provider).isSameAs(view);

    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Person)-[:Knows]->(b:Person) RETURN count(*) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(4L);
    }
  }

  @Test
  void viewListingHidesFiguresFromARestrictedUser() {
    final GraphAnalyticalView view = createViewOverEveryType();
    assertThat(view.getNodeCount()).isEqualTo(17);

    assertThat(listedNodeCount()).isEqualTo(17L);

    bindUser(refusedFiles);
    assertThat(listedNodeCount()).isZero();
  }

  private long listedNodeCount() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'everyType'")) {
      return rs.next().<Number>getProperty("nodeCount").longValue();
    }
  }

  private GraphAnalyticalView createViewOverEveryType() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW everyType UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "everyType");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private Map<String, String> restrictedOutcomes() {
    bindUser(refusedFiles);
    try {
      return outcomes();
    } finally {
      unbindUser();
    }
  }

  private Map<String, String> outcomes() {
    final Map<String, String> outcomes = new LinkedHashMap<>();
    for (final String[] query : QUERIES)
      outcomes.put(query[1], outcome(query[0], query[1]));
    return outcomes;
  }

  private String outcome(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        final List<String> names = new ArrayList<>(row.getPropertyNames());
        Collections.sort(names);
        final StringBuilder line = new StringBuilder();
        for (final String name : names)
          line.append(name).append('=').append(String.valueOf(row.<Object>getProperty(name))).append(';');
        rows.add(line.toString());
      }
      return rows.toString();
    } catch (final RuntimeException e) {
      for (Throwable t = e; t != null; t = t.getCause())
        if (t instanceof SecurityException)
          return "refused";
      throw e;
    }
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** A user that resolves permissions as the server does: per bucket, record reads refused on the given files. */
  private void bindUser(final Set<Integer> unreadableFiles) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return false;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.READ_RECORD || !unreadableFiles.contains(fileId);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
