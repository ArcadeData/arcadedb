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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.ProfiledLimitScope;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13087: a security group's {@code resultSetLimit} caps the rows a Gremlin traversal answers, as it caps the rows of a SQL
 * or an openCypher statement. Gremlin never applied it: it was bounded only by the truncation of every bucket scan, which #12888
 * removed because it changed the answer (a filter missed the records past the first N of a bucket, a count ran over part of the
 * type), so a member of a capped group read every record through Gremlin. The cap is on the rows of the root traversal: what it
 * reads, its child traversals and what it writes are not cut short.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13087GremlinResultSetLimitTest {
  private static final String PATH    = "target/databases/Issue13087GremlinResultSetLimitTest";
  private static final int    RECORDS = 40;
  private static final int    QUOTA   = 2;

  private DatabaseFactory factory;
  private Database        database;
  private ArcadeGraph     graph;

  @BeforeEach
  void setUp() {
    // A SECURITY MANAGER, OR THE DATABASE READS NO QUOTA FROM THE CURRENT USER AT ALL
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Item BUCKETS 4");
    database.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        database.newVertex("Item").set("k", k).save();
    });
    graph = ArcadeGraph.openShared(database);
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    if (database.isTransactionActive())
      database.rollback();
    graph.close();
    database.drop();
    factory.close();
  }

  @Test
  void aPlainScanAnswersTheQuota() {
    assertThat(values("g.V().hasLabel('Item')")).as("no user, no quota").hasSize(RECORDS);

    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item')")).hasSize(QUOTA);
    assertThat(values("g.V()")).hasSize(QUOTA);
    assertThat(values("g.V().hasLabel('Item').values('k')")).hasSize(QUOTA);
    assertThat(ProfiledLimitScope.isActive()).as("nothing is left on the thread").isFalse();
  }

  @Test
  void aFilterFindsARecordPastTheQuotaOfItsBucket() {
    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item').has('k', 40).values('k')")).containsExactly(40);
    assertThat(values("g.V().hasLabel('Item').has('k', gt(37)).values('k')")).hasSize(QUOTA).isSubsetOf(38, 39, 40);
  }

  @Test
  void anAggregateAndAnOrderRunOverEveryRecord() {
    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item').count()")).containsExactly((long) RECORDS);
    assertThat(values("g.V().hasLabel('Item').values('k').max()")).containsExactly(RECORDS);
    assertThat(values("g.V().hasLabel('Item').order().by('k', desc).values('k')")).containsExactly(40, 39);
  }

  @Test
  void aLimitBelowTheQuotaStaysInForce() {
    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item').order().by('k', desc).limit(1).values('k')")).containsExactly(40);
    assertThat(values("g.V().hasLabel('Item').order().by('k', desc).limit(10).values('k')")).containsExactly(40, 39);
  }

  /** The cap is on the rows the root traversal answers: a traversal it runs for each of them reads every record it matches. */
  @Test
  void aChildTraversalIsNotCappedOnItsOwn() {
    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item').has('k', 1).map(__.V().hasLabel('Item').count())")).containsExactly((long) RECORDS);
    assertThat(values("g.V().hasLabel('Item').fold().count(local)")).containsExactly((long) RECORDS);
    assertThat(values("g.V().hasLabel('Item').has('k', 1).union(__.V().hasLabel('Item').values('k'), __.V().hasLabel('Item').values('k'))"))
        .as("a union answers the quota as a whole").hasSize(QUOTA);
  }

  /** A barrier hands over one traverser for many equal rows: the quota counts the rows, not the traversers. */
  @Test
  void aBulkedTraverserCountsForTheRowsItStandsFor() {
    bindUser(quota(QUOTA));
    assertThat(values("g.V().hasLabel('Item').constant('x').barrier()")).containsExactly("x", "x");
  }

  /** The quota caps the rows a write answers, never the records it writes. */
  @Test
  void aWriteAppliesToEveryRecordAndAnswersTheQuota() {
    bindUser(quota(QUOTA));
    database.transaction(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('Item').property('touched', true)")) {
        assertThat(count(rs)).isEqualTo(QUOTA);
      }
    });
    database.transaction(() -> database.command("gremlin", "g.V().hasLabel('Item').property('iterated', true).iterate()").close());

    bindUser(null);
    assertThat(values("g.V().hasLabel('Item').has('touched', true).count()")).containsExactly((long) RECORDS);
    assertThat(values("g.V().hasLabel('Item').has('iterated', true).count()")).containsExactly((long) RECORDS);
  }

  /** An eager terminal step runs the traversal while the script is evaluated: its value holds the quota of rows. */
  @Test
  void anEagerTerminalStepAnswersTheQuota() {
    bindUser(quota(QUOTA));
    final List<Object> rows = values("g.V().hasLabel('Item').values('k').toList()");
    assertThat(rows).hasSize(1);
    assertThat((Collection<?>) rows.getFirst()).hasSize(QUOTA);
  }

  /** The fluent API and a bytecode request build the same traversal a script does, and no source instruction lifts the quota. */
  @Test
  void theFluentApiAnswersTheQuotaAndCannotDropIt() {
    bindUser(quota(QUOTA));
    assertThat(graph.traversal().V().hasLabel("Item").toList()).hasSize(QUOTA);
    assertThat(graph.traversal().V().hasLabel("Item").order().by("k", Order.desc).values("k").toList()).containsExactly(40, 39);
    assertThat(graph.traversal().V().hasLabel("Item").has("k", 1).map(__.V().hasLabel("Item").count()).toList())
        .containsExactly((long) RECORDS);
    assertThat(graph.traversal().withoutStrategies(ArcadeResultSetLimitStrategy.class).V().hasLabel("Item").toList())
        .hasSize(QUOTA);
    assertThat(ArcadeTraversalStrategies.isPinned(ArcadeResultSetLimitStrategy.class)).isTrue();

    bindUser(null);
    assertThat(graph.traversal().V().hasLabel("Item").toList()).hasSize(RECORDS);
  }

  /** A profiled statement runs as the statement does: the rows of the one the profiler times are capped too. */
  @Test
  void aProfiledTraversalIsCapped() {
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.query("gremlin", "g.V().hasLabel('Item').values('k')", Map.of("$profileExecution", true))) {
      assertThat(count(rs)).isEqualTo(QUOTA);
      assertThat(rs.getExecutionPlan()).isPresent();
    }
    // ITS OWN profile() ANSWERS THE METRICS, ONE ROW
    assertThat(values("g.V().hasLabel('Item').values('k').profile()")).hasSize(1);
  }

  /** A traversal run as a part of a statement the quota already caps is not an answer to the user: it reads every row. */
  @Test
  void aTraversalRunByACappedStatementIsNotCapped() {
    bindUser(quota(QUOTA));
    ProfiledLimitScope.enter();
    try {
      assertThat(values("g.V().hasLabel('Item')")).hasSize(RECORDS);
    } finally {
      ProfiledLimitScope.exit();
    }
    assertThat(values("g.V().hasLabel('Item')")).as("the next top-level traversal").hasSize(QUOTA);
  }

  /** A quota of zero answers no row, and still applies every write. */
  @Test
  void aQuotaOfZeroAnswersNoRow() {
    bindUser(quota(0));
    assertThat(values("g.V().hasLabel('Item')")).isEmpty();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('Item').property('zero', true)")) {
        assertThat(count(rs)).isZero();
      }
    });

    bindUser(null);
    assertThat(values("g.V().hasLabel('Item').has('zero', true).count()")).containsExactly((long) RECORDS);
  }

  private List<Object> values(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("gremlin", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.isElement() ? row.getElement().orElseThrow() : row.getProperty("result"));
      }
    }
    return values;
  }

  private static int count(final ResultSet rs) {
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      ++count;
    }
    return count;
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser quota(final long resultSetLimit) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "capped";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return resultSetLimit;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
