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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The {@code readTimeout} of the groups of a user bounds the whole openCypher statement the user reads with, and not only the
 * bucket scans it runs: an index seek, an expansion, an UNWIND, an aggregation, a UNION branch and a PROFILE are bounded as a SQL
 * SELECT or MATCH is. A statement that is paused past the timeout between two pulls fails on the next one, whatever the step that
 * runs it. A user without a timeout, and a timeout longer than the statement, answer every row.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherStatementTimeBoundTest {
  private static final String PATH        = "target/databases/CypherStatementTimeBoundTest";
  private static final int    SPOKES      = 3000;
  private static final int    GENERATED   = 5000;
  private static final long   SHORT_MS    = 1L;
  private static final long   LONG_MS     = 600_000L;
  private static final long   PAUSE_MS    = 50L;
  private static final String UNWIND_ROWS = "UNWIND range(1, " + GENERATED + ") AS i RETURN i";
  private static final String HUB_ROWS    = "MATCH (a:Person {id: 0})-[:KNOWS]->(b:Person) RETURN b.id AS id";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final VertexType person = database.getSchema().createVertexType("Person");
    person.createProperty("id", Integer.class);
    person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createVertexType("Scratch");

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Person").set("id", 0).save();
      for (int i = 1; i <= SPOKES; i++) {
        final MutableVertex spoke = database.newVertex("Person").set("id", i).save();
        hub.newEdge("KNOWS", spoke).save();
      }
    });
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aStatementThatReadsNoBucketIsBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    assertTimesOut(() -> pausedRows(UNWIND_ROWS));
  }

  @Test
  void anIndexSeekWithAnExpansionIsBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    assertTimesOut(() -> pausedRows(HUB_ROWS));
  }

  @Test
  void anAggregateOverAGeneratedListIsBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    assertTimesOut(() -> pausedRows("UNWIND range(1, 2000000) AS i RETURN count(i) AS c"));
  }

  @Test
  void aUnionBranchIsBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    // NEITHER BRANCH ANSWERS A ROW, SO THE UNION ITSELF NEVER HAS A ROW TO CHECK THE TIME ON: THE BOUND HAS TO REACH THE BRANCHES
    assertTimesOut(() -> pausedRows(
        "UNWIND range(1, " + GENERATED + ") AS i WITH i WHERE i < 0 RETURN i UNION ALL UNWIND range(1, " + GENERATED
            + ") AS i WITH i WHERE i < 0 RETURN i"));
  }

  @Test
  void aProfiledStatementIsBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    assertTimesOut(() -> pausedRows("PROFILE UNWIND range(1, 2000000) AS i RETURN count(i) AS c"));
  }

  @Test
  void aTimeoutLongerThanTheStatementAnswersEveryRow() throws InterruptedException {
    bindUser(user(LONG_MS));
    assertThat(pausedRows(UNWIND_ROWS)).isEqualTo(GENERATED);
    assertThat(pausedRows(HUB_ROWS)).isEqualTo(SPOKES);
    assertThat(pausedRows("UNWIND range(1, 1000) AS i RETURN i UNION ALL UNWIND range(1, 1000) AS i RETURN i")).isEqualTo(2000);
  }

  @Test
  void aUserWithoutATimeoutAnswersEveryRow() throws InterruptedException {
    for (final SecurityDatabaseUser user : new SecurityDatabaseUser[] { user(-1L), null }) {
      bindUser(user);
      assertThat(pausedRows(UNWIND_ROWS)).isEqualTo(GENERATED);
      assertThat(pausedRows(HUB_ROWS)).isEqualTo(SPOKES);
    }
  }

  /** The cached plan of a statement carries no time bound: the user who runs it next brings their own. */
  @Test
  void aTimeoutIsNotKeptByTheCachedPlanForTheNextUser() throws InterruptedException {
    for (final String query : new String[] { UNWIND_ROWS, HUB_ROWS }) {
      bindUser(user(SHORT_MS));
      assertTimesOut(() -> pausedRows(query));

      bindUser(null);
      assertThat(pausedRows(query)).as("%s: the user without a timeout runs the statement the first one could not", query)
          .isGreaterThanOrEqualTo(SPOKES);

      bindUser(user(SHORT_MS));
      assertTimesOut(() -> pausedRows(query));
    }
  }

  @Test
  void aUserWithoutATimeoutIsNotBoundedByThePlanTheTimedUserCached() throws InterruptedException {
    bindUser(null);
    assertThat(pausedRows(HUB_ROWS)).isEqualTo(SPOKES);

    bindUser(user(SHORT_MS));
    assertTimesOut(() -> pausedRows(HUB_ROWS));
  }

  @Test
  void theTighterOfTheCommandTimeoutAndTheReadTimeoutApplies() {
    ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, SHORT_MS);
    bindUser(user(LONG_MS));
    assertThat(timeoutMessage(() -> pausedRows(UNWIND_ROWS))).contains(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

    ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, LONG_MS);
    bindUser(user(SHORT_MS));
    assertThat(timeoutMessage(() -> pausedRows(UNWIND_ROWS))).contains("readTimeout").doesNotContain(
        GlobalConfiguration.COMMAND_TIMEOUT.getKey());
  }

  /** A statement that writes keeps the pace it had: the timeout of a user is the one of a read. */
  @Test
  void aStatementThatWritesIsNotBoundedByTheReadTimeout() {
    bindUser(user(SHORT_MS));
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "UNWIND range(1, 2000) AS i CREATE (:Scratch {n: i})")) {
        while (rs.hasNext())
          rs.next();
      }
    });

    bindUser(null);
    try (final ResultSet rs = database.query("opencypher", "MATCH (s:Scratch) RETURN count(s) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2000);
    }
  }

  /** Runs the statement, lets the timeout of the user pass before the first pull, and answers how many rows it gave. */
  private int pausedRows(final String query) throws InterruptedException {
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      Thread.sleep(PAUSE_MS);
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private static void assertTimesOut(final ThrowingCallable work) {
    assertThat(timeoutOf(catchThrowable(work))).as("the statement fails with a timeout").isNotNull();
  }

  private static String timeoutMessage(final ThrowingCallable work) {
    final TimeoutException timeout = timeoutOf(catchThrowable(work));
    assertThat(timeout).as("the statement fails with a timeout").isNotNull();
    return timeout.getMessage();
  }

  /** The timeout a failure is or wraps: a statement that is drained before it returns wraps what it raised. */
  private static TimeoutException timeoutOf(final Throwable thrown) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause())
      if (cause instanceof TimeoutException timeout)
        return timeout;
    return null;
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser user(final long readTimeout) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "timed";
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
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return readTimeout;
      }
    };
  }
}
