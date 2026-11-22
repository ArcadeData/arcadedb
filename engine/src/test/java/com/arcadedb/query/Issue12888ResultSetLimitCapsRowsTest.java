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
package com.arcadedb.query;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A security group's {@code resultSetLimit} caps the rows a query answers, never the data it reads (issue #12888). It used to be
 * applied to every bucket scan, so a user under a quota of 2 on a type of 4 buckets read the first 2 records of each bucket only: a
 * WHERE missed the records past them, and sum, max, count over rows and ORDER BY ran over those 8 records (SQL) or over the first 2
 * of all (openCypher, whose type scan applied it once more), with no error to say the answer was partial. A UNION ALL of openCypher
 * answered twice the quota, one allowance per branch.
 */
class Issue12888ResultSetLimitCapsRowsTest {
  private static final String PATH    = "target/databases/Issue12888ResultSetLimitCapsRowsTest";
  private static final int    RECORDS = 40;
  private static final int    QUOTA   = 2;
  private static final int    SUM     = RECORDS * (RECORDS + 1) / 2;

  private DatabaseFactory factory;
  private Database        database;

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
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void sqlWhereFindsARecordPastTheQuotaOfItsBucket() {
    bindUser(quota(QUOTA));
    assertThat(ks("sql", "SELECT k FROM Item WHERE k = 40")).containsExactly(40);
  }

  @Test
  void cypherWhereFindsARecordPastTheQuotaOfItsBucket() {
    bindUser(quota(QUOTA));
    assertThat(ks("opencypher", "MATCH (i:Item) WHERE i.k = 40 RETURN i.k AS k")).containsExactly(40);
  }

  @Test
  void sqlAggregatesRunOverEveryRecord() {
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.query("sql", "SELECT sum(k) AS s, max(k) AS m, count(*) AS c FROM Item WHERE k > 0")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(SUM);
      assertThat(row.<Number>getProperty("m").intValue()).isEqualTo(RECORDS);
      assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void cypherAggregatesRunOverEveryRecord() {
    bindUser(quota(QUOTA));
    for (final String query : List.of("MATCH (i:Item) RETURN sum(i.k) AS s, max(i.k) AS m, count(*) AS c",
        "MATCH (i:Item) WHERE i.k > 0 RETURN sum(i.k) AS s, max(i.k) AS m, count(*) AS c")) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("s").intValue()).as(query).isEqualTo(SUM);
        assertThat(row.<Number>getProperty("m").intValue()).as(query).isEqualTo(RECORDS);
        assertThat(row.<Number>getProperty("c").intValue()).as(query).isEqualTo(RECORDS);
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  @Test
  void orderByRunsOverEveryRecordAndAnswersTheQuota() {
    bindUser(quota(QUOTA));
    assertThat(ks("sql", "SELECT k FROM Item ORDER BY k DESC")).containsExactly(40, 39);
    assertThat(ks("opencypher", "MATCH (i:Item) RETURN i.k AS k ORDER BY k DESC")).containsExactly(40, 39);
  }

  @Test
  void aPlainScanAnswersTheQuota() {
    bindUser(quota(QUOTA));
    assertThat(count("sql", "SELECT FROM Item")).isEqualTo(QUOTA);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).isEqualTo(QUOTA);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i LIMIT 10")).as("a LIMIT above the quota is capped").isEqualTo(QUOTA);
  }

  @Test
  void aLimitBelowTheQuotaStaysInForce() {
    bindUser(quota(QUOTA));
    assertThat(ks("sql", "SELECT k FROM Item ORDER BY k DESC LIMIT 1")).containsExactly(40);
    assertThat(ks("opencypher", "MATCH (i:Item) RETURN i.k AS k ORDER BY k DESC LIMIT 1")).containsExactly(40);
  }

  @Test
  void aUnionAllAnswersTheQuotaAsAWhole() {
    bindUser(quota(QUOTA));
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i.k AS k UNION ALL MATCH (i:Item) RETURN i.k AS k")).isEqualTo(QUOTA);
    assertThat(count("sql", "SELECT expand(unionall($a, $b)) LET $a = (SELECT FROM Item), $b = (SELECT FROM Item)"))
        .isEqualTo(QUOTA);
  }

  /** The quota caps what the statement answers, not the subqueries it reads its answer from. */
  @Test
  void aCypherSubqueryReadsEveryRecord() {
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (i:Item) WHERE i.k = 1 RETURN COUNT { MATCH (j:Item) RETURN j } AS c, COLLECT { MATCH (j:Item) RETURN j.k } AS ks")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
      assertThat(row.<List<?>>getProperty("ks")).hasSize(RECORDS);
    }
  }

  /** An EXISTS subquery reads the records of its own pattern: the row cap does not reach it, and it does not cap the outer rows. */
  @Test
  void aCypherExistsSubqueryReadsEveryRecord() {
    bindUser(quota(QUOTA));
    // k = 40 sits past the first two records of every bucket, and of the type
    assertThat(ks("opencypher", "MATCH (i:Item) WHERE i.k = 1 AND EXISTS { MATCH (j:Item) WHERE j.k = 40 } RETURN i.k AS k"))
        .containsExactly(1);
    assertThat(ks("opencypher", "MATCH (i:Item) WHERE i.k = 1 AND NOT EXISTS { MATCH (j:Item) WHERE j.k = 40 } RETURN i.k AS k"))
        .isEmpty();
    assertThat(count("opencypher", "MATCH (i:Item) WHERE EXISTS { MATCH (j:Item) WHERE j.k = 40 } RETURN i.k AS k"))
        .as("the outer statement is capped, its EXISTS is not").isEqualTo(QUOTA);
  }

  /** {@code apoc.do.when} runs its branch as a query of its own and drains every row of it. */
  @Test
  void anApocDoWhenBranchReadsEveryRecord() {
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (i:Item) WHERE i.k = 1 CALL apoc.do.when(true, 'MATCH (j:Item) RETURN j.k AS k', '') YIELD value "
            + "RETURN count(value) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(RECORDS);
    }
  }

  /** A user-defined function written in Cypher runs its body as a query of its own, on the thread that evaluates the row. */
  @Test
  void aUserDefinedCypherFunctionReadsEveryRecord() {
    database.command("sql", "DEFINE FUNCTION cap.keys \"MATCH (j:Item) RETURN collect(j.k) AS ks\" LANGUAGE opencypher");
    bindUser(quota(QUOTA));
    // a function call may write, so the statement is a command
    try (final ResultSet rs = database.command("opencypher", "MATCH (i:Item) WHERE i.k = 1 RETURN size(cap.keys()) AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(RECORDS);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * A user-defined function written in SQL runs its body through the SQL engine, which applies the quota to a top-level statement
   * as its LIMIT. Run by a capped openCypher statement, the body is a part of that statement and reads every record: the quota caps
   * the rows the statement answers, not what its function computes from.
   */
  @Test
  void aUserDefinedSqlFunctionRunByACappedStatementReadsEveryRecord() {
    database.command("sql", "DEFINE FUNCTION cap.sqlkeys \"SELECT k FROM Item\" LANGUAGE sql");
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.command("opencypher", "MATCH (i:Item) WHERE i.k = 1 RETURN size(cap.sqlkeys()) AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(RECORDS);
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(count("sql", "SELECT FROM Item")).as("the next top-level SQL statement is capped").isEqualTo(QUOTA);
    assertThat(count("sqlscript", "SELECT FROM Item;")).as("and so is a top-level script statement").isEqualTo(QUOTA);
  }

  /**
   * A callback that runs while a capped statement does - the function above - reads for the statement, which answers the quota of
   * rows whatever the callback read. And nothing it leaves behind lets the next top-level statement of the thread through uncapped.
   */
  @Test
  void aStatementThatRanACallbackLeavesTheThreadCapped() {
    database.command("sql", "DEFINE FUNCTION cap.keys \"MATCH (j:Item) RETURN collect(j.k) AS ks\" LANGUAGE opencypher");
    bindUser(quota(QUOTA));
    assertThat(count(database.command("opencypher", "MATCH (i:Item) RETURN size(cap.keys()) AS n"))).isEqualTo(QUOTA);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).as("the next top-level statement").isEqualTo(QUOTA);
  }

  /** The rows are pulled after execute() returned: the answer is still the quota, and a statement run in between is capped too. */
  @Test
  void aResultSetConsumedLazilyIsCappedAndLeavesTheThreadCapped() {
    bindUser(quota(QUOTA));
    try (final ResultSet lazy = database.query("opencypher", "MATCH (i:Item) RETURN i.k AS k ORDER BY k DESC")) {
      assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).as("another statement while the first is not read").isEqualTo(QUOTA);

      final List<Integer> ks = new ArrayList<>();
      while (lazy.hasNext())
        ks.add(lazy.next().<Number>getProperty("k").intValue());
      assertThat(ks).containsExactly(40, 39);
      assertThat(lazy.hasNext()).isFalse();

      assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).as("and after it was read").isEqualTo(QUOTA);
    }
  }

  /** {@code LIMIT :n} and {@code LIMIT ?} of a script statement are compared with the quota by the value they bind. */
  @Test
  void aScriptStatementKeepsAParameterLimitBelowTheQuota() {
    bindUser(quota(QUOTA));
    final String named = "SELECT k FROM Item ORDER BY k DESC LIMIT :n;";
    assertThat(ks(database.command("sqlscript", named, Map.of("n", 1)))).as("LIMIT :n below the quota").containsExactly(40);
    assertThat(ks(database.command("sqlscript", named, Map.of("n", 10)))).as("LIMIT :n above the quota").containsExactly(40, 39);

    final String positional = "SELECT k FROM Item ORDER BY k DESC LIMIT ?;";
    assertThat(ks(database.command("sqlscript", positional, 1))).as("LIMIT ? below the quota").containsExactly(40);
    assertThat(ks(database.command("sqlscript", positional, 10))).as("LIMIT ? above the quota").containsExactly(40, 39);

    // the statements are parsed for each execution: the first user's value does not stay in the next one's
    bindUser(null);
    assertThat(ks(database.command("sqlscript", named, Map.of("n", 10)))).hasSize(10);
  }

  /** A write runs to its end before the caller reads a row, so the quota caps the rows it answers and none of what it writes. */
  @Test
  void aCypherWriteAppliesEveryRowAndAnswersTheQuota() {
    bindUser(quota(QUOTA));
    database.transaction(() -> assertThat(count(database.command("opencypher",
        "MATCH (i:Item) SET i.touched = true RETURN i.k AS k"))).isEqualTo(QUOTA));

    bindUser(null);
    assertThat(count("sql", "SELECT FROM Item WHERE touched = true")).isEqualTo(RECORDS);
  }

  /** A script statement takes the quota as its LIMIT only where it is tighter, as the same statement run on its own does. */
  @Test
  void aScriptStatementKeepsALimitBelowTheQuota() {
    bindUser(quota(QUOTA));
    assertThat(ks(database.command("sqlscript", "SELECT k FROM Item ORDER BY k DESC LIMIT 1;"))).containsExactly(40);
    assertThat(ks(database.command("sqlscript", "SELECT k FROM Item ORDER BY k DESC;"))).containsExactly(40, 39);
    assertThat(ks(database.command("sqlscript", "SELECT k FROM Item WHERE k = 40;"))).containsExactly(40);
  }

  /**
   * The record API is not a query: it answers every record, as it does to the engine paths that read through it. Gremlin and the
   * MongoDB wire protocol read through it too, so until they apply the quota as a row cap of their own (issue #13087) nothing but
   * the readTimeout bounds what they answer a capped user. This test pins that gap: when #13087 lands, those protocols get a row
   * cap and this API still answers every record.
   */
  @Test
  void theRecordIteratorsReadEveryRecord() {
    bindUser(quota(QUOTA));
    int count = 0;
    for (final Iterator<Record> it = database.iterateType("Item", true); it.hasNext(); it.next())
      ++count;
    assertThat(count).isEqualTo(RECORDS);

    count = 0;
    for (final Bucket bucket : database.getSchema().getType("Item").getBuckets(false))
      for (final Iterator<Record> it = database.iterateBucket(bucket.getName()); it.hasNext(); it.next())
        ++count;
    assertThat(count).isEqualTo(RECORDS);
  }

  /** The readTimeout of the user is what bounds a scan now: it still interrupts the record API, with the quota out of the way. */
  @Test
  void theReadTimeoutStillInterruptsTheRecordIterator() throws InterruptedException {
    bindUser(quota(QUOTA, 1L));
    final Iterator<Record> it = database.iterateType("Item", true);
    Thread.sleep(50);
    assertThatThrownBy(() -> {
      while (it.hasNext())
        it.next();
    }).isInstanceOf(TimeoutException.class);

    // a generous timeout lets the whole type through: the quota is not what stops the scan
    bindUser(quota(QUOTA, 600_000L));
    int count = 0;
    for (final Iterator<Record> all = database.iterateType("Item", true); all.hasNext(); all.next())
      ++count;
    assertThat(count).isEqualTo(RECORDS);
  }

  /** A scan of one bucket is bounded by the readTimeout as a scan of the type is: the quota no longer cuts it short. */
  @Test
  void theReadTimeoutStillInterruptsTheBucketIterator() throws InterruptedException {
    final String bucketName = database.getSchema().getType("Item").getBuckets(false).getFirst().getName();
    bindUser(quota(QUOTA, 1L));
    final Iterator<Record> it = database.iterateBucket(bucketName);
    Thread.sleep(50);
    assertThatThrownBy(() -> {
      while (it.hasNext())
        it.next();
    }).isInstanceOf(TimeoutException.class);

    bindUser(null);
    final long inBucket = database.countBucket(bucketName);
    assertThat(inBucket).as("the bucket holds more than the quota").isGreaterThan(QUOTA);

    bindUser(quota(QUOTA, 600_000L));
    int count = 0;
    for (final Iterator<Record> all = database.iterateBucket(bucketName); all.hasNext(); all.next())
      ++count;
    assertThat(count).isEqualTo(inBucket);
  }

  /**
   * A WHERE of a user under a quota reads past the quota now, so the readTimeout of the group is what bounds it: the scan that
   * goes on after the pause is refused, where the scan cap used to end it without a word.
   */
  @Test
  void theReadTimeoutInterruptsTheSqlScanOfACappedUser() {
    bindUser(quota(QUOTA, 1L));
    // the two rows the quota lets through are the last two of the scan: every record is read to find them
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT k FROM Item WHERE k >= " + (RECORDS - 1))) {
        // THE DEADLINE IS PINNED BY THE FIRST PULL, AND THE PAUSE OUTLASTS IT WHEREVER THE SCAN HAS GOT TO BY THEN
        if (rs.hasNext())
          rs.next();
        Thread.sleep(50);
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(TimeoutException.class);
  }

  /**
   * An aggregate of a user under a quota reads every record as well, in one pull that answers one row: with a filter that outlasts
   * the readTimeout the statement fails, and answers no sum over the part of the type it had read.
   */
  @Test
  void theReadTimeoutInterruptsTheSqlAggregateOfACappedUser() {
    ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("pause12888") {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        try {
          // LONGER THAN THE 1 MS QUOTA, ON EVERY RECORD: THE SCAN CANNOT END BEFORE ITS DEADLINE
          Thread.sleep(5);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return true;
      }

      @Override
      public String getSyntax() {
        return "pause12888()";
      }
    });

    bindUser(quota(QUOTA, 1L));
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT sum(k) AS s FROM Item WHERE pause12888() = true")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(TimeoutException.class);

    // without the timeout the same statement answers the sum of every record, under the same quota
    bindUser(quota(QUOTA));
    try (final ResultSet rs = database.query("sql", "SELECT sum(k) AS s FROM Item WHERE pause12888() = true")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(SUM);
    }
  }

  @Test
  void aUserWithoutAQuotaIsNotCapped() {
    bindUser(quota(-1L));
    assertThat(count("sql", "SELECT FROM Item")).isEqualTo(RECORDS);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).isEqualTo(RECORDS);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i.k AS k UNION ALL MATCH (i:Item) RETURN i.k AS k")).isEqualTo(2 * RECORDS);
    assertThat(ks("opencypher", "MATCH (i:Item) RETURN i.k AS k ORDER BY k DESC LIMIT 3")).containsExactly(40, 39, 38);

    bindUser(null);
    assertThat(count("sql", "SELECT FROM Item")).isEqualTo(RECORDS);
    assertThat(count("opencypher", "MATCH (i:Item) RETURN i")).isEqualTo(RECORDS);
  }

  private List<Integer> ks(final String language, final String query) {
    return ks(database.query(language, query));
  }

  private static List<Integer> ks(final ResultSet resultSet) {
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = resultSet) {
      while (rs.hasNext())
        ks.add(rs.next().<Number>getProperty("k").intValue());
    }
    return ks;
  }

  private int count(final String language, final String query) {
    return count(database.query(language, query));
  }

  private static int count(final ResultSet resultSet) {
    int count = 0;
    try (final ResultSet rs = resultSet) {
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
    }
    return count;
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser quota(final long resultSetLimit) {
    return quota(resultSetLimit, -1L);
  }

  private static SecurityDatabaseUser quota(final long resultSetLimit, final long readTimeout) {
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
        return readTimeout;
      }
    };
  }
}
