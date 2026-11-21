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
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

  /** The record API is not a query: it answers every record, as it does to the engine paths that read through it. */
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
