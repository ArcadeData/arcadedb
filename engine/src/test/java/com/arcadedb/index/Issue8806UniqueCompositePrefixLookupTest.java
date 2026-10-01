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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8806: a lookup by a key PREFIX on a UNIQUE composite index built by many commits returned only part of
 * the matching entries. The unindexed scan over the same rows, and a bounded range over the whole key, were right.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8806UniqueCompositePrefixLookupTest extends TestHelper {
  private static final int HOSTS  = 10;
  private static final int POINTS = 30_000;
  private static final int COMMIT = 1_000;

  private void load(final String unique) throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Point");
    database.command("sql", "CREATE PROPERTY Point.host STRING");
    database.command("sql", "CREATE PROPERTY Point.ts LONG");
    database.command("sql", "CREATE INDEX ON Point (host, ts) " + unique);
    final int total = HOSTS * POINTS;
    database.begin();
    for (int i = 0; i < total; i++) {
      database.newDocument("Point").set("host", "host_" + (i % HOSTS), "ts", (long) (i / HOSTS) * 10).save();
      if ((i + 1) % COMMIT == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    compactAndAssertSeries("Point[host,ts]");
  }

  /** The defect lives in the compacted series: compact now rather than wait for the background compaction. */
  private void compactAndAssertSeries(final String indexName) throws Exception {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      bucketIndex.compact();
    assertThat(typeIndex.getIndexesOnBuckets()).anySatisfy(i -> assertThat(((LSMTreeIndex) i).getMutableIndex().getSubIndex()).isNotNull());
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void check() {
    assertThat(count("SELECT count(*) AS c FROM Point WHERE host = 'host_7'")).isEqualTo(POINTS);
    assertThat(count("SELECT count(*) AS c FROM Point WHERE host.toUpperCase() = 'HOST_7'")).isEqualTo(POINTS);

    try (final ResultSet rs = database.query("sql", "SELECT ts FROM Point WHERE host = 'host_7' ORDER BY ts ASC LIMIT 1")) {
      assertThat(rs.next().<Number>getProperty("ts").longValue()).isZero();
    }

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Point[host,ts]");
    for (final boolean ascending : new boolean[] { true, false }) {
      final IndexCursor cursor = index.range(ascending, new Object[] { "host_7" }, true, new Object[] { "host_7" }, true);
      long n = 0;
      while (cursor.hasNext()) {
        cursor.next();
        n++;
      }
      assertThat(n).as("range ascending=" + ascending).isEqualTo(POINTS);
    }
  }

  @Test
  void uniquePrefixLookupReturnsEveryEntry() throws Exception {
    load("UNIQUE");
    check();
  }

  @Test
  void uniquePrefixLookupAfterReopen() throws Exception {
    load("UNIQUE");
    reopenDatabase();
    check();
  }

  @Test
  void notUniquePrefixLookupReturnsEveryEntry() throws Exception {
    load("NOTUNIQUE");
    check();
  }

  @Test
  void uniqueFirstPropertyOfThreePropertyIndex() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE T3");
    database.command("sql", "CREATE PROPERTY T3.a INTEGER");
    database.command("sql", "CREATE PROPERTY T3.b INTEGER");
    database.command("sql", "CREATE PROPERTY T3.c INTEGER");
    database.command("sql", "CREATE INDEX ON T3 (a, b, c) UNIQUE");
    database.begin();
    for (int i = 0; i < 300_000; i++) {
      database.newDocument("T3").set("a", i % 10, "b", (i / 10) % 10, "c", i / 100).save();
      if ((i + 1) % COMMIT == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    compactAndAssertSeries("T3[a,b,c]");
    assertThat(count("SELECT count(*) AS c FROM T3 WHERE a = 3")).isEqualTo(30_000);
    assertThat(count("SELECT count(*) AS c FROM T3 WHERE a = 3 AND b = 4")).isEqualTo(3_000);
  }
}
