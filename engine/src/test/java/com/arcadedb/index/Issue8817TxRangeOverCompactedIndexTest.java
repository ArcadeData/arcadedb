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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8817: inside a transaction, an index range read missed the records that transaction inserted once the LSM index
 * had been compacted into a sub-index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8817TxRangeOverCompactedIndexTest extends TestHelper {
  private static final int COMMITTED = 20_000;

  private void load(final boolean unique, final boolean compact) throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().createVertexType("V", 1);
      type.createProperty("a", Type.LONG);
      database.getSchema().buildTypeIndex("V", new String[] { "a" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(unique)
          .withPageSize(4096).create();
    });
    for (int base = 0; base < COMMITTED; base += 5_000) {
      final int from = base;
      database.transaction(() -> {
        for (int i = from; i < from + 5_000; i++)
          database.newVertex("V").set("a", 1000L + i).save();
      });
    }
    if (compact)
      for (final Index idx : database.getSchema().getType("V").getAllIndexes(false))
        for (final IndexInternal bucketIndex : ((TypeIndex) idx).getIndexesOnBuckets())
          if (bucketIndex.scheduleCompaction()) {
            bucketIndex.compact();
            assertThat(((LSMTreeIndex) bucketIndex).getMutableIndex().getSubIndex()).as("index must be compacted").isNotNull();
          }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void txInsertsAreVisibleToIndexRangesOverCompactedIndex(final boolean unique) throws Exception {
    load(unique, true);
    database.begin();
    try {
      for (int i = 0; i < 10; i++)
        database.newVertex("V").set("a", 500L + i).save();
      // 50 below every committed key, 50 above, 30 committed deleted, 20 updated to 5
      for (int i = 0; i < 50; i++)
        database.newVertex("V").set("a", (long) i).save();
      for (int i = 0; i < 50; i++)
        database.newVertex("V").set("a", 10_000_000L + i).save();
      database.command("sql", "DELETE FROM V WHERE a >= 1100 AND a < 1130");
      database.command("sql", "UPDATE V SET a = a + 5000000 WHERE a >= 1200 AND a < 1220");

      assertThat(count("SELECT count(*) AS c FROM V WHERE a BETWEEN 500 AND 509")).isEqualTo(10);
      assertThat(count("SELECT count(*) AS c FROM V WHERE a = 505")).isEqualTo(1);
      assertThat(count("SELECT count(*) AS c FROM V WHERE a < 1010")).isEqualTo(count("SELECT count(*) AS c FROM V WHERE a + 0 < 1010"));
      assertThat(count("SELECT count(*) AS c FROM V WHERE a > 9999999")).isEqualTo(50);
      assertThat(count("SELECT count(*) AS c FROM V WHERE a BETWEEN 0 AND 49")).isEqualTo(
          count("SELECT count(*) AS c FROM V WHERE a + 0 BETWEEN 0 AND 49"));
      assertThat(count("SELECT min(a) AS c FROM V")).isEqualTo(0);
      assertThat(count("SELECT max(a) AS c FROM V")).isEqualTo(10_000_049);
      assertThat(count("SELECT count(*) AS c FROM V")).isEqualTo(COMMITTED + 10 + 100 - 30);
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a >= 0 ORDER BY a ASC LIMIT 3")) {
        assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(0);
        assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(1);
        assertThat(rs.next().<Number>getProperty("a").longValue()).isEqualTo(2);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) RETURN min(v.a) AS m, max(v.a) AS x")) {
        final var r = rs.next();
        assertThat(r.<Number>getProperty("m").longValue()).isEqualTo(0);
        assertThat(r.<Number>getProperty("x").longValue()).isEqualTo(10_000_049);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) WHERE v.a < 1010 RETURN count(*) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(count("SELECT count(*) AS c FROM V WHERE a + 0 < 1010"));
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void indexApiRangeSeesTxInserts() throws Exception {
    load(false, true);
    database.begin();
    try {
      for (int i = 0; i < 10; i++)
        database.newVertex("V").set("a", 500L + i).save();
      final TypeIndex index = database.getSchema().getType("V").getIndexesByProperties("a").getFirst();
      int n = 0;
      try (final IndexCursor c = index.range(true, new Object[] { 500L }, true, new Object[] { 1009L }, true)) {
        while (c.hasNext()) {
          c.next();
          n++;
        }
      }
      assertThat(n).isEqualTo(20);
    } finally {
      database.rollback();
    }
  }
}
