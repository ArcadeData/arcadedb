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
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.index.lsm.LSMTreeIndex;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #12160. Since #11615 an index on a property nothing declares keys a list by its canonical text, each
 * floating point element spelled without trailing zeros: {@code [1.5, 2.0]} is the key {@code "[1.5, 2]"}. Builds before it
 * keyed the same list by {@code List.toString()}, {@code "[1.5, 2.0]"}, and a database they wrote keeps those entries. A seek
 * missed them, MERGE created a second node, a uniqueness constraint let a copy in, and a delete left the entry behind.
 * <p>
 * The entries are written here as an older build wrote them: the canonical entry of each record is replaced with the
 * {@code List.toString()} text of its list.
 */
class UndeclaredIndexLegacyListKeyIssue12160Test extends TestHelper {

  @Test
  void legacyEntriesAreFoundMergedAndRemoved() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:U {id: 0, v: $v})", Map.of("v", List.of(1.5, 2.0)));
      database.command("opencypher", "CREATE (:U {id: 1, v: $v})", Map.of("v", List.of(1.0)));
      database.command("opencypher", "CREATE (:U {id: 2, v: $v})", Map.of("v", List.of(List.of(1.0, 2.0), "a")));
      database.command("opencypher", "CREATE (:U {id: 3, v: $v})", Map.of("v", List.of(1, 2)));
      // a STRING that spells the old key of the list of id 0: the same entry text, another value
      database.command("opencypher", "CREATE (:U {id: 4, v: '[1.5, 2.0]'})");
    });
    writeLegacyEntries("U[v]", "U");
    assertThat(checkDatabaseErrors()).isZero();

    final String seek = "MATCH (n:U) WHERE n.v = $p RETURN n.id AS id ORDER BY id";
    assertThat(ids(seek, Map.of("p", List.of(1.5, 2.0)))).containsExactly(0);
    assertThat(ids(seek, Map.of("p", List.of(1.0)))).containsExactly(1);
    assertThat(ids(seek, Map.of("p", List.of(List.of(1.0, 2.0), "a")))).containsExactly(2);
    assertThat(ids(seek, Map.of("p", List.of(1, 2)))).containsExactly(3);
    assertThat(ids(seek, Map.of("p", "[1.5, 2.0]"))).containsExactly(4);
    assertThat(ids("SELECT id FROM U WHERE v = :p", Map.of("p", List.of(1.5, 2.0)), "sql")).containsExactly(0);

    // MERGE finds the node an older build indexed rather than creating a second one
    database.transaction(() -> database.command("opencypher", "MERGE (n:U {v: $v}) ON CREATE SET n.id = 100", Map.of("v", List.of(1.0))));
    assertThat(ids("MATCH (n:U) RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(0, 1, 2, 3, 4);

    // an update and a delete take the old entry with them
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 1}) SET n.v = [9]"));
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 0}) DELETE n"));
    assertThat(ids(seek, Map.of("p", List.of(1.0)))).isEmpty();
    assertThat(ids(seek, Map.of("p", List.of(9)))).containsExactly(1);
    assertThat(ids(seek, Map.of("p", List.of(1.5, 2.0)))).isEmpty();
    assertThat(ids(seek, Map.of("p", "[1.5, 2.0]"))).containsExactly(4);
    assertThat(database.getSchema().getIndexByName("U[v]").countEntries()).isEqualTo(4L);
    assertThat(checkDatabaseErrors()).isZero();
  }

  @Test
  void uniqueConstraintRefusesACopyOfALegacyKey() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.v IS UNIQUE");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:K {id: 0, v: $v})", Map.of("v", List.of(1.5, 2.0)));
      database.command("opencypher", "CREATE (:K {id: 1, v: '[1.0]'})");
    });
    writeLegacyEntries("K[v]", "K");
    assertThat(checkDatabaseErrors()).isZero();

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (:K {id: 2, v: $v})", Map.of("v", List.of(1.5, 2.0)))))
        .isInstanceOf(DuplicatedKeyException.class);
    // the STRING '[1.0]' and the list [1.0] were one key before #11615 and are two keys now: the list is no copy of the string
    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 3, v: $v})", Map.of("v", List.of(1.0))));

    // once the holder of the old key is gone, its value can be written again
    database.transaction(() -> database.command("opencypher", "MATCH (n:K {id: 0}) DELETE n"));
    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 4, v: $v})", Map.of("v", List.of(1.5, 2.0))));
    assertThat(ids("MATCH (n:K) RETURN n.id AS id ORDER BY id", Map.of())).containsExactly(1, 3, 4);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * The STRING '[1.5, 2.0]' spells the old key of the list [1.5, 2.0], and since #11615 the two are different keys: an old entry
   * of the list is no holder of the STRING, for a seek or for the uniqueness check (PR #12166 review).
   */
  @Test
  void aStringSpelledLikeAnOldListKeyIsNotTheList() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.v IS UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 0, v: $v})", Map.of("v", List.of(1.5, 2.0))));
    writeLegacyEntries("K[v]", "K");

    database.transaction(() -> database.command("opencypher", "CREATE (:K {id: 1, v: '[1.5, 2.0]'})"));
    // SQL answers the rows of the seek as they are, so a holder that does not hold the STRING would be one of them
    assertThat(ids("SELECT id FROM K WHERE v = :p", Map.of("p", "[1.5, 2.0]"), "sql")).containsExactly(1);
    assertThat(ids("SELECT id FROM K WHERE v = :p", Map.of("p", List.of(1.5, 2.0)), "sql")).containsExactly(0);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * A seek reads the canonical key and then the old one. A commit between the two reads that moves the record's entry from the old
   * key to the canonical one ([1.0] rewritten as [1], one key) must not hide the record from the seek (PR #12166 review).
   */
  @Test
  void aCommitBetweenTheTwoReadsOfASeekHidesNoRecord() throws Exception {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    database.transaction(() -> database.command("opencypher", "CREATE (:U {id: 0, v: $v})", Map.of("v", List.of(1.0))));
    writeLegacyEntries("U[v]", "U");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("U[v]");

    final AtomicBoolean moved = new AtomicBoolean();
    LSMTreeIndex.betweenLegacyProbesForTesting = () -> {
      if (!moved.compareAndSet(false, true))
        return;
      final Thread writer = new Thread(() -> database.transaction(
          () -> database.command("opencypher", "MATCH (n:U {id: 0}) SET n.v = $v", Map.of("v", List.of(1L)))));
      writer.start();
      try {
        writer.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    try {
      final List<RID> found = new ArrayList<>();
      try (final IndexCursor cursor = index.get(new Object[] { List.of(1.0) })) {
        while (cursor.hasNext())
          found.add(cursor.next().getIdentity());
      }
      assertThat(moved.get()).isTrue();
      assertThat(found).hasSize(1);
    } finally {
      LSMTreeIndex.betweenLegacyProbesForTesting = null;
    }
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * An old entry is found by the spelling it was written with, as the build that wrote it found it. Another spelling of the same
   * list ([1.5, 2] for an entry written for [1.5, 2.0]) is one key only once the index is rebuilt, which writes every entry under
   * the canonical key (#11777 was that gap in the old builds).
   */
  @Test
  void rebuildKeysTheLegacyEntriesCanonically() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    database.transaction(() -> database.command("opencypher", "CREATE (:U {id: 0, v: $v})", Map.of("v", List.of(1.5, 2.0))));
    writeLegacyEntries("U[v]", "U");

    final String seek = "MATCH (n:U) WHERE n.v = $p RETURN n.id AS id ORDER BY id";
    assertThat(ids(seek, Map.of("p", List.of(1.5, 2.0)))).containsExactly(0);
    database.command("sql", "REBUILD INDEX `U[v]`");
    assertThat(ids(seek, Map.of("p", List.of(1.5, 2.0)))).containsExactly(0);
    assertThat(ids(seek, Map.of("p", List.of(1.5, 2)))).containsExactly(0);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * A composite key: the old entry of a list next to a DECIMAL. The index compares 1.0 and 1.00 as one DECIMAL, so the holder of
   * an old entry holds the key whatever scale the lookup gives it (PR #12166 review).
   */
  @Test
  void legacyHolderOfACompositeKeyMatchesUnderTheIndexComparator() {
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE PROPERTY C.d DECIMAL");
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:C) REQUIRE (n.v, n.d) IS UNIQUE");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex("C").set("id", 0).set("v", List.of(1.5, 2.0))
        .set("d", new BigDecimal("1.0")).save().getIdentity());

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("C[v,d]");
    database.transaction(() -> {
      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
        if (bucketIndex.getAssociatedBucketId() == rid[0].getBucketId()) {
          bucketIndex.remove(new Object[] { List.of(1.5, 2.0), new BigDecimal("1.0") }, rid[0]);
          bucketIndex.put(new Object[] { "[1.5, 2.0]", new BigDecimal("1.0") }, new RID[] { rid[0] });
        }
    });

    final List<RID> found = new ArrayList<>();
    try (final IndexCursor cursor = typeIndex.get(new Object[] { List.of(1.5, 2.0), new BigDecimal("1.00") })) {
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    }
    assertThat(found).containsExactly(rid[0]);

    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("C").set("id", 1).set("v", List.of(1.5, 2.0))
        .set("d", new BigDecimal("1.00")).save())).isInstanceOf(DuplicatedKeyException.class);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * Replaces, in the bucket indexes of {@code indexName}, each record's entry with the one a build before #11615 wrote for it:
   * a list keyed by {@code List.toString()}.
   */
  private void writeLegacyEntries(final String indexName, final String typeName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    final List<String> written = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, v FROM " + typeName)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object value = row.getProperty("v");
          if (!(value instanceof List<?> list))
            continue;
          final RID rid = row.getProperty("rid");
          for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
            if (bucketIndex.getAssociatedBucketId() == rid.getBucketId()) {
              bucketIndex.remove(new Object[] { list }, rid);
              bucketIndex.put(new Object[] { list.toString() }, new RID[] { rid });
              written.add(list.toString());
            }
        }
      }
    });
    // the entries are the old ones now
    try (final IndexCursor cursor = typeIndex.iterator(true)) {
      final List<String> keys = new ArrayList<>();
      while (cursor.hasNext()) {
        cursor.next();
        keys.add(String.valueOf(cursor.getKeys()[0]));
      }
      assertThat(written).isNotEmpty();
      assertThat(keys).containsAll(written).doesNotContain("[1.5, 2]", "[1]");
    }
  }

  private long checkDatabaseErrors() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result result = rs.next();
      return result.<Number>getProperty("totalErrors").longValue();
    }
  }

  private List<Integer> ids(final String query, final Map<String, Object> params) {
    return ids(query, params, "opencypher");
  }

  private List<Integer> ids(final String query, final Map<String, Object> params, final String language) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query, params)) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }
}
