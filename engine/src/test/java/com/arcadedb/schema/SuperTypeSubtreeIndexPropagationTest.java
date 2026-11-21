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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12036: {@code ALTER TYPE A SUPERTYPE +W}, where A already has sub types, put W's indexes on A's buckets only.
 * The records of the sub types are W records from then on, so every index of W (and of W's super types) has to cover their
 * buckets too, for every index kind, and a UNIQUE index has to refuse the change, with nothing changed, when the joining subtree
 * holds a key the index already holds, as it does for A's own buckets (#11302).
 */
class SuperTypeSubtreeIndexPropagationTest extends TestHelper {

  @Test
  void uniqueIndexCoversTheWholeJoiningSubtree() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.s STRING");
    database.command("sql", "CREATE INDEX ON W (s) UNIQUE");
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO W SET s = 'w1'");
      database.command("sql", "INSERT INTO A SET s = 'a1'");
      database.command("sql", "INSERT INTO X SET s = 'x1'");
      database.command("sql", "INSERT INTO Y SET s = 'y1'");
    });

    database.command("sql", "ALTER TYPE A SUPERTYPE +W");

    assertUniqueIndexCoversSubtree();
    reopenDatabase();
    assertUniqueIndexCoversSubtree();
  }

  @Test
  void indexOfASuperTypeAboveTheNewOneCoversTheSubtree() {
    database.command("sql", "CREATE VERTEX TYPE V0");
    database.command("sql", "CREATE PROPERTY V0.s STRING");
    database.command("sql", "CREATE INDEX ON V0 (s) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE W EXTENDS V0");
    createSubtree();
    database.transaction(() -> database.command("sql", "INSERT INTO Y SET s = 'y1'"));

    database.command("sql", "ALTER TYPE A SUPERTYPE +W");

    assertThat(keyRids("V0[s]", "y1")).hasSize(1);
    assertThat(subIndexBucketNames("V0[s]")).contains(bucketOf("X"), bucketOf("Y"));
  }

  @Test
  void duplicateInTheSubtreeAgainstTheSuperTypeIsRefusedAndUndone() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.s STRING");
    database.command("sql", "CREATE INDEX ON W (s) UNIQUE");
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO W SET s = 'w1'");
      database.command("sql", "INSERT INTO A SET s = 'a1'");
      database.command("sql", "INSERT INTO Y SET s = 'w1'");
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +W"))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("w1");

    assertRefusedAndUndone();
    reopenDatabase();
    assertRefusedAndUndone();
  }

  @Test
  void duplicateBetweenTwoTypesOfTheSubtreeIsRefusedAndUndone() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.s STRING");
    database.command("sql", "CREATE INDEX ON W (s) UNIQUE");
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO A SET s = 'k'");
      database.command("sql", "INSERT INTO X SET s = 'k'");
    });

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +W"))//
        .isInstanceOf(DuplicatedKeyException.class)//
        .hasMessageContaining("k");

    assertRefusedAndUndone();
  }

  @Test
  void byItemIndexCoversTheSubtree() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.tags LIST OF STRING");
    final String indexName;
    try (final ResultSet rs = database.command("sql", "CREATE INDEX ON W (tags BY ITEM) NOTUNIQUE")) {
      indexName = rs.next().getProperty("name");
    }
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO A SET tags = ['alpha']");
      database.command("sql", "INSERT INTO X SET tags = ['alpha', 'beta']");
      database.command("sql", "INSERT INTO Y SET tags = ['alpha']");
    });

    database.command("sql", "ALTER TYPE A SUPERTYPE +W");

    assertThat(keyRids(indexName, "alpha")).hasSize(3);
    assertThat(keyRids(indexName, "beta")).hasSize(1);
  }

  @Test
  void fullTextIndexCoversTheSubtree() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.t STRING");
    database.command("sql", "CREATE INDEX ON W (t) FULL_TEXT");
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO W SET t = 'alpha'");
      database.command("sql", "INSERT INTO A SET t = 'alpha'");
      database.command("sql", "INSERT INTO X SET t = 'alpha'");
      database.command("sql", "INSERT INTO Y SET t = 'alpha beta'");
    });

    database.command("sql", "ALTER TYPE A SUPERTYPE +W");
    database.transaction(() -> database.command("sql", "INSERT INTO X SET t = 'alpha later'"));

    assertThat(typesFound("SELECT FROM W WHERE SEARCH_INDEX('W[t]', 'alpha') = true"))//
        .containsExactlyInAnyOrder("W", "A", "X", "Y", "X");
  }

  @Test
  void vectorIndexCoversTheSubtree() {
    database.command("sql", "CREATE VERTEX TYPE W");
    database.command("sql", "CREATE PROPERTY W.v ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON W (v) LSM_VECTOR METADATA {dimensions: 2, similarity: 'EUCLIDEAN'}");
    createSubtree();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO W SET v = [0.1, 0.2]");
      database.command("sql", "INSERT INTO A SET v = [0.3, 0.4]");
      database.command("sql", "INSERT INTO X SET v = [0.5, 0.6]");
      database.command("sql", "INSERT INTO Y SET v = [0.7, 0.8]");
    });

    database.command("sql", "ALTER TYPE A SUPERTYPE +W");

    assertThat(typesFound("SELECT expand(vectorNeighbors('W[v]', [0.5, 0.5], 100))"))//
        .containsExactlyInAnyOrder("W", "A", "X", "Y");
  }

  /** A, X EXTENDS A, Y EXTENDS X: a subtree two levels deep below the type that joins. */
  private void createSubtree() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE X EXTENDS A");
    database.command("sql", "CREATE VERTEX TYPE Y EXTENDS X");
  }

  private void assertUniqueIndexCoversSubtree() {
    assertThat(keyRids("W[s]", "x1")).hasSize(1);
    assertThat(keyRids("W[s]", "y1")).hasSize(1);
    assertThat(subIndexBucketNames("W[s]")).contains(bucketOf("A"), bucketOf("X"), bucketOf("Y"));

    for (final String statement : List.of("INSERT INTO X SET s = 'w1'", "INSERT INTO Y SET s = 'x1'", "INSERT INTO A SET s = 'y1'",
        "INSERT INTO X SET s = 'x1'"))
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", statement)))//
          .as(statement)//
          .isInstanceOf(DuplicatedKeyException.class);

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM W")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(4L);
    }
  }

  private void assertRefusedAndUndone() {
    assertThat(database.getSchema().getType("A").isSubTypeOf("W")).isFalse();
    assertThat(database.getSchema().getType("Y").isSubTypeOf("W")).isFalse();
    assertThat(subIndexBucketNames("W[s]")).containsExactly(bucketOf("W"));
    for (final String type : List.of("A", "X", "Y"))
      for (final IndexInternal idx : database.getSchema().getType(type).getPolymorphicBucketIndexByBucketId(
          database.getSchema().getBucketByName(bucketOf(type)).getFileId(), null))
        assertThat(idx.getTypeIndex().getName()).isNotEqualTo("W[s]");
  }

  private String bucketOf(final String type) {
    return database.getSchema().getType(type).getBuckets(false).getFirst().getName();
  }

  private Set<String> subIndexBucketNames(final String indexName) {
    final Set<String> names = new HashSet<>();
    for (final IndexInternal idx : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      names.add(database.getSchema().getBucketById(idx.getAssociatedBucketId()).getName());
    return names;
  }

  private List<RID> keyRids(final String indexName, final Object key) {
    final IndexCursor cursor = database.getSchema().getIndexByName(indexName).get(new Object[] { key });
    final List<RID> rids = new ArrayList<>();
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    return rids;
  }

  private List<String> typesFound(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      // an expanded vectorNeighbors row carries the record under 'record'
      return rs.stream().map(r -> r.getElement().orElseGet(() -> ((Identifiable) r.getProperty("record")).asDocument()).getTypeName())
          .toList();
    }
  }
}
