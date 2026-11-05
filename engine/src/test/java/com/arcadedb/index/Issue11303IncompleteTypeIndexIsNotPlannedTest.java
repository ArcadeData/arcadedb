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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11303: {@link TypeIndex#isReadyForQueries()}, the predicate every planner asks, did not ask {@link TypeIndex#isComplete()},
 * so an index missing the bucket index of one of the buckets it covers was planned on and a lookup through it answered short, while
 * {@link TypeIndex#isValid()} and the load-time SEVERE line both said it was not used. And {@code DROP INDEX} of a bucket index by its
 * own name was the statement that left a type index in that state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11303IncompleteTypeIndexIsNotPlannedTest extends TestHelper {
  private static final String INDEX = "S[k]";

  @Test
  void anIncompleteIndexIsNotPlannedAndTheLookupAnswersWhole() {
    final Bucket subBucket = prepare();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(index.isReadyForQueries()).isTrue();
    assertThat(explain("SELECT FROM S WHERE k = 'a1'")).contains("FETCH FROM INDEX");

    // the Java API still drops a bucket index by name: it is how the repair tests build the state an earlier version left behind
    database.getSchema().dropIndex(bucketIndexOf(index, subBucket).getName());

    assertThat(index.isValid()).isFalse();
    assertThat(index.isComplete()).isFalse();
    assertThat(index.getBucketsWithoutIndex()).containsExactly(subBucket);
    assertThat(index.isReadyForQueries()).as("an index missing a bucket's sub-index is no planning candidate").isFalse();
    assertThat(TypeIndex.filterReadyForQueries(database.getSchema().getType("S").getAllIndexes(true))).doesNotContain(index);

    // SQL: the plan and the answer, which together fail on the code before the fix
    assertThat(explain("SELECT FROM S WHERE k = 'a1'")).doesNotContain("FETCH FROM INDEX");
    assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 'a1'")).isEqualTo(1L);
    assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 's1'")).isEqualTo(1L);
    // the range and IN shapes go through the same candidate list
    assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k >= 'a' AND k < 'b'")).isEqualTo(1L);
    assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k IN ['a1', 's1']")).isEqualTo(2L);
    // openCypher
    assertThat(count("opencypher", "MATCH (n:S) WHERE n.k = 'a1' RETURN count(n) AS c")).isEqualTo(1L);
    assertThat(count("opencypher", "MATCH (n:S {k: 'a1'}) RETURN count(n) AS c")).isEqualTo(1L);
    // the native select API
    assertThat(database.select().fromType("S").where().property("k").eq().value("a1").documents().stream().count()).isEqualTo(1L);

    // SHOW INDEXES does not call it ONLINE, nor POPULATING: nothing is filling it
    assertThat(indexState()).isEqualTo("FAILED");

    // the lookup that has no scan to fall back on refuses, and says why, instead of answering short or "no index"
    final Throwable refused = catchThrowable(() -> database.lookupByKey("S", "k", "a1"));
    assertThat(refused).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(INDEX).hasMessageContaining("REBUILD INDEX");

    // REBUILD INDEX repairs it, and the planner uses it again
    database.command("sql", "REBUILD INDEX `" + INDEX + "`");
    final TypeIndex repaired = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    assertThat(repaired.isReadyForQueries()).isTrue();
    assertThat(explain("SELECT FROM S WHERE k = 'a1'")).contains("FETCH FROM INDEX");
    assertThat(count("sql", "SELECT count(*) AS c FROM S WHERE k = 'a1'")).isEqualTo(1L);
    assertThat(database.lookupByKey("S", "k", "a1").hasNext()).isTrue();
    assertThat(indexState()).isEqualTo("ONLINE");
  }

  private String indexState() {
    try (final ResultSet rs = database.query("opencypher", "SHOW INDEXES")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (INDEX.equals(row.getProperty("name")))
          return row.getProperty("state");
      }
    }
    throw new IllegalStateException("SHOW INDEXES does not list " + INDEX);
  }

  @Test
  void dropIndexOfABucketIndexByNameIsRefusedInSqlAndCypher() {
    final Bucket subBucket = prepare();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final String bucketIndexName = bucketIndexOf(index, subBucket).getName();

    for (final String[] statement : new String[][] { { "sql", "DROP INDEX `" + bucketIndexName + "`" },
        { "sql", "DROP INDEX `" + bucketIndexName + "` IF EXISTS" }, { "opencypher", "DROP INDEX `" + bucketIndexName + "`" } }) {
      final Throwable refused = catchThrowable(() -> database.command(statement[0], statement[1]));
      assertThat(refused).as(statement[1]).isNotNull();
      assertThat(CauseChain.find(refused, SchemaException.class)).as("got %s", refused).isNotNull();
      assertThat(CauseChain.find(refused, SchemaException.class).getMessage()).contains(bucketIndexName).contains(INDEX);

      // nothing changed: the index is whole, planned on, and still refuses a duplicate in the sub type's bucket
      assertThat(database.getSchema().existsIndex(bucketIndexName)).isTrue();
      assertThat(index.isComplete()).isTrue();
      assertThat(index.isReadyForQueries()).isTrue();
    }

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO A SET k = 'a1'")))).isInstanceOf(
        DuplicatedKeyException.class);

    // the type index itself still drops, with every bucket index it holds
    database.command("sql", "DROP INDEX `" + INDEX + "`");
    assertThat(database.getSchema().existsIndex(INDEX)).isFalse();
    assertThat(database.getSchema().existsIndex(bucketIndexName)).isFalse();
  }

  private Bucket prepare() {
    database.command("sql", "CREATE VERTEX TYPE S BUCKETS 1");
    database.command("sql", "CREATE PROPERTY S.k STRING");
    database.command("sql", "CREATE INDEX ON S (k) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE A EXTENDS S BUCKETS 1");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO S SET k = 's1'");
      database.command("sql", "INSERT INTO A SET k = 'a1'");
    });
    assertThat(database.getSchema().getIndexByName(INDEX).getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    return database.getSchema().getType("A").getBuckets(false).getFirst();
  }

  private static IndexInternal bucketIndexOf(final TypeIndex index, final Bucket bucket) {
    for (final IndexInternal onBucket : index.getIndexesOnBuckets())
      if (onBucket.getAssociatedBucketId() == bucket.getFileId())
        return onBucket;
    throw new IllegalStateException("No bucket index for bucket " + bucket.getName());
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private long count(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
