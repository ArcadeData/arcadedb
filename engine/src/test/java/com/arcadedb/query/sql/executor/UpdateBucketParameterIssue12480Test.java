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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12480: an {@code UPDATE} whose target is a bucket named by an input parameter
 * ({@code UPDATE bucket::name}, {@code UPDATE bucket:?}) failed with a {@code NullPointerException} while it was planned. The read
 * side ({@code SELECT FROM bucket::name}) resolved the parameter, the write side read the bucket number of a target that has none.
 * The statement fails before any row is read, so it failed with or without {@code UPSERT}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class UpdateBucketParameterIssue12480Test extends TestHelper {

  /** A vertex type of two buckets, a row in each: Rome in the first, Paris in the second. */
  private List<Bucket> cityWithTwoBuckets() {
    final VertexType type = database.getSchema().createVertexType("City", 2);
    type.createProperty("name", Type.STRING);
    final List<Bucket> buckets = type.getBuckets(false);
    assertThat(buckets).hasSize(2);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO bucket:" + buckets.get(0).getName() + " SET name = 'Rome'");
      database.command("sql", "INSERT INTO bucket:" + buckets.get(1).getName() + " SET name = 'Paris'");
    });
    assertThat(visits(1)).isZero();
    return buckets;
  }

  @Test
  void updateOfABucketGivenAsANamedParameterUpdatesOnlyThatBucket() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    final long updated = update("UPDATE bucket::name SET visits = 1", Map.of("name", buckets.get(0).getName()));

    assertThat(updated).isEqualTo(1L);
    assertThat(visits(1)).isEqualTo(1L);
    assertThat(visitedCities()).containsExactly("Rome");
  }

  @Test
  void updateOfABucketGivenAsAPositionalParameterUpdatesOnlyThatBucket() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    final long[] updated = new long[1];
    database.transaction(() -> updated[0] = count(
        database.command("sql", "UPDATE bucket:? SET visits = 1 WHERE name = ?", buckets.get(1).getName(), "Paris")));

    assertThat(updated[0]).isEqualTo(1L);
    assertThat(visitedCities()).containsExactly("Paris");
  }

  @Test
  void theWhereClauseStillFiltersWithinTheBucketGivenAsAParameter() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    // Paris lives in the second bucket: nothing of the first one matches
    final long updated = update("UPDATE bucket::name SET visits = 1 WHERE name = 'Paris'", Map.of("name", buckets.get(0).getName()));

    assertThat(updated).isZero();
    assertThat(visits(1)).isZero();
  }

  @Test
  void aSecondRunWithAnotherBucketDoesNotReuseTheFirstOne() {
    final List<Bucket> buckets = cityWithTwoBuckets();
    final String statement = "UPDATE bucket::name SET visits = visits + 1";
    database.transaction(() -> database.command("sql", "UPDATE City SET visits = 0"));

    // the same statement text, a different bucket each time: a plan that kept the first bucket would update Rome four times
    assertThat(update(statement, Map.of("name", buckets.get(0).getName()))).isEqualTo(1L);
    assertThat(update(statement, Map.of("name", buckets.get(1).getName()))).isEqualTo(1L);
    assertThat(update(statement, Map.of("name", buckets.get(1).getName()))).isEqualTo(1L);
    assertThat(update(statement, Map.of("name", buckets.get(0).getName()))).isEqualTo(1L);

    assertThat(visitsOf("Rome")).isEqualTo(2);
    assertThat(visitsOf("Paris")).isEqualTo(2);
  }

  /**
   * The read side of the same statement: its plan is cached under the text of the statement, so a plan that kept the bucket of the
   * first execution would answer the rows of that bucket to every later one, and the UPDATE built on it would read one bucket and
   * save into another.
   */
  @Test
  void aSelectOfABucketGivenAsAParameterReadsTheBucketOfEachRun() {
    final List<Bucket> buckets = cityWithTwoBuckets();
    final String statement = "SELECT name FROM bucket::name";

    for (int i = 0; i < 3; i++) {
      assertThat(namesIn(statement, Map.of("name", buckets.get(0).getName()))).containsExactly("Rome");
      assertThat(namesIn(statement, Map.of("name", buckets.get(1).getName()))).containsExactly("Paris");
    }
  }

  @Test
  void aSelectOfABucketGivenAsAPositionalParameterReadsTheBucketOfEachRun() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    for (int i = 0; i < 3; i++) {
      assertThat(database.query("sql", "SELECT name FROM bucket:?", buckets.get(0).getName()).stream()
          .map(r -> r.<String>getProperty("name")).toList()).containsExactly("Rome");
      assertThat(database.query("sql", "SELECT name FROM bucket:?", buckets.get(1).getName()).stream()
          .map(r -> r.<String>getProperty("name")).toList()).containsExactly("Paris");
    }
  }

  @Test
  void aTraverseFromABucketGivenAsAParameterWalksTheBucketOfEachRun() {
    final List<Bucket> buckets = cityWithTwoBuckets();
    final String statement = "TRAVERSE out() FROM bucket::name MAXDEPTH 1";

    for (int i = 0; i < 2; i++) {
      assertThat(namesIn(statement, Map.of("name", buckets.get(0).getName()))).containsExactly("Rome");
      assertThat(namesIn(statement, Map.of("name", buckets.get(1).getName()))).containsExactly("Paris");
    }
  }

  @Test
  void deleteFromABucketGivenAsAParameterDeletesOnlyTheBucketOfEachRun() {
    final List<Bucket> buckets = cityWithTwoBuckets();
    final String statement = "DELETE FROM bucket::name";

    // the same text twice, a different bucket each time: the second run must not delete from the first bucket again
    assertThat(update(statement, Map.of("name", buckets.get(1).getName()))).isEqualTo(1L);
    assertThat(update(statement, Map.of("name", buckets.get(0).getName()))).isEqualTo(1L);

    assertThat(((Number) database.query("sql", "SELECT count(@rid) AS n FROM City").next().getProperty("n")).longValue()).isZero();
  }

  @Test
  void aBucketGivenAsANameAnIdOrAParameterUpdatesTheSameRows() {
    final List<Bucket> buckets = cityWithTwoBuckets();
    final Bucket second = buckets.get(1);

    assertThat(update("UPDATE bucket:" + second.getName() + " SET visits = 1", null)).isEqualTo(1L);
    assertThat(update("UPDATE bucket:" + second.getFileId() + " SET visits = 2", null)).isEqualTo(1L);
    assertThat(update("UPDATE bucket::name SET visits = 3", Map.of("name", second.getName()))).isEqualTo(1L);

    assertThat(visitsOf("Paris")).isEqualTo(3);
    assertThat(visitedCities()).containsExactly("Paris");
  }

  @Test
  void updateReturnAfterOfABucketGivenAsAParameterReturnsTheUpdatedRecords() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> database.command("sql", "UPDATE bucket::name SET visits = 7 RETURN AFTER",
        Map.of("name", buckets.get(0).getName())).forEachRemaining(rows::add));

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("Rome");
    assertThat(rows.getFirst().<Integer>getProperty("visits")).isEqualTo(7);
  }

  @Test
  void anUnknownBucketGivenAsAParameterIsRefusedAsAMissingBucket() {
    cityWithTwoBuckets();

    assertThatThrownBy(() -> update("UPDATE bucket::name SET visits = 1", Map.of("name", "NoSuchBucket"))).isInstanceOf(
        SchemaException.class).hasMessageContaining("does not exist");

    assertThat(visits(1)).isZero();
  }

  @Test
  void aNullBucketParameterIsRefusedAsAMissingBucket() {
    cityWithTwoBuckets();
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("name", null);

    assertThatThrownBy(() -> update("UPDATE bucket::name SET visits = 1", parameters)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("does not exist");

    assertThat(visits(1)).isZero();
  }

  @Test
  void explainOfAnUpdateOfABucketGivenAsAParameterIsPlanned() {
    final List<Bucket> buckets = cityWithTwoBuckets();

    final Result plan = database.command("sql", "EXPLAIN UPDATE bucket::name SET visits = 1",
        Map.of("name", buckets.get(0).getName())).next();

    assertThat(plan.<String>getProperty("executionPlanAsString")).contains(buckets.get(0).getName());
  }

  @Test
  void upsertOnABucketGivenAsAParameterCreatesTheRecordInThatBucketThenFindsIt() {
    final VertexType type = database.getSchema().createVertexType("City", 2);
    type.createProperty("name", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    final Bucket second = type.getBuckets(false).get(1);

    // once to create the vertex, once more to find it: both runs resolve the bucket from the parameter
    for (int i = 0; i < 2; i++)
      assertThat(update("UPDATE bucket::name SET visits = 1 UPSERT WHERE name = 'Rome'", Map.of("name", second.getName()))).isEqualTo(
          1L);

    assertThat(((Number) database.query("sql", "SELECT count(@rid) AS n FROM City").next().getProperty("n")).longValue()).isEqualTo(1L);
    final RID rid = database.query("sql", "SELECT FROM City WHERE name = 'Rome'").next().getIdentity().orElseThrow();
    assertThat(database.getSchema().getBucketById(rid.getBucketId()).getName()).isEqualTo(second.getName());
  }

  /** Runs the statement in its own transaction and answers the number of records it updated. */
  private long update(final String statement, final Map<String, Object> parameters) {
    final long[] updated = new long[1];
    database.transaction(() -> updated[0] = count(
        parameters == null ? database.command("sql", statement) : database.command("sql", statement, parameters)));
    return updated[0];
  }

  private static long count(final ResultSet resultSet) {
    return ((Number) resultSet.next().getProperty("count")).longValue();
  }

  private List<String> namesIn(final String statement, final Map<String, Object> parameters) {
    return database.query("sql", statement, parameters).stream().map(r -> r.<String>getProperty("name")).toList();
  }

  private long visits(final int value) {
    return ((Number) database.query("sql", "SELECT count(@rid) AS n FROM City WHERE visits = ?", value).next()
        .getProperty("n")).longValue();
  }

  private int visitsOf(final String name) {
    return database.query("sql", "SELECT visits FROM City WHERE name = ?", name).next().<Number>getProperty("visits").intValue();
  }

  /** The names of the cities that have been given a number of visits, whatever it is. */
  private List<String> visitedCities() {
    return database.query("sql", "SELECT name FROM City WHERE visits IS NOT NULL").stream().map(r -> r.<String>getProperty("name"))
        .toList();
  }
}
