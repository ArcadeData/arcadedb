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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * https://github.com/ArcadeData/arcadedb/issues/6817
 * <p>
 * {@code Select.json(JSONObject)} mapped the serialized bucket NAMES back to {@code Bucket} objects and then poured
 * that {@code List<Bucket>} into a {@code String[]}. That compiles - {@code <T> T[] toArray(T[])}'s type variable is
 * independent of the element type - and throws {@link ArrayStoreException} at runtime for every non-empty bucket list,
 * so a {@code fromBuckets} select could never be round-tripped at all. {@code fromBuckets(String...)} resolves the
 * names itself, so the mapping step was pointless as well as fatal.
 * <p>
 * The same method also silently dropped state that {@code SelectCompiled.json()} writes ({@code timeoutInMs} /
 * {@code exceptionOnTimeout}) and never covered {@code orderBy} or {@code parallel} on either side, so the two
 * {@code json} methods were not inverse. {@code SelectExecutionTest.okJSON()} exercises only {@code fromType}, which
 * is why none of this showed up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6817SelectJsonRoundTripTest extends TestHelper {

  public Issue6817SelectJsonRoundTripTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V")//
        .createProperty("name", Type.STRING);
    database.getSchema().getType("V").createProperty("id", Type.INTEGER);

    // THE DEFAULT IS ONE BUCKET PER TYPE (GlobalConfiguration.TYPE_DEFAULT_BUCKETS), SO THE MULTI-BUCKET ROUND TRIP
    // NEEDS A TYPE THAT EXPLICITLY ASKS FOR MORE
    database.getSchema().createVertexType("Multi", 3).createProperty("name", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.newVertex("V").set("id", i, "name", "John").save();
        database.newVertex("Multi").set("id", i, "name", "John").save();
      }
    });
  }

  /**
   * The issue's repro: this used to fail with {@code java.lang.ArrayStoreException: com.arcadedb.engine.LocalBucket}.
   */
  @Test
  void fromBucketsRoundTripsInsteadOfThrowing() {
    final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

    final JSONObject json = database.select().fromBuckets(bucket)//
        .where().property("name").eq().value("John").compile().json();

    assertThatNoException().isThrownBy(() -> database.select().json(json));

    final JSONObject roundTripped = database.select().json(json).compile().json();
    assertThat(roundTripped).isEqualTo(json);
  }

  /**
   * A rebuilt select must also still run, and return what the original one returned.
   */
  @Test
  void fromBucketsRoundTrippedSelectStillExecutes() {
    final String bucket = database.getSchema().getType("V").getBuckets(false).getFirst().getName();

    final JSONObject json = database.select().fromBuckets(bucket)//
        .where().property("name").eq().value("John").compile().json();

    final List<Vertex> expected = database.select().fromBuckets(bucket)//
        .where().property("name").eq().value("John").vertices().toList();

    final List<Vertex> got = database.select().json(json).vertices().toList();
    assertThat(got).hasSameSizeAs(expected);
  }

  /**
   * Several buckets at once: the throwing {@code toArray} path only ever ran with {@code a.length == size}, which is
   * exactly what the old code produced, so a multi-bucket list was just as fatal as a single one.
   */
  @Test
  void multipleBucketsRoundTrip() {
    final List<String> buckets = database.getSchema().getType("Multi").getBuckets(false).stream()//
        .map(com.arcadedb.engine.Bucket::getName).toList();
    assertThat(buckets.size()).isGreaterThan(1);

    final JSONObject json = database.select().fromBuckets(buckets.toArray(new String[0]))//
        .where().property("name").eq().value("John").compile().json();

    assertThat(database.select().json(json).compile().json()).isEqualTo(json);
  }

  /**
   * {@code timeoutInMs}/{@code exceptionOnTimeout} were written by {@code SelectCompiled.json()} and never read back,
   * so a round-tripped select ran with no deadline at all.
   */
  @Test
  void timeoutSurvivesTheRoundTrip() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John")//
        .timeout(1234, TimeUnit.MILLISECONDS, true).compile().json();

    assertThat(json.getLong("timeoutInMs")).isEqualTo(1234L);
    assertThat(json.getBoolean("exceptionOnTimeout")).isTrue();

    final Select rebuilt = database.select().json(json);
    assertThat(rebuilt.timeoutInMs).isEqualTo(1234L);
    assertThat(rebuilt.exceptionOnTimeout).isTrue();
    assertThat(rebuilt.compile().json()).isEqualTo(json);
  }

  @Test
  void nonThrowingTimeoutSurvivesTheRoundTrip() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John")//
        .timeout(50, TimeUnit.MILLISECONDS, false).compile().json();

    final Select rebuilt = database.select().json(json);
    assertThat(rebuilt.timeoutInMs).isEqualTo(50L);
    assertThat(rebuilt.exceptionOnTimeout).isFalse();
    assertThat(rebuilt.compile().json()).isEqualTo(json);
  }

  /**
   * {@code orderBy} was neither written nor read, so a round-tripped select lost its ordering entirely.
   */
  @Test
  void orderBySurvivesTheRoundTrip() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John")//
        .orderBy("id", false).compile().json();

    final Select rebuilt = database.select().json(json);
    assertThat(rebuilt.orderBy).hasSize(1);
    assertThat(rebuilt.orderBy.getFirst().getFirst()).isEqualTo("id");
    assertThat(rebuilt.orderBy.getFirst().getSecond()).isFalse();
    assertThat(rebuilt.compile().json()).isEqualTo(json);

    final List<Vertex> got = database.select().json(json).vertices().toList();
    assertThat(got.stream().map(v -> v.getInteger("id")).toList()).containsExactly(9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  }

  @Test
  void multipleOrderBySurvivesTheRoundTrip() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John")//
        .orderBy("name", true).orderBy("id", false).compile().json();

    final Select rebuilt = database.select().json(json);
    assertThat(rebuilt.orderBy).hasSize(2);
    assertThat(rebuilt.orderBy.getFirst().getFirst()).isEqualTo("name");
    assertThat(rebuilt.orderBy.getFirst().getSecond()).isTrue();
    assertThat(rebuilt.orderBy.getLast().getFirst()).isEqualTo("id");
    assertThat(rebuilt.orderBy.getLast().getSecond()).isFalse();
    assertThat(rebuilt.compile().json()).isEqualTo(json);
  }

  /**
   * {@code parallel} lives on {@code SelectCompiled} but is stored on the {@code Select}, and was dropped too.
   */
  @Test
  void parallelSurvivesTheRoundTrip() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John").compile().parallel().json();

    assertThat(json.getBoolean("parallel")).isTrue();

    final Select rebuilt = database.select().json(json);
    assertThat(rebuilt.parallel).isTrue();
    assertThat(rebuilt.compile().json()).isEqualTo(json);
  }

  /**
   * A select with none of the optional state must not gain keys for it - otherwise every existing round trip
   * (including {@code SelectExecutionTest.okJSON()}) would start comparing unequal.
   */
  @Test
  void defaultsAreNotSerialized() {
    final JSONObject json = database.select().fromType("V")//
        .where().property("name").eq().value("John").compile().json();

    assertThat(json.has("orderBy")).isFalse();
    assertThat(json.has("parallel")).isFalse();
    assertThat(json.has("timeoutInMs")).isFalse();
    assertThat(json.has("exceptionOnTimeout")).isFalse();

    assertThat(database.select().json(json).compile().json()).isEqualTo(json);
  }
}
