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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The documented option contract of {@code vector.neighbors}: the bounds of {@code rerank}, {@code groupSize}, {@code maxDistance}
 * and {@code k}, what a null argument says, the arity, the closed upper bound of {@code maxDistance}, and the index-name forms. Each
 * assertion pins a boundary an oracle can state without reading the implementation (the Javadoc of the function and its option
 * error messages).
 */
class VectorNeighborsOptionContractTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
    database.transaction(() -> {
      database.newVertex("Doc").set("name", "a", "vector", new float[] { 1f, 0f }).save();
      database.newVertex("Doc").set("name", "b", "vector", new float[] { 0.8f, 0.6f }).save();
      database.newVertex("Doc").set("name", "c", "vector", new float[] { 0f, 1f }).save();
    });
  }

  private List<String> names(final String call, final Object... args) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(" + call + "))", args)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  private List<Float> distances(final String call, final Object... args) {
    final List<Float> distances = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT distance FROM (SELECT expand(" + call + "))", args)) {
      rs.forEachRemaining(r -> distances.add(r.<Number>getProperty("distance").floatValue()));
    }
    return distances;
  }

  @Test
  void aNegativeRerankIsRefusedAndZeroIsAccepted() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 3, { rerank: -1 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank must be >= 0").hasMessageContaining("-1");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, { rerank: 0 })", (Object) query)).containsExactly("a", "b", "c");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, { rerank: 1 })", (Object) query)).containsExactly("a", "b", "c");
  }

  @Test
  void aGroupSizeBelowOneIsRefusedAndOneIsAccepted() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 3, { groupBy: 'name', groupSize: 0 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("groupSize must be >= 1").hasMessageContaining("0");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 3, { groupBy: 'name', groupSize: -3 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("groupSize must be >= 1").hasMessageContaining("-3");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, { groupBy: 'name', groupSize: 1 })", (Object) query))
        .containsExactly("a", "b", "c");
  }

  @Test
  void aNaNMaxDistanceIsRefused() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 3, ?)", query, Map.of("maxDistance", Double.NaN)))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("maxDistance must be a number");
  }

  @Test
  void maxDistanceIsAClosedBound() {
    final float[] query = { 1f, 0f };
    final List<Float> all = distances("vector.neighbors('Doc[vector]', ?, 3)", (Object) query);
    assertThat(all).hasSize(3).isSorted();
    final float second = all.get(1);

    // a record at exactly maxDistance is in range ("drop neighbors whose distance is strictly greater than maxDistance")
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, ?)", query, Map.of("maxDistance", (double) second))).containsExactly("a", "b");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, ?)", query, Map.of("maxDistance", (double) Math.nextDown(second))))
        .containsExactly("a");
    // the closest record is at distance 0 for an identical vector: the bound 0 keeps it
    assertThat(all.getFirst()).isEqualTo(0f);
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, { maxDistance: 0 })", (Object) query)).containsExactly("a");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 3, { maxDistance: -0.001 })", (Object) query)).isEmpty();
  }

  @Test
  void aNullIndexNameOrKeyIsRefusedWithAParsingError() {
    assertThatThrownBy(() -> names("vector.neighbors(?, [1.0, 0.0], 3)", (Object) null)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("index name is null");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 3)", (Object) null)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("key is null");
  }

  @Test
  void theArityIsThreeOrFourArguments() {
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', [1.0, 0.0])")).hasMessageContaining("vector.neighbors");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', [1.0, 0.0], 3, 10, 5)")).hasMessageContaining("vector.neighbors");
    assertThat(names("vector.neighbors('Doc[vector]', [1.0, 0.0], 3)")).hasSize(3);
    assertThat(names("vector.neighbors('Doc[vector]', [1.0, 0.0], 3, 10)")).hasSize(3);
  }

  @Test
  void aListAndAnArrayQueryVectorAnswerTheSame() {
    final List<String> fromList = names("vector.neighbors('Doc[vector]', ?, 3)", List.of(1.0, 0.0));
    final List<String> fromArray = names("vector.neighbors('Doc[vector]', ?, 3)", (Object) new float[] { 1f, 0f });
    assertThat(fromList).containsExactly("a", "b", "c").isEqualTo(fromArray);
  }

  @Test
  void aSpecWithoutATypeBeforeTheBracketIsAnIndexNameNotATypeProperty() {
    assertThatThrownBy(() -> names("vector.neighbors('[vector]', [1.0, 0.0], 3)")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Index with name '[vector]' was not found");
  }

  @Test
  void anUnknownIndexNameIsRefusedAndNamesTheIndex() {
    assertThatThrownBy(() -> names("vector.neighbors('nope', [1.0, 0.0], 3)")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Index with name 'nope' was not found");
  }

  @Test
  void aTypeWithoutAVectorIndexOnThePropertyIsRefused() {
    assertThatThrownBy(() -> names("vector.neighbors('Doc[name]', [1.0, 0.0], 3)")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("No vector index found on property 'name' for type 'Doc'");
  }

  @Test
  void aKOfZeroAnswersNoRowsWithoutTouchingTheIndex() {
    assertThat(names("vector.neighbors('Doc[vector]', [1.0, 0.0], 0)")).isEmpty();
    // not even a spec that does not resolve is looked at: the empty request is answered first
    assertThat(names("vector.neighbors('nope', [1.0, 0.0], 0)")).isEmpty();
  }

  @Test
  void aGroupedFetchBudgetOfExactlyTheCapIsAcceptedAndOneMoreIsRefused() {
    final float[] query = { 1f, 0f };
    // the pool asked of the index is groups * groupSize * 5 candidates, capped at MAX_FETCH_CANDIDATES (100,000): 20,000 is the last
    // product that fits
    assertThat(VectorUtils.MAX_FETCH_CANDIDATES).isEqualTo(100_000);
    assertThat(names("vector.neighbors('Doc[vector]', ?, 20000, { groupBy: 'name', groupSize: 1 })", (Object) query))
        .containsExactly("a", "b", "c");
    assertThat(names("vector.neighbors('Doc[vector]', ?, 100, { groupBy: 'name', groupSize: 200 })", (Object) query))
        .containsExactly("a", "b", "c");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 20001, { groupBy: 'name', groupSize: 1 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("over-fetch budget exceeded");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, 1, { groupBy: 'name', groupSize: 20001 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("over-fetch budget exceeded");
  }
}
