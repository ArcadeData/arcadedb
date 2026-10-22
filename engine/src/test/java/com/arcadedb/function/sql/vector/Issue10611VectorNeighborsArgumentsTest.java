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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10611: two {@code vector.neighbors} arguments that make no sense were accepted silently. {@code rerank: 2.5} was cut
 * to 2 by {@code intValue()}, where the index METADATA refuses the same value for {@code rerankOversample}; and a negative
 * {@code k} answered no rows, which reads as "no neighbours" when it is a caller mistake. Both are refused now, and so is the
 * same negative {@code k} on the sibling functions that read it the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10611VectorNeighborsArgumentsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"COSINE\" }");
    database.transaction(() -> {
      database.newDocument("Doc").set("name", "a", "vector", new float[] { 0.9f, 0.1f }).save();
      database.newDocument("Doc").set("name", "b", "vector", new float[] { 0.1f, 0.9f }).save();
      database.newDocument("Doc").set("name", "c", "vector", new float[] { 0.8f, 0.2f }).save();
    });
  }

  private List<String> names(final String sql, final Object... args) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM (SELECT expand(" + sql + "))", args)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  @Test
  void aFractionalRerankIsRefusedNotTruncated() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, { rerank: 2.5 })", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", 2.5d)))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", "2.5")))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("rerank").hasMessageContaining("must be an integer");
  }

  @Test
  void theOtherIntegerOptionsAreRefusedWhenFractionalToo() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("efSearch", 10.5d)))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("efSearch").hasMessageContaining("must be an integer");
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("groupBy", "name", "groupSize", 1.5d)))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("groupSize").hasMessageContaining("must be an integer");
  }

  @Test
  void aWholeNumberWrittenAsADecimalStillWorks() {
    final float[] query = { 1f, 0f };
    assertThat(names("vectorNeighbors('Doc[vector]', ?, 3, ?)", query, Map.of("rerank", 2.0d, "efSearch", 20.0d))).hasSize(3);
    assertThat(names("vectorNeighbors('Doc[vector]', ?, 3, { rerank: 2 })", (Object) query)).hasSize(3);
    assertThat(names("vectorNeighbors('Doc[vector]', ?, 3.0)", (Object) query)).hasSize(3);
  }

  @Test
  void aNegativeKIsRefusedAndNamesTheParameter() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, -1)", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative").hasMessageContaining("-1");
    assertThatThrownBy(() -> names("vector.neighbors('Doc[vector]', ?, ?)", query, -5))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative").hasMessageContaining("-5");
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, '-2')", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
  }

  @Test
  void aFractionalKIsRefused() {
    final float[] query = { 1f, 0f };
    assertThatThrownBy(() -> names("vectorNeighbors('Doc[vector]', ?, 2.5)", (Object) query))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must be an integer").hasMessageContaining("2.5");
  }

  @Test
  void aZeroKIsAValidEmptyRequest() {
    final float[] query = { 1f, 0f };
    assertThat(names("vectorNeighbors('Doc[vector]', ?, 0)", (Object) query)).isEmpty();
    assertThat(names("vectorNeighbors('Doc[vector]', ?, 1)", (Object) query)).hasSize(1);
  }

  @Test
  void theSiblingFunctionsRefuseANegativeKToo() {
    assertThatThrownBy(() -> names("`vector.sparseNeighbors`('Doc[vector]', [0, 1], [1.0, 1.0], -1)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
    assertThatThrownBy(() -> names("`vector.discover`('Doc[vector]', [[#1:0, #1:1]], -1)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
    assertThatThrownBy(() -> names("`vector.recommend`('Doc[vector]', [#1:0], [], -1)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
    assertThatThrownBy(() -> names("`vector.rerank`((SELECT FROM Doc), [1.0, 0.0], 'vector', -1)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("k must not be negative");
  }
}
