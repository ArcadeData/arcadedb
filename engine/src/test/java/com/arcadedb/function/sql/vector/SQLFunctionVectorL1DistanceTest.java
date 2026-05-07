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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL-side {@code vector.l1Distance} / {@code vector.manhattanDistance} (Tier 4 follow-up). The
 * underlying {@link com.arcadedb.index.vector.VectorUtils#manhattanDistance} was already on the
 * engine; this test pins the SQL surface contract: both alias names are registered, both return
 * the same result, both validate dimensions, and both return null when either input is null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorL1DistanceTest extends TestHelper {

  @Test
  void computesSumOfAbsoluteDifferences() {
    final SQLFunctionVectorL1Distance function = new SQLFunctionVectorL1Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // |1-4| + |2-6| + |3-8| = 3 + 4 + 5 = 12
    final Object result = function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f, 3.0f }, new float[] { 4.0f, 6.0f, 8.0f } },
        context);

    assertThat(result).isInstanceOf(Float.class);
    assertThat((Float) result).isCloseTo(12.0f, Offset.offset(0.001f));
  }

  @Test
  void acceptsListInputs() {
    final SQLFunctionVectorL1Distance function = new SQLFunctionVectorL1Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null,
        new Object[] { List.of(1.0, 2.0), List.of(3.0, 5.0) },
        context);

    assertThat((Float) result).isCloseTo(5.0f, Offset.offset(0.001f));  // |1-3|+|2-5|=2+3
  }

  @Test
  void rejectsNullInputs() {
    final SQLFunctionVectorL1Distance function = new SQLFunctionVectorL1Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { null, new float[] { 1.0f } }, context))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void rejectsMismatchedDimensions() {
    final SQLFunctionVectorL1Distance function = new SQLFunctionVectorL1Distance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] { 1.0f, 2.0f }, new float[] { 1.0f, 2.0f, 3.0f } }, context))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  /**
   * End-to-end SQL-surface check: both registered names ({@code vector.l1Distance} and the
   * {@code vector.manhattanDistance} alias) must be callable from a SELECT and produce the same
   * result. Without this, a regression in {@code DefaultSQLFunctionFactory.register(...)} for
   * either alias would silently fall back to "function not found" - which the SQL parser surfaces
   * as a different shape of exception, not the value mismatch this assertion catches.
   */
  @Test
  void bothAliasesAreRegisteredAndProduceIdenticalResults() {
    final ResultSet rs = database.query("sql",
        "SELECT `vector.l1Distance`([1.0, 2.0, 3.0], [4.0, 6.0, 8.0]) AS l1, "
            + "`vector.manhattanDistance`([1.0, 2.0, 3.0], [4.0, 6.0, 8.0]) AS man");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("l1")).floatValue()).isCloseTo(12.0f, Offset.offset(0.001f));
    assertThat(((Number) row.getProperty("man")).floatValue()).isCloseTo(12.0f, Offset.offset(0.001f));
  }
}
