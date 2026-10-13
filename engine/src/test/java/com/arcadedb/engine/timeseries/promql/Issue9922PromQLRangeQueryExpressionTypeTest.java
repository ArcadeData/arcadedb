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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9922: a range query keeps only the instant vectors and scalars its steps evaluate to, so a top-level range vector
 * ({@code m[5m]}) or string ({@code "a"}) fell through and answered an empty matrix. Prometheus refuses both with
 * {@code invalid expression type "range vector" for range query, must be Scalar or instant Vector}.
 */
class Issue9922PromQLRangeQueryExpressionTypeTest extends TestHelper {

  private static final String RANGE_VECTOR = "invalid expression type \"range vector\" for range query, must be Scalar or instant Vector";
  private static final String STRING       = "invalid expression type \"string\" for range query, must be Scalar or instant Vector";

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m9922 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO m9922 SET ts = 1000, host = 'h1', value = 1.0");
      database.command("sql", "INSERT INTO m9922 SET ts = 3000, host = 'h1', value = 3.0");
    });
  }

  @Test
  void aTopLevelRangeVectorIsRefused() {
    assertRefused("m9922[5m]", RANGE_VECTOR);
    assertRefused("m9922[5m] offset 1m", RANGE_VECTOR);
    assertRefused("(m9922[5m])", RANGE_VECTOR);
    // no series at all is still a range vector, and must not answer an empty matrix either
    assertRefused("missing_9922[5m]", RANGE_VECTOR);
  }

  @Test
  void aTopLevelStringIsRefused() {
    assertRefused("\"a\"", STRING);
    assertRefused("'a'", STRING);
    assertRefused("(\"a\")", STRING);
  }

  @Test
  void anInstantQueryStillAcceptsBoth() {
    // Prometheus answers an instant query over a range vector or a string, only the range query refuses them
    final PromQLEvaluator evaluator = new PromQLEvaluator(db());
    assertThat(evaluator.evaluateInstant(new PromQLParser("m9922[5m]").parse(), 5000L)).isInstanceOf(RangeVector.class);
    assertThat(evaluator.evaluateInstant(new PromQLParser("\"a\"").parse(), 5000L)).isInstanceOf(ScalarResult.class);
    // and a missing metric is an (empty) instant vector, which is why its range query above answers an empty matrix
    assertThat(evaluator.evaluateInstant(new PromQLParser("missing_9922").parse(), 5000L)).isInstanceOf(InstantVector.class);
  }

  @Test
  void validRangeQueriesStillWork() {
    final MatrixResult vector = range("m9922");
    assertThat(vector.series()).hasSize(1);
    assertThat(vector.series().getFirst().values()).isNotEmpty();

    final MatrixResult scalar = range("1 + 1");
    assertThat(scalar.series()).hasSize(1);
    assertThat(scalar.series().getFirst().values()).hasSize(3);
    assertThat(scalar.series().getFirst().values().getFirst()[1]).isEqualTo(2.0);

    assertThat(range("sum_over_time(m9922[5m])").series()).hasSize(1);
    // an instant vector with no series is still an instant vector: an empty matrix, not a refusal
    assertThat(range("missing_9922").series()).isEmpty();
  }

  private void assertRefused(final String promql, final String message) {
    assertThatThrownBy(() -> range(promql)).as(promql).isInstanceOf(IllegalArgumentException.class).hasMessage(message);
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }

  private MatrixResult range(final String promql) {
    return (MatrixResult) new PromQLEvaluator(db()).evaluateRange(new PromQLParser(promql).parse(), 1000L, 5000L, 2000L);
  }
}
