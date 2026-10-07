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
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #9330 and #9363, on the PromQL evaluator:
 * <ul>
 * <li>#9330: a unary minus and every value-transforming function must delete {@code __name__} from the result labels,
 * as Prometheus does, so a derived series is not labelled with the name of the raw one.</li>
 * <li>#9363: a label whose value is the empty string is the same as an absent label, so it is not reported and
 * {@code {host=""}} selects it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9330PromQLDropMetricNameTest extends TestHelper {

  private static final long EVAL_TIME_MS = 6_000L;

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m9330 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO m9330 SET ts = 1000, host = 'h1', value = 1.5");
      database.command("sql", "INSERT INTO m9330 SET ts = 3000, host = 'h1', value = 3.5");
      database.command("sql", "INSERT INTO m9330 SET ts = 5000, host = 'h1', value = 5.5");
    });
  }

  @Test
  void aSelectorKeepsTheName() {
    assertThat(only("m9330").labels()).containsEntry("__name__", "m9330").containsEntry("host", "h1");
  }

  @Test
  void unaryMinusDropsTheName() {
    final VectorSample s = only("-m9330");
    assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    assertThat(s.value()).isEqualTo(-5.5);
  }

  @Test
  void transformingFunctionsDropTheName() {
    for (final String fn : new String[] { "abs", "ceil", "floor", "round" })
      assertThat(only(fn + "(m9330)").labels()).as(fn).doesNotContainKey("__name__").containsEntry("host", "h1");

    for (final String fn : new String[] { "rate", "irate", "increase", "sum_over_time", "avg_over_time", "min_over_time",
        "max_over_time", "count_over_time" })
      assertThat(only(fn + "(m9330[10s])").labels()).as(fn).doesNotContainKey("__name__").containsEntry("host", "h1");
  }

  @Test
  void anEmptyLabelValueIsAnAbsentLabel() {
    database.command("sql", "CREATE TIMESERIES TYPE e9363 TIMESTAMP ts TAGS (host STRING, zone STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO e9363 SET ts = 1000, host = '', zone = 'z1', value = 1.0");
      database.command("sql", "INSERT INTO e9363 SET ts = 1000, host = 'h2', zone = 'z1', value = 2.0");
    });

    final InstantVector all = evaluate("e9363");
    assertThat(all.samples()).hasSize(2);
    for (final VectorSample s : all.samples())
      if (s.value() == 1.0)
        assertThat(s.labels()).as("an empty label is not reported").doesNotContainKey("host");

    final InstantVector empty = evaluate("e9363{host=\"\"}");
    assertThat(empty.samples()).hasSize(1);
    assertThat(empty.samples().getFirst().value()).isEqualTo(1.0);

    final InstantVector present = evaluate("e9363{host!=\"\"}");
    assertThat(present.samples()).hasSize(1);
    assertThat(present.samples().getFirst().value()).isEqualTo(2.0);
  }

  private DatabaseInternal getDatabaseInternal() {
    return (DatabaseInternal) database;
  }

  private VectorSample only(final String promql) {
    final InstantVector iv = evaluate(promql);
    assertThat(iv.samples()).hasSize(1);
    return iv.samples().getFirst();
  }

  private InstantVector evaluate(final String promql) {
    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(new PromQLParser(promql).parse(),
        EVAL_TIME_MS);
    assertThat(result).isInstanceOf(InstantVector.class);
    return (InstantVector) result;
  }
}
