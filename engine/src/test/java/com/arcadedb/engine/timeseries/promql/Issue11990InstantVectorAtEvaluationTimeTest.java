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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11990: an instant query answered a selector, and every aggregation and operator over it, at the
 * timestamp of the STORED sample each element came from ({@code count(up)} at T+46s stamped T+30s). Prometheus stamps every element
 * of an instant vector with the evaluation time.
 */
class Issue11990InstantVectorAtEvaluationTimeTest extends TestHelper {

  private static final long T = 1_700_005_000_000L;

  @Test
  void everyElementOfAnInstantVectorIsAtTheEvaluationTime() {
    database.command("sql", "CREATE TIMESERIES TYPE g TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO g SET ts = " + T + ", host = 'a', value = 1.0");
      database.command("sql", "INSERT INTO g SET ts = " + (T - 10_000) + ", host = 'b', value = 2.0");
    });

    final long at = T + 30_500;
    for (final String query : new String[] { "g", "sum(g)", "count(g)", "g * 2", "g > 0", "-g", "abs(g)", "topk(1, g)",
        "g offset 1s", "max by (host) (g)" }) {
      final InstantVector vector = (InstantVector) new PromQLEvaluator((DatabaseInternal) database)
          .evaluateInstant(new PromQLParser(query).parse(), at);
      assertThat(vector.samples()).as(query).isNotEmpty();
      for (final VectorSample sample : vector.samples())
        assertThat(sample.timestampMs()).as(query).isEqualTo(at);
    }
  }
}
