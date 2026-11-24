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
package com.arcadedb.server.http.handler;

import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11982: an instant query whose expression is a range selector ({@code cnt[1m]}) evaluates to a range
 * vector, which the response formatter had no branch for, so {@code /prom/api/v1/query} answered {@code {"status":"success","data":{}}}.
 * Prometheus answers the window's raw samples as a matrix; the expected document is what Prometheus v3.5.0 answers for the same
 * samples.
 */
class Issue11982InstantQueryOfARangeSelectorTest {

  @Test
  void aRangeVectorIsAnsweredAsTheMatrixOfItsSamples() {
    final RangeVector vector = new RangeVector(List.of(
        new RangeSeries(Map.of("__name__", "cnt", "host", "a"),
            List.of(new double[] { 1_700_000_000_000L, 10 }, new double[] { 1_700_000_030_000L, 40 }), 1_699_999_980_000L,
            1_700_000_040_000L),
        new RangeSeries(Map.of("__name__", "cnt", "host", "b"), List.<double[]>of(new double[] { 1_700_000_010_000L, 5 }),
            1_699_999_980_000L, 1_700_000_040_000L)));

    final JSONObject response = new JSONObject(PromQLResponseFormatter.formatSuccess(vector));

    assertThat(response.getString("status")).isEqualTo("success");
    final JSONObject data = response.getJSONObject("data");
    assertThat(data.getString("resultType")).isEqualTo("matrix");
    final JSONArray result = data.getJSONArray("result");
    assertThat(result.length()).isEqualTo(2);

    final JSONObject a = result.getJSONObject(0);
    assertThat(a.getJSONObject("metric").toMap()).isEqualTo(Map.of("__name__", "cnt", "host", "a"));
    final JSONArray aValues = a.getJSONArray("values");
    assertThat(aValues.length()).isEqualTo(2);
    assertThat(aValues.getJSONArray(0).getDouble(0)).isEqualTo(1_700_000_000.0);
    assertThat(aValues.getJSONArray(0).getString(1)).isEqualTo("10");
    assertThat(aValues.getJSONArray(1).getDouble(0)).isEqualTo(1_700_000_030.0);
    assertThat(aValues.getJSONArray(1).getString(1)).isEqualTo("40");

    final JSONArray bValues = result.getJSONObject(1).getJSONArray("values");
    assertThat(bValues.length()).isEqualTo(1);
    assertThat(bValues.getJSONArray(0).getString(1)).isEqualTo("5");
  }
}
