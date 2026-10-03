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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The charts the model asks for are checked again before they reach the browser or the saved chat. */
class AiChartsTest {
  private static JSONObject valid() {
    return FakeAiPortal.chart("bar", "SELECT style, count(*) AS beers FROM Beer GROUP BY style", "style", "beers");
  }

  @Test
  void aWellFormedChartSurvivesAsIs() {
    final JSONArray out = AiCharts.clean(new JSONArray().put(valid()));
    assertThat(out.length()).isEqualTo(1);
    final JSONObject chart = out.getJSONObject(0);
    assertThat(chart.getString("type")).isEqualTo("bar");
    assertThat(chart.getString("x")).isEqualTo("style");
    assertThat(chart.getJSONArray("y").getString(0)).isEqualTo("beers");
    assertThat(chart.getString("language")).isEqualTo("sql");
  }

  @Test
  void nothingAtAllIsAnEmptyList() {
    assertThat(AiCharts.clean(null).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray()).length()).isZero();
  }

  @Test
  void anUnknownTypeOrLanguageIsDropped() {
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("type", "radar"))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("language", "python"))).length()).isZero();
  }

  @Test
  void anEmptyOrOversizedQueryIsDropped() {
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("query", "  "))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("query", "x".repeat(AiCharts.MAX_QUERY + 1)))).length()).isZero();
  }

  @Test
  void columnsAreNamesNotExpressions() {
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("x", "style; DROP TYPE Beer"))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("x", ""))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(FakeAiPortal.chart("bar", "SELECT 1", "a", "b c"))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("y", new JSONArray()))).length()).isZero();
    assertThat(AiCharts.clean(new JSONArray().put(FakeAiPortal.chart("bar", "SELECT 1", "a", "1", "2", "3", "4", "5", "6"))).length())
        .isZero();
    assertThat(AiCharts.clean(new JSONArray().put(valid().put("y", new JSONArray().put(7)))).length()).isZero();
  }

  @Test
  void atMostThreeChartsAndOnlyTheValidOnes() {
    final JSONArray raw = new JSONArray().put(valid()).put(valid().put("type", "nope")).put("not an object").put(valid()).put(valid())
        .put(valid());
    final JSONArray out = AiCharts.clean(raw);
    assertThat(out.length()).isEqualTo(AiCharts.MAX_CHARTS);
  }

  @Test
  void aLongTitleIsCutAndOnlyKnownMembersAreKept() {
    final JSONObject chart = valid().put("title", "t".repeat(500)).put("onclick", "alert(1)");
    final JSONObject out = AiCharts.clean(new JSONArray().put(chart)).getJSONObject(0);
    assertThat(out.getString("title").length()).isEqualTo(AiCharts.MAX_TITLE);
    assertThat(out.has("onclick")).isFalse();
  }
}
