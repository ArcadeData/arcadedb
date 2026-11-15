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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11958: {@link JsonSerializer#serializeValue(com.arcadedb.database.Database, Object)} renders a record or a map as the HTTP
 * API renders the same row, and the HTTP rendering itself is pinned here so that the shared serializer's output stays as it was.
 */
class Issue11958SerializeValueTest extends TestHelper {

  private static final String SELECT = "SELECT @this AS rec, {'da': da, 'dt': dt, 'dtn': dtn, 'zdt': zdt} AS m, "
      + "da, dt, dtn, zdt FROM T11958";

  @BeforeEach
  void createRecord() {
    database.command("sql", "CREATE VERTEX TYPE T11958");
    database.command("sql", "CREATE PROPERTY T11958.da DATE");
    database.command("sql", "CREATE PROPERTY T11958.dt DATETIME");
    database.command("sql", "CREATE PROPERTY T11958.dtn DATETIME_NANOS");
    database.command("sql", "CREATE PROPERTY T11958.zdt ZONED_DATETIME");
    database.transaction(() -> database.command("sql", "INSERT INTO T11958 SET da = '2024-02-29', dt = '2024-02-29 13:14:15.123', "
        + "dtn = '2024-02-29 13:14:15.123456789', zdt = '2024-03-10T03:30-04:00[America/New_York]'"));
  }

  @Test
  void httpRowRenderingIsUnchanged() {
    final JSONObject row = httpRow();
    assertTemporals(row);
    assertTemporals(row.getJSONObject("rec"));
    assertTemporals(row.getJSONObject("m"));
  }

  @Test
  void serializeValueMatchesTheHttpRow() {
    final JSONObject row = httpRow();
    try (final ResultSet rs = database.query("sql", SELECT)) {
      final Result result = rs.next();
      final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setIncludeVertexEdges(false)
          .setUseCollectionSize(false).setUseCollectionSizeForEdges(false);
      final Object map = result.getProperty("m");
      assertThat(serializer.serializeValue(database, map).toString()).isEqualTo(row.getJSONObject("m").toString());
      // a map no database owns falls back to the default patterns, which this database keeps
      assertThat(serializer.serializeValue(null, map).toString()).isEqualTo(row.getJSONObject("m").toString());
      assertTemporals((JSONObject) serializer.serializeValue(database, result.getProperty("rec")));
      assertTemporals((JSONObject) serializer.serializeValue(null, Map.of("nested", map)));
    }
  }

  private JSONObject httpRow() {
    try (final ResultSet rs = database.query("sql", SELECT)) {
      return JsonSerializer.createJsonSerializer().setIncludeVertexEdges(false).setUseCollectionSize(false)
          .setUseCollectionSizeForEdges(false).serializeResult(database, rs.next());
    }
  }

  private static void assertTemporals(final JSONObject json) {
    final JSONObject values = json.has("nested") ? json.getJSONObject("nested") : json;
    assertThat(values.get("da")).isEqualTo("2024-02-29");
    assertThat(values.get("dt")).isEqualTo("2024-02-29 13:14:15.123");
    assertThat(values.get("dtn")).isEqualTo("2024-02-29 13:14:15.123456789");
    assertThat(values.get("zdt")).isEqualTo("2024-03-10T03:30-04:00[America/New_York]");
  }
}
