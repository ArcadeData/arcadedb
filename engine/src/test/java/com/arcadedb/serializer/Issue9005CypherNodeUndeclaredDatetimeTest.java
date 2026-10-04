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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9005: a node returned whole by openCypher ({@code RETURN n}) lost the fractional seconds of
 * an undeclared datetime property when serialized to JSON, while SQL and the declared property kept them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9005CypherNodeUndeclaredDatetimeTest extends TestHelper {
  @Test
  void undeclaredDatetimeKeepsItsPrecisionWhenTheNodeIsReturnedWhole() {
    final LocalDateTime t = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_000);
    database.transaction(() -> {
      database.getSchema().createVertexType("V9005").createProperty("declared", Type.DATETIME_MICROS);
      database.newVertex("V9005").set("declared", t).set("undeclared", t).save();
    });

    final JsonSerializer serializer = new JsonSerializer(database);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V9005) RETURN n")) {
      final JSONObject json = serializer.serializeResult(database, rs.next());
      assertThat(json.getString("declared")).isEqualTo("2026-10-03 12:34:56.123456");
      assertThat(json.getString("undeclared")).isEqualTo("2026-10-03 12:34:56.123456");
    }
  }
}
