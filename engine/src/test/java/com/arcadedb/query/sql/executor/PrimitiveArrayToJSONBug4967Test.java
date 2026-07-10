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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.serializer.json.JSONArray;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4967: {@code Result.toJSON()} serialized primitive-array properties
 * (e.g. {@code ARRAY_OF_FLOATS}) as the Java array's {@code toString()} (e.g. {@code "[F@66451058"})
 * instead of a JSON array.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PrimitiveArrayToJSONBug4967Test {

  @Test
  void projectedPrimitiveArraySerializesAsJsonArray() throws Exception {
    TestHelper.executeInNewDatabase("primitiveArrayJson4967", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Item");
      db.command("sql", "CREATE PROPERTY Item.emb ARRAY_OF_FLOATS");
      db.command("sql", "INSERT INTO Item SET emb = [1.5, 2.5]");

      // Projection path (ResultInternal.toJSON building JSON from properties)
      try (final ResultSet rs = db.query("sql", "SELECT emb FROM Item")) {
        final JSONArray emb = rs.next().toJSON().getJSONArray("emb");
        assertThat(emb.length()).isEqualTo(2);
        assertThat(emb.getFloat(0)).isEqualTo(1.5f);
        assertThat(emb.getFloat(1)).isEqualTo(2.5f);
      }

      // Element-backed path (SELECT FROM without projection)
      try (final ResultSet rs = db.query("sql", "SELECT FROM Item")) {
        final JSONArray emb = rs.next().toJSON().getJSONArray("emb");
        assertThat(emb.length()).isEqualTo(2);
        assertThat(emb.getFloat(0)).isEqualTo(1.5f);
        assertThat(emb.getFloat(1)).isEqualTo(2.5f);
      }
    });
  }

  @Test
  void allPrimitiveArrayTypesSerializeAsJsonArray() throws Exception {
    TestHelper.executeInNewDatabase("primitiveArrayJson4967Types", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Item");
      db.command("sql", "CREATE PROPERTY Item.floats ARRAY_OF_FLOATS");
      db.command("sql", "CREATE PROPERTY Item.doubles ARRAY_OF_DOUBLES");
      db.command("sql", "CREATE PROPERTY Item.ints ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY Item.longs ARRAY_OF_LONGS");
      db.command("sql", "CREATE PROPERTY Item.shorts ARRAY_OF_SHORTS");
      db.command("sql",
          """
          INSERT INTO Item SET floats = [1.5, 2.5], doubles = [3.5, 4.5], ints = [1, 2, 3], \
          longs = [10, 20], shorts = [5, 6]""");

      try (final ResultSet rs = db.query("sql", "SELECT floats, doubles, ints, longs, shorts FROM Item")) {
        final var json = rs.next().toJSON();
        assertThat(json.getJSONArray("floats").length()).isEqualTo(2);
        assertThat(json.getJSONArray("doubles").length()).isEqualTo(2);
        assertThat(json.getJSONArray("ints").length()).isEqualTo(3);
        assertThat(json.getJSONArray("longs").length()).isEqualTo(2);
        assertThat(json.getJSONArray("shorts").length()).isEqualTo(2);
        // No "[F@" / "[D@" / "[I@" style Java-array toString anywhere in the output.
        assertThat(json.toString()).doesNotContain("@");
      }
    });
  }
}
