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
import com.arcadedb.database.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #10264: storing a row whose projection holds a SELECT subquery failed with a
 * ClassCastException in the serializer, because the subquery value is a list of {@link Result} and the map
 * branch only knew how to unwrap a JSONObject.
 */
class Issue10264InsertFromSelectSubqueryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Base");
      database.command("sql", "CREATE DOCUMENT TYPE Owner");
      database.command("sql", "CREATE DOCUMENT TYPE T");
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Base SET id = ?, n = ?", i, i);
      database.command("sql", "INSERT INTO Owner SET name = 'a'");
      database.command("sql", "INSERT INTO Owner SET name = 'b'");
    });
  }

  @Test
  void aProjectedSubqueryIsStoredAsAListOfMaps() {
    database.transaction(() -> database.command("sql",
        "INSERT INTO T FROM SELECT id, (SELECT count(*) FROM Owner) AS oc FROM Base WHERE n < 3"));

    try (final ResultSet rs = database.query("sql", "SELECT FROM T ORDER BY id")) {
      for (int i = 0; i < 3; i++) {
        final Document copy = rs.next().toElement();
        assertThat(copy.getInteger("id")).isEqualTo(i);
        final List<?> oc = (List<?>) copy.get("oc");
        assertThat(oc).hasSize(1);
        final Map<?, ?> count = (Map<?, ?>) oc.get(0);
        assertThat(count).hasSize(1);
        assertThat(((Number) count.get("count(*)")).intValue()).isEqualTo(2);
      }
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
