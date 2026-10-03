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
package com.arcadedb.serializer.json;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #9114 (#9044): a stored record whose list holds a {@code byte[]} serialized it as its identity hash.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9114StoredByteArrayInListTest extends TestHelper {

  @Test
  void storedRecordWithByteArrayInList() {
    database.command("sql", "CREATE DOCUMENT TYPE Blob");
    database.transaction(() -> {
      final Map<String, Object> map = new HashMap<>();
      map.put("k", new byte[] { 1, 2 });
      database.newDocument("Blob").set("in_list", new ArrayList<>(List.of(new byte[] { 1, 2 }))).set("in_map", map).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM Blob")) {
      final Result row = rs.next();
      final JSONObject json = row.toJSON();
      assertThat(json.getJSONArray("in_list").toString()).doesNotContain("[B@");
      assertThat(json.getJSONArray("in_list").getJSONArray(0).toString()).isEqualTo("[1,2]");
    }
  }
}
