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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class SQLMethodAsJSONTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodAsJSON();
  }

  @Test
  void testNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void testEmptyJsonIsReturned() {
    final Object result = method.execute("", null, null, null);
    assertThat(result instanceof JSONObject).isTrue();
    assertThat(((JSONObject) result).isEmpty()).isTrue();
  }

  @Test
  void testStringIsReturnedAsString() {
    final Object result = method.execute(new JSONObject().put("name", "robot").toString(), null, null, null);
    assertThat(result instanceof JSONObject).isTrue();
    assertThat(((JSONObject) result).getString("name")).isEqualTo("robot");
  }

  @Test
  void testErrorJsonParsing() {
    try {
      final Object result = method.execute("{\"name\"]", null, null, null);
      fail("");
    } catch (JSONException e) {
      //EXPECTED
    }
  }
}
