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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsJSONTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodAsJSON();
  }

  @Test
  void testNull() {
    final Object result = method.execute(null, null, null, null);
    Assertions.assertNull(result);
  }

  @Test
  void testEmptyJsonIsReturned() {
    final Object result = method.execute("", null, null, null);
    Assertions.assertTrue(result instanceof JSONObject);
    Assertions.assertTrue(((JSONObject) result).isEmpty());
  }

  @Test
  void testStringIsReturnedAsString() {
    final Object result = method.execute(new JSONObject().put("name", "robot").toString(), null, null, null);
    Assertions.assertTrue(result instanceof JSONObject);
    Assertions.assertEquals("robot", ((JSONObject) result).getString("name"));
  }

  @Test
  void testErrorJsonParsing() {
    try {
      final Object result = method.execute("{\"name\"]", null, null, null);
      Assertions.fail();
    } catch (JSONException e) {
      //EXPECTED
    }
  }
}
