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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link NamedParameter#getValue(Map)} reads the key a caller wrote with a colon before the plain one, for a generated literal
 * parameter (issue #9925, review of the per-row string work) as for any other name.
 */
class NamedParameterTest {

  private static NamedParameter parameter(final String name) {
    final NamedParameter result = new NamedParameter();
    result.paramName = name;
    return result;
  }

  @Test
  void plainKeyIsRead() {
    assertThat(parameter("name").getValue(Map.of("name", 7))).isEqualTo(7);
  }

  @Test
  void prefixedKeyWinsOverThePlainOne() {
    final Map<String, Object> params = new HashMap<>();
    params.put("name", 1);
    params.put(":name", 2);
    assertThat(parameter("name").getValue(params)).isEqualTo(2);
  }

  @Test
  void prefixedKeyOfAGeneratedLiteralParameterIsRead() {
    final Map<String, Object> params = new HashMap<>();
    params.put("__lit_i0", 1);
    params.put(":__lit_i0", 2);
    assertThat(parameter("__lit_i0").getValue(params)).isEqualTo(2);
    assertThat(parameter("__lit_i0").getValue(Map.of("__lit_i0", 3))).isEqualTo(3);
  }

  @Test
  void keyBoundToNullEndsTheSearch() {
    final NamedParameter parameter = parameter("name");
    parameter.paramNumber = 0;
    final Map<String, Object> params = new HashMap<>();
    params.put("name", null);
    params.put("0", "positional");
    assertThat(parameter.getValue(params)).isNull();
  }

  @Test
  void positionalKeyIsTheLastResort() {
    final NamedParameter parameter = parameter("name");
    parameter.paramNumber = 3;
    assertThat(parameter.getValue(Map.of("3", "positional"))).isEqualTo("positional");
    assertThat(parameter.getValue(null)).isNull();
  }

  @Test
  void renamedParameterIsNotServedFromTheOldCachedName() {
    final NamedParameter parameter = parameter("a");
    assertThat(parameter.getValue(Map.of(":a", 1, ":b", 2))).isEqualTo(1);
    parameter.paramName = "b";
    assertThat(parameter.getValue(Map.of(":a", 1, ":b", 2))).isEqualTo(2);
  }
}
