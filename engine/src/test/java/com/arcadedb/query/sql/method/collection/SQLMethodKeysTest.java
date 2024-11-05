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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLMethodKeysTest {

  private SQLMethod function;

  @BeforeEach
  public void setup() {
    function = new SQLMethodKeys();
  }

  @Test
  public void testWithResult() {
    final ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    final Object result = function.execute(resultInternal, null, null, null);
    assertThat(result).isEqualTo(Set.of("name", "surname"));
  }

  @Test
  public void testWithCollection() {
    List<Map<String, Object>> collection = List.of(Map.of("key1", "value1"), Map.of("key2", "value2"));

    Object result = function.execute(collection, null, null, null);
    assertThat(result).isEqualTo(List.of("key1", "key2"));
  }

  @Test
  public void testWithNull() {
    Object result = function.execute(null, null, null, null);
    assertThat(result).isNull();
  }
}
