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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodKeysTest extends TestHelper {

  private SQLMethod function;

  @BeforeEach
  void setup() {
    function = new SQLMethodKeys();
  }

  @Test
  void withResult() {
    final ResultInternal resultInternal = new ResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    final Object result = function.execute(resultInternal, null, null, null);
    assertThat(result).isEqualTo(Set.of("name", "surname"));
  }

  @Test
  void withCollection() {
    List<Map<String, Object>> collection = List.of(Map.of("key1", "value1"), Map.of("key2", "value2"));

    Object result = function.execute(collection, null, null, null);
    assertThat(result).isEqualTo(List.of("key1", "key2"));
  }

  @Test
  void withNull() {
    Object result = function.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  // Regression tests for issue #6387

  @Test
  void withDocumentReturnsFlatListNotNestedOneElementList() {
    database.transaction(() -> database.getSchema().createDocumentType("Person"));

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Person");
      doc.set("name", "Alice");
      doc.set("age", 30);
      doc.save();

      final Object result = function.execute(doc, null, null, null);
      assertThat(result).isInstanceOf(List.class);
      assertThat((List<Object>) result).containsExactlyInAnyOrder("name", "age");
    });
  }

  @Test
  void withCollectionContainingScalarAndNullDoesNotThrow() {
    final List<Object> mixed = Arrays.asList(Map.of("a", 1), null, "scalar", 42, Map.of("b", 2));

    final Object result = function.execute(mixed, null, null, null);
    assertThat(result).isEqualTo(List.of("a", "b"));
  }

  @Test
  void withSQLScalarCollectionDoesNotThrow() {
    // SELECT [1,2,3].keys() -- a collection of scalars has no keys, so this used to NPE
    try (final ResultSet resultSet = database.query("sql", "SELECT [1,2,3].keys() AS keys")) {
      final Result record = resultSet.next();
      assertThat(record.<List<Object>>getProperty("keys")).isEqualTo(List.of());
    }
  }
}
