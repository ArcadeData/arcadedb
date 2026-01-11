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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #2965: SQL nested projection does not work with "expand"
 *
 * Solution: Use nested projection AFTER expand(), not inside the parameter:
 *   CORRECT: SELECT expand([array]):{fields}
 *   NOT: SELECT expand([array]:{fields})  // This breaks WHERE clause parsing
 */
public class Issue2965NestedProjectionExpandTest extends TestHelper {

  @Test
  void testNestedProjectionWithoutExpand() {
    // This should work - nested projection on array of maps
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT [{'x':1,'y':2}]:{x} AS test");
      assertThat(result.hasNext()).isTrue();

      var record = result.next();
      Object testValue = record.getProperty("test");

      System.out.println("Result without expand: " + testValue);
      assertThat(testValue).isInstanceOf(List.class);

      @SuppressWarnings("unchecked")
      List<Result> list = (List<Result>) testValue;
      assertThat(list).hasSize(1);
      assertThat(list.get(0).getPropertyNames().contains("x")).isTrue();
      assertThat(list.get(0).getPropertyNames().contains("y")).isFalse();
      assertThat((Object) list.get(0).getProperty("x")).isEqualTo(1);
    });
  }

  @Test
  void testExpandWithoutNestedProjection() {
    // This should work - expand on array of maps
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT expand([{'x':1,'y':2}]) AS test");
      assertThat(result.hasNext()).isTrue();

      var record = result.next();
      System.out.println("Result with expand (no nested projection): " + record.toJSON());

      assertThat((Object) record.getProperty("x")).isEqualTo(1);
      assertThat((Object) record.getProperty("y")).isEqualTo(2);
    });
  }

  @Test
  void testExpandWithNestedProjection() {
    // Solution: nested projection AFTER expand(), not inside parameter
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT expand([{'x':1,'y':2}]):{x}");
      assertThat(result.hasNext()).isTrue();

      var record = result.next();
      System.out.println("Result with expand and nested projection: " + record.toJSON());

      // Should return a record with only 'x' property, not 'y'
      assertThat((Object) record.getProperty("x")).isEqualTo(1);
      assertThat(record.getPropertyNames().contains("y")).isFalse();
    });
  }

  @Test
  void testExpandWithNestedProjectionMultipleRecords() {
    // Test with multiple records in the array - nested projection after expand()
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT expand([{'x':1,'y':2}, {'x':3,'y':4}, {'x':5,'y':6}]):{x}");

      int count = 0;
      while (result.hasNext()) {
        var record = result.next();
        System.out.println("Record " + count + ": " + record.toJSON());

        assertThat(record.getPropertyNames().contains("x")).isTrue();
        assertThat(record.getPropertyNames().contains("y")).isFalse();
        count++;
      }

      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void testExpandWithComplexNestedProjection() {
    // Test with more complex nested projection - multiple fields
    database.transaction(() -> {
      ResultSet result = database.query("sql",
          "SELECT expand([{'a':1,'b':2,'c':3}, {'a':4,'b':5,'c':6}]):{a,c}");

      int count = 0;
      while (result.hasNext()) {
        var record = result.next();
        System.out.println("Record " + count + ": " + record.toJSON());

        assertThat(record.getPropertyNames().contains("a")).isTrue();
        assertThat(record.getPropertyNames().contains("c")).isTrue();
        assertThat(record.getPropertyNames().contains("b")).isFalse();
        count++;
      }

      assertThat(count).isEqualTo(2);
    });
  }
}
