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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for GitHub issue #1625: HTTP/SQL "params" cannot be bucket names.
 * Users should be able to use parameterized bucket names in queries like:
 * SELECT FROM bucket::myparam where myparam contains the bucket name.
 */
class Issue1625BucketParameterTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Create custom buckets
      database.command("sql", "CREATE BUCKET Customer_Europe IF NOT EXISTS");
      database.command("sql", "CREATE BUCKET Customer_Americas IF NOT EXISTS");

      // Create type with custom buckets
      database.command("sql", "CREATE DOCUMENT TYPE Customer BUCKET Customer_Europe,Customer_Americas");

      // Insert data into specific buckets
      database.command("sql", "INSERT INTO bucket:Customer_Europe CONTENT { firstName: 'Enzo', lastName: 'Ferrari', region: 'Europe' }");
      database.command("sql", "INSERT INTO bucket:Customer_Americas CONTENT { firstName: 'Jack', lastName: 'Tramiel', region: 'Americas' }");
    });
  }

  @Test
  void testSelectFromBucketWithNamedParameter() {
    database.transaction(() -> {
      // Test using named parameter for bucket name
      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Europe");

      // This should work: SELECT FROM bucket::bucketName where :bucketName is a parameter
      final ResultSet resultSet = database.query("sql", "SELECT FROM bucket::bucketName", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Enzo");
      assertThat((String) record.getProperty("region")).isEqualTo("Europe");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  @Test
  void testSelectFromBucketWithPositionalParameter() {
    database.transaction(() -> {
      // Test using positional parameter (?) for bucket name
      final Map<String, Object> params = new HashMap<>();
      params.put("0", "Customer_Americas");

      // This should work: SELECT FROM bucket:? where ? is a positional parameter
      final ResultSet resultSet = database.query("sql", "SELECT FROM bucket:?", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Jack");
      assertThat((String) record.getProperty("region")).isEqualTo("Americas");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  @Test
  void testSelectFromBucketParameterWithWhereClause() {
    database.transaction(() -> {
      // Test bucket parameter combined with WHERE clause
      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Europe");
      params.put("lastName", "Ferrari");

      final ResultSet resultSet = database.query("sql",
          "SELECT FROM bucket::bucketName WHERE lastName = :lastName", params);

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Enzo");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }

  @Test
  void testInsertIntoBucketWithParameter() {
    database.transaction(() -> {
      // Test INSERT with bucket parameter
      final Map<String, Object> params = new HashMap<>();
      params.put("bucketName", "Customer_Americas");
      params.put("firstName", "Steve");
      params.put("lastName", "Jobs");

      database.command("sql",
          "INSERT INTO bucket::bucketName CONTENT { firstName: :firstName, lastName: :lastName }", params);

      // Verify the insert worked in the right bucket
      final ResultSet resultSet = database.query("sql",
          "SELECT FROM bucket:Customer_Americas WHERE lastName = 'Jobs'");

      assertThat(resultSet.hasNext()).isTrue();
      final Result record = resultSet.next();
      assertThat((String) record.getProperty("firstName")).isEqualTo("Steve");
      assertThat(resultSet.hasNext()).isFalse();
    });
  }
}
