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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for issue #774: When a bucket is added to a type AFTER indexes have been created,
 * the new bucket should be automatically indexed.
 *
 * This issue was fixed in commit 0c80759ec1 (2023-11-14) as part of the remote schema API implementation.
 * The fix ensures that when addBucket() is called, all existing type indexes are automatically
 * created for the new bucket, and when removeBucket() is called, indexes on that bucket are dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue774AddBucketWithIndexTest extends TestHelper {

  @Test
  void testAddBucketAfterIndexCreation() {
    // Step 1: Create a document type with an indexed property
    database.transaction(() -> {
      final DocumentType dtOrders = database.getSchema().createDocumentType("Order");
      dtOrders.createProperty("p1", Type.STRING);
      dtOrders.createProperty("p2", Type.STRING);
      // Create index on p1
      dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p1");
    });

    // Step 2: Add a new bucket to the type AFTER index creation
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().createBucket("O202203");
      database.getSchema().getType("Order").addBucket(bucket);
    });

    // Step 3: Insert records - one into the new bucket, one into the type
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BUCKET:O202203 SET p1 = 'PENDING', p2 = 'PENDING'");
      database.command("sql", "INSERT INTO Order SET p1 = 'PENDING', p2 = 'PENDING'");
    });

    // Step 4: Query using indexed property p1 - should return BOTH records
    database.transaction(() -> {
      try (final ResultSet resultSet = database.query("sql", "SELECT from Order WHERE p1 = 'PENDING'")) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result result = resultSet.next();
          assertEquals("PENDING", result.getProperty("p1"));
          assertEquals("PENDING", result.getProperty("p2"));
          count++;
        }
        // Verify that the query returns both records (one from the new bucket, one from the default bucket)
        assertThat((int) count).as("Query with indexed property should return both records").isEqualTo(2);
      }
    });

    // Step 5: Query from the specific bucket using indexed property - should return 1 record
    database.transaction(() -> {
      try (final ResultSet resultSet = database.query("sql", "SELECT from bucket:O202203 WHERE p1 = 'PENDING'")) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result result = resultSet.next();
          assertEquals("PENDING", result.getProperty("p1"));
          assertEquals("PENDING", result.getProperty("p2"));
          count++;
        }
        // Verify that the query from the specific bucket returns 1 record
        assertThat((int) count).as("Query from bucket with indexed property should return 1 record").isEqualTo(1);
      }
    });

    // Step 6: Query using non-indexed property p2 - this does a full scan of all buckets
    database.transaction(() -> {
      try (final ResultSet resultSet = database.query("sql", "SELECT from Order WHERE p2 = 'PENDING'")) {
        int count = 0;
        while (resultSet.hasNext()) {
          final Result result = resultSet.next();
          assertEquals("PENDING", result.getProperty("p1"));
          assertEquals("PENDING", result.getProperty("p2"));
          count++;
        }
        // Verify that queries with non-indexed properties also return both records
        assertThat((int) count).as("Query with non-indexed property should return both records").isEqualTo(2);
      }
    });
  }
}
