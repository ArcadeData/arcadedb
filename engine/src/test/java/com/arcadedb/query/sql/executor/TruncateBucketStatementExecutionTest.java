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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TRUNCATE BUCKET statement execution.
 */
class TruncateBucketStatementExecutionTest extends TestHelper {

  @Test
  void truncateBucketByName() {
    database.transaction(() -> {
      // Create a type and insert some records
      final DocumentType type = database.getSchema().createDocumentType("TestTruncateBucket");
      final Bucket bucket = type.getBuckets(false).get(0);
      final String bucketName = bucket.getName();

      // Insert records
      for (int i = 0; i < 10; i++) {
        database.newDocument("TestTruncateBucket").set("name", "record" + i).save();
      }

      // Verify records exist
      assertThat(database.countBucket(bucketName)).isEqualTo(10);

      // Truncate the bucket
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET " + bucketName);
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat((String) row.getProperty("operation")).isEqualTo("truncate bucket");
      assertThat((String) row.getProperty("bucketName")).isEqualTo(bucketName);
      result.close();

      // Verify bucket is empty
      assertThat(database.countBucket(bucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateBucketWithBackticksName() {
    database.transaction(() -> {
      // Create a type with a name that needs backticks
      final DocumentType type = database.getSchema().createDocumentType("TestTruncate_Bucket_Name");
      final Bucket bucket = type.getBuckets(false).get(0);
      final String bucketName = bucket.getName();

      // Insert records
      for (int i = 0; i < 5; i++) {
        database.newDocument("TestTruncate_Bucket_Name").set("value", i).save();
      }

      // Verify records exist
      assertThat(database.countBucket(bucketName)).isEqualTo(5);

      // Truncate the bucket using backticks for the name
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET `" + bucketName + "`");
      assertThat(result.hasNext()).isTrue();
      result.close();

      // Verify bucket is empty
      assertThat(database.countBucket(bucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateEmptyBucket() {
    database.transaction(() -> {
      // Create a type with an empty bucket
      final DocumentType type = database.getSchema().createDocumentType("TestTruncateEmptyBucket");
      final Bucket bucket = type.getBuckets(false).get(0);
      final String bucketName = bucket.getName();

      // Verify bucket is empty
      assertThat(database.countBucket(bucketName)).isEqualTo(0);

      // Truncate the empty bucket - should succeed
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET " + bucketName);
      assertThat(result.hasNext()).isTrue();
      result.close();

      // Verify bucket is still empty
      assertThat(database.countBucket(bucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateVertexBucketRequiresUnsafe() {
    database.transaction(() -> {
      // Create a vertex type and insert vertices
      database.getSchema().createVertexType("TestTruncateVertexBucket");
      final String bucketName = database.getSchema().getType("TestTruncateVertexBucket").getBuckets(false).get(0).getName();

      // Insert vertices
      database.command("sql", "CREATE VERTEX TestTruncateVertexBucket SET name = 'v1'");
      database.command("sql", "CREATE VERTEX TestTruncateVertexBucket SET name = 'v2'");

      // Verify records exist
      assertThat(database.countBucket(bucketName)).isEqualTo(2);

      // Try to truncate without UNSAFE - should fail
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET " + bucketName))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("UNSAFE");

      // Truncate with UNSAFE - should succeed
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET " + bucketName + " UNSAFE");
      assertThat(result.hasNext()).isTrue();
      result.close();

      // Verify bucket is empty
      assertThat(database.countBucket(bucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateEdgeBucketRequiresUnsafe() {
    database.transaction(() -> {
      // Create vertex and edge types
      database.getSchema().createVertexType("TestTruncateEdgeV");
      database.getSchema().createEdgeType("TestTruncateEdgeE");

      // Create vertices and an edge
      database.command("sql", "CREATE VERTEX TestTruncateEdgeV SET name = 'v1'");
      database.command("sql", "CREATE VERTEX TestTruncateEdgeV SET name = 'v2'");

      final ResultSet v1Result = database.query("sql", "SELECT FROM TestTruncateEdgeV WHERE name = 'v1'");
      final ResultSet v2Result = database.query("sql", "SELECT FROM TestTruncateEdgeV WHERE name = 'v2'");
      final String v1Rid = v1Result.next().getIdentity().get().toString();
      final String v2Rid = v2Result.next().getIdentity().get().toString();
      v1Result.close();
      v2Result.close();

      database.command("sql", "CREATE EDGE TestTruncateEdgeE FROM " + v1Rid + " TO " + v2Rid);

      final String edgeBucketName = database.getSchema().getType("TestTruncateEdgeE").getBuckets(false).get(0).getName();

      // Verify edge exists
      assertThat(database.countBucket(edgeBucketName)).isEqualTo(1);

      // Try to truncate edge bucket without UNSAFE - should fail
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET " + edgeBucketName))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("UNSAFE");

      // Truncate with UNSAFE - should succeed
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET " + edgeBucketName + " UNSAFE");
      assertThat(result.hasNext()).isTrue();
      result.close();

      // Verify bucket is empty
      assertThat(database.countBucket(edgeBucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateNonExistentBucket() {
    database.transaction(() -> {
      // Try to truncate a non-existent bucket
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET NonExistentBucket"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("not found");
    });
  }

  @Test
  void truncateBucketMultipleTimes() {
    database.transaction(() -> {
      // Create a type
      final DocumentType type = database.getSchema().createDocumentType("TestTruncateMultiple");
      final String bucketName = type.getBuckets(false).get(0).getName();

      // Insert, truncate, insert again
      for (int i = 0; i < 5; i++) {
        database.newDocument("TestTruncateMultiple").set("round", 1, "index", i).save();
      }
      assertThat(database.countBucket(bucketName)).isEqualTo(5);

      database.command("sql", "TRUNCATE BUCKET " + bucketName);
      assertThat(database.countBucket(bucketName)).isEqualTo(0);

      // Insert more records
      for (int i = 0; i < 3; i++) {
        database.newDocument("TestTruncateMultiple").set("round", 2, "index", i).save();
      }
      assertThat(database.countBucket(bucketName)).isEqualTo(3);

      // Truncate again
      database.command("sql", "TRUNCATE BUCKET " + bucketName);
      assertThat(database.countBucket(bucketName)).isEqualTo(0);
    });
  }

  @Test
  void truncateStandaloneBucket() {
    database.transaction(() -> {
      // Create a standalone bucket (not associated with any type)
      final Schema schema = database.getSchema();
      schema.createBucket("StandaloneBucket");

      final Bucket bucket = schema.getBucketByName("StandaloneBucket");
      assertThat(bucket).isNotNull();

      // Truncate the standalone bucket - should succeed even if empty
      final ResultSet result = database.command("sql", "TRUNCATE BUCKET StandaloneBucket");
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat((String) row.getProperty("bucketName")).isEqualTo("StandaloneBucket");
      result.close();
    });
  }
}
