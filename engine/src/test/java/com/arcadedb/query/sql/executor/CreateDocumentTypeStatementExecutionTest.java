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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class CreateDocumentTypeStatementExecutionTest {
  @Test
  void plain() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testPlain";
      final ResultSet result = db.command("sql", "create document type " + className);
      final Schema schema = db.getSchema();
      final DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();
    });
  }

  @Test
  void clusters() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testClusters";
      final ResultSet result = db.command("sql", "create document type " + className + " buckets 32");
      final Schema schema = db.getSchema();
      final DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      assertThat(clazz.getBuckets(false)).hasSize(32);
      result.close();
    });
  }

  @Test
  void ifNotExists() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = "testIfNotExists";
      ResultSet result = db.command("sql", "create document type " + className + " if not exists");
      final Schema schema = db.getSchema();
      DocumentType clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();

      result = db.command("sql", "create document type " + className + " if not exists");
      clazz = schema.getType(className);
      assertThat(clazz).isNotNull();
      result.close();
    });
  }

  @Test
  void pageSize() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String typeName = "testPageSize";
      final int customPageSize = 32768; // 32KB
      final ResultSet result = db.command("sql", "create document type " + typeName + " pagesize " + customPageSize);
      final Schema schema = db.getSchema();
      final DocumentType type = schema.getType(typeName);
      assertThat(type).isNotNull();
      // Verify the page size was set on the buckets
      assertThat(type.getBuckets(false)).isNotEmpty();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      assertThat(bucket.getPageSize()).isEqualTo(customPageSize);
      result.close();
    });
  }

  @Test
  void pageSizeWithBuckets() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String typeName = "testPageSizeWithBuckets";
      final int customPageSize = 16384; // 16KB
      final ResultSet result = db.command("sql", "create document type " + typeName + " buckets 4 pagesize " + customPageSize);
      final Schema schema = db.getSchema();
      final DocumentType type = schema.getType(typeName);
      assertThat(type).isNotNull();
      assertThat(type.getBuckets(false)).hasSize(4);
      // Verify all buckets have the custom page size
      for (final var bucket : type.getBuckets(false)) {
        assertThat(((LocalBucket) bucket).getPageSize()).isEqualTo(customPageSize);
      }
      result.close();
    });
  }

  @Test
  void vertexTypePageSize() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String typeName = "testVertexPageSize";
      final int customPageSize = 131072; // 128KB
      final ResultSet result = db.command("sql", "create vertex type " + typeName + " pagesize " + customPageSize);
      final Schema schema = db.getSchema();
      final VertexType type = (VertexType) schema.getType(typeName);
      assertThat(type).isNotNull();
      assertThat(type.getBuckets(false)).isNotEmpty();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      assertThat(bucket.getPageSize()).isEqualTo(customPageSize);
      result.close();
    });
  }

  @Test
  void edgeTypePageSize() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String typeName = "testEdgePageSize";
      final int customPageSize = 8192; // 8KB
      final ResultSet result = db.command("sql", "create edge type " + typeName + " pagesize " + customPageSize);
      final Schema schema = db.getSchema();
      final EdgeType type = (EdgeType) schema.getType(typeName);
      assertThat(type).isNotNull();
      assertThat(type.getBuckets(false)).isNotEmpty();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      assertThat(bucket.getPageSize()).isEqualTo(customPageSize);
      result.close();
    });
  }

  @Test
  void edgeTypePageSizeWithUnidirectional() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String typeName = "testEdgePageSizeUnidirectional";
      final int customPageSize = 65536; // 64KB
      final ResultSet result = db.command("sql", "create edge type " + typeName + " unidirectional pagesize " + customPageSize);
      final Schema schema = db.getSchema();
      final EdgeType type = (EdgeType) schema.getType(typeName);
      assertThat(type).isNotNull();
      assertThat(type.getBuckets(false)).isNotEmpty();
      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      assertThat(bucket.getPageSize()).isEqualTo(customPageSize);
      result.close();
    });
  }
}
