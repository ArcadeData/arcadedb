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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaDetailQueryTest extends TestHelper {

  @Test
  void selectFromSchemaDictionary() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("DictTestType");
      database.newDocument("DictTestType").set("name", "test").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:dictionary")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat(r.<Integer>getProperty("totalEntries")).isGreaterThan(0);
      assertThat(r.<Integer>getProperty("totalPages")).isGreaterThan(0);
      final Map<String, Integer> entries = r.getProperty("entries");
      assertThat(entries).isNotNull();
      assertThat(entries).containsKey("DictTestType");

      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void selectFromSchemaBucketDetail() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("BucketTestType");
      for (int i = 0; i < 5; i++)
        database.newDocument("BucketTestType").set("name", "test" + i).save();
    });

    final String bucketName = database.getSchema().getType("BucketTestType").getBuckets(false).get(0).getName();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:bucket:" + bucketName)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("name")).isEqualTo(bucketName);
      assertThat(r.getPropertyNames()).contains("fileId", "pageSize", "totalPages", "totalActiveRecords");

      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void selectFromSchemaIndexDetail() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("IndexTestType");
      type.createProperty("name", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.newDocument("IndexTestType").set("name", "test").save();
    });

    final String indexName = database.getSchema().getType("IndexTestType").getAllIndexes(true).iterator().next().getName();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:" + indexName)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("name")).isEqualTo(indexName);
      assertThat((Object) r.getProperty("indexType")).isNotNull();
      assertThat((boolean) r.getProperty("unique")).isTrue();
      assertThat(r.getPropertyNames()).contains("name", "indexType", "typeName", "unique", "compacting", "valid",
          "supportsOrderedIterations", "nullStrategy");

      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void invalidBucketNameThrowsError() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:bucket:nonExistentBucket")) {
        rs.hasNext();
      }
    }).isInstanceOf(Exception.class);
  }

  @Test
  void invalidIndexNameThrowsError() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:nonExistentIndex")) {
        rs.hasNext();
      }
    }).isInstanceOf(Exception.class);
  }

  @Test
  void invalidSchemaTargetThrowsError() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:invalidTarget")) {
        rs.hasNext();
      }
    }).isInstanceOf(UnsupportedOperationException.class);
  }
}
