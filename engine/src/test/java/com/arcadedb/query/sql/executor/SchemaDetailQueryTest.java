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

import java.util.Map;

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

  /**
   * Issue #5469: the auto-generated name of a compound index is {@code Type[propA,propB]}. The comma inside the name broke the
   * {@code schema:index:<name>} target, so the "Indexes" tab in Studio logged a SQL syntax error for every compound index.
   */
  @Test
  void selectFromSchemaIndexDetailWithCompoundIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("CompoundIndexTestType");
      type.createProperty("propA", Type.STRING);
      type.createProperty("propB", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "propA", "propB");
      database.newDocument("CompoundIndexTestType").set("propA", "test").set("propB", 1).save();
    });

    final String indexName = database.getSchema().getType("CompoundIndexTestType").getAllIndexes(true).iterator().next()
        .getName();
    assertThat(indexName).isEqualTo("CompoundIndexTestType[propA,propB]");

    for (final String target : new String[] { "schema:index:" + indexName, "schema:index:`" + indexName + "`" }) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + target + " limit 20000")) {
        assertThat(rs.hasNext()).as(target).isTrue();
        final Result r = rs.next();

        assertThat((String) r.getProperty("name")).isEqualTo(indexName);
        assertThat((String) r.getProperty("typeName")).isEqualTo("CompoundIndexTestType");
        assertThat((boolean) r.getProperty("unique")).isTrue();

        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  /**
   * Issue #5469: an index can carry any user-supplied name, including characters that are not valid in an unquoted identifier.
   * Back-tick quoting must let those names be addressed too.
   */
  @Test
  void selectFromSchemaIndexDetailWithQuotedCustomName() {
    final String indexName = "my weird index:name-1";

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("QuotedIndexTestType");
      type.createProperty("name", Type.STRING);
      database.command("sql", "CREATE INDEX `" + indexName + "` ON QuotedIndexTestType (name) UNIQUE");
      database.newDocument("QuotedIndexTestType").set("name", "test").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:`" + indexName + "`")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("name")).isEqualTo(indexName);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Issue #5469: the index listing must expose every column the Studio "Indexes" tab renders, so the tab needs one query instead
   * of one detail query per index. {@code valid} was the only field missing from {@code schema:indexes}.
   */
  @Test
  void selectFromSchemaIndexesCarriesListingColumns() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("ListedIndexTestType");
      type.createProperty("propA", Type.STRING);
      type.createProperty("propB", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "propA", "propB");
    });

    final String indexName = database.getSchema().getType("ListedIndexTestType").getAllIndexes(true).iterator().next().getName();

    final Result listed;
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes WHERE name = '" + indexName + "'")) {
      assertThat(rs.hasNext()).isTrue();
      listed = rs.next();
      assertThat(rs.hasNext()).isFalse();
    }

    final Result detail;
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:`" + indexName + "`")) {
      assertThat(rs.hasNext()).isTrue();
      detail = rs.next();
    }

    // fileId/size are absent on a type-level index (they belong to its per-bucket sub-indexes): the point is that the listing and
    // the detail agree, so the Studio table renders the same either way.
    for (final String column : new String[] { "name", "indexType", "typeName", "unique", "compacting", "valid", "fileId",
        "size" }) {
      assertThat(listed.hasProperty(column)).as(column).isEqualTo(detail.hasProperty(column));
      if (detail.hasProperty(column))
        assertThat((Object) listed.getProperty(column)).as(column).isEqualTo(detail.getProperty(column));
    }
    assertThat((boolean) listed.getProperty("valid")).isTrue();
  }

  @Test
  void selectFromSchemaBucketDetailQuoted() {
    database.transaction(() -> database.getSchema().createDocumentType("QuotedBucketTestType"));

    final String bucketName = database.getSchema().getType("QuotedBucketTestType").getBuckets(false).getFirst().getName();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:bucket:`" + bucketName + "`")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("name")).isEqualTo(bucketName);
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
