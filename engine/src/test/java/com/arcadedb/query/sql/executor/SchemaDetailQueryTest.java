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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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

  /**
   * Issue #6005: a manual index is bound to no type and no properties, but {@code IndexMetadata} coerces the missing property list
   * to an empty one, so {@code getPropertyNames()} is never null. The never-null guard therefore published
   * {@code properties: [[]]} - an empty list wrapped in a singleton list - instead of omitting the key, on both the detail and the
   * listing query.
   */
  @Test
  void manualIndexOmitsEmptyProperties() {
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).create();

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:index:manualIdx")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("name")).isEqualTo("manualIdx");
      assertThat(r.hasProperty("properties")).isFalse();

      assertThat(rs.hasNext()).isFalse();
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes WHERE name = 'manualIdx'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();

      assertThat((String) r.getProperty("name")).isEqualTo("manualIdx");
      assertThat(r.hasProperty("properties")).isFalse();

      assertThat(rs.hasNext()).isFalse();
    }
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
    }).isInstanceOf(SchemaException.class);
  }

  // ---- #7654: SELECT FROM schema:triggers ----
  // The catalog listing that closes the hole found while sweeping #7654's exception messages.
  // RemoteSchema.existsTrigger() already issued this exact query, so on a remote database it answered
  // "UnsupportedOperationException: Invalid metadata: triggers" instead of true or false: the target was never
  // registered in SelectExecutionPlanner.handleSchemaAsTarget. CREATE TRIGGER and DROP TRIGGER were both reachable
  // over the wire while nothing could list what they had produced.

  private static final String TRIGGER_CATALOG_TYPE = "Issue7654Audited";

  private void createTriggerCatalogFixture() {
    database.getSchema().createDocumentType(TRIGGER_CATALOG_TYPE).createProperty("v", Type.INTEGER);
  }

  /** Regression test for issue #7654. */
  @Test
  void theCatalogListsEveryTriggerAndItsDefinition() {
    createTriggerCatalogFixture();
    database.command("sql", "CREATE TRIGGER bumpOnCreate BEFORE CREATE ON TYPE " + TRIGGER_CATALOG_TYPE + " EXECUTE SQL 'SELECT 1'");
    database.command("sql", "CREATE TRIGGER auditOnUpdate AFTER UPDATE ON TYPE " + TRIGGER_CATALOG_TYPE + " EXECUTE SQL 'SELECT 2'");

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:triggers")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(2);

      // Ordered by name, case-insensitively, like every sibling catalog listing.
      assertThat(rows.stream().map(r -> r.<String>getProperty("name")).toList())
          .containsExactly("auditOnUpdate", "bumpOnCreate");

      final Result audit = rows.getFirst();
      assertThat(audit.<String>getProperty("typeName")).isEqualTo(TRIGGER_CATALOG_TYPE);
      assertThat(audit.<String>getProperty("timing")).isEqualTo("AFTER");
      assertThat(audit.<String>getProperty("event")).isEqualTo("UPDATE");
      assertThat(audit.<String>getProperty("actionType")).isEqualTo("SQL");
      assertThat(audit.<String>getProperty("actionCode")).contains("SELECT 2");
    }
  }

  /**
   * Regression test for issue #7654.
   * <p>
   * The shape {@code RemoteSchema.existsTrigger()} depends on: before this target existed, the query it issues threw
   * rather than answering, so the remote method could never return false either.
   */
  @Test
  void theCatalogAnswersTheExistenceCheckTheRemoteClientIssues() {
    createTriggerCatalogFixture();
    try (final ResultSet none = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'absent'")) {
      assertThat(none.hasNext()).isFalse();
    }

    database.command("sql", "CREATE TRIGGER presentTrigger BEFORE CREATE ON TYPE " + TRIGGER_CATALOG_TYPE + " EXECUTE SQL 'SELECT 1'");

    try (final ResultSet found = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'presentTrigger'")) {
      assertThat(found.hasNext()).isTrue();
    }

    database.command("sql", "DROP TRIGGER presentTrigger");

    try (final ResultSet gone = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'presentTrigger'")) {
      assertThat(gone.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #7654. */
  @Test
  void theCatalogIsEmptyRatherThanBrokenOnADatabaseWithNoTriggers() {
    createTriggerCatalogFixture();
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:triggers")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- #7898: schema: catalog listings honour LIMIT and SKIP ----
  // Every schema: catalog LISTING step materialised its whole listing on the first pull and then handed back a
  // ResultSet over the entire thing, ignoring the nRecords it was asked for. Both paging steps above it assume a batch
  // is bounded by what they requested, and broke in opposite directions: LimitExecutionStep counted the rows it
  // delivered but never applied its own cut-off, so LIMIT 2 over six types answered six rows; SkipExecutionStep
  // discarded the whole batch it received rather than the rows it still owed, so SKIP 1 answered none at all.
  // The listings are asserted together rather than one per test because the shape is shared - a new one of exactly
  // this kind was added for schema:triggers the same week the counting half was fixed, which is why the paging now
  // lives in a single base class.

  private static final int CATALOG_PAGING_TYPES = 6;

  private void createCatalogPagingFixture() {
    database.transaction(() -> {
      for (int i = 0; i < CATALOG_PAGING_TYPES; i++) {
        database.command("sql", "CREATE DOCUMENT TYPE T" + i);
        database.command("sql", "CREATE PROPERTY T" + i + ".name STRING");
        database.command("sql", "CREATE INDEX ON T" + i + " (name) NOTUNIQUE");
        database.command("sql", "CREATE TRIGGER trg" + i + " AFTER CREATE ON TYPE T" + i
            + " EXECUTE SQL \"SELECT 1\"");
      }
    });
  }

  /** Regression test for issue #7898. */
  @Test
  void limitBoundsEveryCatalogListing() {
    createCatalogPagingFixture();
    for (final String catalog : List.of("schema:types", "schema:buckets", "schema:indexes", "schema:triggers")) {
      assertThat(count("SELECT FROM " + catalog)).as(catalog + " without a limit").isGreaterThanOrEqualTo(CATALOG_PAGING_TYPES);
      assertThat(count("SELECT FROM " + catalog + " LIMIT 2")).as(catalog + " LIMIT 2").isEqualTo(2);
      assertThat(count("SELECT FROM " + catalog + " LIMIT 1")).as(catalog + " LIMIT 1").isEqualTo(1);
      assertThat(count("SELECT name FROM " + catalog + " LIMIT 2")).as(catalog + " projected LIMIT 2").isEqualTo(2);
    }
  }

  /** Regression test for issue #7898. */
  @Test
  void skipDropsOnlyTheRowsItOwes() {
    createCatalogPagingFixture();
    final List<String> all = names("SELECT name FROM schema:types");
    assertThat(all).hasSize(CATALOG_PAGING_TYPES);

    assertThat(names("SELECT name FROM schema:types SKIP 1")).isEqualTo(all.subList(1, CATALOG_PAGING_TYPES));
    assertThat(names("SELECT name FROM schema:types SKIP 4")).isEqualTo(all.subList(4, CATALOG_PAGING_TYPES));
    assertThat(names("SELECT name FROM schema:types SKIP " + CATALOG_PAGING_TYPES)).isEmpty();
  }

  /** Regression test for issue #7898. */
  @Test
  void skipAndLimitComposeOverACatalogListing() {
    createCatalogPagingFixture();
    final List<String> all = names("SELECT name FROM schema:types");

    assertThat(names("SELECT name FROM schema:types SKIP 1 LIMIT 2")).isEqualTo(all.subList(1, 3));
    assertThat(names("SELECT name FROM schema:types SKIP 4 LIMIT 5")).isEqualTo(all.subList(4, CATALOG_PAGING_TYPES));
    assertThat(names("SELECT name FROM schema:types LIMIT 3")).isEqualTo(all.subList(0, 3));
  }

  /**
   * Regression test for issue #7898.
   * <p>
   * A listing is no use paged if paging reshuffles it, and the whole-listing answer must not change either: the
   * cut-off is applied on the way out, so the rows and their order stay what an unpaged read reports.
   */
  @Test
  void pagingPreservesTheListingAndItsOrder() {
    createCatalogPagingFixture();
    final List<String> all = names("SELECT name FROM schema:types");
    final List<String> paged = new ArrayList<>();
    for (int skip = 0; skip < CATALOG_PAGING_TYPES; skip++)
      paged.addAll(names("SELECT name FROM schema:types SKIP " + skip + " LIMIT 1"));

    assertThat(paged).isEqualTo(all);
  }

  /**
   * Regression test for issue #7898.
   * <p>
   * ORDER BY forces the whole listing through a sort before the limit, which must not double-count either.
   */
  @Test
  void limitStillAppliesUnderAnOrderBy() {
    createCatalogPagingFixture();
    assertThat(count("SELECT name FROM schema:types ORDER BY name DESC LIMIT 2")).isEqualTo(2);
    assertThat(names("SELECT name FROM schema:types ORDER BY name DESC LIMIT 2"))
        .isEqualTo(List.of("T" + (CATALOG_PAGING_TYPES - 1), "T" + (CATALOG_PAGING_TYPES - 2)));
  }

  /**
   * Regression test for issue #7898.
   * <p>
   * A single-row catalog has no paging to do, but must not start answering nothing.
   */
  @Test
  void singleRowCatalogsAreUnaffected() {
    createCatalogPagingFixture();
    assertThat(count("SELECT FROM schema:database")).isEqualTo(1);
    assertThat(count("SELECT FROM schema:database LIMIT 5")).isEqualTo(1);
    assertThat(count("SELECT FROM schema:database SKIP 1")).isZero();
  }

  private int count(final String sql) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private List<String> names(final String sql) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
