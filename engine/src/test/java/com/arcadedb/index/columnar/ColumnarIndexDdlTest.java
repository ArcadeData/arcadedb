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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeColumnarIndexBuilder;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DDL of the COLUMNAR index type: SQL and Java creation, METADATA, refusals and persistence. */
class ColumnarIndexDdlTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 3");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.author STRING");
    database.command("sql", "CREATE PROPERTY Asset.tags LIST OF STRING");
    database.command("sql", "CREATE PROPERTY Asset.price DECIMAL");
    database.command("sql", "CREATE PROPERTY Asset.size LONG");
    database.command("sql", "CREATE PROPERTY Asset.columnar STRING");
  }

  @Test
  void createIndexColumnarBySql() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    final TypeIndex index = typeIndex("Asset[brand]_columnar");
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(index.isUnique()).isFalse();
    assertThat(index.getSubIndexes()).hasSize(3);
    assertThat(index.getSubIndexes()).allSatisfy(sub -> {
      assertThat(sub).isInstanceOf(ColumnarIndex.class);
      assertThat(sub.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    });

    final ColumnDefinition column = columnar(index).columnDefinition();
    assertThat(column.property()).isEqualTo("brand");
    assertThat(column.kind()).isEqualTo(ColumnKind.ORDINAL);
    assertThat(column.byItem()).isFalse();
    assertThat(column.indexFileId()).isEqualTo(((IndexInternal) index.getSubIndexes().getFirst()).getFileId());
  }

  @Test
  void createIndexColumnarByJavaApi() {
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Asset", new String[] { "size" });
    final TypeColumnarIndexBuilder columnarBuilder = builder.withType(Schema.INDEX_TYPE.COLUMNAR).withColumnarType();
    columnarBuilder.withMaxCardinality(50).create();

    final ColumnarIndex sub = columnar(typeIndex("Asset[size]_columnar"));
    assertThat(sub.columnDefinition().kind()).isEqualTo(ColumnKind.LONG);
    assertThat(sub.getColumnarMetadata().getMaxCardinality()).isEqualTo(50);
    assertThat(sub.getColumnarMetadata().getHierarchy()).isNull();
  }

  @Test
  void byItemList() {
    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR");

    final ColumnDefinition column = columnar(typeIndex("Asset[tagsbyitem]_columnar")).columnDefinition();
    assertThat(column.property()).isEqualTo("tags");
    assertThat(column.byItem()).isTrue();
    assertThat(column.kind()).isEqualTo(ColumnKind.ORDINAL_LIST);
  }

  @Test
  void hierarchyMetadataIsPersisted() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}, \"maxCardinality\": 500}");

    final JSONObject expected = new JSONObject().put("vertexType", "Tag").put("key", "path").put("parentEdge", "ParentOf");
    assertThat(columnar(typeIndex("Asset[brand]_columnar")).getColumnarMetadata().getHierarchy()).isEqualTo(expected);

    reopenDatabase();

    final TypeIndex reopened = typeIndex("Asset[brand]_columnar");
    assertThat(reopened.getSubIndexes()).allSatisfy(sub -> {
      final ColumnarIndexMetadata metadata = ((ColumnarIndex) sub).getColumnarMetadata();
      assertThat(metadata.getHierarchy()).isEqualTo(expected);
      assertThat(metadata.getMaxCardinality()).isEqualTo(500);
    });
  }

  @Test
  void hierarchyAcceptsAParentDirection() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\",\"parentDirection\":\"IN\"}}");

    assertThat(columnar(typeIndex("Asset[brand]_columnar")).getColumnarMetadata().getHierarchy().getString("parentDirection")).isEqualTo("IN");
  }

  @Test
  void unknownMetadataKeyIsAParsingError() {
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"foo\": 1}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("foo");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
  }

  @Test
  void malformedHierarchyIsAParsingError() {
    // a required key missing, an unknown key, a value that is not a string, a hierarchy that is not an object
    for (final String hierarchy : List.of("{\"vertexType\":\"Tag\",\"key\":\"path\"}",
        "{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\",\"depth\":\"3\"}",
        "{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":7}", "\"Tag\""))
      assertThatThrownBy(
          () -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"hierarchy\":" + hierarchy + "}"))
          .as(hierarchy).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("hierarchy");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 0}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("maxCardinality");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
  }

  @Test
  void metadataOnAnotherIndexTypeStillRefused() {
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE METADATA {\"maxCardinality\": 5}"))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void compositeKeyIsRefused() {
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand, author) COLUMNAR"))
        .isInstanceOf(IndexException.class).hasMessageContaining("one index per property");
    assertThat(database.getSchema().existsIndex("Asset[brand,author]_columnar")).isFalse();
  }

  @Test
  void uniqueIsRefused() {
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Asset", new String[] { "brand" });
    final TypeIndexBuilder columnar = builder.withType(Schema.INDEX_TYPE.COLUMNAR);
    columnar.withUnique(true);
    assertThatThrownBy(columnar::create).isInstanceOf(IndexException.class).hasMessageContaining("unique");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
  }

  @Test
  void unsupportedTypeIsRefused() {
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (price) COLUMNAR"))
        .isInstanceOf(IndexException.class).hasMessageContaining("DECIMAL");
    // a LIST without BY ITEM has no single value per record
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (tags) COLUMNAR"))
        .isInstanceOf(IndexException.class).hasMessageContaining("BY ITEM");
    assertThat(database.getSchema().existsIndex("Asset[price]_columnar")).isFalse();
    assertThat(database.getSchema().existsIndex("Asset[tags]_columnar")).isFalse();
  }

  @Test
  void cardinalityLimitRefusesCreation() {
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        // every value twice: duplicates must not count
        for (int copy = 0; copy < 2; copy++)
          database.newDocument("Asset").set("brand", "b" + i).save();
      database.newDocument("Asset").save();
    });

    // exactly at the limit: created
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 10}");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isTrue();
    database.command("sql", "DROP INDEX `Asset[brand]_columnar`");

    database.transaction(() -> database.newDocument("Asset").set("brand", "b10").save());

    // one over: refused, and nothing is left behind
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 10}"))
        .isInstanceOf(IndexException.class).hasMessageContaining("more than 10 distinct values");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
  }

  @Test
  void cardinalityOfAListCountsItems() {
    database.transaction(() -> {
      final List<String> first = new ArrayList<>();
      final List<String> second = new ArrayList<>();
      for (int i = 0; i < 6; i++)
        first.add("t" + i);
      // overlaps the first list on t5: 6 + 6 - 1 = 11 distinct items
      for (int i = 5; i < 11; i++)
        second.add("t" + i);
      database.newDocument("Asset").set("tags", first).save();
      database.newDocument("Asset").set("tags", second).save();
    });

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA {\"maxCardinality\": 10}"))
        .isInstanceOf(IndexException.class).hasMessageContaining("more than 10 distinct values");

    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA {\"maxCardinality\": 11}");
    assertThat(database.getSchema().existsIndex("Asset[tagsbyitem]_columnar")).isTrue();
  }

  @Test
  void cardinalityOfANestedPathIsChecked() {
    database.command("sql", "CREATE PROPERTY Asset.meta MAP");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Asset").set("meta", Map.of("brand", "b" + i)).save();
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (`meta.brand`) COLUMNAR METADATA {\"maxCardinality\": 2}"))
        .isInstanceOf(IndexException.class).hasMessageContaining("more than 2 distinct values");
    database.command("sql", "CREATE INDEX ON Asset (`meta.brand`) COLUMNAR METADATA {\"maxCardinality\": 3}");
    assertThat(columnar(typeIndex("Asset[meta.brand]_columnar")).columnDefinition().kind()).isEqualTo(ColumnKind.ORDINAL);
  }

  @Test
  void cardinalityOfAnUndeclaredPropertyIsChecked() {
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Asset").set("free", "f" + i).save();
    });

    final TypeIndexBuilder refused = database.getSchema().buildTypeIndex("Asset", new String[] { "free" })
        .withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.COLUMNAR);
    refused.withColumnarType().withMaxCardinality(2);
    assertThatThrownBy(refused::create).isInstanceOf(IndexException.class).hasMessageContaining("more than 2 distinct values");
    assertThat(database.getSchema().existsIndex("Asset[free]_columnar")).isFalse();

    final TypeIndexBuilder accepted = database.getSchema().buildTypeIndex("Asset", new String[] { "free" })
        .withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.COLUMNAR);
    accepted.withColumnarType().withMaxCardinality(3);
    accepted.create();
    assertThat(database.getSchema().existsIndex("Asset[free]_columnar")).isTrue();
  }

  @Test
  void byKeyIsRefusedByTheBuilderAndTheFactory() {
    database.command("sql", "CREATE PROPERTY Asset.attributes MAP");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Asset (attributes BY KEY) COLUMNAR"))
        .isInstanceOf(IndexException.class).hasMessageContaining("BY KEY");
    // the factory refuses it too, for a builder that comes without the type builder's checks
    final String bucket = database.getSchema().getType("Asset").getBuckets(false).getFirst().getName();
    final IndexBuilder<?> builder = database.getSchema().buildBucketIndex("Asset", bucket, new String[] { "attributes" })
        .withType(Schema.INDEX_TYPE.COLUMNAR).withKeyTypes(new Type[] { Type.STRING })
        .withMetadata(new IndexMetadata("Asset", new String[] { "attributes by key" }, -1));
    assertThatThrownBy(() -> new ColumnarIndexFactoryHandler().create(builder)).isInstanceOf(IndexException.class)
        .hasMessageContaining("BY KEY");
  }

  @Test
  void aManualColumnarIndexIsRefused() {
    // refused by the manual builder already, and by the factory for any builder without a type
    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualColumnar", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.COLUMNAR).create()).hasMessageContaining("manual");
    final String bucket = database.getSchema().getType("Asset").getBuckets(false).getFirst().getName();
    final IndexBuilder<?> typeless = database.getSchema().buildBucketIndex("Asset", bucket, new String[] { "brand" })
        .withType(Schema.INDEX_TYPE.COLUMNAR).withKeyTypes(new Type[] { Type.STRING })
        .withMetadata(new IndexMetadata(null, null, -1));
    assertThatThrownBy(() -> new ColumnarIndexFactoryHandler().create(typeless)).isInstanceOf(IndexException.class)
        .hasMessageContaining("manual");
    assertThat(database.getSchema().existsIndex("manualColumnar")).isFalse();
  }

  @Test
  void survivesReopen() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final List<Integer> fileIds = subIndexFileIds(typeIndex("Asset[brand]_columnar"));

    reopenDatabase();

    final TypeIndex reopened = typeIndex("Asset[brand]_columnar");
    assertThat(reopened.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(reopened.getSubIndexes()).allSatisfy(sub -> assertThat(sub).isInstanceOf(ColumnarIndex.class));
    assertThat(subIndexFileIds(reopened)).isEqualTo(fileIds);
    assertThat(columnar(reopened).columnDefinition().kind()).isEqualTo(ColumnKind.ORDINAL);
  }

  @Test
  void propertyNamedAfterKeywordStillParses() {
    database.command("sql", "CREATE INDEX ON Asset (columnar) COLUMNAR");
    assertThat(typeIndex("Asset[columnar]_columnar").getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
  }

  @Test
  void rebuildKeepsTheTypeAndSettings() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 9}");
    database.command("sql", "REBUILD INDEX `Asset[brand]_columnar`");

    final TypeIndex rebuilt = typeIndex("Asset[brand]_columnar");
    assertThat(rebuilt.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(rebuilt.getSubIndexes()).hasSize(3).allSatisfy(sub -> assertThat(sub).isInstanceOf(ColumnarIndex.class));
    assertThat(columnar(rebuilt).getColumnarMetadata().getMaxCardinality()).isEqualTo(9);
  }

  @Test
  void ifNotExistsComparesTheNamedSettings() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 9}");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 9}");
    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 8}"))
        .hasMessageContaining("maxCardinality");
    assertThat(columnar(typeIndex("Asset[brand]_columnar")).getColumnarMetadata().getMaxCardinality()).isEqualTo(9);
  }

  @Test
  void dropIndexRemovesIt() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "DROP INDEX `Asset[brand]_columnar`");
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().existsIndex("Asset[brand]_columnar")).isFalse();
  }

  @Test
  void keyLookupOnTheIndexIsRefused() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final ColumnarIndex sub = columnar(typeIndex("Asset[brand]_columnar"));
    assertThatThrownBy(() -> sub.get(new Object[] { "x" })).isInstanceOf(IndexException.class).hasMessageContaining("facets");
  }

  @Test
  void definitionIsSerialized() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 7}");
    final JSONObject json = columnar(typeIndex("Asset[brand]_columnar")).toJSON();
    assertThat(json.getString("type")).isEqualTo("COLUMNAR");
    assertThat(json.getBoolean("unique")).isFalse();
    assertThat(json.getInt("maxCardinality")).isEqualTo(7);
    assertThat(json.has("hierarchy")).isFalse();
    assertThat(json.getJSONArray("properties").getString(0)).isEqualTo("brand");
  }

  @Test
  void cypherShowIndexesNamesTheType() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    try (final ResultSet rs = database.command("opencypher", "SHOW INDEXES YIELD name, type, indexProvider")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Asset[brand]_columnar");
      assertThat(row.<String>getProperty("type")).isEqualTo("COLUMNAR");
      assertThat(row.<String>getProperty("indexProvider")).isEqualTo("columnar-1.0");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void cypherShowIndexesReportsItOnlineWithARunnableStatement() {
    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA {\"maxCardinality\": 12}");
    final String statement;
    try (final ResultSet rs = database.command("opencypher", "SHOW INDEXES YIELD name, state, createStatement")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("state")).isEqualTo("ONLINE");
      statement = row.getProperty("createStatement");
    }
    assertThat(statement).isEqualTo(
        "CREATE INDEX `Asset[tagsbyitem]_columnar` ON `Asset` (`tags` BY ITEM) COLUMNAR METADATA {\"maxCardinality\":12}");

    database.command("sql", "DROP INDEX `Asset[tagsbyitem]_columnar`");
    database.command("sql", statement);
    final ColumnarIndex recreated = columnar(typeIndex("Asset[tagsbyitem]_columnar"));
    assertThat(recreated.columnDefinition().kind()).isEqualTo(ColumnKind.ORDINAL_LIST);
    assertThat(recreated.getColumnarMetadata().getMaxCardinality()).isEqualTo(12);
  }

  private TypeIndex typeIndex(final String name) {
    final Index index = database.getSchema().getIndexByName(name);
    assertThat(index).isInstanceOf(TypeIndex.class);
    return (TypeIndex) index;
  }

  private static ColumnarIndex columnar(final TypeIndex index) {
    return (ColumnarIndex) index.getSubIndexes().getFirst();
  }

  private static List<Integer> subIndexFileIds(final TypeIndex index) {
    final List<Integer> ids = new ArrayList<>();
    for (final IndexInternal sub : index.getIndexesOnBuckets())
      ids.add(sub.getFileId());
    ids.sort(Integer::compare);
    return ids;
  }
}
