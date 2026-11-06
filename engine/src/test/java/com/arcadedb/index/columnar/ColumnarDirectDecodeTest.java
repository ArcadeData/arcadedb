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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DefaultDataEncryption;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 2 of plan 7 (#10662), second lever (direct decode): the apply pass reads the projected values of a record straight
 * from its bytes, a string of an ordinal column looked up in the dictionary from its UTF-8 bytes, instead of building
 * an {@code ImmutableDocument} and a String per value. For every record kind, the values, failures and coerced flags it produces must
 * be those of the record path, and a record the direct read cannot serve exactly (external properties) must be left to the record path.
 */
class ColumnarDirectDecodeTest extends TestHelper {
  @Test
  void directDecodeEqualsRecordDecodeForEveryRecordKind() throws Exception {
    // vertices with in and out edges, a DOUBLE and a DATE column, explicit nulls and missing properties
    final ColumnarTestData data = new ColumnarTestData(42);
    data.asVertices = true;
    data.edgesPerVertex = 2;
    data.doubleColumn = true;
    data.dateColumn = true;
    data.explicitNulls = true;
    data.createSchema(database);
    data.insertAssets(database, 10_000);

    // an edge type with COLUMNAR properties
    database.command("sql", "CREATE PROPERTY " + ColumnarTestData.EDGE_TYPE + ".label STRING");
    database.command("sql", "CREATE PROPERTY " + ColumnarTestData.EDGE_TYPE + ".weight LONG");
    database.command("sql", "CREATE INDEX ON " + ColumnarTestData.EDGE_TYPE + " (label) COLUMNAR");
    database.command("sql", "CREATE INDEX ON " + ColumnarTestData.EDGE_TYPE + " (weight) COLUMNAR");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex(ColumnarTestData.TYPE).set("brand", "edge-a").save();
      final MutableVertex b = database.newVertex(ColumnarTestData.TYPE).set("brand", "edge-b").save();
      for (int i = 0; i < 50; i++)
        a.newEdge(ColumnarTestData.EDGE_TYPE, b, "label", i % 3 == 0 ? "Lé" + i : "l" + (i % 7), "weight", (long) i);
      a.newEdge(ColumnarTestData.EDGE_TYPE, b);
    });

    // values stored before their property was declared: a number in a STRING column, a string in a LONG column, values that fail
    database.command("sql", "CREATE DOCUMENT TYPE Loose");
    database.transaction(() -> {
      database.newDocument("Loose").set("brand", 5).set("size", "12").set("tags", List.of("a", "b", "a", "ümläut", "😀"))
          .save();
      database.newDocument("Loose").set("brand", "Zürich").set("size", "not a number").set("score", "x").set("tags", "lone").save();
      final List<Object> mixed = new ArrayList<>(Arrays.asList("x", null, 7, 7L, "x", List.of("nested"), ""));
      database.newDocument("Loose").set("brand", List.of("in", "a", "list")).set("size", 3.5d).set("score", 2).set("tags", mixed).save();
      database.newDocument("Loose").set("brand", null).set("tags", List.of()).save();
      database.newDocument("Loose").save();
      database.newDocument("Loose").set("brand", "").set("size", 9L).set("score", -0.0d).set("tags", new String[] { "arr", "arr" }).save();
    });
    // through the schema API, which does not check the stored values (SQL refuses a declaration that existing records violate)
    final DocumentType looseType = database.getSchema().getType("Loose");
    looseType.createProperty("brand", Type.STRING);
    looseType.createProperty("size", Type.LONG);
    looseType.createProperty("score", Type.DOUBLE);
    looseType.createProperty("tags", Type.LIST);
    database.command("sql", "CREATE INDEX ON Loose (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Loose (size) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Loose (score) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Loose (tags BY ITEM) COLUMNAR");

    // a type whose bucket has a paired external-property bucket: left to the record path
    database.command("sql", "CREATE DOCUMENT TYPE Ext");
    database.command("sql", "CREATE PROPERTY Ext.brand STRING");
    database.command("sql", "CREATE PROPERTY Ext.body STRING (EXTERNAL true)");
    database.command("sql", "CREATE INDEX ON Ext (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Ext").set("brand", "e" + (i % 4)).set("body", "x".repeat(200)).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final int[] vertices = assertDecodesAgree(ColumnarTestData.TYPE, true);
    assertThat(vertices[0]).as("vertex records decoded directly").isGreaterThan(10_000);
    final int[] edges = assertDecodesAgree(ColumnarTestData.EDGE_TYPE, true);
    assertThat(edges[0]).as("edge records decoded directly, the 51 labelled ones included").isGreaterThan(51);
    final int[] loose = assertDecodesAgree("Loose", true);
    assertThat(loose[0]).isEqualTo(6);
    assertThat(loose[1]).as("coerced values").isPositive();
    assertThat(loose[2]).as("failing values").isPositive();
    final int[] external = assertDecodesAgree("Ext", false);
    assertThat(external[3]).as("records declined by the direct read").isEqualTo(20);
  }

  /**
   * The two declines that keep the direct read exact without a bucket-level sign: with data encryption every stored value is
   * ciphertext (a String read from its bytes would be hashed into the dictionary as garbage), and a type declaring a property of a
   * type that converts a stored String when read (OFFSET_TIME, LOCAL_TIME, ZONED_DATETIME, DURATION) reads values the bytes do not
   * hold. Both must be left to the record path, and the projection must still answer what GROUP BY answers.
   */
  @Test
  void directDecodeDeclinesEncryptedAndConvertingTypes() throws Exception {
    for (final String converting : new String[] { "OFFSET_TIME", "LOCAL_TIME", "ZONED_DATETIME", "DURATION" }) {
      final String typeName = "Conv_" + converting;
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
      database.command("sql", "CREATE PROPERTY " + typeName + ".brand STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".tags LIST OF STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".at " + converting);
      database.command("sql", "CREATE INDEX ON " + typeName + " (brand) COLUMNAR");
      database.command("sql", "CREATE INDEX ON " + typeName + " (tags BY ITEM) COLUMNAR");
      insertBrands(typeName, 30);
      FacetOracle.awaitCaughtUp(database);
      assertThat(((LocalDocumentType) database.getSchema().getType(typeName)).mayConvertStoredStrings()).as(typeName).isTrue();
      assertThat(assertDecodesAgree(typeName, false)[3]).as(typeName + " records declined").isEqualTo(30);
      FacetOracle.assertEquivalent(database, typeName, "brand", null);
      FacetOracle.assertEquivalent(database, typeName, "tags", null);
    }

    database.setDataEncryption(DefaultDataEncryption.useDefaults(DefaultDataEncryption.getSecretKeyFromPasswordUsingDefaults("password",
        "salt")));
    try {
      // a String column alone: a list column would decline on its own (its ciphertext count does not read) and hide a missing guard
      database.command("sql", "CREATE DOCUMENT TYPE Enc");
      database.command("sql", "CREATE PROPERTY Enc.brand STRING");
      database.command("sql", "CREATE INDEX ON Enc (brand) COLUMNAR");
      database.transaction(() -> {
        for (int i = 0; i < 30; i++)
          database.newDocument("Enc").set("brand", "b-" + (i % 4)).save();
      });
      FacetOracle.awaitCaughtUp(database);
      assertThat(assertDecodesAgree("Enc", false)[3]).as("encrypted records declined").isEqualTo(30);
      FacetOracle.assertEquivalent(database, "Enc", "brand", null);
      // the record path read the plain values, and no ciphertext was ever taken for a value
      final ColumnarProjection projection = manager().projection(database.getSchema().getType("Enc").getBuckets(false).getFirst().getFileId());
      final GlobalDictionary brands = projection.dictionary("brand");
      assertThat(brands.size()).isEqualTo(4);
      for (int i = 0; i < 4; i++)
        assertThat(brands.ordinalOf("b-" + i)).isNotNegative();
    } finally {
      // the integrity check after the test reads every record without the key: the encrypted ones go first
      database.command("sql", "DROP TYPE Enc UNSAFE");
      database.setDataEncryption(null);
    }
  }

  private void insertBrands(final String typeName, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(typeName).set("brand", "b-" + (i % 4)).set("tags", List.of("t" + (i % 3), "Z\u00fcrich")).save();
    });
  }

  /**
   * Under the apply lock: every record of the type's buckets decoded from its committed bytes by the direct path and by the record
   * path (alternating which goes first, so a dictionary value appended by one is found by the other), with the same values, failures
   * and coerced flags. Returns {records decoded directly, coerced values, failing values, records declined}.
   */
  private int[] assertDecodesAgree(final String typeName, final boolean direct) throws Exception {
    final DocumentType type = database.getSchema().getType(typeName);
    final int[] counts = new int[4];
    for (final var bucket : type.getBuckets(false)) {
      final ColumnarProjection projection = manager().projection(bucket.getFileId());
      assertThat(projection).as("projection of " + bucket.getName()).isNotNull();
      final ColumnDefinition[] columns = projection.columns();
      final GlobalDictionary[] dictionaries = new GlobalDictionary[columns.length];
      for (int k = 0; k < columns.length; k++)
        if (columns[k].kind() == ColumnKind.ORDINAL || columns[k].kind() == ColumnKind.ORDINAL_LIST)
          dictionaries[k] = projection.dictionary(columns[k].property());
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final ColumnarApplier.RowDecoder fast = new ColumnarApplier.RowDecoder(typeName, columns, dictionaries, projection.stats());
      final ColumnarApplier.RowDecoder slow = new ColumnarApplier.RowDecoder(typeName, columns, dictionaries, projection.stats());
      fast.prepare(local, type, projection.externalFileId() >= 0);
      slow.prepare(local, type, projection.externalFileId() >= 0);
      final LocalBucket localBucket = (LocalBucket) bucket;
      final List<Long> positions = new ArrayList<>();
      final Iterator<Record> iterator = database.iterateBucket(bucket.getName());
      while (iterator.hasNext())
        positions.add(iterator.next().getIdentity().getPosition());

      projection.applyLock().lock();
      try {
        int n = 0;
        for (final long position : positions) {
          final boolean directFirst = n++ % 2 == 0;
          final Binary[] read = new Binary[1];
          final int[] kinds = new int[1];
          localBucket.visitCommittedRecord(position, (at, kind, content, offPagePages, offPageCount) -> {
            kinds[0] = kind;
            read[0] = content == null ? null : content.copyOfContent();
          });
          assertThat(kinds[0]).isEqualTo(LocalBucket.CommittedSlotVisitor.RECORD);
          assertThat(read[0]).isNotNull();
          compare(local, type, bucket.getFileId(), position, read[0], fast, slow, columns, directFirst, direct, counts);
        }
      } finally {
        projection.applyLock().unlock();
      }
    }
    return counts;
  }

  private void compare(final LocalDatabase local, final DocumentType type, final int bucketId, final long at, final Binary content,
      final ColumnarApplier.RowDecoder fast, final ColumnarApplier.RowDecoder slow, final ColumnDefinition[] columns,
      final boolean directFirst, final boolean direct, final int[] counts) {
    final String typeName = type.getName();
    content.position(0);
    if (!directFirst)
      decodeRecord(local, type, bucketId, at, content, slow);
    final boolean decoded = fast.decode(content);
    assertThat(content.position()).as("the direct read leaves the content where it was").isZero();
    if (directFirst)
      decodeRecord(local, type, bucketId, at, content, slow);
    if (!direct) {
      assertThat(decoded).as("record " + at + " of " + typeName + ": declined").isFalse();
      counts[3]++;
      return;
    }
    assertThat(decoded).as("record " + at + " of " + typeName + " decoded directly").isTrue();
    counts[0]++;
    for (int k = 0; k < columns.length; k++) {
      final String label = typeName + " #" + at + " " + columns[k].property();
      assertThat(fast.coerced[k]).as(label + " coerced").isEqualTo(slow.coerced[k]);
      assertThat(fast.failures[k]).as(label + " failures").isEqualTo(slow.failures[k]);
      assertSameValue(columns[k], k, slow.fresh, fast.fresh, label);
      if (fast.coerced[k])
        counts[1]++;
      counts[2] += fast.failures[k];
    }
  }

  private void decodeRecord(final LocalDatabase local, final DocumentType type, final int bucketId, final long position, final Binary content,
      final ColumnarApplier.RowDecoder decoder) {
    final int before = content.position();
    final Record record = local.getRecordFactory()
        .newImmutableRecord(local.getWrappedDatabaseInstance(), type, new RID(bucketId, position), content, null);
    decoder.decode((Document) record);
    content.position(before);
  }

  private static void assertSameValue(final ColumnDefinition column, final int k, final RowValues expected, final RowValues actual,
      final String label) {
    assertThat(actual.present[k]).as(label + " present").isEqualTo(expected.present[k]);
    if (!expected.present[k])
      return;
    switch (column.kind()) {
    case ORDINAL -> assertThat(actual.ordinal[k]).as(label).isEqualTo(expected.ordinal[k]);
    case LONG -> assertThat(actual.longs[k]).as(label).isEqualTo(expected.longs[k]);
    case DOUBLE -> assertThat(Double.doubleToLongBits(actual.doubles[k])).as(label).isEqualTo(Double.doubleToLongBits(expected.doubles[k]));
    case ORDINAL_LIST -> assertThat(Arrays.copyOf(actual.list[k], actual.listCount[k])).as(label)
        .containsExactly(Arrays.copyOf(expected.list[k], expected.listCount[k]));
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
