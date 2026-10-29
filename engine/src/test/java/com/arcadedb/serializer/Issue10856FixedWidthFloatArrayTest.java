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
package com.arcadedb.serializer;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10856: an {@code ARRAY_OF_FLOATS} value stored each element as a zigzag varint of its IEEE-754 bit pattern, which
 * for any ordinary non-zero float is a large integer: 5 bytes per float, so a 1536-d embedding row took 7,695 bytes instead
 * of about 6,150, and 7 rows fit a 64 KiB bucket page instead of 9. {@code ARRAY_OF_DOUBLES} had the same shape at 9-10
 * bytes per double. Both are now written as the element count followed by the raw elements, under two new type ids, and the
 * varint ids are still read, so a database written before needs no migration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10856FixedWidthFloatArrayTest extends TestHelper {
  private static final int DIM = 1536;

  @AfterEach
  void restoreThePolicy() {
    serializer().setFixedWidthFloatArrays(null);
  }

  /** The figure of the issue: a 1536-d float vector costs its 4 bytes per float, not 5. */
  @Test
  void aFloatVectorCostsFourBytesPerElement() {
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
    database.getSchema().getType("Doc").createProperty("id", Type.INTEGER);

    final MutableDocument doc = database.newDocument("Doc").set("id", 1).set("embedding", unitVector(new Random(7), DIM));

    final int fixed = serializer().serialize((DatabaseInternal) database, doc).size();
    serializer().setFixedWidthFloatArrays(() -> false);
    final int varint = serializer().serialize((DatabaseInternal) database, doc).size();

    assertThat(fixed).as("4 bytes per float plus a few bytes of header").isBetween(4 * DIM, 4 * DIM + 32);
    assertThat(varint).as("the varint form, 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);
  }

  /** A double vector costs its 8 bytes per element, not 9 or 10. */
  @Test
  void aDoubleVectorCostsEightBytesPerElement() {
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_DOUBLES);
    final double[] vector = new double[DIM];
    final Random random = new Random(11);
    for (int i = 0; i < DIM; i++)
      vector[i] = random.nextGaussian() / Math.sqrt(DIM);

    final MutableDocument doc = database.newDocument("Doc").set("embedding", vector);
    final int fixed = serializer().serialize((DatabaseInternal) database, doc).size();

    assertThat(fixed).isBetween(8 * DIM, 8 * DIM + 32);
  }

  /** Every bit pattern comes back as it went in: signed zero, NaN, the infinities, subnormals and the extremes. */
  @Test
  void everyValueRoundTripsBitForBit() {
    final float[] floats = { 0f, -0f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE,
        -Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE, -Float.MAX_VALUE, 1f, -1f, 0.05f, 3.4e-39f, Float.intBitsToFloat(0x7fc00001) };
    final double[] doubles = { 0d, -0d, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE,
        -Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE, -Double.MAX_VALUE, 1d, -1d, 0.05d, Double.longBitsToDouble(0x7ff8000000000001L) };

    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("floats", Type.ARRAY_OF_FLOATS);
    type.createProperty("doubles", Type.ARRAY_OF_DOUBLES);
    type.createProperty("boxedFloats", Type.ARRAY_OF_FLOATS);
    type.createProperty("boxedDoubles", Type.ARRAY_OF_DOUBLES);

    final RID rid = saveInTransaction(() -> database.newDocument("Doc")
        .set("floats", floats).set("doubles", doubles)
        .set("boxedFloats", new Float[] { 1.5f, -2.25f, Float.NaN }).set("boxedDoubles", new Double[] { 1.5d, -0d })
        .set("empty", new float[0]).set("undeclared", new double[] { 7.125d, -1e-300 }));

    reopenDatabase();
    final Document doc = database.lookupByRID(rid, true).asDocument();

    assertBitsEqual((float[]) doc.get("floats"), floats);
    assertBitsEqual((double[]) doc.get("doubles"), doubles);
    assertBitsEqual((float[]) doc.get("boxedFloats"), new float[] { 1.5f, -2.25f, Float.NaN });
    assertBitsEqual((double[]) doc.get("boxedDoubles"), new double[] { 1.5d, -0d });
    assertThat((float[]) doc.get("empty")).isEmpty();
    assertBitsEqual((double[]) doc.get("undeclared"), new double[] { 7.125d, -1e-300 });
  }

  /** An array inside a list, a map and an embedded document is written in the same form, and read back the same. */
  @Test
  void nestedArraysRoundTrip() {
    database.getSchema().createDocumentType("Doc");
    database.getSchema().createDocumentType("Inner");
    final float[] vector = unitVector(new Random(3), 64);

    final RID rid = saveInTransaction(() -> {
      final MutableDocument doc = database.newDocument("Doc");
      // A float[] only: a double[] or long[] nested in a collection is turned into a list on set(), as a JSON array would be
      doc.set("list", List.of(vector, vector));
      doc.set("map", Map.of("v", vector));
      doc.newEmbeddedDocument("Inner", "inner").set("v", vector);
      return doc;
    });

    reopenDatabase();
    final Document doc = database.lookupByRID(rid, true).asDocument();

    assertBitsEqual((float[]) ((List<?>) doc.get("list")).get(0), vector);
    assertBitsEqual((float[]) ((List<?>) doc.get("list")).get(1), vector);
    assertBitsEqual((float[]) ((Map<?, ?>) doc.get("map")).get("v"), vector);
    assertBitsEqual((float[]) doc.getEmbedded("inner").get("v"), vector);
  }

  /**
   * A database written before the fixed-width form holds varint arrays, which are read as before; the first update of such a
   * record rewrites the value in the fixed-width form, so it shrinks.
   */
  @Test
  void aVarintRecordIsReadAndAnUpdateRewritesItInTheFixedForm() {
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
    final float[] vector = unitVector(new Random(5), DIM);

    // WRITTEN AS A BUILD BEFORE #10856 WROTE IT
    serializer().setFixedWidthFloatArrays(() -> false);
    final RID rid = saveInTransaction(() -> database.newDocument("Doc").set("embedding", vector));
    final int varintSize = storedSize(rid);
    assertThat(varintSize).isGreaterThanOrEqualTo(5 * DIM);

    serializer().setFixedWidthFloatArrays(null);
    reopenDatabase();
    assertBitsEqual((float[]) database.lookupByRID(rid, true).asDocument().get("embedding"), vector);

    database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("touched", true).save());
    assertThat(storedSize(rid)).isLessThan(4 * DIM + 64);
    assertBitsEqual((float[]) database.lookupByRID(rid, true).asDocument().get("embedding"), vector);
  }

  /** The page arithmetic of the issue: 9 rows of a 1536-d vector fit a 64 KiB bucket page, not 7. */
  @Test
  void nineVectorRowsFitABucketPage() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
    type.createProperty("id", Type.INTEGER);
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

    final int rows = 270;
    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newDocument("Doc").set("id", i).set("embedding", unitVector(random, DIM)).save();
    });

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    assertThat(bucket.getPageSize()).isEqualTo(65536);
    assertThat(bucket.getTotalPages()).as("%d rows at 9 per page", rows).isLessThanOrEqualTo(rows / 9 + 1);
  }

  /**
   * {@code arcadedb.serializer.fixedWidthFloatArrays=false} keeps the varint form a build before #10856 reads, whatever the
   * HA policy answers: the rollback path of a deployment that may have to go back to such a build.
   */
  @Test
  void theSettingKeepsTheVarintFormForARollback() {
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
    final float[] vector = unitVector(new Random(13), DIM);
    final MutableDocument doc = database.newDocument("Doc").set("embedding", vector);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERIALIZER_FIXED_WIDTH_FLOAT_ARRAYS, false);
    final BinarySerializer varintOnly = new BinarySerializer(configuration);
    varintOnly.setFixedWidthFloatArrays(() -> true);

    final Binary content = varintOnly.serialize((DatabaseInternal) database, doc).copyOfContent();
    assertThat(content.size()).as("the varint form, 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);

    // AND THE DATABASE'S OWN SERIALIZER, WITH THE DEFAULT, READS IT
    content.position(Binary.BYTE_SERIALIZED_SIZE); // PAST THE RECORD TYPE
    final Map<String, Object> read = serializer().deserializeProperties(database, content, null, null);
    assertBitsEqual((float[]) read.get("embedding"), vector);
  }

  /**
   * The setting saved in the database's {@code configuration.json} is honored as soon as it is set, and again after a
   * reopen, where that file is loaded after the serializer was built.
   */
  @Test
  void theSavedSettingIsHonoredAtOnceAndAfterAReopen() {
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
    final float[] vector = unitVector(new Random(17), DIM);

    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` false").close();
    final RID atOnce = saveInTransaction(() -> database.newDocument("Doc").set("embedding", vector));
    assertThat(storedSize(atOnce)).as("the varint form right after the change").isGreaterThanOrEqualTo(5 * DIM);

    reopenDatabase();
    final RID afterReopen = saveInTransaction(() -> database.newDocument("Doc").set("embedding", vector));
    assertThat(storedSize(afterReopen)).as("the varint form after a reopen").isGreaterThanOrEqualTo(5 * DIM);
    assertBitsEqual((float[]) database.lookupByRID(afterReopen, true).asDocument().get("embedding"), vector);

    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` true").close();
    final RID fixed = saveInTransaction(() -> database.newDocument("Doc").set("embedding", vector));
    assertThat(storedSize(fixed)).as("the fixed-width form once it is set back").isLessThan(4 * DIM + 64);
  }

  private RID saveInTransaction(final Supplier<MutableDocument> document) {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> rid.set(document.get().save().getIdentity()));
    return rid.get();
  }

  private int storedSize(final RID rid) {
    final Binary record = ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
    return record.size();
  }

  private BinarySerializer serializer() {
    return ((DatabaseInternal) database).getSerializer();
  }

  private static float[] unitVector(final Random random, final int dim) {
    final float[] vector = new float[dim];
    double norm = 0;
    for (int i = 0; i < dim; i++) {
      vector[i] = (float) random.nextGaussian();
      norm += vector[i] * vector[i];
    }
    final float inverse = (float) (1 / Math.sqrt(norm));
    for (int i = 0; i < dim; i++)
      vector[i] *= inverse;
    return vector;
  }

  private static void assertBitsEqual(final float[] actual, final float[] expected) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.length; i++)
      assertThat(Float.floatToRawIntBits(actual[i])).as("element %d", i).isEqualTo(Float.floatToRawIntBits(expected[i]));
  }

  private static void assertBitsEqual(final double[] actual, final double[] expected) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.length; i++)
      assertThat(Double.doubleToRawLongBits(actual[i])).as("element %d", i).isEqualTo(Double.doubleToRawLongBits(expected[i]));
  }
}
