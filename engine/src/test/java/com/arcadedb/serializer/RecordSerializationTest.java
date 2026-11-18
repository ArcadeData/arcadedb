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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableEdge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the record, array and graph element serialization, one nested class per reported issue.
 */
class RecordSerializationTest {
  /**
   * Regression for #10607: {@link BinaryTypes#getTypeFromValue} routes a {@link Result} to one of three labels - {@code TYPE_RID} for a record,
   * {@code TYPE_MAP} for a row that is not a record, {@code TYPE_EMBEDDED} for an element that carries no RID - and the
   * serializer unwrapped it for two of them only (#10264 fixed the map). The third cast the {@code Result} to a
   * {@code Document} and failed with a {@code ClassCastException}, so a projected subquery that yields a newly built element
   * could not be stored by {@code INSERT ... FROM}, {@code CREATE MATERIALIZED VIEW} or an {@code INSERT ... SET}.
   * <p>
   * The test is parameterised over the three kinds of {@code Result}, over every place a value is serialised from (a plain
   * property, a list entry, a map value) and over the statements that hand a {@code Result} to the serializer.
   */
  @Nested
  class ResultUnwrappedForEveryLabel extends TestHelper {

    private enum Kind {
      /** a record with an identity: typed {@code TYPE_RID} */
      RECORD,
      /** a row that is not a record: typed {@code TYPE_MAP} */
      ROW,
      /** an element that was never saved: typed {@code TYPE_EMBEDDED} */
      NEW_ELEMENT
    }

    private RID savedRid;

    @BeforeEach
    void schema() {
      database.getSchema().createDocumentType("Emb");
      database.getSchema().createDocumentType("Holder").createProperty("emb", Type.EMBEDDED).setOfType("Emb");
      database.getSchema().createDocumentType("Sink");
      database.getSchema().createDocumentType("Base").createProperty("id", Type.INTEGER);
      database.getSchema().createDocumentType("Owner");
      database.transaction(() -> {
        savedRid = database.newDocument("Owner").set("k", 7).save().getIdentity();
        for (int i = 0; i < 3; i++) {
          final MutableDocument holder = database.newDocument("Holder");
          holder.newEmbeddedDocument("Emb", "emb").set("x", i);
          holder.save();
          database.newDocument("Base").set("id", i).save();
        }
      });
    }

    private Result resultOf(final Kind kind) {
      return switch (kind) {
        case RECORD -> new ResultInternal(savedRid.asDocument());
        case ROW -> new ResultInternal(new HashMap<>(Map.of("x", 1)));
        case NEW_ELEMENT -> {
          final MutableEmbeddedDocument element = database.newDocument("Sink").newEmbeddedDocument("Emb", "tmp");
          element.set("x", 1);
          yield new ResultInternal(element);
        }
      };
    }

    /** the value the stored {@code Result} must read back as: the record, or the properties of the row or element */
    private void assertReadsBack(final Kind kind, final Object stored) {
      switch (kind) {
      case RECORD -> {
        assertThat(stored).isInstanceOfAny(RID.class, Document.class);
        assertThat(((Identifiable) stored).getIdentity()).isEqualTo(savedRid);
      }
      case ROW -> assertThat(stored).isInstanceOf(Map.class).extracting(m -> ((Map<?, ?>) m).get("x")).isEqualTo(1);
      case NEW_ELEMENT -> {
        assertThat(stored).isInstanceOf(Document.class);
        assertThat(((Document) stored).getInteger("x")).isEqualTo(1);
      }
      }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void aResultAsAPlainPropertyIsStored(final Kind kind) {
      final RID[] sink = new RID[1];
      database.transaction(() -> sink[0] = database.newDocument("Sink").set("value", resultOf(kind)).save().getIdentity());

      assertReadsBack(kind, sink[0].asDocument().get("value"));
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void aResultInAListIsStored(final Kind kind) {
      final RID[] sink = new RID[1];
      database.transaction(() -> sink[0] = database.newDocument("Sink").set("values", List.of(resultOf(kind))).save().getIdentity());

      final List<?> values = sink[0].asDocument().getList("values");
      assertThat(values).hasSize(1);
      assertReadsBack(kind, values.getFirst());
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void aResultAsAMapValueIsStored(final Kind kind) {
      final RID[] sink = new RID[1];
      database.transaction(() -> sink[0] = database.newDocument("Sink").set("values", Map.of("k", resultOf(kind))).save().getIdentity());

      final Map<String, Object> values = sink[0].asDocument().getMap("values");
      assertThat(values).hasSize(1);
      assertReadsBack(kind, values.get("k"));
    }

    /** one subquery per kind of {@code Result}: a record, a row, and a newly built element (an embedded document, expanded) */
    private static String subqueryOf(final Kind kind) {
      return switch (kind) {
        case RECORD -> "(SELECT FROM Owner)";
        case ROW -> "(SELECT count(*) AS c FROM Owner)";
        case NEW_ELEMENT -> "(SELECT expand(emb) FROM Holder)";
      };
    }

    private void assertStoredSubquery(final Kind kind, final String source, final int expectedEntries) {
      int rows = 0;
      try (final ResultSet rs = database.query("sql", "SELECT sub FROM " + source)) {
        while (rs.hasNext()) {
          final List<?> sub = rs.next().getProperty("sub");
          assertThat(sub).hasSize(expectedEntries);
          if (kind == Kind.ROW)
            assertThat(((Map<?, ?>) sub.getFirst()).get("c")).isEqualTo(1L);
          else if (kind == Kind.RECORD)
            assertThat(((Identifiable) sub.getFirst()).getIdentity()).isEqualTo(savedRid);
          else
            assertThat(sub).allSatisfy(e -> assertThat(e).isInstanceOf(Document.class));
          ++rows;
        }
      }
      assertThat(rows).isGreaterThan(0);
    }

    private int entriesOf(final Kind kind) {
      return kind == Kind.NEW_ELEMENT ? 3 : 1;
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void insertFromAProjectedSubquery(final Kind kind) {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT id, " + subqueryOf(kind) + " AS sub FROM Base").close());

      assertStoredSubquery(kind, "T", entriesOf(kind));
    }

    // a single-column row assigned with SET is the column's value, not a row (that is the SET rule, not the serializer's), so
    // the ROW kind is covered by the three routes above
    @ParameterizedTest
    @EnumSource(value = Kind.class, names = { "RECORD", "NEW_ELEMENT" })
    void insertSetOfASubquery(final Kind kind) {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET sub = " + subqueryOf(kind)).close());

      assertStoredSubquery(kind, "T", entriesOf(kind));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " REFRESH INCREMENTAL" })
    void materializedViewOfAProjectedSubquery(final String refresh) {
      for (final Kind kind : Kind.values()) {
        final String view = "mv" + kind.ordinal();
        database.command("sql", "CREATE MATERIALIZED VIEW " + view + " AS SELECT id, " + subqueryOf(kind) + " AS sub FROM Base" + refresh)
            .close();
        assertStoredSubquery(kind, view, entriesOf(kind));
      }
    }
  }

  /**
   * Regression for #10856: an {@code ARRAY_OF_FLOATS} value stored each element as a zigzag varint of its IEEE-754 bit pattern, which
   * for any ordinary non-zero float is a large integer: 5 bytes per float, so a 1536-d embedding row took 7,695 bytes instead
   * of about 6,150, and 7 rows fit a 64 KiB bucket page instead of 9. {@code ARRAY_OF_DOUBLES} had the same shape at 9-10
   * bytes per double. Both are now written as the element count followed by the raw elements, under two new type ids, and the
   * varint ids are still read, so a database written before needs no migration.
   */
  @Nested
  class FixedWidthFloatArray extends TestHelper {
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

  /**
   * Regression for #10905: an element row carries an {@code @props} hint only for a property the schema does not declare whose
   * value is a non-finite number (or a collection holding one), so the remote client can tell the marker string from text.
   */
  @Nested
  class NonFiniteElementRowHint extends TestHelper {

    private JSONObject serialize(final MutableDocument doc, final boolean hints) {
      return new JsonSerializer(database).setIncludeTypeHints(hints).serializeResult(database, new ResultInternal(doc));
    }

    private MutableDocument document() {
      database.getSchema().createDocumentType("Rec").createProperty("declared", Type.DOUBLE);
      return database.newDocument("Rec");
    }

    @Test
    void undeclaredNonFiniteGetsAHint() {
      final MutableDocument doc = document();
      doc.set("declared", Double.NaN).set("u", Double.POSITIVE_INFINITY).set("f", Float.NEGATIVE_INFINITY);
      final JSONObject json = serialize(doc, true);
      assertThat(json.getString("@props")).isEqualTo("u:" + Type.DOUBLE.getId() + ",f:" + Type.FLOAT.getId());
    }

    @Test
    void mixedCollectionGetsADoubleElementHint() {
      final MutableDocument doc = document();
      final List<Object> list = new ArrayList<>();
      list.add(Float.NaN);
      list.add(1.5D);
      list.add(2);
      doc.set("l", list);
      assertThat(serialize(doc, true).getString("@props")).isEqualTo("l:" + Type.LIST.getId() + "(" + Type.DOUBLE.getId() + ")");
    }

    @Test
    void collectionWithAnElementADoubleCannotHoldGetsNoHint() {
      final MutableDocument doc = document();
      final List<Object> list = new ArrayList<>();
      list.add(Double.NaN);
      list.add("text");
      doc.set("l", list);
      assertThat(serialize(doc, true).has("@props")).isFalse();
    }

    @Test
    void collectionWithALongADoubleCannotHoldExactlyGetsNoHint() {
      // (double) Long.MAX_VALUE is 2^63, which the cast back to long saturates to Long.MAX_VALUE: the round trip only looks exact
      final MutableDocument doc = document();
      final List<Object> list = new ArrayList<>();
      list.add(Double.NaN);
      list.add(Long.MAX_VALUE);
      doc.set("l", list);
      assertThat(serialize(doc, true).has("@props")).isFalse();
    }

    @Test
    void integerKindsADoubleHoldsExactlyKeepTheDoubleElementHint() {
      final MutableDocument doc = document();
      final List<Object> list = new ArrayList<>();
      list.add(Double.NaN);
      list.add((byte) 1);
      list.add((short) 2);
      list.add(3);
      list.add(4L);
      list.add(Long.MIN_VALUE);
      doc.set("l", list);
      assertThat(serialize(doc, true).getString("@props")).isEqualTo("l:" + Type.LIST.getId() + "(" + Type.DOUBLE.getId() + ")");
    }

    @Test
    void rowWithoutAnUndeclaredNonFiniteValueKeepsItsShape() {
      final MutableDocument doc = document();
      doc.set("declared", Double.NaN).set("u", 1.5D).set("s", "NaN");
      assertThat(serialize(doc, true).has("@props")).isFalse();
    }

    @Test
    void noHintWhenHintsAreOff() {
      final MutableDocument doc = document();
      doc.set("u", Double.NaN);
      assertThat(serialize(doc, false).has("@props")).isFalse();
    }
  }

  /**
   * Regression for #5600: a value stored in a {@code STRING} property must read back as the very same {@code String}. The
   * deserializer used to sniff the first characters of every string it read and, when they looked like the head of a WKT
   * geometry, return a spatial4j {@code Shape} instead - changing the declared type of the value at the storage layer and
   * mangling any free text that happened to start with {@code POLYGON}, {@code POINT}, and friends.
   */
  @Nested
  class StringWktRoundTrip extends TestHelper {

    @Test
    void declaredStringPropertyKeepsWktText() {
      database.getSchema().createDocumentType("T").createProperty("location", Type.STRING);

      final AtomicReference<RID> rid = new AtomicReference<>();
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("T");
        doc.set("location", "POINT (10.0 45.0)");
        doc.save();
        rid.set(doc.getIdentity());
      });

      final Document reloaded = database.lookupByRID(rid.get(), true).asDocument();
      assertThat(reloaded.get("location")).isInstanceOf(String.class);
      assertThat(reloaded.getString("location")).isEqualTo("POINT (10.0 45.0)");
    }

    @Test
    void freeTextStartingWithAGeometryKeywordIsNotTouched() {
      database.getSchema().createDocumentType("Note").createProperty("description", Type.STRING);

      final String[] texts = { //
          "POLYGON shaped, see attached", //
          "POINTs of interest along the way", //
          "LINESTRING is a WKT keyword", //
          "CIRCLE the wagons", //
          "ENVELOPE (please open it)", //
          "BUFFER overflow report" };

      database.transaction(() -> {
        for (final String text : texts) {
          final MutableDocument doc = database.newDocument("Note");
          doc.set("description", text);
          doc.save();
        }
      });

      final ResultSet rs = database.query("sql", "SELECT description FROM Note ORDER BY @rid");
      int i = 0;
      while (rs.hasNext())
        assertThat(rs.next().<Object>getProperty("description")).isEqualTo(texts[i++]);
      assertThat(i).isEqualTo(texts.length);
    }

    @Test
    void wktStringSurvivesInsideListsAndMaps() {
      database.getSchema().createDocumentType("Container");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Container");
        doc.set("list", List.of("POINT (1 2)", "plain"));
        doc.set("map", Map.of("k", "POLYGON ((0 0, 1 0, 1 1, 0 0))"));
        doc.save();
      });

      final Result row = database.query("sql", "SELECT list, map FROM Container").next();
      assertThat(row.<List<Object>>getProperty("list")).containsExactly("POINT (1 2)", "plain");
      assertThat(row.<Map<String, Object>>getProperty("map").get("k")).isEqualTo("POLYGON ((0 0, 1 0, 1 1, 0 0))");
    }

    @Test
    void wktStringIsIndexableAndLookupableAsAString() {
      database.getSchema().createDocumentType("Indexed").createProperty("code", Type.STRING);
      database.command("sql", "CREATE INDEX ON Indexed (code) UNIQUE");

      database.transaction(() -> database.command("sql", "INSERT INTO Indexed SET code = 'POINT (3 4)'"));

      final ResultSet rs = database.query("sql", "SELECT code FROM Indexed WHERE code = 'POINT (3 4)'");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("POINT (3 4)");
    }

    /**
     * A real spatial4j Shape has its own binary type and must keep coming back as a Shape.
     */
    @Test
    void shapeValueStillRoundTripsAsShape() {
      database.getSchema().createDocumentType("Geo");

      database.transaction(
          () -> database.command("sql", "INSERT INTO Geo SET position = geo.geomFromText('POINT (10.0 45.0)')"));

      final Object position = database.query("sql", "SELECT position FROM Geo").next().getProperty("position");
      assertThat(position).isInstanceOf(Shape.class);
      assertThat(((Point) position).getX()).isEqualTo(10.0);
      assertThat(((Point) position).getY()).isEqualTo(45.0);
    }

    /**
     * Geometry functions keep accepting a WKT string read back from a STRING property, so a pre-26.2.1 database - where
     * every shape was written as WKT text - keeps working without the deserializer converting anything.
     */
    @Test
    void geoFunctionsStillAcceptWktStoredAsText() {
      database.getSchema().createDocumentType("City").createProperty("coords", Type.STRING);

      database.transaction(() -> {
        database.command("sql", "INSERT INTO City SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
        database.command("sql", "INSERT INTO City SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      });

      final ResultSet rs = database.query("sql",
          "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Rome");
      assertThat(rs.hasNext()).isFalse();

      final ResultSet asText = database.query("sql", "SELECT geo.asText(coords) AS wkt FROM City WHERE name = 'Rome'");
      assertThat(asText.next().<String>getProperty("wkt")).contains("POINT");
    }

    /**
     * The two legacy SQL methods used to require an already-parsed Shape, which only the deserializer's sniff ever
     * produced from a WKT column. They now do the conversion themselves, like every geo.* function.
     */
    @Test
    void legacyShapeMethodsAcceptWktText() {
      database.getSchema().createDocumentType("Place").createProperty("coords", Type.STRING);

      database.transaction(() -> {
        database.command("sql", "INSERT INTO Place SET name = 'inside', coords = 'POINT (12.5 41.9)'");
        database.command("sql", "INSERT INTO Place SET name = 'outside', coords = 'POINT (9.2 45.5)'");
      });

      final String box = "'POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))'";

      final ResultSet within = database.query("sql",
          "SELECT name FROM Place WHERE coords.isWithin(geo.geomFromText(" + box + ")) = true");
      assertThat(within.next().<String>getProperty("name")).isEqualTo("inside");
      assertThat(within.hasNext()).isFalse();

      // The parameter may be WKT text too, not only a Shape
      final ResultSet intersects = database.query("sql",
          "SELECT name FROM Place WHERE coords.intersectsWith(" + box + ") = true");
      assertThat(intersects.next().<String>getProperty("name")).isEqualTo("inside");
      assertThat(intersects.hasNext()).isFalse();
    }

    /**
     * A malformed geometry written in the QUERY is a mistake to report, while a row that simply does not hold a geometry
     * is filtered out: failing the whole query over one bad row would be worse than skipping it.
     */
    @Test
    void aMalformedParameterIsReportedButABadRowIsJustSkipped() {
      database.getSchema().createDocumentType("Mixed").createProperty("coords", Type.STRING);

      database.transaction(() -> {
        database.command("sql", "INSERT INTO Mixed SET name = 'good', coords = 'POINT (12.5 41.9)'");
        database.command("sql", "INSERT INTO Mixed SET name = 'junk', coords = 'not a geometry at all'");
      });

      // The bad row does not match; the good one still does, and the query completes
      final ResultSet rs = database.query("sql",
          "SELECT name FROM Mixed WHERE coords.intersectsWith('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))') = true");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("good");
      assertThat(rs.hasNext()).isFalse();

      // A typo in the literal is surfaced rather than silently answering "no match"
      assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Mixed WHERE coords.isWithin('POLYGONN ((0 0))') = true")
          .hasNext()).hasMessageContaining("POLYGONN");
    }

    /**
     * The remaining operand shapes of the two relation methods: no parameter at all is a query mistake, while a null or
     * empty operand has nothing to relate and simply does not match.
     */
    @Test
    void relationMethodsHandleTheDegenerateOperands() {
      database.getSchema().createDocumentType("Deg").createProperty("coords", Type.STRING);

      database.transaction(() -> {
        database.command("sql", "INSERT INTO Deg SET name = 'here', coords = 'POINT (12.5 41.9)'");
        database.command("sql", "INSERT INTO Deg SET name = 'nowhere'");
      });

      // A missing parameter is a mistake in the query, the same way it always was
      assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Deg WHERE coords.isWithin() = true").hasNext())
          .hasMessageContaining("requires a shape as parameter");
      assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Deg WHERE coords.intersectsWith(null) = true")
          .hasNext()).hasMessageContaining("requires a shape as parameter");

      // An empty operand has no geometry to relate: no match, no error. The row without coords is skipped the same way.
      final ResultSet empty = database.query("sql", "SELECT name FROM Deg WHERE coords.isWithin('') = true");
      assertThat(empty.hasNext()).isFalse();

      final ResultSet present = database.query("sql",
          "SELECT name FROM Deg WHERE coords.intersectsWith('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))') = true");
      assertThat(present.next().<String>getProperty("name")).isEqualTo("here");
      assertThat(present.hasNext()).isFalse();
    }
  }

  /**
   * Regression for #7247: serializing one element of a primitive array used to catch every exception, log a message with no
   * throwable and throw a {@link SerializationException} with no cause. The element's own {@code toString()} is in
   * the message, but which nested {@code serializeValue} threw - and why - was lost on both paths.
   */
  @Nested
  class ArraySerializationCause {

    @Test
    void anArrayElementFailureKeepsItsCause() throws Exception {
      // A fixed buffer that cannot grow: the first few elements fit, then a write inside the loop fails, which is
      // the only way to reach the catch without a database or a schema.
      final Binary content = new Binary(8);
      content.setAutoResizable(false);

      final BinarySerializer serializer = new BinarySerializer(new ContextConfiguration());

      assertThatThrownBy(() -> serializer.serializeValue(null, content, BinaryTypes.TYPE_LIST, new long[64], false))
          .isInstanceOf(SerializationException.class)
          .hasMessageContaining("Error on serializing array value for element")
          .cause().isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("autoResizable=false");
    }

    @Test
    void aFittingArrayStillSerializes() throws Exception {
      final Binary content = new Binary(1024);

      new BinarySerializer(new ContextConfiguration()).serializeValue(null, content, BinaryTypes.TYPE_LIST,
          new long[] { 1L, 2L, 3L }, false);

      assertThat(content.size()).isGreaterThan(0);
    }
  }

  /**
   * Regression for #7778: {@link JsonSerializer#serializeDocument(Document)} called {@code serializeCollection()} for a
   * collection property and threw its result away, so the raw {@link java.util.Collection} reached
   * {@code JSONObject.put}'s generic {@code Iterable} branch. That branch ignores {@code useCollectionSize} and
   * renders an embedded document element with {@code toJSON(false)}, dropping its {@code @type} and {@code @cat}
   * - unlike the same embedded document stored directly on a property, and unlike the same record read through
   * {@code serializeResult()}.
   */
  @Nested
  class SerializeDocumentCollection extends TestHelper {
    private RID rid;

    @BeforeEach
    void createDocument() {
      database.transaction(() -> {
        final DocumentType doc = database.getSchema().createDocumentType("Issue7778Doc");
        doc.createProperty("items", Type.LIST);
        database.getSchema().createDocumentType("Issue7778Item");

        final MutableDocument d = database.newDocument("Issue7778Doc");
        // newEmbeddedDocument() needs an owner property to attach to: build each element on a scratch property,
        // then drop it, so the embedded documents end up only inside the list.
        final List<Object> items = new ArrayList<>();
        for (int n = 1; n <= 2; n++) {
          final MutableEmbeddedDocument item = d.newEmbeddedDocument("Issue7778Item", "tmp");
          item.set("n", n);
          item.set("at", LocalDateTime.of(2026, 9, 29, 10, 15, 30));
          items.add(item);
        }
        d.remove("tmp");
        d.set("items", items);

        // A LIST that nests a MAP whose value is an embedded document: the element has to be routed through
        // serializeMap -> serializeObject -> serializeDocument to keep its @type, not through new JSONObject(map).
        final Map<String, Object> nested = new HashMap<>();
        nested.put("inner", d.newEmbeddedDocument("Issue7778Item", "tmp").set("n", 3));
        d.remove("tmp");
        d.set("nested", List.of(nested));

        d.set("empty", new ArrayList<>());
        d.set("scalars", List.of(1, 2, 3));
        d.set("dates", List.of(LocalDateTime.of(2026, 9, 29, 10, 15, 30), LocalDateTime.of(2026, 9, 30, 11, 0, 0)));
        d.set("nonFinite", List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        // A primitive array (e.g. a vector embedding) is not a Collection: it must honour the same flags.
        d.set("vector", new float[] { 1.5F, 2.5F, 3.5F, 4.5F });
        d.set("bytes", new byte[] { 1, 2, 3, 4, 5 });
        rid = d.save().getIdentity();
      });
    }

    @Test
    void embeddedDocumentsInAListKeepTypeAndCategory() {
      final JSONObject json = JsonSerializer.createJsonSerializer().serializeDocument(rid.asDocument());

      final JSONArray items = json.getJSONArray("items");
      assertThat(items.length()).isEqualTo(2);
      for (int i = 0; i < items.length(); i++) {
        final JSONObject item = items.getJSONObject(i);
        assertThat(item.getString("@type")).isEqualTo("Issue7778Item");
        assertThat(item.getString("@cat")).isEqualTo("d");
        assertThat(item.getInt("n")).isEqualTo(i + 1);
        // the element goes through serializeDocument, so its temporal is formatted like the query path renders it,
        // not emitted as the epoch millis toJSON(false) produced
        assertThat(item.get("at")).isInstanceOf(String.class);
        assertThat(item.getString("at")).startsWith("2026-09-29");
      }

      final JSONObject inner = json.getJSONArray("nested").getJSONObject(0).getJSONObject("inner");
      assertThat(inner.getString("@type")).isEqualTo("Issue7778Item");
      assertThat(inner.getString("@cat")).isEqualTo("d");

      assertThat(json.getJSONArray("empty").length()).isEqualTo(0);
      assertThat(json.getJSONArray("scalars").toList()).containsExactly(1, 2, 3);
      assertThat(json.getJSONArray("vector").length()).isEqualTo(4);
      assertThat(json.getJSONArray("nonFinite").toList()).containsExactly("NaN", "PosInfinity", "NegInfinity");
    }

    @Test
    void useCollectionSizeIsHonouredForACollectionProperty() {
      final JSONObject json = JsonSerializer.createJsonSerializer().setUseCollectionSize(true)
          .serializeDocument(rid.asDocument());

      assertThat(json.getInt("items")).isEqualTo(2);
      assertThat(json.getInt("nested")).isEqualTo(1);
      assertThat(json.getInt("empty")).isEqualTo(0);
      assertThat(json.getInt("scalars")).isEqualTo(3);
      assertThat(json.getInt("nonFinite")).isEqualTo(3);
      assertThat(json.getInt("dates")).isEqualTo(2);
      assertThat(json.getInt("vector")).isEqualTo(4);
      assertThat(json.getInt("bytes")).isEqualTo(5);
    }

    /**
     * The document path and the query path must render the same record's collection properties identically,
     * for every combination of the two collection-size flags. A persisted record cannot hold Edge instances in a
     * property (links are stored as RIDs), so useCollectionSizeForEdges must never change the output here - the loop
     * pins exactly that. Object[]/String[] values are persisted as lists, so the primitive float[] is the only array
     * shape a loaded record can carry.
     */
    @Test
    void documentAndQueryPathsAgreeOnCollectionProperties() {
      for (final boolean collectionSize : new boolean[] { false, true }) {
        for (final boolean collectionSizeForEdges : new boolean[] { false, true }) {
          final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize)
              .setUseCollectionSizeForEdges(collectionSizeForEdges);

          final JSONObject fromDocument = serializer.serializeDocument(rid.asDocument());
          final JSONObject fromQuery;
          try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7778Doc")) {
            fromQuery = serializer.serializeResult(database, rs.next());
          }

          for (final String property : List.of("items", "nested", "empty", "scalars", "dates", "nonFinite", "vector", "bytes"))
            assertThat(fromDocument.toMap().get(property))
                .as("property '%s' with useCollectionSize=%s useCollectionSizeForEdges=%s", property, collectionSize,
                    collectionSizeForEdges)
                .isEqualTo(fromQuery.toMap().get(property));
        }
      }
    }
  }

  /**
   * Regression for #7908: {@code JavaBinarySerializer.writeExternal} branched on the INTERFACES {@code Vertex}
   * and {@code Edge} but cast to the concrete MUTABLE classes {@code MutableVertex}/{@code MutableEdge}. Every graph
   * record the engine hands back - from a scan, a query or {@code lookupByRID} - is an {@code ImmutableVertex},
   * {@code ImmutableEdge} or {@code ImmutableLightEdge}, so Java-serializing one threw a {@link ClassCastException}
   * from inside {@code writeObject}, while the identical document path worked. The narrowing bought nothing: the
   * head-chunk pointers are declared on {@code VertexInternal} and the endpoints on {@code Edge} itself.
   * <p>
   * Only the WRITE half was affected, which is why the round-trips below read back into a mutable instance:
   * {@code readExternal} rejects a non-{@code MutableDocument} target up front.
   */
  @Nested
  class SerializeImmutableGraphRecord extends TestHelper {

    private static final String VERTEX_TYPE = "Issue7908Node";
    private static final String EDGE_TYPE   = "Issue7908Link";
    private static final String LIGHT_TYPE  = "Issue7908LightLink";

    private RID v1Rid;
    private RID v2Rid;
    private RID edgeRid;

    @Override
    protected void beginTest() {
      final VertexType vertexType = database.getSchema().createVertexType(VERTEX_TYPE);
      vertexType.createProperty("id", Type.LONG);
      final EdgeType edgeType = database.getSchema().createEdgeType(EDGE_TYPE);
      edgeType.createProperty("weight", Type.LONG);
      database.getSchema().createEdgeType(LIGHT_TYPE);

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex(VERTEX_TYPE).set("id", 1L, "name", "John");
        v1.save();
        final MutableVertex v2 = database.newVertex(VERTEX_TYPE).set("id", 2L, "name", "Jay");
        v2.save();

        final MutableEdge edge = v1.newEdge(EDGE_TYPE, v2).set("weight", 7L);
        edge.save();

        v1.newLightEdge(LIGHT_TYPE, v2);

        v1Rid = v1.getIdentity();
        v2Rid = v2.getIdentity();
        edgeRid = edge.getIdentity();
      });
    }

    @Test
    void anImmutableVertexReadBackFromTheDatabaseSerializes() {
      database.transaction(() -> {
        final Vertex loaded = database.lookupByRID(v1Rid, true).asVertex();
        // The premise of the issue: what a read hands back is NOT a MutableVertex.
        assertThat(loaded).isInstanceOf(ImmutableVertex.class);
        assertThat(loaded).isNotInstanceOf(MutableVertex.class);

        // The exact shape of the repro: a plain ObjectOutputStream.writeObject, which is what a cache, a session
        // replication layer or an RMI/Spark-style transport does. This is where the ClassCastException came from.
        assertThat(javaSerialize(loaded)).isNotEmpty();

        final MutableVertex restored = database.newVertex(VERTEX_TYPE);
        roundTrip(loaded, restored);

        assertThat(restored.getIdentity()).isEqualTo(v1Rid);
        assertThat((Long) restored.get("id")).isEqualTo(1L);
        assertThat((String) restored.get("name")).isEqualTo("John");
        assertThat(restored.getOutEdgesHeadChunk()).isEqualTo(((VertexInternal) loaded).getOutEdgesHeadChunk());
        assertThat(restored.getInEdgesHeadChunk()).isEqualTo(((VertexInternal) loaded).getInEdgesHeadChunk());
      });
    }

    @Test
    void anImmutableEdgeReadBackFromTheDatabaseSerializes() {
      database.transaction(() -> {
        final Edge loaded = database.lookupByRID(edgeRid, true).asEdge();
        assertThat(loaded).isInstanceOf(ImmutableEdge.class);
        assertThat(loaded).isNotInstanceOf(MutableEdge.class);

        assertThat(javaSerialize(loaded)).isNotEmpty();

        final MutableEdge restored = new MutableEdge(database, (EdgeType) database.getSchema().getType(EDGE_TYPE), null);
        roundTrip(loaded, restored);

        assertThat(restored.getIdentity()).isEqualTo(edgeRid);
        assertThat((Long) restored.get("weight")).isEqualTo(7L);
        assertThat(restored.getOut()).isEqualTo(v1Rid);
        assertThat(restored.getIn()).isEqualTo(v2Rid);
      });
    }

    /**
     * A LIGHTWEIGHT edge takes the same {@code instanceof Edge} arm, and {@code newLightEdge} hands the immutable
     * form back directly - so this one never had a mutable instance to hide the defect behind.
     */
    @Test
    void anImmutableLightEdgeSerializes() {
      database.transaction(() -> {
        final Edge light = database.lookupByRID(v1Rid, true).asVertex()
            .getEdges(Vertex.DIRECTION.OUT, LIGHT_TYPE).iterator().next();
        assertThat(light).isInstanceOf(ImmutableLightEdge.class);

        assertThat(javaSerialize(light)).isNotEmpty();
      });
    }

    /**
     * The iteration path named in the issue, which is what a scan or a query goes through.
     */
    @Test
    void everyVertexHandedBackByAScanSerializes() {
      database.transaction(() -> {
        int count = 0;
        for (final Iterator<Record> it = database.iterateType(VERTEX_TYPE, false); it.hasNext(); ) {
          assertThat(javaSerialize(it.next())).isNotEmpty();
          ++count;
        }
        assertThat(count).isEqualTo(2);
      });
    }

    /**
     * A document was always fine; kept so a future narrowing of the document arm is caught by the same class.
     */
    @Test
    void anImmutableDocumentStillSerializes() {
      database.getSchema().getOrCreateDocumentType("Issue7908Doc");
      database.transaction(() -> {
        final RID rid = database.newDocument("Issue7908Doc").set("id", 1L).save().getIdentity();
        assertThat(javaSerialize(database.lookupByRID(rid, true))).isNotEmpty();
      });
    }

    private static byte[] javaSerialize(final Object record) {
      try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
          final ObjectOutput out = new ObjectOutputStream(bytes)) {
        out.writeObject(record);
        out.flush();
        return bytes.toByteArray();
      } catch (final Exception e) {
        throw new IllegalStateException("Cannot Java-serialize " + record.getClass().getName(), e);
      }
    }

    /**
     * {@code writeExternal}/{@code readExternal} directly, the way the sibling {@code JavaBinarySerializerTest} does:
     * {@code readObject} cannot be used here because the immutable classes have no public no-arg constructor, and the
     * read half is not what this issue is about.
     */
    private static void roundTrip(final Object source, final Externalizable target) {
      try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
          final ObjectOutput out = new ObjectOutputStream(bytes)) {
        ((Externalizable) source).writeExternal(out);
        out.flush();

        try (final ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
            final ObjectInput objectIn = new ObjectInputStream(in)) {
          target.readExternal(objectIn);
        }
      } catch (final Exception e) {
        throw new IllegalStateException("Cannot round-trip " + source.getClass().getName(), e);
      }
    }
  }

  /**
   * Regression for #8624: {@link JsonSerializer} rendered a primitive array (a vector embedding is a {@code float[]} of a thousand
   * elements or more, serialized once per search hit) by boxing it into an intermediate list and then running every element
   * back through the whole {@code serializeObject()} dispatch. The array is now written straight into the JSON array, and
   * the output must stay byte-identical to what the generic path produces for the same values held in a {@link List}:
   * that list path is the reference every assertion below compares against.
   */
  @Nested
  class PrimitiveArraySerialization extends TestHelper {

    /** Every primitive array shape, with the values whose rendering is the easiest to get subtly wrong. */
    private static Map<String, Object> arrays() {
      final Map<String, Object> arrays = new LinkedHashMap<>();
      arrays.put("floats", new float[] { 1.5F, 0.1F, -0.0F, 3.4028235E38F, 1.4E-45F, Float.NaN, Float.POSITIVE_INFINITY,
          Float.NEGATIVE_INFINITY, Float.intBitsToFloat(0x7fc00001) });
      arrays.put("doubles", new double[] { 1.5D, 0.1D, -0.0D, Double.MAX_VALUE, Double.MIN_VALUE, Double.NaN,
          Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.longBitsToDouble(0x7ff8000000000001L) });
      arrays.put("ints", new int[] { Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE });
      arrays.put("longs", new long[] { Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE });
      arrays.put("shorts", new short[] { Short.MIN_VALUE, 0, Short.MAX_VALUE });
      arrays.put("bytes", new byte[] { Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE });
      arrays.put("booleans", new boolean[] { true, false, true });
      arrays.put("chars", new char[] { 'a', '"', '\\', 'è', '\n' });
      arrays.put("emptyFloats", new float[0]);
      arrays.put("strings", new String[] { "a", null, "c" });
      return arrays;
    }

    /** The same values boxed into a list, which goes through the generic collection path. */
    private static List<Object> boxed(final Object array) {
      final int length = Array.getLength(array);
      final List<Object> list = new ArrayList<>(length);
      for (int i = 0; i < length; i++)
        list.add(Array.get(array, i));
      return list;
    }

    @Test
    void aPrimitiveArrayRendersExactlyLikeTheSameValuesInAList() {
      for (final boolean collectionSize : new boolean[] { false, true }) {
        final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize);

        for (final Map.Entry<String, Object> entry : arrays().entrySet()) {
          final ResultInternal asArray = new ResultInternal();
          asArray.setProperty("v", entry.getValue());
          final ResultInternal asList = new ResultInternal();
          asList.setProperty("v", boxed(entry.getValue()));

          assertThat(serializer.serializeResult(database, asArray).toString())
              .as("%s with useCollectionSize=%s", entry.getKey(), collectionSize)
              .isEqualTo(serializer.serializeResult(database, asList).toString());
        }
      }
    }

    @Test
    void nonFiniteValuesKeepTheirStringNames() {
      final ResultInternal result = new ResultInternal();
      result.setProperty("f", new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 2F });
      result.setProperty("d", new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 2D });

      final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, result);

      assertThat(json.getJSONArray("f").toString()).isEqualTo("[\"NaN\",\"PosInfinity\",\"NegInfinity\",2.0]");
      assertThat(json.getJSONArray("d").toString()).isEqualTo("[\"NaN\",\"PosInfinity\",\"NegInfinity\",2.0]");
    }

    @Test
    void arraysNestedInACollectionOrAMapUseTheSameRendering() {
      final List<Object> embeddings = List.of(new float[] { 1F, Float.NaN }, new double[] { 0.25D });
      final Map<String, Object> byName = Map.of("e", new float[] { 0.5F, 0.75F });

      final ResultInternal asArrays = new ResultInternal();
      asArrays.setProperty("list", embeddings);
      asArrays.setProperty("map", byName);

      final ResultInternal asLists = new ResultInternal();
      asLists.setProperty("list", List.of(boxed(embeddings.get(0)), boxed(embeddings.get(1))));
      asLists.setProperty("map", Map.of("e", boxed(byName.get("e"))));

      final JsonSerializer serializer = JsonSerializer.createJsonSerializer();
      assertThat(serializer.serializeResult(database, asArrays).toString())
          .isEqualTo(serializer.serializeResult(database, asLists).toString());
    }

    /**
     * The document path: the arrays a record persists (the ARRAY_OF_* types and BINARY) are loaded back as primitive
     * arrays, and must render like the same values stored as a LIST.
     */
    @Test
    void persistedArraysRenderLikePersistedLists() {
      final Map<String, Object> persistable = new LinkedHashMap<>();
      for (final String name : List.of("floats", "doubles", "ints", "longs", "shorts", "bytes"))
        persistable.put(name, arrays().get(name));

      final RID[] rids = new RID[2];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue8624Doc");
        final MutableDocument asArrays = database.newDocument("Issue8624Doc");
        final MutableDocument asLists = database.newDocument("Issue8624Doc");
        for (final Map.Entry<String, Object> entry : persistable.entrySet()) {
          asArrays.set(entry.getKey(), entry.getValue());
          asLists.set(entry.getKey(), boxed(entry.getValue()));
        }
        rids[0] = asArrays.save().getIdentity();
        rids[1] = asLists.save().getIdentity();
      });

      for (final boolean collectionSize : new boolean[] { false, true }) {
        final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize);
        final JSONObject fromArrays = serializer.serializeDocument(rids[0].asDocument());
        final JSONObject fromLists = serializer.serializeDocument(rids[1].asDocument());

        for (final String name : persistable.keySet()) {
          assertThat(rids[0].asDocument().get(name).getClass().isArray()).as("%s must load back as an array", name).isTrue();
          assertThat(String.valueOf(fromArrays.get(name)))
              .as("%s with useCollectionSize=%s", name, collectionSize)
              .isEqualTo(String.valueOf(fromLists.get(name)));
        }
      }
    }
  }

  /**
   * Regression for #9842: {@code JavaBinarySerializer.writeExternal} took its property list from {@code Document.toMap()}, which is
   * {@code toMap(true)} and so INCLUDES the metadata ({@code @cat}, {@code @type}, {@code @rid} and, for an edge, {@code @in} and
   * {@code @out}). They were written into the stream as properties, on top of the identity the method already writes in its header
   * and the endpoints it writes again in the trailing block of a vertex or an edge: a one-property edge was streamed as six
   * properties, and {@code MutableDocument.fromMap} decodes every one of them on the way back only to discard each {@code @}-prefixed
   * entry.
   * <p>
   * The other half of the report - that a record declares {@code Externalizable} but {@code ObjectInputStream.readObject} can never
   * build one, because the JDK needs a public no-argument constructor and a record needs a database - is the contract issue #186
   * settled on: the caller creates a database-bound record of the right type and asks IT to read the stream. The tests below pin both
   * sides of that, so the limit is stated by the build and not discovered in another process.
   */
  @Nested
  class ExternalizedRecordStream extends TestHelper {

    /** What {@code writeExternal} put in the stream, decoded the way {@code readExternal} reads it. */
    private record Stream(RID identity, List<String> propertyNames, RID firstPointer, RID secondPointer) {
    }

    private static byte[] externalize(final Document record) throws IOException {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        ((Externalizable) record).writeExternal(out);
      }
      return bytes.toByteArray();
    }

    private Stream decode(final byte[] bytes, final boolean hasTrailingPointers) throws IOException {
      try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        final RID identity = database.newRID(in.readInt(), in.readLong());
        final int count = in.readInt();
        final List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          names.add(in.readUTF());
          in.readFully(new byte[in.readInt()]);
        }
        if (!hasTrailingPointers)
          return new Stream(identity, names, null, null);
        return new Stream(identity, names, database.newRID(in.readInt(), in.readLong()), database.newRID(in.readInt(), in.readLong()));
      }
    }

    @Test
    void aDocumentStreamCarriesItsPropertiesAndNoMetadata() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
      database.setAutoTransaction(true);
      final MutableDocument doc = database.newDocument("Doc").set("n", 1).set("s", "x");
      doc.save();

      final Stream stream = decode(externalize(doc), false);

      assertThat(stream.identity()).isEqualTo(doc.getIdentity());
      assertThat(stream.propertyNames()).as("only the properties of the record, not @cat/@type/@rid").containsExactlyInAnyOrder("n", "s");
    }

    @Test
    void aVertexStreamCarriesItsPropertiesAndNoMetadata() throws Exception {
      database.getSchema().createVertexType("V").createProperty("n", Type.INTEGER);
      database.setAutoTransaction(true);
      final MutableVertex vertex = database.newVertex("V").set("n", 1);
      vertex.save();

      final Stream stream = decode(externalize(vertex), true);

      assertThat(stream.propertyNames()).containsExactly("n");
    }

    /** The endpoints are written once, in the trailing block, and not a second time as {@code @in}/{@code @out} properties. */
    @Test
    void anEdgeStreamCarriesItsEndpointsOnceAndNoMetadata() throws Exception {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E").createProperty("w", Type.INTEGER);
      database.setAutoTransaction(true);
      final MutableVertex from = database.newVertex("V");
      from.save();
      final MutableVertex to = database.newVertex("V");
      to.save();
      final Edge edge = from.newEdge("E", to, "w", 7);

      final Stream stream = decode(externalize(edge), true);

      assertThat(stream.propertyNames()).as("a one-property edge is one property in the stream").containsExactly("w");
      assertThat(stream.firstPointer()).isEqualTo(from.getIdentity());
      assertThat(stream.secondPointer()).isEqualTo(to.getIdentity());
    }

    /** What the supported path preserves: a record created for the type, bound to the database, and asked to read the stream. */
    @Test
    void aRecordReadsItsStreamBackIntoABoundRecordOfTheSameType() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
      database.setAutoTransaction(true);
      final MutableDocument doc = database.newDocument("Doc").set("n", 1).set("s", "x");
      doc.save();

      final MutableDocument target = database.newDocument("Doc");
      try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(externalize(doc)))) {
        target.readExternal(in);
      }

      assertThat(target.getIdentity()).isEqualTo(doc.getIdentity());
      assertThat(target.toMap(false)).isEqualTo(doc.toMap(false));
      assertThat(target.getString("s")).isEqualTo("x");
    }

    /**
     * A stream written before the fix carries {@code @cat}, {@code @type} and {@code @rid} among its properties. It must still read:
     * {@code fromMap} discards every {@code @}-prefixed entry, so the metadata never reaches the record and the identity is the
     * one of the header. The stream is built by hand here, in the old layout, because the writer no longer produces it.
     */
    @Test
    void aStreamWrittenWithMetadataPropertiesStillReads() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
      final DatabaseInternal db = (DatabaseInternal) database;
      final RID identity = db.newRID(1, 0);

      final Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("n", 1);
      properties.put("s", "x");
      properties.put("@cat", "d");
      properties.put("@type", "Doc");
      properties.put("@rid", "#1:0");

      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        out.writeInt(identity.getBucketId());
        out.writeLong(identity.getPosition());
        out.writeInt(properties.size());
        final Binary buffer = new Binary();
        for (final Map.Entry<String, Object> property : properties.entrySet()) {
          final byte type = BinaryTypes.getTypeFromValue(property.getValue(), null);
          buffer.clear();
          buffer.putByte(type);
          db.getSerializer().serializeValue(db, buffer, type, property.getValue());
          buffer.flip();
          out.writeUTF(property.getKey());
          out.writeInt(buffer.size());
          out.write(buffer.getContent(), 0, buffer.size());
        }
      }

      final MutableDocument target = database.newDocument("Doc");
      try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        target.readExternal(in);
      }

      assertThat(target.getIdentity()).isEqualTo(identity);
      assertThat(target.toMap(false)).containsOnlyKeys("n", "s");
      assertThat(target.getInteger("n")).isEqualTo(1);
      assertThat(target.getString("s")).isEqualTo("x");
      assertThat(target.getTypeName()).as("the type is the caller's, not the stream's").isEqualTo("Doc");
    }

    /**
     * The limit the contract states: {@code ObjectInputStream.readObject} cannot build a record from scratch. The JDK instantiates
     * an {@code Externalizable} through a public no-argument constructor before it calls {@code readExternal}, and a record has
     * none - it is a handle on a page of an open database, so there is nothing to construct it from. Pinned so that lifting the
     * limit is a decision somebody takes, not an accident.
     */
    @Test
    void objectInputStreamCannotBuildARecordFromScratch() throws Exception {
      database.getSchema().createDocumentType("Doc");
      final MutableDocument doc = database.newDocument("Doc").set("n", 1);

      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
        out.writeObject(doc);
      }

      assertThatThrownBy(() -> {
        try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
          in.readObject();
        }
      }).isInstanceOf(InvalidClassException.class).hasMessageContaining("no valid constructor");
    }
  }

  /**
   * Regression for #9997: a String read from a record is converted when its property is declared as a native temporal type (issue
   * #8572), and finding that out cost a polymorphic property lookup on every String read of every type. Each type now
   * remembers whether it, or a super type, declares a property of such a type, and the read skips the lookup while it does
   * not. The answer is per type and exact: a temporal property elsewhere in the schema does not make this type pay, and
   * dropping the last one gives the fast path back. These tests pin both sides of that switch, and every schema change that
   * moves it (create, drop, rename, a super type added or removed, a reopen).
   */
  @Nested
  class StoredStringConversionMemo extends TestHelper {

    @Test
    void stringsAreReturnedAsTheyAreWhileNoTemporalPropertyIsDeclared() {
      database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
      database.getSchema().getType("Plain").createProperty("n", Type.LONG);
      database.transaction(() -> database.newDocument("Plain").set("code", "P1D").set("free", "10:15:30").set("n", 1L).save());

      assertThat(converts("Plain")).isFalse();
      final Result r = first("SELECT code, free, n FROM Plain");
      assertThat(r.<Object>getProperty("code")).isEqualTo("P1D");
      assertThat(r.<Object>getProperty("free")).isEqualTo("10:15:30");
      assertThat(r.<Object>getProperty("n")).isEqualTo(1L);
    }

    @Test
    void aTemporalPropertyOnAnotherTypeDoesNotTurnThisTypeOn() {
      database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
      database.getSchema().createDocumentType("Timed").createProperty("d", Type.DURATION);

      assertThat(converts("Plain")).isFalse();
      assertThat(converts("Timed")).isTrue();
    }

    @Test
    void declaringATemporalPropertyAfterTheStringWasStoredTurnsTheConversionOnAlsoAfterAReopen() {
      database.getSchema().createDocumentType("Old");
      database.transaction(() -> database.newDocument("Old").set("lt", "10:15:30").set("d", "P1M2DT3S").set("s", "10:15:30").save());
      assertThat(converts("Old")).isFalse();
      assertThat(first("SELECT lt FROM Old").<Object>getProperty("lt")).isEqualTo("10:15:30");

      database.getSchema().getType("Old").createProperty("lt", Type.LOCAL_TIME);
      database.getSchema().getType("Old").createProperty("d", Type.DURATION);

      assertThat(converts("Old")).isTrue();
      assertReadsConverted();
      // a property of another type still reads as the String it holds
      assertThat(first("SELECT s FROM Old").<Object>getProperty("s")).isEqualTo("10:15:30");

      reopenDatabase();

      assertThat(converts("Old")).isTrue();
      assertReadsConverted();
    }

    @Test
    void aReopenedDatabaseWithoutTemporalPropertiesStaysOff() {
      database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
      database.transaction(() -> database.newDocument("Plain").set("code", "P1D").save());

      reopenDatabase();

      assertThat(converts("Plain")).isFalse();
      assertThat(first("SELECT code FROM Plain").<Object>getProperty("code")).isEqualTo("P1D");
    }

    @Test
    void droppingTheLastTemporalPropertyTurnsTheConversionOff() {
      database.getSchema().createDocumentType("Old");
      database.transaction(() -> database.newDocument("Old").set("d", "P1D").save());
      database.getSchema().getType("Old").createProperty("d", Type.DURATION);
      assertThat(converts("Old")).isTrue();
      assertThat(first("SELECT d FROM Old").<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));

      database.getSchema().getType("Old").dropProperty("d");

      assertThat(converts("Old")).isFalse();
      assertThat(first("SELECT d FROM Old").<Object>getProperty("d")).isEqualTo("P1D");
    }

    @Test
    void renamingATemporalPropertyMovesTheConversionToTheNewName() {
      database.getSchema().createDocumentType("Renamed");
      database.transaction(() -> database.newDocument("Renamed").set("a", "P1D").set("b", "P2D").save());
      database.getSchema().getType("Renamed").createProperty("a", Type.DURATION);
      assertThat(first("SELECT a, b FROM Renamed").<Object>getProperty("b")).isEqualTo("P2D");

      database.getSchema().getType("Renamed").renameProperty("a", "b");

      assertThat(converts("Renamed")).isTrue();
      final Result r = first("SELECT a, b FROM Renamed");
      assertThat(r.<Object>getProperty("a")).isEqualTo("P1D");
      assertThat(r.<Object>getProperty("b")).isEqualTo(new CypherDuration(0, 2, 0, 0));
    }

    @Test
    void aSubTypeFollowsTheTemporalPropertiesOfItsSuperTypes() {
      database.getSchema().createDocumentType("Base");
      database.getSchema().createDocumentType("Child");
      database.transaction(() -> database.newDocument("Child").set("lt", "10:15:30").save());
      assertThat(converts("Child")).isFalse();

      // A SUPER TYPE ADDED LATER BRINGS ITS TEMPORAL PROPERTY
      database.getSchema().getType("Base").createProperty("lt", Type.LOCAL_TIME);
      assertThat(converts("Child")).isFalse();
      database.getSchema().getType("Child").addSuperType("Base");
      assertThat(converts("Child")).isTrue();
      assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));

      // ...AND TAKES IT AWAY WHEN IT IS REMOVED
      database.getSchema().getType("Child").removeSuperType("Base");
      assertThat(converts("Child")).isFalse();
      assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo("10:15:30");

      // A PROPERTY DECLARED ON THE SUPER TYPE AFTER THE LINK REACHES THE SUB TYPE TOO
      database.getSchema().getType("Base").dropProperty("lt");
      database.getSchema().getType("Child").addSuperType("Base");
      assertThat(converts("Child")).isFalse();
      database.getSchema().getType("Base").createProperty("lt", Type.LOCAL_TIME);
      assertThat(converts("Child")).isTrue();
      assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
    }

    @Test
    void droppingTheSuperTypeThatDeclaresTheTemporalPropertyTurnsTheSubTypeOff() {
      database.getSchema().createDocumentType("Top");
      database.getSchema().createDocumentType("Middle").createProperty("lt", Type.LOCAL_TIME);
      database.getSchema().createDocumentType("Leaf");
      database.getSchema().getType("Leaf").addSuperType("Middle");
      database.transaction(() -> database.newDocument("Leaf").set("lt2", "10:15:30").save());
      assertThat(converts("Leaf")).isTrue();

      // A MIDDLE TYPE WITHOUT SUPER TYPES: THE DROP RE-LINKS NOTHING, SO NO addSuperType() MOVES THE SERIAL FOR IT
      database.getSchema().dropType("Middle");

      assertThat(database.getSchema().getType("Leaf").getSuperTypes()).isEmpty();
      assertThat(converts("Leaf")).isFalse();

      // ...AND ONE BETWEEN TWO TYPES: THE SUB TYPE IS RE-LINKED TO THE TOP, WHICH DECLARES NOTHING
      database.getSchema().createDocumentType("Middle2").createProperty("lt", Type.LOCAL_TIME);
      database.getSchema().getType("Middle2").addSuperType("Top");
      database.getSchema().getType("Leaf").addSuperType("Middle2");
      assertThat(converts("Leaf")).isTrue();

      database.getSchema().dropType("Middle2");

      assertThat(database.getSchema().getType("Leaf").getSuperTypes()).extracting("name").containsExactly("Top");
      assertThat(converts("Leaf")).isFalse();
    }

    @Test
    void anEmbeddedDocumentFollowsItsOwnType() {
      database.getSchema().createDocumentType("Slot");
      database.getSchema().createDocumentType("Agenda");
      database.transaction(() -> {
        final var agenda = database.newDocument("Agenda");
        agenda.newEmbeddedDocument("Slot", "slot").set("at", "10:15:30").set("note", "10:15:30");
        agenda.save();
      });
      // DECLARED AFTER THE VALUE WAS STORED AS A String. IT IS THE EMBEDDED DOCUMENT'S TYPE THAT DECIDES, NOT THE PARENT'S,
      // WHICH DECLARES NOTHING
      database.getSchema().getType("Slot").createProperty("at", Type.LOCAL_TIME);
      assertThat(converts("Agenda")).isFalse();
      assertThat(converts("Slot")).isTrue();
      final Result r = first("SELECT slot.at AS at, slot.note AS note FROM Agenda");
      assertThat(r.<Object>getProperty("at")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(r.<Object>getProperty("note")).isEqualTo("10:15:30");
    }

    /**
     * deserializeProperties() now tells only a value that can hold an embedded document where it lives: an embedded
     * document nested in a list and in a map, read through the whole-record path (toJSON() and a SELECT of the
     * collections), must still find its owner, and its own temporal property must still be converted.
     */
    @Test
    void embeddedDocumentsInsideAListAndAMapReadThroughTheWholeRecordPath() {
      database.getSchema().createDocumentType("Slot");
      database.getSchema().createDocumentType("Planner");
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument planner = database.newDocument("Planner");
        planner.set("title", "10:15:30");
        planner.set("slots", new ArrayList<>());
        planner.newEmbeddedDocument("Slot", "slots").set("at", "10:15:30");
        planner.newEmbeddedDocument("Slot", "byName", "morning").set("at", "11:00");
        rid[0] = planner.save().getIdentity();
      });
      // DECLARED AFTER THE VALUES WERE STORED AS Strings
      database.getSchema().getType("Slot").createProperty("at", Type.LOCAL_TIME);

      final Document reloaded = database.lookupByRID(rid[0], true).asDocument();
      final JSONObject json = reloaded.toJSON();
      assertThat(json.getString("title")).isEqualTo("10:15:30");
      assertThat(json.getJSONArray("slots").getJSONObject(0).getString("@type")).isEqualTo("Slot");
      assertThat(json.getJSONObject("byName").getJSONObject("morning").getString("@type")).isEqualTo("Slot");

      final List<?> slots = reloaded.getList("slots");
      assertThat(((Document) slots.getFirst()).get("at")).isEqualTo(LocalTime.of(10, 15, 30));
      final Map<String, ?> byName = reloaded.getMap("byName");
      assertThat(((Document) byName.get("morning")).get("at")).isEqualTo(LocalTime.of(11, 0));

      final Result r = first("SELECT slots, byName, title FROM Planner");
      assertThat(r.<Object>getProperty("title")).isEqualTo("10:15:30");
      assertThat(((Document) r.<List<?>>getProperty("slots").getFirst()).get("at")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(((Document) r.<Map<String, ?>>getProperty("byName").get("morning")).get("at")).isEqualTo(LocalTime.of(11, 0));
    }

    private boolean converts(final String typeName) {
      return ((LocalDocumentType) database.getSchema().getType(typeName)).mayConvertStoredStrings();
    }

    private Result first(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.next();
      }
    }

    private void assertReadsConverted() {
      final Result r = first("SELECT lt, d FROM Old");
      assertThat(r.<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(1, 2, 3, 0));
    }
  }
}
