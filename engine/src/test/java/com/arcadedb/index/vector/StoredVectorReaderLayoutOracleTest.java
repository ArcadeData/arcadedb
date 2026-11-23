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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Oracle tests of {@link StoredVectorReader}, the zero-copy reader of the stored vector of a record (graph build, INT8 and
 * BINARY rerank, unquantized walk without a cache): whatever the record looks like, its answer must be the one the record layer
 * gives for the same property.
 * <p>
 * The oracle is the record layer itself ({@code database.lookupByRID(...).get(property)} converted through
 * {@link VectorUtils#toFloatArray(Object, VectorEncoding)}), applied to records of every shape the header walk has to get past:
 * the vector among many properties, header numbers of one and of several bytes, the fixed-width and the varint form of the
 * array, documents and vertices, a vector of another arity, none, a null, an all-zero one, and a value that is a list or an
 * array of doubles.
 */
class StoredVectorReaderLayoutOracleTest extends TestHelper {
  private static final int    DIMENSIONS = 8;
  private static final String PROPERTY   = "vector";

  /** A record shape: the properties written BEFORE the vector, the vector, and the properties after it. */
  private record Shape(String label, Map<String, Object> before, Object vector, Map<String, Object> after) {
  }

  @Test
  void documentAndVertexAnswerLikeTheRecordLayerAtEveryPositionOfTheVector() {
    createTypes();
    final Random random = new Random(7);
    final List<RID> rids = new ArrayList<>();
    final List<float[]> expected = new ArrayList<>();
    database.transaction(() -> {
      for (final String type : new String[] { "Doc", "Vtx" })
        for (final int before : new int[] { 0, 1, 2, 5, 40, 130 })
          for (final int after : new int[] { 0, 1, 3, 130 }) {
            final float[] vector = randomVector(random);
            rids.add(save(type, props("b", before, random), vector, props("a", after, random)));
            expected.add(vector);
          }
    });

    // The reader of the thread, with and without a scratch array
    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    final float[] scratch = new float[DIMENSIONS + 3];
    for (int i = 0; i < rids.size(); i++) {
      final float[] viaScratch = reader.read(rids.get(i), scratch);
      assertThat(viaScratch).as("%s: read in place into the scratch array", rids.get(i)).isSameAs(scratch);
      assertThat(Arrays.copyOf(viaScratch, DIMENSIONS)).containsExactly(expected.get(i));
      assertThat(reader.read(rids.get(i), null)).as("%s: a new array", rids.get(i)).containsExactly(expected.get(i));
      assertThat(reader.readInPlaceOnly(rids.get(i))).as("%s: in place", rids.get(i)).containsExactly(expected.get(i));
      assertThat(recordLayer(rids.get(i), PROPERTY, DIMENSIONS, true)).containsExactly(expected.get(i));
    }
  }

  @Test
  void largeHeaderNumbersAreWalkedExactly() {
    createTypes();
    final Random random = new Random(11);
    // Property names that push the dictionary ids and the value offsets of the header past one varint byte (127) and two (16383)
    final Map<String, Object> before = new HashMap<>();
    for (int i = 0; i < 300; i++)
      before.put("many" + i, i % 3 == 0 ? "s" + i : (Object) i);
    before.put("blob", "x".repeat(20_000));
    final float[] vector = randomVector(random);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = save("Doc", before, vector, Map.of("tail", "t")));

    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    assertThat(reader.readInPlaceOnly(rid[0])).containsExactly(vector);
    assertThat(reader.read(rid[0], new float[DIMENSIONS])).containsExactly(vector);

    // A property whose name was registered after hundreds of others: its dictionary id takes more than one byte
    final float[] late = randomVector(random);
    final RID[] lateRid = new RID[1];
    database.transaction(() -> lateRid[0] = database.newDocument("Doc").set("late_vector_property", late).set("other", "o").save()
        .getIdentity());
    final StoredVectorReader lateReader = reader("late_vector_property", DIMENSIONS, true);
    assertThat(lateReader.readInPlaceOnly(lateRid[0])).containsExactly(late);
    assertThat(reader.read(lateRid[0], null)).as("the property the record does not hold").isNull();
  }

  @Test
  void varintFormOfTheArrayIsReadInPlaceToo() {
    createTypes();
    database.command("sql", "ALTER DATABASE `arcadedb.serializer.fixedWidthFloatArrays` false").close();
    final Random random = new Random(13);
    final List<RID> rids = new ArrayList<>();
    final List<float[]> expected = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        final float[] vector = randomVector(random);
        // Every sign and magnitude, whole numbers, zeros and subnormals: the bits of each float travel as a varint
        if (i == 1)
          Arrays.fill(vector, 0f);
        if (i == 2)
          vector[3] = Float.MIN_VALUE;
        if (i == 3)
          vector[0] = -Float.MAX_VALUE;
        rids.add(save(i % 2 == 0 ? "Doc" : "Vtx", props("b", i % 5, random), vector, props("a", i % 3, random)));
        expected.add(vector);
      }
    });

    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, false);
    for (int i = 0; i < rids.size(); i++) {
      assertThat(reader.readInPlaceOnly(rids.get(i))).as("%s in place", rids.get(i)).containsExactly(expected.get(i));
      final float[] scratch = new float[DIMENSIONS];
      assertThat(reader.read(rids.get(i), scratch)).isSameAs(scratch).containsExactly(expected.get(i));
    }
  }

  @Test
  void valuesThatAreNotAFloatArrayAreLeftToTheRecordLayer() {
    database.getSchema().createDocumentType("Listy").createProperty(PROPERTY, Type.LIST);
    database.getSchema().createDocumentType("Doubly").createProperty(PROPERTY, Type.ARRAY_OF_DOUBLES);
    database.getSchema().createDocumentType("Stringy").createProperty(PROPERTY, Type.STRING);
    final float[] vector = randomVector(new Random(17));
    final List<Float> asList = new ArrayList<>();
    final double[] asDoubles = new double[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++) {
      asList.add(vector[i]);
      asDoubles[i] = vector[i];
    }
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newDocument("Listy").set(PROPERTY, asList).set("x", 1).save().getIdentity();
      rids[1] = database.newDocument("Doubly").set(PROPERTY, asDoubles).set("x", 1).save().getIdentity();
      rids[2] = database.newDocument("Stringy").set(PROPERTY, "[1.0, 2.0]").set("x", 1).save().getIdentity();
    });

    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    // In place only answers what lies as a float array on the page: a list or a double array is the record layer's to convert
    assertThat(reader.readInPlaceOnly(rids[0])).isNull();
    assertThat(reader.readInPlaceOnly(rids[1])).isNull();
    assertThat(reader.read(rids[0], null)).as("list").containsExactly(vector);
    assertThat(reader.read(rids[1], null)).as("double array").hasSize(DIMENSIONS);
    for (int i = 0; i < DIMENSIONS; i++)
      assertThat(reader.read(rids[1], null)[i]).isEqualTo((float) asDoubles[i]);
    // A string is no float array on the page either: the record layer's conversion decides, in place or not
    assertThat(reader.readInPlaceOnly(rids[2])).isNull();
    Object viaRecordLayer;
    try {
      viaRecordLayer = recordLayer(rids[2], PROPERTY, DIMENSIONS, true);
    } catch (final RuntimeException e) {
      viaRecordLayer = e.getClass();
    }
    Object viaReader;
    try {
      viaReader = reader.read(rids[2], null);
    } catch (final RuntimeException e) {
      viaReader = e.getClass();
    }
    assertThat(viaReader).isEqualTo(viaRecordLayer);
  }

  @Test
  void arityNullAbsentAndZeroVectorsAnswerLikeTheRecordLayer() {
    createTypes();
    final float[] vector = randomVector(new Random(19));
    final float[] zero = new float[DIMENSIONS];
    final RID[] rids = new RID[5];
    database.transaction(() -> {
      rids[0] = save("Doc", Map.of("n", 1), vector, Map.of());
      rids[1] = save("Doc", Map.of("n", 2), zero, Map.of());
      rids[2] = database.newDocument("Doc").set("n", 3).save().getIdentity(); // no vector property
      rids[3] = database.newDocument("Doc").set("n", 4).set(PROPERTY, (Object) null).save().getIdentity(); // a null value
      rids[4] = database.newDocument("Doc").save().getIdentity(); // no property at all
    });

    for (final boolean zeroIsUnscorable : new boolean[] { true, false }) {
      final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, zeroIsUnscorable);
      assertThat(reader.read(rids[0], null)).containsExactly(vector);
      assertThat(reader.read(rids[1], null)).as("all-zero, unscorable=%s", zeroIsUnscorable)
          .isEqualTo(zeroIsUnscorable ? null : zero);
      assertThat(reader.readInPlaceOnly(rids[1])).isEqualTo(zeroIsUnscorable ? null : zero);
      assertThat(reader.read(rids[2], null)).as("property missing").isNull();
      assertThat(reader.read(rids[3], null)).as("null value").isNull();
      assertThat(reader.read(rids[4], null)).as("no property").isNull();
      assertThat(recordLayer(rids[1], PROPERTY, DIMENSIONS, zeroIsUnscorable)).isEqualTo(zeroIsUnscorable ? null : zero);
    }

    // Another arity than the index has: no vector it can score, in place and through the record layer alike
    for (final int other : new int[] { DIMENSIONS - 1, DIMENSIONS + 1, 1, 2 * DIMENSIONS }) {
      final StoredVectorReader reader = reader(PROPERTY, other, true);
      assertThat(reader.read(rids[0], null)).as("reader of %d dimensions", other).isNull();
      assertThat(reader.readInPlaceOnly(rids[0])).isNull();
      assertThat(recordLayer(rids[0], PROPERTY, other, true)).isNull();
    }

    // A property name nothing ever wrote
    final StoredVectorReader unknown = reader("never_written_property_name", DIMENSIONS, true);
    assertThat(unknown.read(rids[0], null)).isNull();
    assertThat(unknown.readInPlaceOnly(rids[0])).isNull();
  }

  @Test
  void recordTheTransactionWroteIsReadFromTheTransaction() {
    createTypes();
    final float[] before = randomVector(new Random(23));
    final float[] after = randomVector(new Random(29));
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = save("Doc", Map.of("n", 1), before, Map.of()));
    final RID rid = holder[0];
    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    assertThat(reader.readInPlaceOnly(rid)).containsExactly(before);

    database.begin();
    try {
      database.lookupByRID(rid, true).asDocument().modify().set(PROPERTY, after).save();
      // The page still holds the old vector: the answer is the transaction's own
      assertThat(reader.readInPlaceOnly(rid)).as("in place only never answers a record the transaction wrote").isNull();
      assertThat(reader.read(rid, null)).containsExactly(after);
      final RID created = database.newDocument("Doc").set(PROPERTY, after).save().getIdentity();
      assertThat(reader.read(created, null)).containsExactly(after);
    } finally {
      database.rollback();
    }
    assertThat(reader.read(rid, null)).as("after the rollback").containsExactly(before);
  }

  @Test
  void deletedRecordIsNotFound() {
    createTypes();
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = save("Doc", Map.of("n", 1), randomVector(new Random(31)), Map.of()));
    final RID rid = holder[0];
    database.transaction(() -> database.lookupByRID(rid, true).delete());
    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    assertThat(reader.readInPlaceOnly(rid)).isNull();
    assertThatThrownBy(() -> reader.read(rid, null)).isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  void manyRecordsAcrossPagesAndBucketsAreAllRead() {
    database.getSchema().createDocumentType("Wide", 3).createProperty(PROPERTY, Type.ARRAY_OF_FLOATS);
    final Random random = new Random(37);
    final List<RID> rids = new ArrayList<>();
    final List<float[]> expected = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3000; i++) {
        final float[] vector = randomVector(random);
        rids.add(database.newDocument("Wide").set(PROPERTY, vector).set("n", i).set("pad", "p".repeat(random.nextInt(200))).save()
            .getIdentity());
        expected.add(vector);
      }
    });
    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    final float[] scratch = new float[DIMENSIONS];
    for (int i = 0; i < rids.size(); i++)
      assertThat(reader.read(rids.get(i), scratch)).as("%s", rids.get(i)).containsExactly(expected.get(i));
    // A reader of another thread is not the one of this thread
    assertThat(reader.isOwnedByCurrentThread()).isTrue();
  }

  @Test
  void readerIsOwnedByItsThreadOnly() throws Exception {
    createTypes();
    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    final boolean[] foreign = new boolean[1];
    final Thread other = new Thread(() -> foreign[0] = reader.isOwnedByCurrentThread());
    other.start();
    other.join();
    assertThat(reader.isOwnedByCurrentThread()).isTrue();
    assertThat(foreign[0]).isFalse();
  }

  @Test
  void anInt8EncodedIndexIsAlwaysReadByTheRecordLayer() {
    final Random random = new Random(23);
    database.getSchema().createDocumentType("Bin").createProperty(PROPERTY, Type.BINARY);
    database.getSchema().createDocumentType("Doc").createProperty(PROPERTY, Type.ARRAY_OF_FLOATS);
    final byte[] codes = new byte[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      codes[i] = (byte) (1 + random.nextInt(127));
    final float[] floats = randomVector(random);
    final RID[] rids = new RID[6];
    database.transaction(() -> {
      rids[0] = database.newDocument("Bin").set(PROPERTY, codes).save().getIdentity();
      rids[1] = database.newDocument("Doc").set(PROPERTY, floats).save().getIdentity();
      rids[2] = database.newDocument("Bin").save().getIdentity();
      rids[3] = database.newDocument("Doc").set(PROPERTY, new float[DIMENSIONS + 1]).save().getIdentity();
      rids[4] = database.newDocument("Doc").set(PROPERTY, new float[DIMENSIONS]).save().getIdentity();
      rids[5] = database.newDocument("Doc").set(PROPERTY, new float[] { 1f, 2f, 3f }).save().getIdentity();
    });

    final StoredVectorReader reader = new StoredVectorReader((DatabaseInternal) database, PROPERTY, DIMENSIONS, VectorEncoding.INT8,
        true);
    // A BINARY vector is dequantized by the record layer, whatever the transaction state
    assertThat(reader.read(rids[0], null)).containsExactly(VectorUtils.dequantizeInt8ToFloat(codes));
    assertThat(reader.readInPlaceOnly(rids[0])).as("an INT8 vector is never read in place").isNull();
    // Not even a float array is: the encoding of the index decides, not the shape of the record
    assertThat(reader.read(rids[1], null)).containsExactly(floats);
    assertThat(reader.readInPlaceOnly(rids[1])).as("the encoding of the index is not FLOAT32").isNull();
    // What the record layer answers when there is no vector the index can score: none, another arity, all zeros
    assertThat(reader.read(rids[2], null)).as("no property").isNull();
    assertThat(reader.read(rids[3], null)).as("one dimension too many").isNull();
    assertThat(reader.read(rids[4], null)).as("all zeros are unscorable").isNull();
    assertThat(reader.read(rids[5], null)).as("another arity").isNull();
  }

  @Test
  void anEdgeRecordIsLeftToTheRecordLayer() {
    final Random random = new Random(29);
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("Link").createProperty(PROPERTY, Type.ARRAY_OF_FLOATS);
    final float[] vector = randomVector(random);
    final RID[] edge = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").save();
      final MutableVertex b = database.newVertex("Node").save();
      edge[0] = a.newEdge("Link", b).set(PROPERTY, vector).save().getIdentity();
      edge[1] = a.newEdge("Link", b).save().getIdentity();
    });

    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    assertThat(reader.read(edge[0], null)).as("the vector of an edge").containsExactly(vector);
    assertThat(reader.readInPlaceOnly(edge[0])).as("an edge is not a document or a vertex of the in place layout").isNull();
    assertThat(reader.read(edge[1], null)).as("an edge without the property has no vector").isNull();
  }

  @Test
  void aReadListenerOfTheDatabaseOrOfTheTypeSeesTheReadsOfTheReader() {
    createTypes();
    final float[] vector = randomVector(new Random(31));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = save("Doc", Map.of(), vector, Map.of()));

    final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
    assertThat(reader.readInPlaceOnly(rid[0])).as("no listener: read where the record lies").containsExactly(vector);

    final AtomicInteger before = new AtomicInteger();
    final AtomicInteger after = new AtomicInteger();
    final BeforeRecordReadListener beforeListener = record -> {
      before.incrementAndGet();
      return true;
    };
    final AfterRecordReadListener afterListener = record -> {
      after.incrementAndGet();
      return record;
    };
    final RecordEvents[] registries = { database.getEvents(), database.getSchema().getType("Doc").getEvents() };
    for (final RecordEvents events : registries) {
      // Registered while the reader is in use: the reads that follow are the record layer's, which tells the listeners
      events.registerListener(beforeListener);
      before.set(0);
      assertThat(reader.read(rid[0], null)).as("before-read listener: the vector is still answered").containsExactly(vector);
      assertThat(reader.readInPlaceOnly(rid[0])).as("before-read listener: not read in place").isNull();
      assertThat(before.get()).as("before-read listener of %s", events == registries[0] ? "the database" : "the type").isPositive();
      events.unregisterListener(beforeListener);
      assertThat(reader.readInPlaceOnly(rid[0])).as("listener gone: read in place again").containsExactly(vector);

      events.registerListener(afterListener);
      after.set(0);
      assertThat(reader.read(rid[0], null)).as("after-read listener: the vector is still answered").containsExactly(vector);
      assertThat(reader.readInPlaceOnly(rid[0])).as("after-read listener: not read in place").isNull();
      assertThat(after.get()).as("after-read listener of %s", events == registries[0] ? "the database" : "the type").isPositive();
      events.unregisterListener(afterListener);
      assertThat(reader.readInPlaceOnly(rid[0])).as("listener gone: read in place again").containsExactly(vector);
    }

    // A listener that hides the record: the reader raises what the record layer raises
    database.getEvents().registerListener((BeforeRecordReadListener) record -> false);
    final Throwable expected = catchThrowable(() -> recordLayer(rid[0], PROPERTY, DIMENSIONS, true));
    assertThat(expected).isNotNull();
    assertThatThrownBy(() -> reader.read(rid[0], null)).isInstanceOf(expected.getClass());
  }

  @Test
  void aSingleDamagedByteOfTheRecordNeverMakesTheReaderAnswerAnythingButWhatTheRecordLayerAnswers() throws Exception {
    createTypes();
    final Random random = new Random(5);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (final String type : new String[] { "Doc", "Vtx" })
        for (final int extra : new int[] { 0, 2, 6 })
          rids.add(save(type, props("a", extra, random), randomVector(random), props("z", extra, random)));
    });

    final int[] fixed = { 0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 127, 128, 200, 254, 255 };
    int cases = 0;
    for (final RID rid : rids) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
      final int[] location = new int[2];
      final BasePage page = bucket.getPlainRecordForAbsoluteReads(rid.getPosition(), location);
      final PageId pageId = page.getPageId();
      final int start = location[0];
      final int length = location[1];
      for (int offset = start; offset < start + length; offset++) {
        final byte original = page.readByte(offset);
        // The values that sit on the edges of what a header number can hold: fixed ones, and the ones that point at the end
        // of the record, one before it, and one past it
        final Set<Integer> values = new TreeSet<>();
        for (final int value : fixed)
          values.add(value);
        for (int delta = -2; delta <= 2; delta++) {
          values.add((length + delta) & 0xFF);
          values.add((start + length - offset + delta) & 0xFF);
          values.add((original + delta) & 0xFF);
        }
        for (final int value : values) {
          if ((byte) value == original)
            continue;
          writeByte(bucket, pageId, offset, (byte) value);
          final StoredVectorReader reader = reader(PROPERTY, DIMENSIONS, true);
          assertThat(outcome(() -> reader.read(rid, new float[DIMENSIONS])))
              .as("%s: byte %d of %d set to %d (was %d)", rid, offset - start, length, value, original)
              .isEqualTo(outcome(() -> recordLayer(rid, PROPERTY, DIMENSIONS, true)));
          cases++;
        }
        writeByte(bucket, pageId, offset, original);
      }
    }
    assertThat(cases).isGreaterThan(10_000);
  }

  /** What a read answers, for comparing an answer or the failure it raises. */
  private static String outcome(final Supplier<float[]> read) {
    try {
      final float[] vector = read.get();
      return vector == null ? "null" : Arrays.toString(vector);
    } catch (final RuntimeException e) {
      return "raises " + e.getClass().getName();
    }
  }

  private void writeByte(final LocalBucket bucket, final PageId pageId, final int offset, final byte value) {
    database.transaction(() -> {
      try {
        ((DatabaseInternal) database).getTransaction().getPageToModify(pageId, bucket.getPageSize(), false).writeByte(offset, value);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void createTypes() {
    database.getSchema().createDocumentType("Doc").createProperty(PROPERTY, Type.ARRAY_OF_FLOATS);
    database.getSchema().createVertexType("Vtx").createProperty(PROPERTY, Type.ARRAY_OF_FLOATS);
  }

  private StoredVectorReader reader(final String property, final int dimensions, final boolean zeroIsUnscorable) {
    return new StoredVectorReader((DatabaseInternal) database, property, dimensions, VectorEncoding.FLOAT32, zeroIsUnscorable);
  }

  /** The answer of the record layer: what {@code StoredVectorReader.readThroughDatabase} is, written out against the public API. */
  private float[] recordLayer(final RID rid, final String property, final int dimensions, final boolean zeroIsUnscorable) {
    final Document doc = (Document) database.lookupByRID(rid, false);
    final Object raw = doc.get(property);
    if (raw == null)
      return null;
    final float[] vector = VectorUtils.toFloatArray(raw, VectorEncoding.FLOAT32);
    if (vector.length != dimensions)
      return null;
    if (zeroIsUnscorable) {
      boolean any = false;
      for (final float v : vector)
        any |= v != 0f;
      if (!any)
        return null;
    }
    return vector;
  }

  private RID save(final String type, final Map<String, Object> before, final Object vector, final Map<String, Object> after) {
    if (database.getSchema().getType(type) instanceof VertexType) {
      final MutableVertex vertex = database.newVertex(type);
      before.forEach(vertex::set);
      vertex.set(PROPERTY, vector);
      after.forEach(vertex::set);
      return vertex.save().getIdentity();
    }
    final MutableDocument doc = database.newDocument(type);
    before.forEach(doc::set);
    doc.set(PROPERTY, vector);
    after.forEach(doc::set);
    return doc.save().getIdentity();
  }

  /** {@code count} properties of mixed types under a prefix; the iteration order of a HashMap is irrelevant to the header walk. */
  private static Map<String, Object> props(final String prefix, final int count, final Random random) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < count; i++)
      map.put(prefix + i, switch (i % 5) {
        case 0 -> random.nextInt();
        case 1 -> "text-" + random.nextInt(1000);
        case 2 -> random.nextLong();
        case 3 -> random.nextDouble();
        default -> List.of(1, 2, 3);
      });
    return map;
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat() * 2f - 1f;
    return vector;
  }
}
