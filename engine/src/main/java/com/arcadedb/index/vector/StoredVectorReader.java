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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.serializer.BinaryTypes;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Reads the indexed vector of a record where it lies on its bucket page, for the rerank of a quantized search, which reads
 * the stored vector of every candidate it scores (issue #11298).
 * <p>
 * Through the record layer each read was a lazy record looked up, loaded by a second lookup, a copy of the whole record and a
 * fresh {@code float[]} of the property: twelve kilobytes of garbage per candidate at 1536 dimensions, for the hundred
 * candidates a default BINARY search reranks. Here the record is located in its slot
 * ({@link LocalBucket#getPlainRecordForAbsoluteReads}), its header is walked to the vector property, and the floats are copied
 * straight from the page into the array the caller hands in, so a rerank that reuses one array allocates nothing per
 * candidate.
 * <p>
 * The answer is the one the record layer gives, and the record layer answers wherever the in-place read cannot vouch for
 * that:
 * <ul>
 * <li>a record the transaction has written: its value is in the transaction's own copy, not on the page yet;</li>
 * <li>a type with a read listener, or a database with one: the listener must see the read;</li>
 * <li>a record that does not lie whole in its own slot (a chunk chain, a placeholder), an edge, or a header the walk finds
 * damaged: the database reads it, and logs or raises what it always did;</li>
 * <li>a value that is not a float array (a list, a double array, an external value) or an index that does not store FLOAT32
 * vectors: the conversion of {@link VectorUtils#toFloatArray(Object, VectorEncoding)} answers.</li>
 * </ul>
 * Not thread-safe: one instance per reading thread, reused for the reads of one search. The caller holds the database read
 * lock for the reads, as every record lookup they replace did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StoredVectorReader {
  // RECORD TYPE (1 BYTE), THEN OUT AND IN EDGE-LIST HEADS AS BUCKET (INT) + POSITION (LONG), AS ImmutableVertex READS THEM
  private static final int     VERTEX_PREFIX_SIZE   =
      Binary.BYTE_SERIALIZED_SIZE + 2 * (Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE);
  private static final int     DOCUMENT_PREFIX_SIZE = Binary.BYTE_SERIALIZED_SIZE;
  private static final int     UNRESOLVED           = Integer.MIN_VALUE;
  // THE IN-PLACE READ'S ANSWER FOR A SHAPE THE RECORD LAYER MUST READ; null IS "NO USABLE VECTOR"
  private static final float[] FALLBACK             = new float[0];

  private final DatabaseInternal database;
  private final String           propertyName;
  private final int              dimensions;
  private final VectorEncoding   encoding;
  private final boolean          zeroIsUnscorable;
  private final int[]            location     = new int[2];
  private       int              propertyId   = UNRESOLVED;
  // THE BUCKET OF THE LAST READ AND WHETHER ITS RECORDS CAN BE READ IN PLACE: AN INDEX OF ONE BUCKET ASKS ONCE PER SEARCH
  private       int              lastBucketId = -1;
  private       LocalBucket      lastBucket;
  private       PageId           lastPageId;

  /**
   * @param zeroIsUnscorable whether an all-zero vector is refused, as it is by every similarity but EUCLIDEAN
   */
  StoredVectorReader(final DatabaseInternal database, final String propertyName, final int dimensions,
      final VectorEncoding encoding, final boolean zeroIsUnscorable) {
    this.database = database;
    this.propertyName = propertyName;
    this.dimensions = dimensions;
    this.encoding = encoding;
    this.zeroIsUnscorable = zeroIsUnscorable;
  }

  /**
   * The vector the record at {@code rid} stores under the indexed property.
   *
   * @param scratch receives the vector when it is read in place, at least {@code dimensions} long; {@code null} to have a new
   *                array allocated for it, for a caller that keeps the vector
   *
   * @return the vector: {@code scratch} (or the new array) when it was read in place, an array the record owns when the record
   * layer answered (never to be modified), or {@code null} when the record carries no vector this index can score: none, one of
   * another arity, an all-zero one
   *
   * @throws com.arcadedb.exception.RecordNotFoundException when the record is gone, and whatever else a record lookup raises
   */
  float[] read(final RID rid, final float[] scratch) {
    final float[] vector = readInPlace(rid, scratch);
    if (vector == FALLBACK)
      return readThroughDatabase(rid);
    return vector != null && usable(vector) ? vector : null;
  }

  /**
   * The vector of the record at {@code rid} when it can be read where the record lies, in a new array, or {@code null} for
   * every other answer: a shape left to the record layer, no vector, one this index cannot score. For a caller that reads
   * the record itself in all those cases, to report them as it always has.
   */
  float[] readInPlaceOnly(final RID rid) {
    final float[] vector = readInPlace(rid, null);
    return vector != FALLBACK && vector != null && usable(vector) ? vector : null;
  }

  private float[] readInPlace(final RID rid, final float[] scratch) {
    if (encoding != VectorEncoding.FLOAT32)
      return FALLBACK;

    // A thread with no context of this database yet (a worker of the graph build) has no transaction to ask: the record layer
    // reads its first record, and sets the context up on the way as every record read does
    final TransactionContext transaction = database.getTransactionIfExists();
    if (transaction == null || transaction.getWrittenRecord(rid) != null)
      return FALLBACK;

    final LocalBucket bucket = bucketReadInPlace(rid.getBucketId());
    if (bucket == null)
      return FALLBACK;

    final BasePage page;
    try {
      page = bucket.getPlainRecordForAbsoluteReads(rid.getPosition(), location, lastPageId);
    } catch (final IOException e) {
      return FALLBACK;
    }
    if (page == null)
      // NOT A PLAIN RECORD OF ITS OWN SLOT, OR GONE: THE DATABASE READS IT, AND RAISES WHAT IT RAISES
      return FALLBACK;
    lastPageId = page.getPageId();

    if (propertyId == UNRESOLVED)
      propertyId = database.getSchema().getDictionary().getIdByName(propertyName, false);

    try {
      return readFromRecord(page, location[0], location[1], scratch);
    } catch (final RuntimeException e) {
      // A NUMBER OR AN ARRAY THAT RUNS OFF THE PAGE: THE DATABASE READS THE RECORD AND LOGS IT AS DAMAGED, AS IT ALWAYS DID. FINE
      // HERE, SO A PARSE THAT WRONGLY GIVES UP ON A SOUND RECORD (AND QUIETLY TAKES THE SLOW PATH) CAN STILL BE SEEN
      LogManager.instance().log(this, Level.FINE, "Stored vector of %s not read in place, read through the database: %s", rid,
          e.getMessage());
      return FALLBACK;
    }
  }

  private float[] readFromRecord(final BasePage page, final int start, final int length, final float[] scratch) {
    final int end = start + length;
    final byte recordType = page.readByte(start);
    final int properties;
    if (recordType == Document.RECORD_TYPE)
      properties = start + DOCUMENT_PREFIX_SIZE;
    else if (recordType == Vertex.RECORD_TYPE)
      properties = start + VERTEX_PREFIX_SIZE;
    else
      return FALLBACK;

    if (properties + Binary.INT_SERIALIZED_SIZE >= end)
      return FALLBACK;

    // THE HEADER BinarySerializer WRITES: WHERE IT ENDS (FROM THE START OF THE RECORD), THE PROPERTY COUNT, THEN FOR EACH
    // PROPERTY ITS NAME ID AND THE OFFSET OF ITS VALUE FROM THE END OF THE HEADER
    final int headerEndOffset = page.readInt(properties);
    if (headerEndOffset < 0 || headerEndOffset > length)
      return FALLBACK;
    final int headerEnd = start + headerEndOffset;

    int cursor = properties + Binary.INT_SERIALIZED_SIZE;
    final long count = page.readUnsignedNumber(cursor);
    cursor += page.readNumberSize(cursor);
    if (count < 0 || count * 2 > headerEnd - cursor)
      return FALLBACK;

    if (propertyId < 0)
      // A PROPERTY NAME IS ALWAYS WRITTEN THROUGH THE DICTIONARY: ONE IT DOES NOT KNOW IS IN NO RECORD
      return null;

    int value = -1;
    for (long i = 0; i < count; i++) {
      final long nameId = page.readUnsignedNumber(cursor);
      cursor += page.readNumberSize(cursor);
      final long contentPosition = page.readUnsignedNumber(cursor);
      cursor += page.readNumberSize(cursor);
      if (cursor > headerEnd)
        return FALLBACK;
      if (nameId == propertyId) {
        if (contentPosition < 0 || contentPosition >= length - headerEndOffset)
          return FALLBACK;
        value = headerEnd + (int) contentPosition;
        break;
      }
    }
    if (value < 0)
      return null;

    final byte type = page.readByte(value);
    if (type == BinaryTypes.TYPE_NULL)
      return null;
    if (type != BinaryTypes.TYPE_ARRAY_OF_FLOATS_FIXED && type != BinaryTypes.TYPE_ARRAY_OF_FLOATS)
      // A LIST, A DOUBLE ARRAY, AN EXTERNAL VALUE...: THE CONVERSION OF THE RECORD LAYER ANSWERS
      return FALLBACK;

    cursor = value + Binary.BYTE_SERIALIZED_SIZE;
    final long elements = page.readUnsignedNumber(cursor);
    cursor += page.readNumberSize(cursor);
    if (elements < 0 || elements > end - cursor)
      return FALLBACK;
    if (elements != dimensions)
      // ANOTHER ARITY: NOT A VECTOR THIS INDEX CAN SCORE, AS THE RECORD LAYER'S READ SAYS TOO
      return null;

    if (type == BinaryTypes.TYPE_ARRAY_OF_FLOATS_FIXED) {
      if ((long) cursor + (long) dimensions * Binary.FLOAT_SERIALIZED_SIZE > end)
        return FALLBACK;
      final float[] vector = scratch != null ? scratch : new float[dimensions];
      page.readFloatArray(cursor, vector, dimensions);
      return vector;
    }

    // THE VARINT FORM OF THE BUILDS BEFORE #10856: THE BITS OF EACH FLOAT AS A SIGNED NUMBER
    final float[] vector = scratch != null ? scratch : new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      if (cursor >= end)
        return FALLBACK;
      vector[i] = Float.intBitsToFloat((int) page.readNumber(cursor));
      cursor += page.readNumberSize(cursor);
    }
    return cursor <= end ? vector : FALLBACK;
  }

  /** The bucket of {@code bucketId} when its records can be read in place, null when they must be read through the database. */
  private LocalBucket bucketReadInPlace(final int bucketId) {
    if (bucketId != lastBucketId) {
      lastBucketId = bucketId;
      lastPageId = null;
      lastBucket = !hasReadListeners(bucketId) && database.getSchema().getBucketByIdIfExists(bucketId) instanceof LocalBucket bucket ?
          bucket :
          null;
    }
    return lastBucket;
  }

  private boolean hasReadListeners(final int bucketId) {
    final RecordEvents events = database.getEvents();
    if (!(events instanceof RecordEventsRegistry registry) || registry.hasBeforeReadListeners() || registry.hasAfterReadListeners())
      return true;
    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    return type != null && (!(type instanceof LocalDocumentType localType) || localType.hasBeforeReadListeners()
        || localType.hasAfterReadListeners());
  }

  private float[] readThroughDatabase(final RID rid) {
    final Document doc = (Document) database.lookupByRID(rid, false);
    final Object raw = doc.get(propertyName);
    if (raw == null)
      return null;
    final float[] vector = VectorUtils.toFloatArray(raw, encoding);
    return vector.length == dimensions && usable(vector) ? vector : null;
  }

  private boolean usable(final float[] vector) {
    if (!zeroIsUnscorable)
      return true;
    for (int i = 0; i < dimensions; i++)
      if (vector[i] != 0.0f)
        return true;
    return false;
  }
}
