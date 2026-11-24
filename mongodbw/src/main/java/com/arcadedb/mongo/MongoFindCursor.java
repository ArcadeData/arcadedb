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
package com.arcadedb.mongo;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * The rows a {@code find} selects, read and converted to reply documents one batch at a time (issue #13052: a {@code find} used to
 * convert every row before it answered, so a big collection was held twice in memory, the first document waited for the last one
 * and a reply past the 48 MB a driver accepts made the whole command fail).
 * <p>
 * It is a plain {@link Cursor}: {@link #takeDocuments} serves a {@code getMore}. The rows are pulled from a live iterator (a type
 * scan, or a SQL {@link com.arcadedb.query.sql.executor.ResultSet}), so the cursor holds that resource until it is exhausted or
 * {@link #close() closed}: the creator closes it when the first batch drains the rows, and the database wrapper closes it on
 * {@code killCursors}, when it has been idle for too long, when the collection is dropped, or when the database it reads is
 * replaced. It is not tied to a connection: the batches of one cursor may be asked on different connections, and threads.
 * <p>
 * Skip and limit are counted on the rows, before any conversion: a skipped row is never converted, and the rows past the limit are
 * never read. The cursor is used by one request at a time (a {@code getMore} racing the {@code killCursors} of the same id is
 * possible), so every method is synchronized.
 */
final class MongoFindCursor implements Cursor {
  /** The size of the first batch mongod answers when the command names none. */
  static final int  DEFAULT_FIRST_BATCH = 101;
  /** What mongod keeps a batch under (BSONObjMaxUserSize): the reply is held in memory whole, and a driver refuses one past 48 MB. */
  static final long MAX_BATCH_BYTES     = 16L * 1024 * 1024;

  private final Iterator<?>    rows;
  private final AutoCloseable  resource;
  private final MongoProjection projection;
  private       long           toSkip;
  private       long           remaining;
  private       boolean        closed;
  private       boolean        drained;
  /** The document converted for a batch that had no room for it, answered first by the next one, with its estimated size. */
  private       Document       pending;
  private       long           pendingSize;

  // WHAT THE DATABASE WRAPPER KNOWS ABOUT A REGISTERED CURSOR: SET BEFORE THE CURSOR IS PUBLISHED IN ITS MAP, WHICH PUBLISHES THEM TOO
  private volatile long    id;
  private          String  owner;
  private          String  collection;
  /** Asked for with {@code noCursorTimeout}: never closed for being idle. */
  private          boolean noTimeout;
  private volatile long    lastUsedNanos;

  /**
   * @param rows       the rows in the order they are answered: ArcadeDB records or rows, or the documents of an answer that had to be
   *                   built whole (a sort)
   * @param resource   what to release when the rows are done with, or {@code null}
   * @param skip       the rows to pass over before the first one is answered
   * @param limit      how many rows are answered at most, 0 or less for all of them
   * @param projection applied to every document answered, or {@code null}
   */
  MongoFindCursor(final Iterator<?> rows, final AutoCloseable resource, final long skip, final long limit,
      final MongoProjection projection) {
    this.rows = rows;
    this.resource = resource;
    this.projection = projection;
    this.toSkip = Math.max(skip, 0L);
    this.remaining = limit > 0L ? limit : Long.MAX_VALUE;
  }

  /** A cursor that answers nothing. */
  static MongoFindCursor empty() {
    final MongoFindCursor cursor = new MongoFindCursor(Collections.emptyIterator(), null, 0L, 0L, null);
    cursor.drained = true;
    return cursor;
  }

  @Override
  public long getId() {
    return id;
  }

  /** True when no document is left: the rows are done (or the limit is reached), and what the cursor held is released. */
  @Override
  public synchronized boolean isEmpty() {
    if (closed)
      return true;
    if (pending != null)
      return false;
    if (remaining <= 0L) {
      close();
      return true;
    }
    try {
      skipRows();
      if (rows.hasNext())
        return false;
    } catch (final RuntimeException | Error e) {
      close();
      throw e;
    }
    close();
    return true;
  }

  @Override
  public List<Document> takeDocuments(final int batchSize) {
    return takeBatch(batchSize > 0 ? batchSize : Integer.MAX_VALUE, MAX_BATCH_BYTES);
  }

  /**
   * Converts and answers the next documents: at most {@code maxDocuments}, and as few as keep the batch under {@code maxBytes}
   * (the first one is answered whatever its size, or the cursor would never advance past it). The cursor is released the moment a
   * batch finds the rows done, so a batch that ends at its size is the only one that leaves it open without knowing.
   */
  synchronized List<Document> takeBatch(final int maxDocuments, final long maxBytes) {
    if (closed || maxDocuments <= 0)
      return new ArrayList<>(0);
    final List<Document> batch = new ArrayList<>((int) Math.min(maxDocuments, 128L));
    try {
      skipRows();
      final boolean budgeted = maxBytes != Long.MAX_VALUE;
      long bytes = 0L;
      while (batch.size() < maxDocuments) {
        final Document document;
        long size = 0L;
        if (pending != null) {
          // the document the batch before it had no room for
          document = pending;
          size = pendingSize;
          pending = null;
        } else {
          if (remaining <= 0L || !rows.hasNext()) {
            drained = true;
            break;
          }
          final Document converted = toDocument(rows.next());
          document = projection != null ? projection.apply(converted) : converted;
          // the size is not asked of a batch with no budget (an OP_QUERY answers every document in one reply): it would walk every value
          if (budgeted)
            size = estimateSize(document);
        }
        // a document that does not fit what the batch holds waits for the next one, which it opens: a batch is never filled past its
        // budget, except by its first document, which is answered whatever its size or the cursor would never advance past it
        if (budgeted && !batch.isEmpty() && bytes + size > maxBytes) {
          pending = document;
          pendingSize = size;
          break;
        }
        batch.add(document);
        bytes += size;
        // the limit is reached with this document: known without reading a row more
        if (--remaining <= 0L) {
          drained = true;
          break;
        }
        if (budgeted && bytes >= maxBytes)
          break;
      }
    } catch (final RuntimeException | Error e) {
      close();
      throw e;
    }
    if (drained)
      close();
    return batch;
  }

  /**
   * True once a batch has seen the rows end or the limit reached, which costs no read of its own: a batch that stopped at its size
   * does not know whether a row follows, and the cursor stays open for the {@code getMore} that finds out.
   */
  synchronized boolean isDrained() {
    return drained || closed;
  }

  private void skipRows() {
    // the rows are consumed, not converted: a skipped row costs one read
    while (toSkip > 0L && rows.hasNext()) {
      rows.next();
      --toSkip;
    }
    // the rows ran out inside the skip: nothing is left to answer
    if (toSkip > 0L)
      remaining = 0L;
  }

  /** Releases what the rows hold. Idempotent. */
  synchronized void close() {
    if (closed)
      return;
    closed = true;
    pending = null;
    if (resource != null)
      try {
        resource.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error on closing the rows of a MongoDB cursor", e);
      }
  }

  synchronized boolean isClosed() {
    return closed;
  }

  static Document toDocument(final Object row) {
    if (row instanceof Document document)
      return document;
    if (row instanceof Result result)
      return MongoDBToSqlTranslator.convertDocumentToMongoDB(result);
    if (row instanceof com.arcadedb.database.Document document)
      return MongoDBToSqlTranslator.convertDocumentToMongoDB(document);
    throw new IllegalArgumentException("Object not supported: " + (row != null ? row.getClass().getName() : null));
  }

  // ---- the part the database wrapper uses to keep it between requests

  /** Called before the cursor is published to the requests of the other connections: what they read of it is set by then. */
  void register(final long id, final String owner, final String collection, final boolean noTimeout, final long nowNanos) {
    this.id = id;
    this.owner = owner;
    this.collection = collection;
    this.noTimeout = noTimeout;
    touch(nowNanos);
  }

  void touch(final long nowNanos) {
    this.lastUsedNanos = nowNanos;
  }

  String getOwner() {
    return owner;
  }

  String getCollection() {
    return collection;
  }

  long getLastUsedNanos() {
    return lastUsedNanos;
  }

  boolean isNoTimeout() {
    return noTimeout;
  }

  // ---- the size a document takes in a reply

  /**
   * What a document takes in a BSON reply, estimated from its values: a string counts the bytes of its UTF-8 text, a binary its
   * length, a number or a date the 16 bytes that cover the widest of them. Nothing is encoded, this only decides where a batch ends:
   * the budget is a third of the message a driver accepts, and no batch is filled past it but by its first document.
   */
  static long estimateSize(final Document document) {
    long size = 5L;
    for (final Map.Entry<String, Object> entry : document.entrySet())
      size += 2L + utf8Length(entry.getKey()) + estimateSize(entry.getValue());
    return size;
  }

  private static long estimateSize(final Object value) {
    if (value == null)
      return 0L;
    if (value instanceof String string)
      return 5L + utf8Length(string);
    if (value instanceof Document document)
      return estimateSize(document);
    if (value instanceof Collection<?> collection) {
      long size = 5L;
      for (final Object item : collection)
        size += 4L + estimateSize(item);
      return size;
    }
    if (value instanceof byte[] bytes)
      return 5L + bytes.length;
    // what a byte[] property is answered as (MongoDBToSqlTranslator): a binary blob is the one value that can take megabytes
    if (value instanceof BinData binary)
      return 5L + binary.getData().length;
    if (value instanceof BsonRegularExpression regex)
      return 2L + utf8Length(regex.getPattern()) + (regex.getOptions() != null ? regex.getOptions().length() : 0);
    if (value instanceof Map<?, ?> map) {
      long size = 5L;
      for (final Map.Entry<?, ?> entry : map.entrySet())
        size += 2L + utf8Length(String.valueOf(entry.getKey())) + estimateSize(entry.getValue());
      return size;
    }
    // numbers, booleans, dates, object ids: 16 bytes covers the widest of them
    return 16L;
  }

  /** The bytes of the UTF-8 encoding of {@code text} (a surrogate pair is the 4 bytes of its code point, 2 for each of its chars). */
  private static long utf8Length(final String text) {
    final int length = text.length();
    long bytes = length;
    for (int i = 0; i < length; i++) {
      final char c = text.charAt(i);
      if (c >= 0x80)
        bytes += c < 0x800 || Character.isSurrogate(c) ? 1L : 2L;
    }
    return bytes;
  }
}
