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
package com.arcadedb.database;

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.NumberUtils;

import java.io.*;
import java.util.*;

/**
 * It represents the logical address of a record in the database. The record id is composed by the bucket id (the bucket containing the record) and the offset
 * as the absolute position of the record in the bucket.
 * <br>
 * Immutable class. The database is resolved from the thread-local context on demand, keeping this object lightweight (only bucketId + offset).
 */
public class RID implements Identifiable, Comparable<Object>, Serializable {
  protected final int  bucketId;
  protected final long offset;

  public RID(final int bucketId, final long offset) {
    this.bucketId = bucketId;
    this.offset = offset;
  }

  public RID(final String value) {
    if (!value.startsWith("#"))
      throw new IllegalArgumentException("The RID '" + value + "' is not valid");

    final String stripped = value.substring(1);
    final List<String> parts = CodeUtils.split(stripped, ':', 2);
    this.bucketId = Integer.parseInt(parts.getFirst());
    this.offset = Long.parseLong(parts.get(1));
  }

  /**
   * Factory that returns a {@link DatabaseRID} when {@code database} is non-null, otherwise a bare {@link RID}. Use this at boundaries where the database
   * reference is optional (e.g. deserialization paths, SQL conversions with nullable {@link com.arcadedb.query.sql.executor.CommandContext}) to avoid
   * repeating the null-check at every call site.
   */
  public static RID create(final BasicDatabase database, final int bucketId, final long offset) {
    return database != null ? database.newRID(bucketId, offset) : new RID(bucketId, offset);
  }

  /**
   * String-form counterpart of {@link #create(BasicDatabase, int, long)}.
   */
  public static RID create(final BasicDatabase database, final String value) {
    return database != null ? database.newRID(value) : new RID(value);
  }

  public static boolean is(final Object value) {
    if (value instanceof RID)
      return true;
    else if (value instanceof String string)
      return is(string);
    return false;
  }

  public static boolean is(final String valueAsString) {
    if (valueAsString.length() > 3 && valueAsString.charAt(0) == '#') {
      final List<String> parts = CodeUtils.split(valueAsString.substring(1), ':', 3);
      return parts.size() == 2 && NumberUtils.isIntegerNumber(parts.getFirst()) && NumberUtils.isIntegerNumber(parts.get(1));
    }
    return false;
  }

  public int getBucketId() {
    return bucketId;
  }

  public long getPosition() {
    return offset;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(12);
    buffer.append('#');
    buffer.append(bucketId);
    buffer.append(':');
    buffer.append(offset);
    return buffer.toString();
  }

  @Override
  public RID getIdentity() {
    return this;
  }

  @Override
  public Record getRecord() {
    return getRecord(true);
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    return resolveActiveDatabase("getRecord").lookupByRID(this, loadContent);
  }

  public Document asDocument() {
    return asDocument(true);
  }

  public Document asDocument(final boolean loadContent) {
    return (Document) resolveActiveDatabase("asDocument").lookupByRID(this, loadContent);
  }

  public Vertex asVertex() {
    return asVertex(true);
  }

  public Vertex asVertex(final boolean loadContent) {
    try {
      return (Vertex) resolveActiveDatabase("asVertex").lookupByRID(this, loadContent);
    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final ClassCastException e) {
      throw new RecordNotFoundException("Record " + this + " not found", this, e);
    }
  }

  public Edge asEdge() {
    return asEdge(false);
  }

  public Edge asEdge(final boolean loadContent) {
    return (Edge) resolveActiveDatabase("asEdge").lookupByRID(this, loadContent);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof Identifiable))
      return false;

    final RID o = ((Identifiable) obj).getIdentity();
    return bucketId == o.bucketId && offset == o.offset;
  }

  @Override
  public int hashCode() {
    return bucketId * 31 + Long.hashCode(offset);
  }

  @Override
  public int compareTo(final Object o) {
    RID otherRID;
    if (o instanceof RID iD)
      otherRID = iD;
    else if (o instanceof String string)
      otherRID = new RID(string);
    else
      return -1;

    if (bucketId > otherRID.bucketId)
      return 1;
    else if (bucketId < otherRID.bucketId)
      return -1;

    if (offset > otherRID.offset)
      return 1;
    else if (offset < otherRID.offset)
      return -1;

    return 0;
  }

  public boolean isValid() {
    return bucketId > -1 && offset > -1;
  }

  public PageId getPageId(final BasicDatabase db) {
    return new PageId(db, bucketId,
        (int) (getPosition() / ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage()));
  }

  /**
   * Resolves the database to use for record loading. Returns the single active database on the current thread if exactly one is in scope (the common single-DB
   * case, matching pre-26.4.1 behaviour); throws otherwise - either because no database context is active, or because multiple databases are open with
   * ambiguous active transactions. In multi-DB scenarios callers should hold a {@link DatabaseRID} (produced by {@code database.newRID(...)}) which resolves
   * directly against the owning database and does not enter this path.
   */
  private BasicDatabase resolveActiveDatabase(final String op) {
    final BasicDatabase db = DatabaseContext.INSTANCE.getActiveDatabase();
    if (db == null)
      throw unboundRid(op);
    return db;
  }

  private DatabaseOperationException unboundRid(final String op) {
    return new DatabaseOperationException(
        "Cannot call " + op + "() on bare RID " + this
            + ": no unambiguous active database on this thread. Either open a transaction on the owning database before calling, or use a DatabaseRID (e.g. database.newRID(...)) / database.lookupByRID(rid)");
  }
}
