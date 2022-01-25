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

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.utility.NumberUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * It represents the logical address of a record in the database. The record id is composed by the bucket id (the bucket containing the record) and the offset
 * as the absolute position of the record in the bucket.
 * <br>
 * Immutable class.
 */
public class RID implements Identifiable, Comparable<Identifiable>, Serializable {
  private transient final Database database;
  protected final         int      bucketId;
  protected final         long     offset;

  public RID(final Database database, final int bucketId, final long offset) {
    if (database == null)
      // RETRIEVE THE DATABASE FROM THE THREAD LOCAL
      this.database = DatabaseContext.INSTANCE.getActiveDatabase();
    else
      this.database = database;

    this.bucketId = bucketId;
    this.offset = offset;
  }

  public RID(final Database database, String value) {
    if (database == null)
      // RETRIEVE THE DATABASE FROM THE THREAD LOCAL
      this.database = DatabaseContext.INSTANCE.getActiveDatabase();
    else
      this.database = database;

    if (!value.startsWith("#"))
      throw new IllegalArgumentException("The RID '" + value + "' is not valid");

    value = value.substring(1);

    final String[] parts = value.split(":", 2);
    this.bucketId = Integer.parseInt(parts[0]);
    this.offset = Long.parseLong(parts[1]);
  }

  public static boolean is(final Object value) {
    if (value instanceof RID)
      return true;

    if (value instanceof String) {
      final String valueAsString = value.toString();
      if (valueAsString.length() > 3 && valueAsString.charAt(0) == '#') {
        final String[] parts = valueAsString.substring(1).split(":");
        if (parts.length == 2 && NumberUtils.isIntegerNumber(parts[0]) && NumberUtils.isIntegerNumber(parts[1])) {
          return true;
        }
      }
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
    return database.lookupByRID(this, loadContent);
  }

  public Document asDocument() {
    return asDocument(true);
  }

  public Document asDocument(final boolean loadContent) {
    return (Document) database.lookupByRID(this, loadContent);
  }

  public Vertex asVertex() {
    return asVertex(true);
  }

  public Vertex asVertex(final boolean loadContent) {
    try {
      return (Vertex) database.lookupByRID(this, loadContent);
    } catch (Exception e) {
      throw new RecordNotFoundException("Record " + this + " not found", this, e);
    }
  }

  public Edge asEdge() {
    return asEdge(true);
  }

  public Edge asEdge(final boolean loadContent) {
    return (Edge) database.lookupByRID(this, loadContent);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;

    if (!(obj instanceof Identifiable))
      return false;

    final RID o = ((Identifiable) obj).getIdentity();
    return Objects.equals(database, o.getDatabase()) && bucketId == o.bucketId && offset == o.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, bucketId, offset);
  }

  @Override
  public int compareTo(final Identifiable o) {
    final Database otherDb = o.getIdentity().getDatabase();
    if (database != null) {
      if (otherDb != null) {
        final int res = database.getName().compareTo(otherDb.getName());
        if (res != 0)
          return res;
      } else
        return -1;
    } else if (otherDb != null)
      return 1;

    final RID other = o.getIdentity();
    if (bucketId > other.bucketId)
      return 1;
    else if (bucketId < other.bucketId)
      return -1;

    if (offset > other.offset)
      return 1;
    else if (offset < other.offset)
      return -1;

    return 0;
  }

  public Database getDatabase() {
    return database;
  }

  public boolean isValid() {
    return bucketId > -1 && offset > -1;
  }
}
