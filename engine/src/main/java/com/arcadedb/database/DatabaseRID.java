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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.io.*;

/**
 * {@link RID} variant that carries a reference to its originating {@link BasicDatabase}. Used at API boundaries (record identity, query results, user-supplied
 * RID factories) so that shortcut methods like {@link #asVertex()} resolve against the correct database even when multiple databases are open on the same
 * thread. The engine keeps using the lightweight {@link RID} internally (indexes, edge segments, intermediate projections) to minimise memory footprint; only
 * values that leak to user code are wrapped in this subclass.
 * <p>
 * {@link #equals(Object)} and {@link #hashCode()} are inherited from {@link RID} and compare only bucket id and offset, so a {@link DatabaseRID} is fully
 * interchangeable with a bare {@link RID} inside {@link java.util.Set} and {@link java.util.Map} instances.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseRID extends RID {
  private final transient BasicDatabase database;

  public DatabaseRID(final BasicDatabase database, final int bucketId, final long offset) {
    super(bucketId, offset);
    this.database = database;
  }

  public DatabaseRID(final BasicDatabase database, final String value) {
    super(value);
    this.database = database;
  }

  public DatabaseRID(final BasicDatabase database, final RID rid) {
    super(rid.getBucketId(), rid.getPosition());
    this.database = database;
  }

  public BasicDatabase getBoundDatabase() {
    return database;
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    return database.lookupByRID(this, loadContent);
  }

  @Override
  public Document asDocument(final boolean loadContent) {
    return (Document) database.lookupByRID(this, loadContent);
  }

  @Override
  public Vertex asVertex(final boolean loadContent) {
    try {
      return (Vertex) database.lookupByRID(this, loadContent);
    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final Exception e) {
      throw new RecordNotFoundException("Record " + this + " not found", this, e);
    }
  }

  @Override
  public Edge asEdge(final boolean loadContent) {
    return (Edge) database.lookupByRID(this, loadContent);
  }

  public PageId getPageId() {
    return new PageId(database, bucketId,
        (int) (getPosition() / ((LocalBucket) database.getSchema().getBucketById(bucketId)).getMaxRecordsInPage()));
  }

  /**
   * Serialize as a bare {@link RID}: the bound database reference is not portable across JVMs or process boundaries.
   */
  @Serial
  private Object writeReplace() {
    return new RID(bucketId, offset);
  }
}
