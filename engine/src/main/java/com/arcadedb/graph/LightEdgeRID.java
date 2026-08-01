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
package com.arcadedb.graph;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;

/**
 * Identity of a lightweight edge.
 * <p>
 * A lightweight edge has no record, so it has no address. Its bucket/offset pair is {@code #<edgeType first
 * bucket>:-1}: the bucket names the edge type and the negative offset says "there is nothing to load". That pair is
 * therefore shared by <b>every</b> lightweight edge of the type and cannot serve as an identity - which is what this
 * class supplies, by carrying the endpoints the edge connects.
 * <p>
 * The identity of a lightweight edge is the triple <b>(edge type, out vertex, in vertex)</b>, and nothing else: with no
 * properties there is no observable difference between two lightweight edges of the same type over the same ordered
 * pair, so they are the same edge. See the lightweight edge section of the documentation for what happens if an
 * application creates a second one anyway.
 * <p>
 * The bucket and offset are unchanged from what the edge-list chunk holds, so this class costs <b>nothing on disk</b>:
 * {@link com.arcadedb.graph.MutableEdgeSegment} writes {@link #getBucketId()} and {@link #getPosition()} exactly as
 * before. It also costs nothing extra in allocations - it replaces the throwaway marker RID that the traversal path
 * already built for every lightweight edge it materialised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LightEdgeRID extends DatabaseRID {
  /**
   * Offset stamped on every record-less RID. Negative by contract: {@link RID#equals} and the edge-list walk both use
   * {@code offset < 0} to mean "this RID addresses no record".
   */
  public static final long NO_RECORD_OFFSET = -1L;

  private final RID out;
  private final RID in;

  /**
   * Extends {@link DatabaseRID} rather than {@link RID} deliberately: {@code BaseRecord.upgradeRID} rebuilds any RID
   * that is not already a {@code DatabaseRID} from its bucket and offset alone, which would drop the endpoints and
   * put every lightweight edge of a type back on one shared identity.
   */
  public LightEdgeRID(final BasicDatabase database, final int edgeTypeBucketId, final RID out, final RID in) {
    super(database, edgeTypeBucketId, NO_RECORD_OFFSET);
    this.out = out;
    this.in = in;
  }

  @Override
  public RID getOutRID() {
    return out;
  }

  @Override
  public RID getInRID() {
    return in;
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    throw new RecordNotFoundException(
        "Lightweight edges have no record: " + this + " (" + out + " -> " + in + "). Read it from one of its vertices",
        this);
  }
}
