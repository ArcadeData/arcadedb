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
package com.arcadedb.server.http.handler.batch;

import com.arcadedb.database.RID;

import java.util.Arrays;

/**
 * Resolves edges against the POSITION of the vertex in the payload: the first vertex is {@code 0}, the second
 * {@code 1}, and an edge points at them with {@code "@from": 0} (the {@code v0} form our own client emits is
 * accepted too).
 * <p>
 * A client that splits one logical load into several requests keeps a single counter across all of them, so the
 * payload of the second request starts at a position that is not 0. {@code ordinalBase} declares where it starts;
 * everything below it belongs to an earlier request and has to be referenced by RID, which is what the client
 * already does for the vertices it has resolved.
 * <p>
 * This is the shape a bulk load wants (issue #5470). There is no key to store and nothing to hash or compare: the
 * mapping is two primitive arrays indexed by the ordinal, so it costs <b>12 bytes per vertex</b> against ~87 for an
 * arbitrary temporary id (~190MB instead of ~1.4GB for 16M vertices), and resolving an edge is an array read
 * instead of a hash lookup - let alone an index lookup, which at that scale would thrash the page cache and defeat
 * the point of the bulk endpoint.
 * <p>
 * The trade is that the client has to number its vertices in stream order. Numbering that does not match the
 * payload is rejected on the spot rather than silently resolving an edge to the wrong vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OrdinalVertexRefResolver implements VertexRefResolver {

  private static final int DEFAULT_CAPACITY = 4_096;

  private final long   ordinalBase;
  private       int[]  bucketIds;
  private       long[] positions;
  private       int    size;

  public OrdinalVertexRefResolver(final int expectedVertices, final long ordinalBase) {
    final int capacity = expectedVertices > 0 ? expectedVertices : DEFAULT_CAPACITY;
    bucketIds = new int[capacity];
    positions = new long[capacity];
    this.ordinalBase = ordinalBase;
  }

  @Override
  public void put(final String tempId, final int ordinal, final RID rid) {
    if (ordinal < 0)
      throw new IllegalArgumentException(
          "A single request cannot carry more than " + Integer.MAX_VALUE + " vertices in refMode=ordinal");

    if (ordinal >= bucketIds.length) {
      final int capacity = Math.max(ordinal + 1, bucketIds.length * 2);
      bucketIds = Arrays.copyOf(bucketIds, capacity);
      positions = Arrays.copyOf(positions, capacity);
    }

    bucketIds[ordinal] = rid.getBucketId();
    positions[ordinal] = rid.getPosition();
    if (ordinal >= size)
      size = ordinal + 1;
  }

  @Override
  public RID get(final String ref, final int lineNumber) {
    final long local = parseOrdinal(ref, lineNumber) - ordinalBase;

    if (local < 0)
      throw new IllegalArgumentException("Reference '" + ref + "' at line " + lineNumber + " points at a vertex of an "
          + "earlier request: this payload starts at position " + ordinalBase + ", so anything before it has to be "
          + "referenced by RID (#bucket:position)");

    if (local >= size)
      throw new IllegalArgumentException("Reference '" + ref + "' at line " + lineNumber + " points at vertex number "
          + (ordinalBase + local) + " but only " + size + " vertices were loaded before it. Vertices must appear "
          + "before the edges that reference them");

    return new RID(bucketIds[(int) local], positions[(int) local]);
  }

  @Override
  public void checkVertexId(final String tempId, final int ordinal, final int lineNumber) {
    if (tempId == null)
      // The position in the payload is the id: declaring it is optional.
      return;

    final long declared = parseOrdinal(tempId, lineNumber);
    if (declared != ordinalBase + ordinal)
      throw new IllegalArgumentException("Vertex at line " + lineNumber + " declares @id '" + tempId
          + "' but with refMode=ordinal the vertices are numbered in payload order: expected '"
          + (ordinalBase + ordinal) + "' or no @id at all");
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public long retainedBytes() {
    return (long) bucketIds.length * Integer.BYTES + (long) positions.length * Long.BYTES;
  }

  @Override
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < size; i++)
      consumer.accept(Long.toString(ordinalBase + i), new RID(bucketIds[i], positions[i]));
  }

  /**
   * Parses a vertex position, accepting both the bare number and the {@code v<number>} form that
   * {@code RemoteGraphBatch} generates.
   */
  private static long parseOrdinal(final String ref, final int lineNumber) {
    final int start = !ref.isEmpty() && (ref.charAt(0) == 'v' || ref.charAt(0) == 'V') ? 1 : 0;

    if (start == ref.length())
      throw notAnOrdinal(ref, lineNumber);

    long ordinal = 0;
    for (int i = start; i < ref.length(); i++) {
      final char c = ref.charAt(i);
      if (c < '0' || c > '9')
        throw notAnOrdinal(ref, lineNumber);

      ordinal = ordinal * 10 + (c - '0');
      if (ordinal < 0)
        throw notAnOrdinal(ref, lineNumber);
    }
    return ordinal;
  }

  private static IllegalArgumentException notAnOrdinal(final String ref, final int lineNumber) {
    return new IllegalArgumentException("Reference '" + ref + "' at line " + lineNumber
        + " is not a vertex position: with refMode=ordinal edges reference vertices by their 0-based position in "
        + "the payload (0, 1, 2... or v0, v1, v2...) or by RID (#bucket:position)");
  }
}
