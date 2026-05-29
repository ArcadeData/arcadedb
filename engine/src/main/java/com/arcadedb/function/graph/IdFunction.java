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
package com.arcadedb.function.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * id() function - returns the internal ID of a node or relationship as a numeric (Long) value, to match Neo4j semantics where id() returns INTEGER. The two
 * components of an ArcadeDB {@link RID} (bucketId, offset) are packed into a single Long so it can participate in numeric predicates like
 * {@code WHERE id(n) >= 0}. For a stable string identifier use {@code elementId()} instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class IdFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "id";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("id() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof Identifiable identifiable)
      return encodeRidAsLong(identifiable.getIdentity());
    return null;
  }

  /**
   * Packs an ArcadeDB {@link RID} (bucketId, offset) into a single non-negative {@code long} so it can be used in numeric predicates. Out of the 63 usable bits
   * (bit 63 is always kept clear so the result is non-negative, preserving the Neo4j {@code id(n) >= 0} semantics), the upper bits hold the bucketId and the
   * lower bits the offset. The split is governed by {@link GlobalConfiguration#OPENCYPHER_ID_BUCKET_BITS} (default 16 bits for the bucketId, leaving 47 for the
   * offset). Throws if either component does not fit the reserved width, so an out-of-range value fails loudly instead of silently colliding with another RID.
   * The encoding is reversible via {@link #decodeLongToRidString(long)} as long as both use the same configuration.
   */
  public static long encodeRidAsLong(final RID rid) {
    final int bucketBits = GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.getValueAsInteger();
    final int positionBits = 63 - bucketBits;
    final int bucketId = rid.getBucketId();
    final long position = rid.getPosition();

    if (bucketId < 0 || bucketId >= (1L << bucketBits))
      throw new CommandExecutionException(
          "Cannot encode RID " + rid + " as a numeric id(): bucketId " + bucketId + " does not fit the " + bucketBits
              + " bits reserved for buckets (max " + ((1L << bucketBits) - 1) + "). Increase `"
              + GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.getKey() + "`.");

    final long positionMask = (1L << positionBits) - 1;
    if (position < 0 || position > positionMask)
      throw new CommandExecutionException(
          "Cannot encode RID " + rid + " as a numeric id(): position " + position + " does not fit the " + positionBits
              + " bits reserved for positions (max " + positionMask + "). Decrease `"
              + GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.getKey() + "`.");

    return ((long) bucketId << positionBits) | (position & positionMask);
  }

  /**
   * Reverses {@link #encodeRidAsLong(RID)} into a native {@link RID}, using the same bit split configured by {@link GlobalConfiguration#OPENCYPHER_ID_BUCKET_BITS}.
   * Lets a Cypher-style numeric id be resolved back to a record in O(1) via {@code lookupByRID} (e.g. the SQL {@code cypherRID()} function or
   * {@code MATCH (n) WHERE id(n) = $longId}). A valid encoded id is always non-negative (the sign bit is reserved); callers that accept untrusted input should
   * reject negative values before calling this.
   */
  public static RID decodeLongToRid(final BasicDatabase database, final long encoded) {
    final int bucketBits = GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.getValueAsInteger();
    final int positionBits = 63 - bucketBits;
    return RID.create(database, (int) (encoded >>> positionBits), encoded & ((1L << positionBits) - 1));
  }

  /**
   * Reverses {@link #encodeRidAsLong(RID)} into the canonical {@code #bucketId:offset} string form. Used by lookup paths that resolve a Long-encoded RID back to
   * a record (e.g. {@code MATCH (n) WHERE id(n) = $longId}).
   */
  public static String decodeLongToRidString(final long encoded) {
    return decodeLongToRid(null, encoded).toString();
  }
}
