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
   * Packs an ArcadeDB {@link RID} (bucketId, offset) into a single non-negative {@code long} so it can be used in numeric predicates. The bucketId occupies the
   * upper 32 bits and the offset the lower 32 bits. Because valid bucketIds are non-negative and below {@code 2^31}, the resulting value is always
   * non-negative, which preserves the Neo4j {@code id(n) >= 0} semantics. The encoding is reversible via {@link #decodeLongToRidString(long)} as long as the
   * offset stays below {@code 2^32}.
   */
  public static long encodeRidAsLong(final RID rid) {
    return ((long) rid.getBucketId() << 32) | (rid.getPosition() & 0xFFFFFFFFL);
  }

  /**
   * Reverses {@link #encodeRidAsLong(RID)} into the canonical {@code #bucketId:offset} string form. Used by lookup paths that need to resolve a Long-encoded
   * RID back to a record (e.g. {@code MATCH (n) WHERE id(n) = $longId}).
   */
  public static String decodeLongToRidString(final long encoded) {
    final int bucketId = (int) (encoded >>> 32);
    final long offset = encoded & 0xFFFFFFFFL;
    return "#" + bucketId + ":" + offset;
  }
}
