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
package com.arcadedb.redis;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONArray;

/**
 * Parses the textual key an index lookup arrives with over the Redis wire protocol (HGET / HMGET / HEXISTS / HDEL)
 * and over the {@code redis} query language, in the ONE place both share.
 * <p>
 * It used to be copy/pasted per call site instead, and the copies drifted: the {@code HMGET} copy cast the key to
 * {@code String[]} rather than parsing it, so every bracketed composite key answered with a
 * {@code ClassCastException} (#6757) while its {@code HGET} sibling answered with the record.
 * <p>
 * Three key shapes are accepted:
 * <ul>
 *   <li>{@code [v1,v2]} - a JSON array, i.e. the composite key of a multi-property index;</li>
 *   <li>{@code "v"} - a quoted single value, for a value that would otherwise look like one of the other shapes;</li>
 *   <li>{@code v} - a bare single value.</li>
 * </ul>
 * Every component is handed to the index as it was written. Narrowing it to the property's declared type is the
 * index's own job ({@code LSMTreeIndexAbstract.convertKeysToDeclaredTypes}), and doing it here instead would be
 * strictly worse: this layer knows the key's characters but not the type they are a key FOR, so it can only guess
 * - and a guess turns the {@code STRING} key {@code "007"} into the {@code Long} {@code 7} that no longer finds
 * its record, and the 30-digit one into an uncaught {@code NumberFormatException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisIndexKeys {
  /** Attempts a lookup makes past a concurrent CREATE, REBUILD or DROP INDEX before telling the client to retry (issue #9811). */
  static final int MAX_LOOKUP_ATTEMPTS = 8;

  private RedisIndexKeys() {
  }

  /**
   * Looks a key up through the named index, for every Redis read and delete by key ({@code HGET}, {@code HMGET},
   * {@code HEXISTS}, {@code HDEL} and the {@code redis} query language), and answers the record it maps to, or
   * {@code null} only when the key really is not there (issue #9811).
   * <p>
   * The index is read whole or not at all ({@link TypeIndex#getForQuery}): one a concurrent DDL is creating, rebuilding or
   * dropping answers short - empty, for a key that exists - without an error, and on this protocol an empty answer IS "the
   * key does not exist". Such a read is made again on the index the name resolves to then, a few times, and reported as a
   * {@code TRYAGAIN} error if the DDL is still going on, rather than as a key that was never set. The entry and its record
   * are read against one committed state ({@link ConsistentKeyLookup}), so a concurrent commit cannot make the key vanish
   * either.
   *
   * @param key the key exactly as the client wrote it, see {@link #parse}
   */
  public static Identifiable lookup(final Database database, final String indexName, final String key) {
    final Object[] keys = parse(key);
    for (int attempt = 1; ; attempt++) {
      // resolved on every attempt: a DROP and a CREATE replace the index object under the same name, and between the two the
      // name resolves to nothing. That is retried as well; a name still unknown after the last attempt is reported as such,
      // since it may simply not exist
      final Index index;
      try {
        index = database.getSchema().getIndexByName(indexName);
      } catch (final SchemaException e) {
        if (attempt >= MAX_LOOKUP_ATTEMPTS)
          throw e;
        pause(attempt, e);
        continue;
      }
      try {
        // A name can also resolve to a single index rather than a type index - a manual index, or one bucket's. That one has no
        // sub-indexes a drop could take away one at a time, so its read is whole or fails, and a failure under a concurrent drop
        // ("File with id n was not found") is a stale read the catch below retries like any other
        try (final IndexCursor cursor = index instanceof TypeIndex typeIndex ?
            ConsistentKeyLookup.getForQuery(database, typeIndex, keys) :
            index.get(keys)) {
          return cursor.hasNext() ? cursor.next() : null;
        }
      } catch (final RuntimeException e) {
        if (!TypeIndex.isStaleRead(index, e))
          throw e;
        if (attempt >= MAX_LOOKUP_ATTEMPTS) {
          final RedisException retry = RedisException.withKind("TRYAGAIN",
              "Index '" + indexName + "' is being created, rebuilt or dropped. Retry the command");
          retry.initCause(e);
          throw retry;
        }
        pause(attempt, e);
      }
    }
  }

  /** A short, growing pause between two attempts: a DDL on the index is usually over in a few milliseconds. */
  private static void pause(final int attempt, final RuntimeException cause) {
    try {
      Thread.sleep(attempt);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw cause;
    }
  }

  /**
   * @param key the key exactly as the client wrote it
   *
   * @return the index key components, one entry per indexed property
   *
   * @throws RedisException if the key is a malformed or empty JSON array
   */
  public static Object[] parse(final String key) {
    if (key.isEmpty())
      return new Object[] { key };

    if (key.charAt(0) == '[') {
      final Object[] compositeKey;
      try {
        compositeKey = new JSONArray(key).toList().toArray();
      } catch (final Exception e) {
        throw new RedisException("Composite index key '" + key + "' is not a valid JSON array. Example: [\"John\",\"Doe\"]", e);
      }
      if (compositeKey.length == 0)
        throw new RedisException("Composite index key '" + key + "' is empty. Example: [\"John\",\"Doe\"]");
      return compositeKey;
    }

    // A LONE OR UNTERMINATED QUOTE IS PART OF THE VALUE, NOT A QUOTING OF IT: STRIPPING BOTH ENDS REGARDLESS WOULD
    // EITHER THROW (KEY `"`) OR SILENTLY DROP THE LAST CHARACTER (KEY `"abc`)
    if (key.length() > 1 && key.charAt(0) == '"' && key.charAt(key.length() - 1) == '"')
      return new Object[] { key.substring(1, key.length() - 1) };

    return new Object[] { key };
  }
}
