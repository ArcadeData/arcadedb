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
  private RedisIndexKeys() {
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
