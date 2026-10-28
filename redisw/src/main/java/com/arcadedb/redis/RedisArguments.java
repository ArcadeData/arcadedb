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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which arguments of a RESP command are opaque bytes, the one table the wire path decides it with (issues #9057, #10752).
 * <p>
 * {@link RedisBinaryCodec#decode} keeps every byte of a bulk string, escaping a byte that is not valid UTF-8 as a lone surrogate.
 * An argument whose text is PARSED (a JSON document, a type or index name, an amount, an option) is sanitized instead, each escape
 * becoming U+FFFD, because a parser would not carry a lone surrogate through. A KEY is never parsed: it is the key of a map, the
 * connection's own or a database's global variables, so it keeps its bytes. Sanitized, the 128 invalid byte values all became the
 * same character, and two binary keys that differ only in such a byte were one key: a SET of one overwrote the other.
 * <p>
 * Positions are {first, last} argument indexes (the command name is index 0), last -1 for every argument from first on. A command
 * that is not listed has no opaque argument. A new command that takes a key, or a value it stores, is added here, and the census
 * test of issue #10752 probes every key position this table lists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RedisArguments {
  /**
   * The arguments that name a key. The hash commands name one only in their transient form ({@code HGET <database> <key>}), where the
   * key is a global variable of the database, the same key space as GET's; in their persistent form the same position is an index
   * key or a RID, whose lookup matches nothing a lone surrogate could spoil.
   */
  private static final Map<String, int[]> KEYS = Map.ofEntries(//
      Map.entry("GET", new int[] { 1, 1 }), Map.entry("GETDEL", new int[] { 1, 1 }),//
      Map.entry("EXISTS", new int[] { 1, -1 }), Map.entry("SET", new int[] { 1, 1 }),//
      Map.entry("INCR", new int[] { 1, 1 }), Map.entry("INCRBY", new int[] { 1, 1 }),//
      Map.entry("INCRBYFLOAT", new int[] { 1, 1 }), Map.entry("DECR", new int[] { 1, 1 }),//
      Map.entry("DECRBY", new int[] { 1, 1 }),//
      Map.entry("HGET", new int[] { 2, 2 }), Map.entry("HEXISTS", new int[] { 2, 2 }),//
      Map.entry("HMGET", new int[] { 2, -1 }), Map.entry("HDEL", new int[] { 2, -1 }));

  /** The arguments that are a value stored or echoed as it came. */
  private static final Map<String, int[]> VALUES = Map.of(//
      "SET", new int[] { 2, 2 }, "ECHO", new int[] { 1, 1 }, "PING", new int[] { 1, 1 });

  /** KEYS and VALUES of a command merged as {keyFirst, keyLast, valueFirst, valueLast}, what the argument loop reads. */
  private static final Map<String, int[]> OPAQUE;

  static {
    final Map<String, int[]> opaque = new HashMap<>();
    final Set<String> commands = new HashSet<>(KEYS.keySet());
    commands.addAll(VALUES.keySet());
    for (final String command : commands) {
      final int[] keys = KEYS.get(command);
      final int[] values = VALUES.get(command);
      opaque.put(command, new int[] { keys != null ? keys[0] : 0, keys != null ? keys[1] : 0, values != null ? values[0] : 0,
          values != null ? values[1] : 0 });
    }
    OPAQUE = Map.copyOf(opaque);
  }

  private RedisArguments() {
  }

  /** The commands with at least one key argument. */
  public static Set<String> keyCommands() {
    return KEYS.keySet();
  }

  /** The {first, last} key positions of {@code command}, null when it takes no key. */
  public static int[] keyPositions(final String command) {
    final int[] positions = KEYS.get(command);
    return positions != null ? positions.clone() : null;
  }

  /**
   * The opaque positions of {@code command}: {keyFirst, keyLast, valueFirst, valueLast}, an absent range as {0, 0}. Null when the
   * command has none. One lookup per command, not one per argument; the caller must not modify the array.
   */
  static int[] opaquePositions(final String command) {
    return OPAQUE.get(command);
  }

  /** Whether argument {@code index} is opaque, given the positions {@link #opaquePositions} answered for its command. */
  static boolean isOpaque(final int[] positions, final int index) {
    return positions != null && (within(positions[0], positions[1], index) || within(positions[2], positions[3], index));
  }

  private static boolean within(final int first, final int last, final int index) {
    return first > 0 && index >= first && (last < 0 || index <= last);
  }
}
