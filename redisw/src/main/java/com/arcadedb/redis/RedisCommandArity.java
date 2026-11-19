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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Argument counts (command name included) real Redis enforces before it runs a command: {min, max}, max -1 = no upper
 * bound. One table for both surfaces, the RESP wire path ({@code RedisNetworkExecutor}, #9059) and the {@code redis} query
 * language ({@code RedisQueryEngine}, #9161), so they cannot disagree about what a command accepts: without it a surplus
 * argument was read as the INCR/DECR amount, a missing one defaulted to 1, and a missing argument surfaced as
 * {@code Index 1 out of bounds for length 1}.
 * <p>
 * Every command a surface dispatches with a fixed shape is listed (PING and SELECT were missing until #11450, so a missing
 * SELECT argument surfaced as a Java exception text and a surplus one was ignored); {@code RedisCommandArityCensusTest} (census
 * module) fails on a command the dispatch switch of either surface names and this table does not. The hash commands and SELECT
 * are listed apart because their shape is the wire one ({@code HGET <bucket> <key>}): the query language names an index or a
 * RID instead, with the counts of {@link #QUERY_LANGUAGE_HASH} (issue #11783), and has no SELECT.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RedisCommandArity {
  /** The commands with the same shape on the wire and in the query language: PING and the ones on the shared RAM map. */
  private static final Map<String, int[]> RAM = Map.ofEntries(//
      Map.entry("PING", new int[] { 1, 2 }),//
      Map.entry("DECR", new int[] { 2, 2 }), Map.entry("DECRBY", new int[] { 3, 3 }),//
      Map.entry("INCR", new int[] { 2, 2 }), Map.entry("INCRBY", new int[] { 3, 3 }),//
      Map.entry("INCRBYFLOAT", new int[] { 3, 3 }),//
      Map.entry("GET", new int[] { 2, 2 }), Map.entry("GETDEL", new int[] { 2, 2 }),//
      Map.entry("EXISTS", new int[] { 2, -1 }), Map.entry("SET", new int[] { 3, -1 }));

  /** The commands only the wire path has in this shape: the hash commands and SELECT. */
  private static final Map<String, int[]> WIRE_ONLY = Map.ofEntries(//
      Map.entry("HDEL", new int[] { 2, -1 }), Map.entry("HEXISTS", new int[] { 3, 3 }),//
      Map.entry("HGET", new int[] { 3, 3 }), Map.entry("HMGET", new int[] { 3, -1 }),//
      Map.entry("HSET", new int[] { 3, -1 }), Map.entry("HMSET", new int[] { 3, -1 }),//
      Map.entry("SELECT", new int[] { 2, 2 }));

  /**
   * The hash commands of the query language, which name an index ({@code HGET Person[id] 1}) or RIDs ({@code HGET #10:5}) instead
   * of a bucket: {ridMin, ridMax, indexMin, indexMax}, the form chosen by whether the first argument is a RID. They used to check
   * a minimum count only, so {@code HGET Person[id] 1 2 3} answered key 1 (HMGET was meant) and {@code HGET #28:0 extra junk}
   * answered the record (issue #11783).
   */
  private static final Map<String, int[]> QUERY_LANGUAGE_HASH = Map.ofEntries(//
      Map.entry("HGET", new int[] { 2, 2, 3, 3 }), Map.entry("HEXISTS", new int[] { 2, 2, 3, 3 }),//
      Map.entry("HMGET", new int[] { 2, -1, 3, -1 }), Map.entry("HDEL", new int[] { 2, -1, 3, -1 }),//
      Map.entry("HSET", new int[] { 3, -1, 3, -1 }), Map.entry("HMSET", new int[] { 3, -1, 3, -1 }));

  private RedisCommandArity() {
  }

  /**
   * Same as {@link #checkRam} for every command of the {@code redis} query language, its hash commands included in the form the
   * first argument selects.
   *
   * @param command   the upper-case command name
   * @param arguments the words of the command, its name included
   */
  public static void checkQueryLanguage(final String command, final List<String> arguments) {
    final int[] ram = RAM.get(command);
    if (ram != null) {
      check(ram, command, arguments.size());
      return;
    }
    final int[] forms = QUERY_LANGUAGE_HASH.get(command);
    if (forms == null)
      return;
    final boolean ridForm = arguments.size() > 1 && arguments.get(1).startsWith("#");
    final int offset = ridForm ? 0 : 2;
    check(forms[offset], forms[offset + 1], command, arguments.size());
  }

  /** The commands {@link #checkQueryLanguage} knows, upper case. */
  public static Set<String> queryLanguageCommands() {
    final Set<String> commands = new HashSet<>(RAM.keySet());
    commands.addAll(QUERY_LANGUAGE_HASH.keySet());
    return commands;
  }

  /**
   * Refuses a RAM command (the ones the wire path and the query language share) called with a wrong number of arguments.
   *
   * @param command       the upper-case command name
   * @param argumentCount the arguments including the command name
   *
   * @throws RedisException when the count is outside the command's bounds; a caller that is not the wire path translates it to its own
   *                        kind of client error (the query language answers a {@code CommandParsingException}, an HTTP 4xx)
   */
  public static void checkRam(final String command, final int argumentCount) {
    check(RAM.get(command), command, argumentCount);
  }

  /** Same as {@link #checkRam} for every command of the wire protocol, the hash commands included. */
  public static void checkWire(final String command, final int argumentCount) {
    final int[] arity = RAM.get(command);
    check(arity != null ? arity : WIRE_ONLY.get(command), command, argumentCount);
  }

  /** The commands {@link #checkRam} knows, upper case. */
  public static Set<String> ramCommands() {
    return RAM.keySet();
  }

  /** The commands {@link #checkWire} knows, upper case. */
  public static Set<String> wireCommands() {
    final Set<String> commands = new HashSet<>(RAM.keySet());
    commands.addAll(WIRE_ONLY.keySet());
    return commands;
  }

  private static void check(final int[] arity, final String command, final int argumentCount) {
    if (arity != null)
      check(arity[0], arity[1], command, argumentCount);
  }

  private static void check(final int min, final int max, final String command, final int argumentCount) {
    if (argumentCount < min || (max >= 0 && argumentCount > max))
      throw wrongArity(command);
  }

  public static RedisException wrongArity(final String command) {
    return new RedisException("wrong number of arguments for '" + command.toLowerCase(Locale.ROOT) + "' command");
  }
}
