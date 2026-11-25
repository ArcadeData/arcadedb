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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The argument counts (command name included) {@link RedisCommandArity} enforces, at both edges of every command's range: one
 * below the minimum, the minimum, the maximum and one above it. Real Redis answers {@code wrong number of arguments for '<cmd>'
 * command} outside of them.
 */
class RedisCommandArityTest {
  /** {min, max} per command, max -1 for none: the shape of the commands both surfaces share. */
  private static final Map<String, int[]> RAM = Map.ofEntries(Map.entry("PING", new int[] { 1, 2 }), Map.entry("DECR", new int[] { 2, 2 }),
      Map.entry("DECRBY", new int[] { 3, 3 }), Map.entry("INCR", new int[] { 2, 2 }), Map.entry("INCRBY", new int[] { 3, 3 }),
      Map.entry("INCRBYFLOAT", new int[] { 3, 3 }), Map.entry("GET", new int[] { 2, 2 }), Map.entry("GETDEL", new int[] { 2, 2 }),
      Map.entry("EXISTS", new int[] { 2, -1 }), Map.entry("SET", new int[] { 3, -1 }));

  /** The commands only the wire path has: the hash commands (bucket form) and SELECT. */
  private static final Map<String, int[]> WIRE_ONLY = Map.ofEntries(Map.entry("HDEL", new int[] { 2, -1 }),
      Map.entry("HEXISTS", new int[] { 3, 3 }), Map.entry("HGET", new int[] { 3, 3 }), Map.entry("HMGET", new int[] { 3, -1 }),
      Map.entry("HSET", new int[] { 3, -1 }), Map.entry("HMSET", new int[] { 3, -1 }), Map.entry("SELECT", new int[] { 2, 2 }));

  private static void assertRange(final String command, final int[] range, final BiConsumer<String, Integer> check) {
    final int min = range[0];
    final int max = range[1];
    if (min > 0)
      assertThatThrownBy(() -> check.accept(command, min - 1)).as("%s with %d arguments", command, min - 1).isInstanceOf(RedisException.class)
          .hasMessage("wrong number of arguments for '" + command.toLowerCase(Locale.ROOT) + "' command");
    assertThatCode(() -> check.accept(command, min)).as("%s with %d arguments", command, min).doesNotThrowAnyException();
    if (max < 0) {
      assertThatCode(() -> check.accept(command, 1000)).as("%s with 1000 arguments", command).doesNotThrowAnyException();
    } else {
      assertThatCode(() -> check.accept(command, max)).as("%s with %d arguments", command, max).doesNotThrowAnyException();
      assertThatThrownBy(() -> check.accept(command, max + 1)).as("%s with %d arguments", command, max + 1)
          .isInstanceOf(RedisException.class);
    }
  }

  @Test
  void theSharedCommandsAreBoundedAtBothEdges() {
    assertThat(RedisCommandArity.ramCommands()).containsExactlyInAnyOrderElementsOf(RAM.keySet());
    for (final Map.Entry<String, int[]> entry : RAM.entrySet())
      assertRange(entry.getKey(), entry.getValue(), RedisCommandArity::checkRam);
  }

  @Test
  void theWireCommandsAreTheSharedOnesAndTheHashCommandsAndSelect() {
    final List<String> expected = new ArrayList<>(RAM.keySet());
    expected.addAll(WIRE_ONLY.keySet());
    assertThat(RedisCommandArity.wireCommands()).containsExactlyInAnyOrderElementsOf(expected);
    for (final Map.Entry<String, int[]> entry : RAM.entrySet())
      assertRange(entry.getKey(), entry.getValue(), RedisCommandArity::checkWire);
    for (final Map.Entry<String, int[]> entry : WIRE_ONLY.entrySet())
      assertRange(entry.getKey(), entry.getValue(), RedisCommandArity::checkWire);
  }

  @Test
  void aCommandNoTableNamesIsNotCheckedHere() {
    assertThatCode(() -> RedisCommandArity.checkRam("NOSUCH", 0)).doesNotThrowAnyException();
    assertThatCode(() -> RedisCommandArity.checkWire("NOSUCH", 99)).doesNotThrowAnyException();
    // the hash commands and SELECT are wire commands only: the shared check does not know them
    assertThatCode(() -> RedisCommandArity.checkRam("SELECT", 99)).doesNotThrowAnyException();
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("NOSUCH", List.of("NOSUCH"))).doesNotThrowAnyException();
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("SELECT", List.of("SELECT", "0", "1"))).doesNotThrowAnyException();
  }

  @Test
  void theQueryLanguageChecksTheSharedCommandsLikeTheWire() {
    assertThat(RedisCommandArity.queryLanguageCommands()).containsAll(RAM.keySet()).doesNotContain("SELECT");
    for (final Map.Entry<String, int[]> entry : RAM.entrySet())
      assertRange(entry.getKey(), entry.getValue(), (command, count) -> RedisCommandArity.checkQueryLanguage(command, words(command, count)));
  }

  @Test
  void theQueryLanguageHashCommandsTakeTheShapeTheFirstArgumentSelects() {
    // a RID first argument ("#bucket:position") selects the RID form, anything else the index form
    // HGET: RID form is {2, 2} (HGET #1:0 field), index form {3, 3} (HGET Type[idx] key)
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET", "#1:0"))).doesNotThrowAnyException();
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET", "#1:0", "extra")))
        .isInstanceOf(RedisException.class);
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET"))).isInstanceOf(RedisException.class);
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET", "Person[id]", "1"))).doesNotThrowAnyException();
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET", "Person[id]")))
        .isInstanceOf(RedisException.class);
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HGET", List.of("HGET", "Person[id]", "1", "2")))
        .isInstanceOf(RedisException.class);

    // HEXISTS the same shape as HGET
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("HEXISTS", List.of("HEXISTS", "#1:0"))).doesNotThrowAnyException();
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HEXISTS", List.of("HEXISTS", "#1:0", "x")))
        .isInstanceOf(RedisException.class);
    assertThatCode(() -> RedisCommandArity.checkQueryLanguage("HEXISTS", List.of("HEXISTS", "Person[id]", "1"))).doesNotThrowAnyException();
    assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage("HEXISTS", List.of("HEXISTS", "Person[id]")))
        .isInstanceOf(RedisException.class);

    // HMGET and HDEL: any number of keys, at least one RID / at least one key
    for (final String command : List.of("HMGET", "HDEL")) {
      assertThatCode(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "#1:0"))).doesNotThrowAnyException();
      assertThatCode(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "#1:0", "#1:1", "#1:2"))).doesNotThrowAnyException();
      assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command))).isInstanceOf(RedisException.class);
      assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "Person[id]"))).isInstanceOf(RedisException.class);
      assertThatCode(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "Person[id]", "1", "2", "3"))).doesNotThrowAnyException();
    }

    // HSET and HMSET: three or more words in both forms
    for (final String command : List.of("HSET", "HMSET")) {
      assertThatCode(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "#1:0", "{}"))).doesNotThrowAnyException();
      assertThatCode(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "Person[id]", "1", "{}"))).doesNotThrowAnyException();
      assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "#1:0"))).isInstanceOf(RedisException.class);
      assertThatThrownBy(() -> RedisCommandArity.checkQueryLanguage(command, List.of(command, "Person[id]"))).isInstanceOf(RedisException.class);
    }
  }

  private static List<String> words(final String command, final int count) {
    final List<String> words = new ArrayList<>();
    if (count > 0)
      words.add(command);
    for (int i = 1; i < count; i++)
      words.add("w" + i);
    return words;
  }
}
