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
package com.arcadedb.census;

import com.arcadedb.redis.RedisCommandArity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11450: PING and SELECT were dispatched by the RESP wire switch but missing from {@link RedisCommandArity}, the one table
 * of argument counts both Redis surfaces enforce (#9059, #9161), so {@code SELECT} with no argument answered a Java
 * {@code IndexOutOfBoundsException} text and {@code PING a b} was accepted.
 * <p>
 * The census ranges over the authority: every word the {@code redisw} module compares a string with, in any method of any of its
 * classes (a string {@code switch} compiles to one {@code String.equals} against each label, and an {@code "AUTH".equals(cmd)} or
 * {@code "AUTH".equalsIgnoreCase(...)} test to the same call; the operands are read through {@link CallOperands}, so a word held in
 * a local first, or compared with the value a call returned, is seen too). Each word is a command of the surface that compares it -
 * in the table, or listed below with the reason it checks its own arguments - or is listed, with the method that compares it, as a
 * word that is not a command name. The census used to read one hardcoded method per surface, so the batch-level dispatch of the
 * query language ({@code executeMultipleCommands}: MULTI, EXEC, DISCARD) and any dispatch added to another method or class escaped
 * it (#12024).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisCommandArityCensusTest {
  private static final String MODULE = "redisw";

  /** The RESP wire surface, every method of it: its commands are checked by {@code RedisCommandArity.checkWire}. */
  private static final String WIRE_CLASS  = "redis/RedisNetworkExecutor";
  /** The redis query language, every method of it: its commands are checked by {@code RedisCommandArity.checkRam}. */
  private static final String QUERY_CLASS = "redis/query/RedisQueryEngine";

  /** Wire commands dispatched before the table is consulted, each checking its own shape. */
  private static final Map<String, String> WIRE_EXEMPT = Map.of(//
      "AUTH", "accepted before authentication, so before the table check; auth() takes 1 or 2 arguments and refuses any other count",
      "HELLO", "accepted before authentication; a variable list of options, parsed and refused by hello() itself");

  /** Query-language words that are commands of a shape the table does not hold, or no command at all, each with the reason. */
  private static final Map<String, String> QUERY_EXEMPT = Map.ofEntries(//
      Map.entry("HSET", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("HMSET", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("HGET", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("HMGET", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("HEXISTS", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("HDEL", "names an index or a RID instead of a bucket and checks its own forms"),//
      Map.entry("MULTI", "batch structure keyword of executeMultipleCommands, matched as a whole line, so it takes no arguments"),//
      Map.entry("EXEC", "batch structure keyword of executeMultipleCommands, matched as a whole line, so it takes no arguments"),//
      Map.entry("DISCARD", "batch structure keyword of executeMultipleCommands, matched as a whole line, so it takes no arguments"));

  /** Words compared in a method that are not command names, and why. */
  private record NotCommands(Set<String> words, String reason) {
  }

  /** The words compared in the module that are not command names, by the method ({@code class#method}) that compares them. */
  private static final Map<String, NotCommands> NOT_COMMANDS = Map.of(//
      WIRE_CLASS + "#set", new NotCommands(Set.of("NX", "XX", "GET", "EX", "PX", "EXAT", "PXAT", "KEEPTTL"), "the options of SET"),//
      WIRE_CLASS + "#hello", new NotCommands(Set.of("AUTH"), "the AUTH option of HELLO"),//
      WIRE_CLASS + "#parseInlineCommand", new NotCommands(Set.of("POST", "HOST:"),
          "the first words of an HTTP request sent to the port, refused as a cross-protocol request"),//
      "redis/RedisCounterOperations#isNonFiniteSpelling", new NotCommands(Set.of("nan", "inf", "infinity"),
          "the spellings of a non-finite number an increment refuses"));

  @Test
  void everyCommandTheWireDispatchesHasAnArity() throws Exception {
    final Set<String> dispatched = commandsOf(comparedWords(redisClasses()), WIRE_CLASS);
    assertThat(dispatched).as("the census must reach the commands RedisNetworkExecutor dispatches")
        .contains("GET", "PING", "SELECT", "AUTH");

    final Set<String> missing = new TreeSet<>(dispatched);
    missing.removeAll(RedisCommandArity.wireCommands());
    missing.removeAll(WIRE_EXEMPT.keySet());
    assertThat(missing).as(
        "a command the RESP wire path dispatches with no entry in RedisCommandArity accepts any argument count (issue #11450)")
        .isEmpty();
  }

  @Test
  void everyCommandTheQueryLanguageDispatchesHasAnArity() throws Exception {
    final Set<String> dispatched = commandsOf(comparedWords(redisClasses()), QUERY_CLASS);
    assertThat(dispatched).as("the census must reach the commands RedisQueryEngine dispatches, the batch-level ones included (#12024)")
        .contains("GET", "PING", "HGET", "MULTI", "EXEC", "DISCARD");

    final Set<String> missing = new TreeSet<>(dispatched);
    missing.removeAll(RedisCommandArity.ramCommands());
    missing.removeAll(QUERY_EXEMPT.keySet());
    assertThat(missing).as(
        "a command the redis query language dispatches with no entry in RedisCommandArity accepts any argument count (issue #11450)")
        .isEmpty();
  }

  @Test
  void noOtherClassOfTheModuleComparesACommandName() throws Exception {
    final Map<String, Set<String>> compared = comparedWords(redisClasses());
    assertThat(compared.keySet()).as("the census must reach the comparisons of the classes besides the two surfaces")
        .contains("redis/RedisCounterOperations#isNonFiniteSpelling");

    final Map<String, Set<String>> unclassified = new TreeMap<>();
    for (final Map.Entry<String, Set<String>> entry : compared.entrySet()) {
      final String className = classOf(entry.getKey());
      if (className.equals(WIRE_CLASS) || className.equals(QUERY_CLASS))
        continue;
      final Set<String> words = new TreeSet<>(entry.getValue());
      words.removeAll(notCommandsOf(entry.getKey()));
      if (!words.isEmpty())
        unclassified.put(entry.getKey(), words);
    }
    assertThat(unclassified).as("a command dispatched outside RedisNetworkExecutor and RedisQueryEngine is checked by neither "
        + "surface: dispatch it there, or list the word in NOT_COMMANDS with the reason it is not a command (#12024)").isEmpty();
  }

  @Test
  void theExemptionsAreStillCompared() throws Exception {
    // an exemption for a word nobody compares any more is a stale entry that could hide a new one under its name
    final Map<String, Set<String>> compared = comparedWords(redisClasses());
    assertThat(commandsOf(compared, WIRE_CLASS)).containsAll(WIRE_EXEMPT.keySet());
    assertThat(commandsOf(compared, QUERY_CLASS)).containsAll(QUERY_EXEMPT.keySet());
    for (final Map.Entry<String, NotCommands> entry : NOT_COMMANDS.entrySet())
      assertThat(compared.getOrDefault(entry.getKey(), Set.of())).as("the words %s still compares (%s)", entry.getKey(),
          entry.getValue().reason()).containsAll(entry.getValue().words());
  }

  @Test
  void theScanSeesEveryShapeOfAComparison() throws Exception {
    // control: the shapes the census relies on, compiled by the same javac
    final Map<String, Set<String>> compared = comparedWords(CompiledClasses.ofTheProbes());
    assertThat(compared.get("census/RedisCommandArityCensusTest#probe")).containsExactlyInAnyOrder("ALPHA", "BETA", "GAMMA", "DELTA",
        "EPSILON", "ZETA", "ETA", "THETA");
  }

  @SuppressWarnings("unused")
  private static int probe(final String command, final List<Object> arguments) {
    if ("ALPHA".equals(command))
      return 0;
    // COMPARED WITH THE VALUE A CALL RETURNED, WITH A WORD HELD IN A LOCAL FIRST, AND IGNORING THE CASE
    if ("ZETA".equalsIgnoreCase((String) arguments.getFirst()))
      return 4;
    final String held = "ETA";
    if (command.equals(held))
      return 5;
    if (command.equalsIgnoreCase("THETA"))
      return 6;
    switch (command) {
    case "BETA":
      return 1;
    case "GAMMA":
    case "DELTA":
      return 2;
    default:
      return switch (command) {
        case "EPSILON" -> 3;
        default -> -1;
      };
    }
  }

  private static Map<String, byte[]> redisClasses() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.byModule().get(MODULE);
    assertThat(classes).as("the census must read the " + MODULE + " module").isNotNull();
    return classes;
  }

  /** The words compared in the methods of {@code className}, less the ones listed as no command name for their method. */
  private static Set<String> commandsOf(final Map<String, Set<String>> compared, final String className) {
    final Set<String> commands = new TreeSet<>();
    for (final Map.Entry<String, Set<String>> entry : compared.entrySet())
      if (classOf(entry.getKey()).equals(className)) {
        final Set<String> words = new TreeSet<>(entry.getValue());
        words.removeAll(notCommandsOf(entry.getKey()));
        commands.addAll(words);
      }
    assertThat(commands).as("the census must reach the comparisons of " + className).isNotEmpty();
    return commands;
  }

  private static Set<String> notCommandsOf(final String method) {
    final NotCommands notCommands = NOT_COMMANDS.get(method);
    return notCommands != null ? notCommands.words() : Set.of();
  }

  private static String classOf(final String method) {
    return method.substring(0, method.indexOf('#'));
  }

  /**
   * The string constants compared with {@code String.equals} or {@code String.equalsIgnoreCase} in each method of {@code classes},
   * keyed {@code class#method} (lambdas folded into the method they are written in): the labels of a string switch
   * ({@code cmd.equals("LABEL")}), the receiver of a {@code "LABEL".equals(cmd)} test and the argument of a
   * {@code cmd.equalsIgnoreCase("LABEL")} alike, each operand followed back to what produced it. A {@code static final String}
   * constant is folded into an {@code LDC} by javac, so {@code cmd.equals(NAME)} is seen too; a name read from a field that is not a
   * compile-time constant ({@code GETSTATIC}) is not, and a dispatch written that way would leave the census.
   */
  private static Map<String, Set<String>> comparedWords(final Map<String, byte[]> classes) {
    final Map<String, Set<String>> compared = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, descriptor) -> "java/lang/String".equals(owner) && ("equals".equals(name) || "equalsIgnoreCase".equals(name))))
      for (int operand = 0; operand < 2; operand++) {
        final String word = CallOperands.stringConstant(site.operand(operand));
        if (word != null)
          compared.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(word);
      }
    return compared;
  }
}
