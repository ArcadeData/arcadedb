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

import org.junit.jupiter.api.Test;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10373: a remote call that reads the server's answer with {@code next()} or {@code nextIfAvailable()} and no emptiness
 * check fails with a bare {@code NoSuchElementException} (or a {@code NullPointerException}) the moment the server answers
 * nothing - which it does for a write a listener vetoed - where the embedded engine raises a typed exception the caller can catch.
 * The question is not "did the method name start with save or delete", it is "can the server answer nothing where the caller's
 * return type cannot express nothing"; the behaviour is pinned by {@code Issue10373RemoteNewEdgeVetoIT} and its siblings.
 * <p>
 * The census ranges over the authority (#11288): every {@code next()} and {@code nextIfAvailable()} of a {@code ResultSet} in the
 * compiled classes of the remote clients ({@link CompiledClasses}), read with its data flow ({@link CallOperands}). A
 * {@code next()} is guarded when the same method asks {@code hasNext()} of the same result set, or is itself an iterator's
 * {@code next()} forwarding to one (its caller asked), and a {@code nextIfAvailable()} when the row it answers is tested against
 * {@code null}; any other is listed with the reason the answer cannot be empty. The
 * source-text census it replaces took an {@code hasNext()} or {@code isEmpty()} of anything in the eight lines before a read as
 * its guard, and keyed its audit by the text of the line.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteUnguardedNextCensusTest {
  /** The remote record layer: the HTTP driver, and the gRPC one that extends it. */
  private static final List<String> REMOTE_MODULES = List.of("network", "grpc-client");
  private static final String       RESULT_SET     = "com/arcadedb/query/sql/executor/ResultSet";
  private static final Set<String>  READS          = Set.of("next", "nextIfAvailable");

  /** Reads with no emptiness check, each with why the answer cannot be empty. */
  private static final Map<String, String> AUDITED = new TreeMap<>(Map.of(//
      "remote/RemoteDatabase#countType", "count(*) over a type answers its one row, an empty type included",//
      "remote/RemoteDatabase#countBucket", "count(*) over a bucket answers its one row, an empty bucket included",//
      "remote/RemoteSchema#createBucket", "CREATE BUCKET answers the bucket it created, and no listener can veto a schema statement"));

  @Test
  void everyReadOfTheServersAnswerAsksIfThereIsOneOrIsAudited() throws Exception {
    final Map<String, byte[]> classes = new HashMap<>();
    for (final String module : REMOTE_MODULES) {
      assertThat(CompiledClasses.byModule()).as("the census must read the " + module + " module").containsKey(module);
      classes.putAll(CompiledClasses.byModule().get(module));
    }
    final Set<String> resultSets = CompiledClasses.implementationsOf(RESULT_SET, CompiledClasses.ofEveryModule());

    final List<CallOperands.Site> reads = CallOperands.of(classes,
        (owner, name, desc) -> resultSets.contains(owner) && READS.contains(name) && desc.startsWith("()"));
    assertThat(reads.size()).as("the census must reach the reads of the remote record layer").isGreaterThanOrEqualTo(20);

    final Map<String, Set<String>> unguarded = new TreeMap<>();
    for (final CallOperands.Site read : reads)
      if (!isGuarded(read, resultSets))
        unguarded.computeIfAbsent(read.key(), k -> new TreeSet<>()).add(read.call().name);

    assertThat(CompiledClasses.unreviewed(unguarded, AUDITED)).as(
        "a next()/nextIfAvailable() on the server's answer with no hasNext() of the same result set, or no null test of the row: an "
            + "empty answer - a vetoed write - reaches the caller as a NoSuchElementException. Ask first and raise the typed "
            + "exception the embedded engine raises, or audit the call into AUDITED with the reason the answer is never empty "
            + "(issue #10373)").isEmpty();
    assertThat(CompiledClasses.stale(unguarded, AUDITED)).as("AUDITED entries that are no longer an unguarded read: delete them")
        .isEmpty();
  }

  private static boolean isGuarded(final CallOperands.Site read, final Set<String> resultSets) {
    if ("next".equals(read.call().name)) {
      // an iterator's own next() forwarding to the result set: the contract puts the hasNext() on its caller
      if ("next".equals(read.method().name))
        return true;
      final Set<Object> resultSet = valueOf(read, read.operand(0));
      for (final AbstractInsnNode insn : read.method().instructions)
        if (insn instanceof MethodInsnNode ask && "hasNext".equals(ask.name) && resultSets.contains(ask.owner) && valueOf(read,
            read.in().stackBefore(ask, 0)).equals(resultSet))
          return true;
      return false;
    }
    // nextIfAvailable answers null for no row: the row has to be tested
    for (final AbstractInsnNode insn : read.method().instructions)
      if ((insn.getOpcode() == Opcodes.IFNULL || insn.getOpcode() == Opcodes.IFNONNULL) && read.in().stackBefore(insn, 0).contains(
          read.call()))
        return true;
    return false;
  }

  /** The value {@code origins} is: a parameter by its index (javac copies one into a local for a try-with-resources), or the insns. */
  private static Set<Object> valueOf(final CallOperands.Site site, final Set<AbstractInsnNode> origins) {
    final Set<Object> value = new HashSet<>();
    for (final AbstractInsnNode origin : origins) {
      final int parameter = site.parameterOf(origin);
      value.add(parameter >= 0 ? (Object) parameter : origin);
    }
    return value;
  }
}
