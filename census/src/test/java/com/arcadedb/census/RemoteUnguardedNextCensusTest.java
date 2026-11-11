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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

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
 * The census ranges over the authority (#11288, #11452). The remote drivers are the implementations of {@code BasicDatabase}
 * that are not a {@code Database} - the embedded engine is one, a client of a server is not - found in the compiled classes of
 * every module ({@link CompiledClasses}), never in a list of module names. A read of the server's answer is every {@code next()}
 * and {@code nextIfAvailable()} of a {@code ResultSet}
 * <ul>
 *   <li>written in a module that holds a remote driver, whatever produced the result set: the driver's own plumbing;</li>
 *   <li>written in any other module, when its data flow ({@link CallOperands}) leads to a call on a type a remote driver is -
 *   {@code BasicDatabase} included, which is what the console holds - directly or through the helpers that return it.</li>
 * </ul>
 * A {@code next()} is guarded when the same method asks {@code hasNext()} of the same result set, or is itself an iterator's
 * {@code next()} forwarding to one (its caller asked), and a {@code nextIfAvailable()} when the row it answers is tested against
 * {@code null}; any other is listed with the reason the answer cannot be empty. The census used to range over a hard-coded pair
 * of module names, so a remote answer read in the console, or in a third driver module, was outside the guarantee.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteUnguardedNextCensusTest {
  private static final String      BASIC_DATABASE = "com/arcadedb/database/BasicDatabase";
  private static final String      DATABASE       = "com/arcadedb/database/Database";
  private static final String      RESULT_SET     = "com/arcadedb/query/sql/executor/ResultSet";
  private static final Set<String> READS          = Set.of("next", "nextIfAvailable");
  /** How many helpers returning a result set the data flow is followed through, from the read back to the remote call. */
  private static final int         HELPER_DEPTH   = 3;

  /** Reads with no emptiness check, each with why the answer cannot be empty. */
  private static final Map<String, String> AUDITED = new TreeMap<>(Map.of(//
      "remote/RemoteDatabase#countType", "count(*) over a type answers its one row, an empty type included",//
      "remote/RemoteDatabase#countBucket", "count(*) over a bucket answers its one row, an empty bucket included",//
      "remote/RemoteSchema#createBucket", "CREATE BUCKET answers the bucket it created, and no listener can veto a schema statement"));

  @Test
  void everyReadOfTheServersAnswerAsksIfThereIsOneOrIsAudited() throws Exception {
    final Map<String, byte[]> every = CompiledClasses.ofEveryModule();
    final Set<String> remoteDatabases = remoteDatabases(every);
    assertThat(remoteDatabases).as("the census must find the remote drivers among the implementations of BasicDatabase")
        .contains("com/arcadedb/remote/RemoteDatabase", "com/arcadedb/remote/grpc/RemoteGrpcDatabase");

    final Set<String> driverModules = new TreeSet<>();
    final Set<String> driverClasses = new HashSet<>();
    for (final Map.Entry<String, Map<String, byte[]>> module : CompiledClasses.byModule().entrySet())
      for (final Map.Entry<String, byte[]> classFile : module.getValue().entrySet())
        if (remoteDatabases.contains(classNameOf(classFile.getKey())) && !isInterface(classFile.getValue())) {
          driverModules.add(module.getKey());
          driverClasses.addAll(module.getValue().keySet());
          break;
        }
    assertThat(driverModules).as("the census must reach the modules of the remote drivers it found").contains("network",
        "grpc-client");

    final Set<String> reached = new TreeSet<>();
    final Map<String, Set<String>> unguarded = unguardedRemoteReads(every, driverClasses, remoteDatabases, every, reached);
    assertThat(reached.size()).as("the census must reach the reads of the remote record layer").isGreaterThanOrEqualTo(20);
    assertThat(reached).as("the census must reach the reads of a remote answer outside the driver modules: the console holds its "
        + "database as a BasicDatabase").anyMatch(key -> key.startsWith("console/Console#"));

    assertThat(CompiledClasses.unreviewed(unguarded, AUDITED)).as(
        "a next()/nextIfAvailable() on the server's answer with no hasNext() of the same result set, or no null test of the row: an "
            + "empty answer - a vetoed write - reaches the caller as a NoSuchElementException. Ask first and raise the typed "
            + "exception the embedded engine raises, or audit the call into AUDITED with the reason the answer is never empty "
            + "(issue #10373)").isEmpty();
    assertThat(CompiledClasses.stale(unguarded, AUDITED)).as("AUDITED entries that are no longer an unguarded read: delete them")
        .isEmpty();
  }

  @Test
  void theCensusSeesARemoteAnswerOutsideTheDriverModules() throws Exception {
    final Map<String, byte[]> every = CompiledClasses.ofEveryModule();
    final Set<String> reached = new TreeSet<>();
    final Map<String, Set<String>> unguarded = unguardedRemoteReads(CompiledClasses.ofTheProbes(), Set.of(),
        remoteDatabases(every), every, reached);

    assertThat(unguarded).containsKeys("census/ShapeProbes$RemoteAnswer#unguarded", "census/ShapeProbes$RemoteAnswer#throughALocal",
        "census/ShapeProbes$RemoteAnswer#throughAHelper");
    // a guarded read is reached but not reported, and the embedded engine's answer is not a server's
    assertThat(reached).contains("census/ShapeProbes$RemoteAnswer#guarded");
    assertThat(unguarded).doesNotContainKeys("census/ShapeProbes$RemoteAnswer#guarded", "census/ShapeProbes$RemoteAnswer#embedded");
    assertThat(reached).doesNotContain("census/ShapeProbes$RemoteAnswer#embedded");
  }

  /**
   * The unguarded reads of a remote answer in {@code classes}, keyed {@code class#method}. {@code reached} receives the key of
   * every read the census looked at, guarded or not.
   */
  private static Map<String, Set<String>> unguardedRemoteReads(final Map<String, byte[]> classes, final Set<String> driverClasses,
      final Set<String> remoteDatabases, final Map<String, byte[]> every, final Set<String> reached) {
    final Set<String> resultSets = CompiledClasses.implementationsOf(RESULT_SET, every);
    final List<CallOperands.Site> reads = CallOperands.of(classes,
        (owner, name, desc) -> resultSets.contains(owner) && READS.contains(name) && desc.startsWith("()"));

    final Map<String, Set<String>> unguarded = new TreeMap<>();
    for (final CallOperands.Site read : reads) {
      if (!driverClasses.contains(read.owner().name + ".class") && !isRemoteAnswer(read.operand(0), remoteDatabases, resultSets,
          List.of(classes, every), HELPER_DEPTH))
        continue;
      reached.add(read.key());
      if (!isGuarded(read, resultSets))
        unguarded.computeIfAbsent(read.key(), k -> new TreeSet<>()).add(read.call().name);
    }
    return unguarded;
  }

  /** The implementations of {@code BasicDatabase} that are not a {@code Database}: the clients of a server, and the interface. */
  private static Set<String> remoteDatabases(final Map<String, byte[]> every) {
    final Set<String> remote = new HashSet<>(CompiledClasses.implementationsOf(BASIC_DATABASE, every));
    remote.removeAll(CompiledClasses.implementationsOf(DATABASE, every));
    return remote;
  }

  /**
   * Whether the result set {@code origins} produced is the answer of a call on a remote driver: a call whose receiver is one of
   * {@code remoteDatabases}, or a helper declared in one of {@code declaring} that returns one, followed up to {@code depth}
   * helpers deep.
   */
  private static boolean isRemoteAnswer(final Set<AbstractInsnNode> origins, final Set<String> remoteDatabases,
      final Set<String> resultSets, final List<Map<String, byte[]>> declaring, final int depth) {
    for (final AbstractInsnNode origin : origins) {
      if (!(origin instanceof MethodInsnNode call) || !returnsAResultSet(call, resultSets))
        continue;
      if (remoteDatabases.contains(call.owner))
        return true;
      if (depth > 0) {
        final CallOperands.Method helper = declarationOf(declaring, call);
        if (helper != null && isRemoteAnswer(helper.returned(), remoteDatabases, resultSets, declaring, depth - 1))
          return true;
      }
    }
    return false;
  }

  private static CallOperands.Method declarationOf(final List<Map<String, byte[]>> declaring, final MethodInsnNode call) {
    for (final Map<String, byte[]> classes : declaring) {
      final CallOperands.Method method = CallOperands.declarationOf(classes, call);
      if (method != null)
        return method;
    }
    return null;
  }

  private static boolean returnsAResultSet(final MethodInsnNode call, final Set<String> resultSets) {
    final Type returned = Type.getReturnType(call.desc);
    return returned.getSort() == Type.OBJECT && resultSets.contains(returned.getInternalName());
  }

  private static String classNameOf(final String classFile) {
    return classFile.substring(0, classFile.length() - ".class".length());
  }

  private static boolean isInterface(final byte[] bytes) {
    return (new ClassReader(bytes).getAccess() & Opcodes.ACC_INTERFACE) != 0;
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
