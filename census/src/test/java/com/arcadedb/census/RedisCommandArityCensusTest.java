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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11450: PING and SELECT were dispatched by the RESP wire switch but missing from {@link RedisCommandArity}, the one table
 * of argument counts both Redis surfaces enforce (#9059, #9161), so {@code SELECT} with no argument answered a Java
 * {@code IndexOutOfBoundsException} text and {@code PING a b} was accepted.
 * <p>
 * The census ranges over the authority: the commands each surface DISPATCHES, read from the compiled dispatch method (a string
 * {@code switch} compiles to one {@code String.equals} against each label, and an {@code "AUTH".equals(cmd)} test to the same
 * call), and checks each one against the table's key set. A command a surface dispatches is either in the table or listed below
 * with the reason it checks its own arguments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisCommandArityCensusTest {
  private static final String MODULE = "redisw";

  private static final String WIRE_CLASS  = "com/arcadedb/redis/RedisNetworkExecutor.class";
  private static final String WIRE_METHOD = "executeCommand";

  private static final String QUERY_CLASS  = "com/arcadedb/redis/query/RedisQueryEngine.class";
  private static final String QUERY_METHOD = "executeSingleCommandInternal";

  /** Wire commands dispatched before the table is consulted, each checking its own shape. */
  private static final Map<String, String> WIRE_EXEMPT = Map.of(//
      "AUTH", "accepted before authentication, so before the table check; auth() takes 1 or 2 arguments and refuses any other count",
      "HELLO", "accepted before authentication; a variable list of options, parsed and refused by hello() itself");

  /** Query-language commands whose shape differs from the wire one, so they are not in the shared table. */
  private static final Map<String, String> QUERY_EXEMPT = Map.of(//
      "HSET", "names an index or a RID instead of a bucket and checks its own forms",//
      "HMSET", "names an index or a RID instead of a bucket and checks its own forms",//
      "HGET", "names an index or a RID instead of a bucket and checks its own forms",//
      "HMGET", "names an index or a RID instead of a bucket and checks its own forms",//
      "HEXISTS", "names an index or a RID instead of a bucket and checks its own forms",//
      "HDEL", "names an index or a RID instead of a bucket and checks its own forms");

  @Test
  void everyCommandTheWireSwitchDispatchesHasAnArity() throws Exception {
    final Set<String> dispatched = dispatchedCommands(CompiledClasses.byModule().get(MODULE), WIRE_CLASS, WIRE_METHOD);
    assertThat(dispatched).as("the census must reach the commands RedisNetworkExecutor.executeCommand dispatches")
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
    final Set<String> dispatched = dispatchedCommands(CompiledClasses.byModule().get(MODULE), QUERY_CLASS, QUERY_METHOD);
    assertThat(dispatched).as("the census must reach the commands RedisQueryEngine.executeSingleCommandInternal dispatches")
        .contains("GET", "PING", "HGET");

    final Set<String> missing = new TreeSet<>(dispatched);
    missing.removeAll(RedisCommandArity.ramCommands());
    missing.removeAll(QUERY_EXEMPT.keySet());
    assertThat(missing).as(
        "a command the redis query language dispatches with no entry in RedisCommandArity accepts any argument count (issue #11450)")
        .isEmpty();
  }

  @Test
  void theExemptionsAreStillDispatched() throws Exception {
    // an exemption for a command nobody dispatches any more is a stale entry that could hide a new one under its name
    final Map<String, byte[]> classes = CompiledClasses.byModule().get(MODULE);
    assertThat(dispatchedCommands(classes, WIRE_CLASS, WIRE_METHOD)).containsAll(WIRE_EXEMPT.keySet());
    assertThat(dispatchedCommands(classes, QUERY_CLASS, QUERY_METHOD)).containsAll(QUERY_EXEMPT.keySet());
  }

  @Test
  void theScanSeesTheLabelsOfAStringSwitchAndAnEqualsTest() throws Exception {
    // control: the shapes the census relies on, compiled by the same javac
    final Set<String> seen = dispatchedCommands(CompiledClasses.ofTheProbes(), "com/arcadedb/census/RedisCommandArityCensusTest.class",
        "probe");
    assertThat(seen).containsExactlyInAnyOrder("ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON");
  }

  @SuppressWarnings("unused")
  private static int probe(final String command) {
    if ("ALPHA".equals(command))
      return 0;
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

  /**
   * The string constants compared with {@code String.equals} in {@code method} of {@code className}: the labels of a string switch
   * ({@code cmd.equals("LABEL")}) and the receivers of a {@code "LABEL".equals(cmd)} test alike. A {@code static final String}
   * constant is folded into an {@code LDC} by javac, so {@code cmd.equals(NAME)} is seen too; a name read from a field that is not
   * a compile-time constant ({@code GETSTATIC}) is not, and a dispatch written that way would leave the census.
   */
  private static Set<String> dispatchedCommands(final Map<String, byte[]> classes, final String className, final String method) {
    assertThat(classes).as("the census must read the " + MODULE + " module").isNotNull();
    final byte[] bytes = classes.get(className);
    assertThat(bytes).as("the census must find " + className).isNotNull();

    final Set<String> commands = new TreeSet<>();
    final boolean[] found = { false };
    new ClassReader(bytes).accept(new ClassVisitor(Opcodes.ASM9) {
      @Override
      public MethodVisitor visitMethod(final int access, final String name, final String descriptor, final String signature,
          final String[] exceptions) {
        if (!method.equals(name))
          return null;
        found[0] = true;
        return new MethodVisitor(Opcodes.ASM9) {
          /** The constant pushed by the last two instructions, the other operand being the variable: null when neither was one. */
          private String recent;
          private int    sinceConstant = 2;

          private void step() {
            if (++sinceConstant > 1)
              recent = null;
          }

          @Override
          public void visitLdcInsn(final Object value) {
            recent = value instanceof String s ? s : null;
            sinceConstant = 0;
          }

          @Override
          public void visitVarInsn(final int opcode, final int varIndex) {
            step();
          }

          @Override
          public void visitInsn(final int opcode) {
            recent = null;
          }

          @Override
          public void visitIntInsn(final int opcode, final int operand) {
            recent = null;
          }

          @Override
          public void visitTypeInsn(final int opcode, final String type) {
            recent = null;
          }

          @Override
          public void visitFieldInsn(final int opcode, final String owner, final String name, final String desc) {
            recent = null;
          }

          @Override
          public void visitJumpInsn(final int opcode, final Label label) {
            recent = null;
          }

          @Override
          public void visitMethodInsn(final int opcode, final String owner, final String name, final String desc,
              final boolean isInterface) {
            if (recent != null && "java/lang/String".equals(owner) && "equals".equals(name))
              commands.add(recent);
            recent = null;
          }

          @Override
          public void visitInvokeDynamicInsn(final String name, final String desc, final Handle bootstrap,
              final Object... bootstrapArguments) {
            recent = null;
          }
        };
      }
    }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    assertThat(found[0]).as("the census must reach the method " + method + " of " + className).isTrue();
    return commands;
  }
}
