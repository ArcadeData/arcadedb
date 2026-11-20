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
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The expressions a parallel label scan evaluates on several workers at once share one instance (issue #8725), so every
 * instance field of them must be final or volatile: a cache added to one of them later fails here instead of racing under a
 * parallel scan.
 * <p>
 * The census ranges over the authority (issue #12383): the expression types a parallel scan admits are the cases of the
 * pattern switch in {@code ParallelSafeExpressions.isParallelSafe} (and any {@code instanceof} test it makes), read from its
 * bytecode. The engine test it replaces listed them in a hand-written array, which a new {@code case} did not reach until
 * somebody edited the array too ({@code StarExpression} had already drifted out of it). A case admits every subclass of its
 * type as well, so the subclasses are checked, and each class is checked with every field it inherits from a super class of
 * the compiled modules.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParallelSafeExpressionsStateCensusTest {
  private static final String AUTHORITY        = "com/arcadedb/query/opencypher/executor/operators/ParallelSafeExpressions";
  private static final String AUTHORITY_METHOD = "isParallelSafe";
  private static final String AST              = "com/arcadedb/query/opencypher/ast/";

  @Test
  void everyExpressionAParallelScanAdmitsHoldsNoUnsafeMutableState() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();

    final Set<String> admitted = admittedTypes(classes);
    assertThat(admitted).as("the census must reach the cases of ParallelSafeExpressions#isParallelSafe")
        .contains(AST + "StarExpression", AST + "InExpression", AST + "ComparisonExpression", AST + "LiteralExpression");

    // A CASE ADMITS EVERY SUBCLASS OF ITS TYPE: THE SUBCLASSES SHARE THE ONE INSTANCE TOO
    final Set<String> checked = new TreeSet<>();
    for (final String type : admitted)
      checked.addAll(CompiledClasses.implementationsOf(type, classes));
    assertThat(checked).as("the census must reach the classes of the admitted types").containsAll(admitted);

    final Set<String> unsafe = new TreeSet<>();
    for (final String type : checked)
      for (String current = type; current != null; ) {
        final byte[] bytes = classes.get(current + ".class");
        if (bytes == null)
          break; // A SUPER CLASS OUTSIDE THE COMPILED MODULES (java.lang.Object, A JDK TYPE)
        final ClassNode node = new ClassNode();
        new ClassReader(bytes).accept(node, ClassReader.SKIP_CODE | ClassReader.SKIP_FRAMES);
        for (final FieldNode field : node.fields)
          if ((field.access & (Opcodes.ACC_STATIC | Opcodes.ACC_FINAL | Opcodes.ACC_VOLATILE)) == 0)
            unsafe.add(simpleNameOf(type) + (current.equals(type) ? "" : " (from " + simpleNameOf(current) + ")") + "." + field.name);
        current = node.superName;
      }
    assertThat(unsafe).as("instance fields, neither final nor volatile, of an expression ParallelSafeExpressions#isParallelSafe "
        + "admits to a parallel scan, whose workers share one instance (issue #8725): make the field final or volatile, or "
        + "remove the case").isEmpty();
  }

  /** The types {@code isParallelSafe} switches on ({@code case X x ->}) or tests with {@code instanceof}. */
  private static Set<String> admittedTypes(final Map<String, byte[]> classes) {
    final byte[] bytes = classes.get(AUTHORITY + ".class");
    assertThat(bytes).as("the census must reach " + AUTHORITY).isNotNull();

    final ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, ClassReader.SKIP_FRAMES);
    final Set<String> admitted = new TreeSet<>();
    boolean reached = false;
    for (final MethodNode method : node.methods) {
      if (!AUTHORITY_METHOD.equals(method.name))
        continue;
      reached = true;
      for (final AbstractInsnNode insn : method.instructions)
        if (insn instanceof InvokeDynamicInsnNode indy && "typeSwitch".equals(indy.name)) {
          for (final Object label : indy.bsmArgs)
            if (label instanceof Type type)
              admitted.add(type.getInternalName());
        } else if (insn instanceof TypeInsnNode test && test.getOpcode() == Opcodes.INSTANCEOF)
          admitted.add(test.desc);
    }
    assertThat(reached).as("the census must reach the method ParallelSafeExpressions#isParallelSafe").isTrue();
    return admitted;
  }

  private static String simpleNameOf(final String type) {
    return type.substring(type.lastIndexOf('/') + 1);
  }
}
