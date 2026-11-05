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
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5602: every AST expression type is either descended into by {@code CypherExpressionWalker} or declared a leaf. The
 * walker's {@code default} arm is the one place a regression can hide in silence: a composite type added later without a case
 * falls into it, and everything it nests escapes every check routed through the walker with nothing failing to say so. That is not
 * hypothetical - {@code PatternComprehensionExpression} sat in the default arm until #5602, so a function call in its
 * {@code WHERE} or projection was never validated.
 * <p>
 * The census ranges over the authority (#11288), in the compiled classes of every module ({@link CompiledClasses}): the expression
 * types are every concrete class implementing {@code Expression}, {@code BooleanExpression} or {@code PropertyReadExpression},
 * wherever it is declared, and the types the walker descends into are the cases of its pattern switches and its
 * {@code instanceof} tests, read from its bytecode. The source-text census it replaces listed the files of the {@code ast}
 * directory named {@code *Expression.java} that spell {@code implements Expression}, and looked for the text
 * {@code case X } in the walker, so a type named otherwise, a type implementing the interface through a super class, or a case in
 * a comment decided it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExpressionWalkerCensusTest {
  private static final String       AST         = "com/arcadedb/query/opencypher/ast/";
  private static final List<String> EXPRESSIONS = List.of(AST + "Expression", AST + "BooleanExpression", AST + "PropertyReadExpression");
  private static final String       WALKER      = "com/arcadedb/query/opencypher/parser/CypherExpressionWalker";

  /** Expression types that genuinely nest nothing, so the walker's {@code default} arm is the right answer for them. */
  private static final Set<String> LEAF_EXPRESSIONS = Set.of("LiteralExpression", "VariableExpression", "ParameterExpression",
      "CaseOperandExpression", "PropertyAccessExpression", "StarExpression");

  /**
   * The classes that write an expression as an anonymous class, which no case can name, each with why it nests no expression the
   * walker would have to reach. Keyed by the class it is written in, since the number javac gives it moves with every edit.
   */
  private static final Map<String, String> ANONYMOUS_EXPRESSIONS = Map.of(//
      "com/arcadedb/query/opencypher/optimizer/CypherOptimizer",
      "the label filter of an expand target, built by the optimizer after the walk: it reads a vertex's type, nests nothing",//
      "com/arcadedb/query/sql/antlr/SQLASTBuilder", "wraps a SQL expression for the openCypher operators: no openCypher AST inside");

  @Test
  void everyExpressionTypeIsEitherWalkedOrDeclaredALeaf() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();

    final Set<String> types = new TreeSet<>();
    for (final String expression : EXPRESSIONS)
      for (final String type : CompiledClasses.implementationsOf(expression, classes))
        if (isConcrete(classes.get(type + ".class")))
          types.add(type);

    // AN ANONYMOUS CLASS IS FOLDED INTO THE CLASS IT IS WRITTEN IN, WHICH outerClassOf ANSWERS AND A NAMED TYPE IS NOT
    final Set<String> anonymousIn = new TreeSet<>();
    for (final String type : types)
      if (!CompiledClasses.outerClassOf(type).equals(type))
        anonymousIn.add(CompiledClasses.outerClassOf(type));
    types.removeIf(type -> !CompiledClasses.outerClassOf(type).equals(type));
    assertThat(anonymousIn).as("anonymous expression classes: no case can name one, so each says why it nests no expression "
        + "the walker would have to reach").isEqualTo(ANONYMOUS_EXPRESSIONS.keySet());
    assertThat(types).as("the census must reach the expression types of the openCypher AST").contains(AST + "FunctionCallExpression",
        AST + "LiteralExpression");

    final Set<String> walked = walkedTypes(classes);
    assertThat(walked).as("the census must read the cases of the walker").contains(AST + "PatternComprehensionExpression");

    final Set<String> unhandled = new TreeSet<>();
    for (final String type : types)
      if (!walked.contains(type) && !LEAF_EXPRESSIONS.contains(type.substring(type.lastIndexOf('/') + 1)))
        unhandled.add(type);
    assertThat(unhandled).as("expression types the walker's default arm swallows: add a case to CypherExpressionWalker, or list the "
        + "type in LEAF_EXPRESSIONS if it nests no expression (issue #5602)").isEmpty();

    final Set<String> leaves = new TreeSet<>();
    for (final String type : types)
      leaves.add(type.substring(type.lastIndexOf('/') + 1));
    assertThat(leaves).as("LEAF_EXPRESSIONS names a type that no longer exists: remove it").containsAll(LEAF_EXPRESSIONS);
    for (final String type : walked)
      assertThat(LEAF_EXPRESSIONS).as("LEAF_EXPRESSIONS names a type the walker now descends into: remove it").doesNotContain(
          type.substring(type.lastIndexOf('/') + 1));
  }

  /** Every type the walker's classes switch on ({@code case X x ->}) or test with {@code instanceof}. */
  private static Set<String> walkedTypes(final Map<String, byte[]> classes) {
    final Set<String> walked = new HashSet<>();
    for (final Map.Entry<String, byte[]> entry : classes.entrySet()) {
      if (!entry.getKey().equals(WALKER + ".class") && !entry.getKey().startsWith(WALKER + "$"))
        continue;
      final ClassNode walker = new ClassNode();
      new ClassReader(entry.getValue()).accept(walker, ClassReader.SKIP_FRAMES);
      for (final MethodNode method : walker.methods)
        for (final AbstractInsnNode insn : method.instructions)
          if (insn instanceof InvokeDynamicInsnNode indy && "typeSwitch".equals(indy.name)) {
            for (final Object label : indy.bsmArgs)
              if (label instanceof Type type)
                walked.add(type.getInternalName());
          } else if (insn instanceof TypeInsnNode test && test.getOpcode() == Opcodes.INSTANCEOF)
            walked.add(test.desc);
    }
    return walked;
  }

  private static boolean isConcrete(final byte[] bytes) {
    return bytes != null && (new ClassReader(bytes).getAccess() & (Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT)) == 0;
  }
}
