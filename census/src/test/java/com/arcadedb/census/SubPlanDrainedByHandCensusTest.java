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
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.JumpInsnNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #10080 and #10160: {@code OperationHeapLimit.materialize()} is "the one way a step materialises a sub-plan, so none can
 * hold one on the heap without the limits seeing it", and was applied to every STEP that materialises one and to none of the
 * EXPRESSIONS that do. A subquery written inline - in a projection, a function argument, a SET value, a {@code CREATE EDGE ... FROM}
 * clause - ran the hand-rolled drain loop {@code materialize()} was created to retire, so the identical subquery was refused as a
 * LET and accepted inline. The behaviour is pinned by {@code Issue10080ExpressionSubqueryHeapLimitTest}.
 * <p>
 * The census ranges over the authority (#11288): the positions where a sub-plan is evaluated for its value are the expressions,
 * conditions, statements and functions of both languages - every class extending the SQL {@code SimpleNode}, implementing an
 * openCypher {@code Expression} or {@code BooleanExpression}, a {@code Function}, a {@code FunctionDefinition} or a
 * {@code Procedure}, with the anonymous classes and lambdas written in them - in the compiled classes of every module
 * ({@link CompiledClasses}). A loop on {@code ResultSet.hasNext()} in any of them is an offender. The steps of the executor pull
 * their input in batches, and the ones that materialise a sub-plan go through {@code materialize()}. The source-text census it
 * replaces matched {@code while (x.hasNext())} in six directories, with {@code x} declared a {@code ResultSet} in the same file,
 * and allowed by file name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SubPlanDrainedByHandCensusTest {
  private static final String       RESULT_SET = "com/arcadedb/query/sql/executor/ResultSet";
  private static final List<String> POSITIONS  = List.of("com/arcadedb/query/sql/parser/SimpleNode",
      "com/arcadedb/query/opencypher/ast/Expression", "com/arcadedb/query/opencypher/ast/BooleanExpression",
      "com/arcadedb/function/Function", "com/arcadedb/function/FunctionDefinition", "com/arcadedb/function/procedure/Procedure");

  /**
   * The loops that read a result set without holding it - or hold something other than rows - each with why. A new entry is a
   * decision that the result set it drains is bounded some other way, not a way to silence this census.
   */
  private static final Map<String, String> BOUNDED = new TreeMap<>(Map.of(//
      "query/sql/parser/InCondition#evaluateExpressionThreeValued", "tests membership row by row and keeps none",//
      "query/sql/parser/ProfileStatement#profile", "runs the profiled statement to completion and keeps no row",//
      "query/sql/parser/TraverseProjectionItem#handleStar", "reads the elements of a value a record already holds",//
      "query/sql/parser/FindReferencesStatement#collectTargetRids",
      "keeps the RID of each row of an administrative statement, not the row",//
      "query/opencypher/ast/CountExpression#countRows", "counts the rows of a COUNT { ... } subquery and keeps none"));

  @Test
  void onlyOperationHeapLimitDrainsASubPlanIntoAList() throws Exception {
    final Map<String, byte[]> everyModule = CompiledClasses.ofEveryModule();
    final Set<String> positions = new HashSet<>();
    for (final String type : POSITIONS)
      positions.addAll(CompiledClasses.implementationsOf(type, everyModule));
    assertThat(positions).as("the census must reach the expression positions of both languages").contains(
        "com/arcadedb/query/sql/parser/SelectStatement", "com/arcadedb/query/opencypher/ast/CountExpression");

    final Map<String, byte[]> classes = new HashMap<>();
    for (final Map.Entry<String, byte[]> entry : everyModule.entrySet())
      if (positions.contains(CompiledClasses.outerClassOf(entry.getKey())))
        classes.put(entry.getKey(), entry.getValue());

    final Set<String> resultSets = CompiledClasses.implementationsOf(RESULT_SET, everyModule);
    final Map<String, Set<String>> drains = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, desc) -> resultSets.contains(owner) && "hasNext".equals(name)))
      if (isALoopCondition(site.method().instructions, site.call()))
        drains.computeIfAbsent(site.key(), k -> new TreeSet<>()).add("ResultSet.hasNext loop");

    assertThat(CompiledClasses.unreviewed(drains, BOUNDED)).as(
        "a ResultSet drained by hand in an expression, a condition, a statement or a function, past both heap limits: use "
            + "OperationHeapLimit.materialize() (issues #10080, #10160)").isEmpty();
    assertThat(CompiledClasses.stale(drains, BOUNDED)).as("entries that no longer drain a result set: remove them").isEmpty();
  }

  /** Whether a jump after {@code call} goes back to before it: the call is evaluated on every turn of a loop. */
  private static boolean isALoopCondition(final InsnList instructions, final AbstractInsnNode call) {
    final int at = instructions.indexOf(call);
    for (final AbstractInsnNode insn : instructions)
      if (insn instanceof JumpInsnNode jump && instructions.indexOf(jump) > at && instructions.indexOf(jump.label) <= at)
        return true;
    return false;
  }
}
