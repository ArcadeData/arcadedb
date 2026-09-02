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
package com.arcadedb.query.opencypher.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A GQL Quantified Path Pattern (ISO/IEC 39075:2024 §15.4) that cannot be lowered onto the
 * variable-length-relationship engine, i.e. everything issue #4531 calls "Phase B":
 * a repeated inner pattern of two or more hops, an inner {@code WHERE} evaluated once per
 * repetition, or inner variables that have to surface outside the group as parallel lists.
 *
 * <pre>
 *   MATCH (a) ( (x)-[r1:R1]-&gt;(y)-[r2:R2]-&gt;(z) WHERE r1.w &gt; 5 ){1,3} (b)
 * </pre>
 *
 * <p>It is modelled as a {@link RelationshipPattern} so the whole group occupies exactly one hop of
 * the enclosing {@link PathPattern}, keeping the {@code nodes.size() == relationships.size() + 1}
 * invariant every consumer of a path pattern relies on. The enclosing pattern's boundary nodes are
 * the group's endpoints; the inner pattern's own endpoint constraints are re-checked per repetition
 * by the executor, since a boundary node cannot carry them for repetitions past the first.
 *
 * <p>The synthesised hop always reports a non-null minimum, so {@link #isVariableLength()} is true
 * and every shape-matching fast path that already declines a variable-length hop declines this one
 * too. Execution belongs to {@code QuantifiedPathStep}; callers that cannot honour the repetition
 * semantics must test for this type explicitly rather than treat it as a plain hop.
 *
 * <p>Phase A - a single-relationship inner pattern with anonymous endpoints and no inner
 * {@code WHERE} - is still lowered to a plain variable-length relationship by
 * {@code CypherASTBuilder}, because for that shape the two are equivalent and the existing
 * traversal machinery is faster.
 */
public class QuantifiedPathPattern extends RelationshipPattern {
  private final PathPattern      inner;
  private final BooleanExpression innerWhere;

  /**
   * @param inner      the parenthesized inner pattern that is repeated
   * @param innerWhere predicate evaluated once per repetition against that repetition's bindings, or null
   * @param min        minimum repetitions; null means 1
   * @param max        maximum repetitions; null means unbounded
   */
  public QuantifiedPathPattern(final PathPattern inner, final BooleanExpression innerWhere, final Integer min,
      final Integer max) {
    // The inherited variable, types, direction and property map are PLACEHOLDERS, not traversal semantics:
    // a group has no single relationship type or direction, they live on the inner pattern's own hops.
    // Only the hop bounds are real, so that isVariableLength() is true and every shape check that already
    // declines a variable-length hop declines this one. Any new code that loops over
    // PathPattern#getRelationships() and reads getDirection()/getTypes() must special-case this type the
    // way CypherExecutionPlan#buildMatchStep does, rather than rely on an incidental guard elsewhere.
    super(null, null, Direction.BOTH, null, null, min != null ? min : 1, max, null);
    if (inner == null || inner.getRelationshipCount() < 1)
      throw new IllegalArgumentException("A quantified path pattern must repeat at least one relationship");
    this.inner = inner;
    this.innerWhere = innerWhere;
  }

  public PathPattern getInnerPattern() {
    return inner;
  }

  public BooleanExpression getInnerWhere() {
    return innerWhere;
  }

  /** Minimum number of repetitions; 0 is legal and matches the two boundary nodes as the same vertex. */
  public int getMinRepetitions() {
    return getEffectiveMinHops();
  }

  /** Maximum number of repetitions, {@link Integer#MAX_VALUE} when the quantifier is open-ended. */
  public int getMaxRepetitions() {
    return getEffectiveMaxHops();
  }

  /**
   * Returns every variable written inside the group, in written order. Each of them binds outside the
   * group to a list with one element per repetition - {@code LIST<NODE>} for a node variable,
   * {@code LIST<RELATIONSHIP>} for a relationship variable - which is what makes them "group variables"
   * in GQL terms.
   */
  public List<String> getGroupVariables() {
    final Set<String> variables = new LinkedHashSet<>();
    for (int i = 0; i < inner.getNodeCount(); i++) {
      final String variable = inner.getNode(i).getVariable();
      if (variable != null && !variable.isEmpty())
        variables.add(variable);
      if (i < inner.getRelationshipCount()) {
        final String relVariable = inner.getRelationship(i).getVariable();
        if (relVariable != null && !relVariable.isEmpty())
          variables.add(relVariable);
      }
    }
    return variables.isEmpty() ? Collections.emptyList() : new ArrayList<>(variables);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("(").append(inner);
    if (innerWhere != null)
      sb.append(" WHERE ").append(innerWhere);
    sb.append(")");
    final int min = getMinRepetitions();
    final int max = getMaxRepetitions();
    if (max == Integer.MAX_VALUE)
      sb.append(min == 0 ? "*" : (min == 1 ? "+" : "{" + min + ",}"));
    else if (min == max)
      sb.append("{").append(min).append("}");
    else
      sb.append("{").append(min).append(",").append(max).append("}");
    return sb.toString();
  }
}
