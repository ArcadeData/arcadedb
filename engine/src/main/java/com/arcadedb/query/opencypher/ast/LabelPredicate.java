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

import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;

/**
 * A full openCypher label expression - {@code !A}, {@code %}, {@code A&!B}, {@code (A|C)&!B} - kept as the tree the
 * grammar parsed rather than flattened into a label list.
 * <p>
 * The pattern and {@code WHERE} forms used to carry only the label names and a flag saying whether {@code |} was
 * written, so negation and the wildcard were dropped and a mix of {@code &} and {@code |} was read as one or the
 * other: {@code (n:!A)} returned the {@code A} nodes and {@code [r:!R]} returned the {@code R} relationships
 * (issue #8992). A label list plus a flag still describes the plain conjunction and the plain disjunction, which is
 * what the planner knows how to turn into a label scan; anything else is evaluated through this tree.
 * <p>
 * A node satisfies a label name when its type is, or inherits from, that label (the same test
 * {@link Labels#matches(DocumentType, List, boolean)} applies), and {@code %} when it carries at least one label. A
 * relationship has exactly one type, so a name holds when it is that type or a subtype of it, {@code %} always holds,
 * and a conjunction holds when the type is an instance of every operand (issue #12339).
 * <p>
 * A Cypher 25 dynamic label {@code $(expr)}, {@code $all(expr)} or {@code $any(expr)} is a {@link Dynamic} operand, evaluated
 * against the row being checked, so it combines with every other operator ({@code !$(expr)}, {@code A&$any(expr)},
 * {@code (A|$(expr))&!B}) instead of being refused at parse time.
 */
public abstract sealed class LabelPredicate {

  /**
   * True when the predicate holds for a node of the given type. {@code row} and {@code context} are what a dynamic
   * {@code $(expr)} operand is evaluated against; a static predicate ignores them.
   */
  public abstract boolean matchesVertexType(DocumentType type, Result row, CommandContext context);

  /**
   * True when the predicate holds for a relationship of the given type. A type name holds for a subtype too, the same
   * way a relationship pattern's type filter matches it ({@code MatchRelationshipStep} tests {@code instanceOf}), so
   * {@code [r:R&!S]} returns every relationship {@code [r:R]} returns that is not an {@code S}.
   */
  public abstract boolean matchesEdgeType(DocumentType edgeType, Result row, CommandContext context);

  /**
   * The label names every match must carry: the plain positive names among the top-level conjuncts. A node that
   * lacks one of them cannot satisfy the predicate, so a pattern may keep them as its label constraint and let the
   * planner pick a label scan, while the predicate filters the rest. Empty when no such name exists - {@code !A},
   * {@code %} or a top-level disjunction.
   */
  public List<String> requiredLabels() {
    final List<String> out = new ArrayList<>();
    collectRequired(this, out);
    return out;
  }

  private static void collectRequired(final LabelPredicate predicate, final List<String> out) {
    if (predicate instanceof Name name) {
      if (!out.contains(name.label))
        out.add(name.label);
    } else if (predicate instanceof And and) {
      for (final LabelPredicate operand : and.operands)
        collectRequired(operand, out);
    }
  }

  /** A single label or relationship type name. */
  public static final class Name extends LabelPredicate {
    private final String label;

    public Name(final String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      return type != null && type.instanceOf(label);
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      return edgeType != null && edgeType.instanceOf(label);
    }

    @Override
    public String toString() {
      return label;
    }
  }

  /** The {@code %} wildcard: any label at all. */
  public static final class AnyLabel extends LabelPredicate {
    public static final AnyLabel INSTANCE = new AnyLabel();

    private AnyLabel() {
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      if (type == null)
        return false;
      // The common case - an ordinary type that extends nothing - is a label by itself; only the no-label sentinel
      // is not. Anything else goes through the label walk, which allocates.
      if (type.getSuperTypes().isEmpty())
        return !Labels.NO_LABEL_TYPE.equals(type.getName());
      return !Labels.getLabels(type).isEmpty();
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      return edgeType != null;
    }

    @Override
    public String toString() {
      return "%";
    }
  }

  /** {@code !operand}. */
  public static final class Not extends LabelPredicate {
    private final LabelPredicate operand;

    public Not(final LabelPredicate operand) {
      this.operand = operand;
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      return !operand.matchesVertexType(type, row, context);
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      return !operand.matchesEdgeType(edgeType, row, context);
    }

    @Override
    public String toString() {
      return "!" + operand;
    }
  }

  /** {@code a & b & ...}, also written {@code a:b}. */
  public static final class And extends LabelPredicate {
    private final LabelPredicate[] operands;

    public And(final LabelPredicate[] operands) {
      this.operands = operands.clone();
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      for (final LabelPredicate operand : operands)
        if (!operand.matchesVertexType(type, row, context))
          return false;
      return true;
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      for (final LabelPredicate operand : operands)
        if (!operand.matchesEdgeType(edgeType, row, context))
          return false;
      return true;
    }

    @Override
    public String toString() {
      return join("&");
    }

    private String join(final String operator) {
      final StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < operands.length; i++) {
        if (i > 0)
          sb.append(operator);
        sb.append(operands[i]);
      }
      return sb.append(')').toString();
    }
  }

  /**
   * A Cypher 25 dynamic label {@code $(expr)} (the same as {@code $all(expr)}) or {@code $any(expr)}, evaluated per row. The
   * value is read through {@link Labels#appendDynamicMatchLabels}, so it is the same type error there is on every other read
   * path when it is not a string or a list of strings (issue #12338). {@code $all} of an empty list holds (no label is
   * missing) and {@code $any} of one does not, as in Neo4j.
   */
  public static final class Dynamic extends LabelPredicate {
    private final Expression expression;
    private final boolean    any;

    public Dynamic(final Expression expression, final boolean any) {
      this.expression = expression;
      this.any = any;
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      return type != null && matches(type, row, context);
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      return edgeType != null && matches(edgeType, row, context);
    }

    private boolean matches(final DocumentType type, final Result row, final CommandContext context) {
      final List<String> labels = new ArrayList<>(2);
      Labels.appendDynamicMatchLabels(labels, OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, row, context));
      for (int i = 0; i < labels.size(); i++)
        if (type.instanceOf(labels.get(i)) == any)
          return any;
      return !any;
    }

    @Override
    public String toString() {
      return (any ? "$any(" : "$(") + expression.getText() + ")";
    }
  }

  /** {@code a | b | ...}. */
  public static final class Or extends LabelPredicate {
    private final LabelPredicate[] operands;

    public Or(final LabelPredicate[] operands) {
      this.operands = operands.clone();
    }

    @Override
    public boolean matchesVertexType(final DocumentType type, final Result row, final CommandContext context) {
      for (final LabelPredicate operand : operands)
        if (operand.matchesVertexType(type, row, context))
          return true;
      return false;
    }

    @Override
    public boolean matchesEdgeType(final DocumentType edgeType, final Result row, final CommandContext context) {
      for (final LabelPredicate operand : operands)
        if (operand.matchesEdgeType(edgeType, row, context))
          return true;
      return false;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < operands.length; i++) {
        if (i > 0)
          sb.append('|');
        sb.append(operands[i]);
      }
      return sb.append(')').toString();
    }
  }
}
