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
 * relationship has exactly one type, so a name holds when it is that type, {@code %} always holds, and a conjunction
 * of two different names never does.
 */
public abstract sealed class LabelPredicate {

  /** True when the predicate holds for a node of the given type. */
  public abstract boolean matchesVertexType(DocumentType type);

  /** True when the predicate holds for a relationship of the given type. */
  public abstract boolean matchesEdgeType(String edgeType);

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
    public boolean matchesVertexType(final DocumentType type) {
      return type != null && type.instanceOf(label);
    }

    @Override
    public boolean matchesEdgeType(final String edgeType) {
      return label.equals(edgeType);
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
    public boolean matchesVertexType(final DocumentType type) {
      return type != null && !Labels.getLabels(type).isEmpty();
    }

    @Override
    public boolean matchesEdgeType(final String edgeType) {
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
    public boolean matchesVertexType(final DocumentType type) {
      return !operand.matchesVertexType(type);
    }

    @Override
    public boolean matchesEdgeType(final String edgeType) {
      return !operand.matchesEdgeType(edgeType);
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
      this.operands = operands;
    }

    @Override
    public boolean matchesVertexType(final DocumentType type) {
      for (final LabelPredicate operand : operands)
        if (!operand.matchesVertexType(type))
          return false;
      return true;
    }

    @Override
    public boolean matchesEdgeType(final String edgeType) {
      for (final LabelPredicate operand : operands)
        if (!operand.matchesEdgeType(edgeType))
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

  /** {@code a | b | ...}. */
  public static final class Or extends LabelPredicate {
    private final LabelPredicate[] operands;

    public Or(final LabelPredicate[] operands) {
      this.operands = operands;
    }

    @Override
    public boolean matchesVertexType(final DocumentType type) {
      for (final LabelPredicate operand : operands)
        if (operand.matchesVertexType(type))
          return true;
      return false;
    }

    @Override
    public boolean matchesEdgeType(final String edgeType) {
      for (final LabelPredicate operand : operands)
        if (operand.matchesEdgeType(edgeType))
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
