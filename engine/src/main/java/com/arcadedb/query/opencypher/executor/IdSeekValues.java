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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.BooleanWrapperExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The record ids a {@code WHERE} clause pins a node or a relationship to, so a plan fetches the records by RID instead of scanning a
 * label or walking every edge list (issue #12134).
 * <p>
 * A conjunct of the top-level {@code AND} chain qualifies when it is {@code elementId(x) = v}, {@code id(x) = v} (either operand
 * order), {@code elementId(x) IN list} or {@code id(x) IN list}, and every value is one a caller passed
 * ({@link RowIndependentValues#isParameterDerived}: a literal, a parameter or a part of one). The first such conjunct is taken: any
 * one of them holds on every row the statement returns, so the records it names are a superset of the answer.
 * <p>
 * The seek is a prefilter only: every plan that takes it still evaluates the predicate on each record it fetches, so a value the
 * lookup reads more loosely than Cypher's equality (a string handed to {@code id()}, a number to {@code elementId()}) costs a
 * lookup and is then rejected, never answered.
 */
public final class IdSeekValues {
  private IdSeekValues() {
  }

  /**
   * The id values {@code where} pins {@code variable} to, or null when no conjunct does. An equality value is one element marked
   * scalar; {@code IN $ids} is one element marked as the whole list; a list literal gives one element per item.
   */
  public static InListValues find(final BooleanExpression where, final String variable) {
    if (where == null || variable == null || variable.isEmpty())
      return null;

    if (where instanceof BooleanWrapperExpression wrapper)
      return find(wrapper.getBooleanExpression(), variable);

    if (where instanceof LogicalExpression logical) {
      if (logical.getOperator() != LogicalExpression.Operator.AND)
        return null;
      final InListValues left = find(logical.getLeft(), variable);
      return left != null ? left : find(logical.getRight(), variable);
    }

    if (where instanceof ComparisonExpression comparison && comparison.getOperator() == ComparisonExpression.Operator.EQUALS) {
      final Expression value = isIdOf(comparison.getLeft(), variable) ? comparison.getRight()
          : isIdOf(comparison.getRight(), variable) ? comparison.getLeft() : null;
      if (value == null || !RowIndependentValues.isParameterDerived(value))
        return null;
      final Set<Expression> scalar = Collections.newSetFromMap(new IdentityHashMap<>());
      scalar.add(value);
      return new InListValues(List.of(value), scalar, Collections.emptySet());
    }

    if (where instanceof InExpression in && isIdOf(in.getExpression(), variable)) {
      List<Expression> list = in.getList();
      if (list == null || list.isEmpty())
        return null;
      final boolean wholeList = list.size() == 1 && !(list.getFirst() instanceof ListExpression);
      if (list.size() == 1 && list.getFirst() instanceof ListExpression listExpression)
        list = listExpression.getElements();
      for (final Expression element : list)
        if (!RowIndependentValues.isParameterDerived(element))
          return null;
      if (!wholeList)
        return new InListValues(list);
      final Set<Expression> whole = Collections.newSetFromMap(new IdentityHashMap<>());
      whole.addAll(list);
      return new InListValues(list, Collections.emptySet(), whole);
    }

    return null;
  }

  /** Whether {@code expression} is {@code id(variable)} or {@code elementId(variable)}. */
  public static boolean isIdOf(final Expression expression, final String variable) {
    if (!(expression instanceof FunctionCallExpression call) || call.isDistinct() || call.getArguments().size() != 1)
      return false;
    final String name = call.getFunctionName();
    if (!"id".equalsIgnoreCase(name) && !"elementid".equalsIgnoreCase(name))
      return false;
    return call.getArguments().getFirst() instanceof VariableExpression argument && variable.equals(argument.getVariableName());
  }

  /**
   * The RIDs {@code ids} stands for in this execution, each once, in the order written. A value that addresses no stored record
   * is left out: null, a list compared whole by an equality, a text that is not {@code #bucket:position}, a negative position
   * (the marker a lightweight edge carries) or a negative number. {@code IN} over a value that is not a list is refused as the
   * evaluation of the {@code IN} refuses it.
   */
  public static List<RID> resolve(final InListValues ids, final CommandContext context) {
    final Set<RID> rids = new LinkedHashSet<>();
    for (final Expression element : ids.getValues()) {
      final Object value = RowIndependentValues.resolve(element, context);
      if (value == null)
        continue;
      if (ids.isWholeList(element)) {
        for (final Object item : InExpression.listOperand(value))
          addRid(rids, item, context);
      } else
        addRid(rids, value, context);
    }
    return new ArrayList<>(rids);
  }

  private static void addRid(final Set<RID> rids, final Object value, final CommandContext context) {
    final RID rid;
    if (value instanceof Identifiable identifiable)
      rid = identifiable.getIdentity();
    else if (value instanceof Number number) {
      // id() packs a RID into a non-negative long; a fraction can still equal it (1.0 = 1), a negative number never does
      final long encoded = number.longValue();
      if (encoded < 0)
        return;
      rid = IdFunction.decodeLongToRid(context.getDatabase(), encoded);
    } else if (value instanceof String text && RID.is(text)) {
      try {
        rid = context.getDatabase().newRID(text);
      } catch (final NumberFormatException e) {
        // digits past the range of a bucket id or a position: no record has that address
        return;
      }
    } else
      return;
    if (rid != null && rid.getBucketId() >= 0 && rid.getPosition() >= 0)
      rids.add(rid);
  }

  /**
   * Whether every relationship of {@code types} (any edge type when empty) is a record a RID fetches: none of them holds
   * lightweight edges, which have no record and are found only by walking the edge lists, as every other read path that trusts the
   * buckets of an edge type checks first ({@link EdgeType#holdsLightweightEdges}). A type the schema does not have is refused as
   * well: the walk answers it, with nothing.
   */
  public static boolean relationshipsHaveRecords(final Schema schema, final List<String> types) {
    if (types == null || types.isEmpty()) {
      for (final DocumentType type : schema.getTypes())
        if (type instanceof EdgeType edgeType && edgeType.isLightweight())
          return false;
      return true;
    }
    for (final String name : types) {
      final DocumentType type = schema.getTypeOrNull(name);
      if (!(type instanceof EdgeType) || EdgeType.holdsLightweightEdges(type))
        return false;
    }
    return true;
  }
}
