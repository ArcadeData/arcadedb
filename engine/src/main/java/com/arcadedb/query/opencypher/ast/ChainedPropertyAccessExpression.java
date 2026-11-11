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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.Map;

/**
 * Expression for chained property access where the base is not a simple variable.
 * Example: list[0].property, func().property, $param.field
 * <p>
 * {@code CypherExpressionWalker} descends into the base - the fix for issue #6567 depends on the walk reaching it: without that
 * arm the walker had treated {@code startNode(r).name} as a leaf and never seen the "r" inside. Part of the AST package (it was a
 * nested class of the parser, issue #11480), so {@link CypherReferencedVariables} classifies it: unclassified, every expression
 * holding a field of a parameter, {@code $d.u}, read as "may name anything", and no index seek could be built from one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ChainedPropertyAccessExpression implements PropertyReadExpression {
  private final Expression baseExpression;
  private final String propertyName;

  public ChainedPropertyAccessExpression(final Expression baseExpression, final String propertyName) {
    this.baseExpression = baseExpression;
    this.propertyName = propertyName;
  }

  public Expression getBaseExpression() {
    return baseExpression;
  }

  /** {@code a.link.v} and {@code properties(n).v} read the property {@code v} of the record the base evaluates to. */
  @Override
  public PropertyRead propertyRead(final Result row, final CommandContext context) {
    final Document record = PropertyReadExpression.recordOf(baseExpression, row, context);
    return record != null ? new PropertyRead(record, propertyName) : null;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object baseValue = baseExpression.evaluate(result, context);
    if (baseValue == null) {
      return null;
    }

    // Handle a persisted node-valued property: ArcadeDB stores a NODE property as a LINK/RID,
    // so once the writing transaction commits, a chained property access (e.g. holder.ref.id)
    // sees the stored RID rather than the live Vertex it saw during the write. Dereference it
    // transparently, applying the same temporal-type restoration as the single-level
    // PropertyAccessExpression, so the same expression keeps working across the transaction
    // boundary (#5800) and stays consistent with the variable-bound access path - literally so: the
    // dereference itself lives in PropertyAccessExpression, which also turns a link that resolves to
    // nothing into a Cypher-flavored TypeError instead of a raw RecordNotFoundException (#5898).
    if (baseValue instanceof RID rid)
      return PropertyAccessExpression.readLinkedProperty(rid, propertyName);

    // Handle Document types
    // Read like the variable-bound access path, so a temporal is the Cypher type its schema declares (issue #9647)
    if (baseValue instanceof Document document)
      return TemporalUtil.convertFromStorage(document, propertyName);

    // Handle Map types
    if (baseValue instanceof Map) {
      return ((Map<?, ?>) baseValue).get(propertyName);
    }

    // Handle Result types
    if (baseValue instanceof Result) {
      return ((Result) baseValue).getProperty(propertyName);
    }

    // path.nodes / path.relationships / path.length on a procedure's path, which used to be a map (issue #9913)
    if (baseValue instanceof TraversalPath path)
      return path.getMember(propertyName);

    // Handle temporal types (e.g., date().year, datetime().hour)
    if (baseValue instanceof CypherTemporalValue) {
      return ((CypherTemporalValue) baseValue).getTemporalProperty(propertyName);
    }
    // A native temporal (a parameter, a value read off a map): every class fromCoreJavaType wraps, java.util.Calendar
    // included (issue #9647), where only LocalDate and LocalDateTime used to be accepted
    if (TemporalUtil.isNativeTemporal(baseValue)) {
      final Object coerced = TemporalUtil.fromCoreJavaType(baseValue);
      if (coerced instanceof CypherTemporalValue temporal)
        return temporal.getTemporalProperty(propertyName);
    }

    // Type validation: property access only works on property-bearing types. Primitive types
    // (Integer, String, Boolean, List, etc.) don't have properties, so accessing one is a type
    // error - mirroring PropertyAccessExpression (variable-bound access) and Neo4j semantics.
    // Note: a null base short-circuits to null above (property access on null propagates null).
    throw new TypeMismatchException(
        "TypeError: Cannot access property '" + propertyName + "' on a " + CypherFunctionHelper.cypherTypeName(baseValue)
            + " value");
  }

  @Override
  public boolean isAggregation() {
    return baseExpression.isAggregation();
  }

  @Override
  public boolean containsAggregation() {
    return baseExpression.containsAggregation();
  }

  @Override
  public String getText() {
    return baseExpression.getText() + "." + propertyName;
  }
}
