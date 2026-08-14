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
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.temporal.*;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.time.temporal.Temporal;
import java.util.Date;
import java.util.Map;

/**
 * Expression representing property access on a variable.
 * Example: n.name, person.age
 */
public class PropertyAccessExpression implements Expression {
  private final String variableName;
  private final String propertyName;

  public PropertyAccessExpression(final String variableName, final String propertyName) {
    this.variableName = variableName;
    this.propertyName = propertyName;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object variable = result.getProperty(variableName);
    DeletedEntityMarker.checkNotDeleted(variable);

    if (variable == null)
      return null;

    if (variable instanceof RID rid) {
      // Lazy resolution: a persisted node-valued property is stored as a LINK, and algorithm procedures store RIDs to
      // avoid loading all vertices upfront. Either way the record is loaded only when a property is actually read.
      return readLinkedProperty(rid, propertyName);
    } else if (variable instanceof Document) {
      final Object rawValue = ((Document) variable).get(propertyName);
      return TemporalUtil.convertFromStorage(rawValue);
    } else if (variable instanceof Map) {
      // Handle Map types (e.g., from UNWIND with parameter maps)
      return ((Map<?, ?>) variable).get(propertyName);
    } else if (variable instanceof Result) {
      // Handle Result types (nested results)
      return ((Result) variable).getProperty(propertyName);
    } else if (variable instanceof CypherTemporalValue) {
      // Handle temporal value property access (e.g., date.year, time.hour)
      return ((CypherTemporalValue) variable).getTemporalProperty(propertyName);
    } else if (variable instanceof Temporal || variable instanceof Date) {
      // Native java.time / java.util.Date value (e.g. a temporal parameter or a stored
      // ZonedDateTime/Date) → wrap into its Cypher temporal type for component access (date.year, ...).
      final Object coerced = TemporalUtil.fromCoreJavaType(variable);
      if (coerced instanceof CypherTemporalValue temporal)
        return temporal.getTemporalProperty(propertyName);
    }

    // Type validation: property access only works on property-bearing types
    // Primitive types (Integer, String, Boolean, List, etc.) don't have properties
    throw new CommandExecutionException(
        "TypeError: Cannot access property '" + propertyName + "' on " +
        variable.getClass().getSimpleName() + " value");
  }

  /**
   * Reads {@code propertyName} through a persisted LINK, shared by this (variable-bound) access path and the chained
   * access path in {@code CypherExpressionBuilder.ChainedPropertyAccessExpression} so both dereference a RID identically.
   * <p>
   * Three decisions here are load-bearing and easy to undo by accident:
   * <ul>
   *   <li>The target is a {@link Document}, not a {@code Vertex}. Vertex, edge and plain document are all
   *       {@code Document}s and the adjacent {@code instanceof Document} branch already reads all three the same way,
   *       so a LINK to a non-vertex record is not an error - which is what {@code RID.asVertex()} made it, by
   *       reporting its own {@code ClassCastException} as a {@link RecordNotFoundException}.</li>
   *   <li>{@code getRecord()} plus {@code instanceof} rather than the shorter {@code RID.asDocument()}, which casts
   *       internally: using it would mean a {@code catch (ClassCastException)} around a record lookup, which also
   *       swallows any unrelated cast failure raised deeper inside it and reports a real engine fault as "the linked
   *       record has no properties".</li>
   *   <li>The {@link RecordNotFoundException} is NOT kept as the cause. {@code AbstractServerHttpHandler} classifies a
   *       {@code CommandExecutionException} by {@code getCause()} when there is one, so attaching it routes the
   *       response through the catch-all arm ("Error on transaction commit") instead of the Cypher-error arm ("Cannot
   *       execute command") - the very message this replaced (issue #5898). Nothing is lost: the RID and the reason
   *       are both in the message.</li>
   * </ul>
   */
  public static Object readLinkedProperty(final RID rid, final String propertyName) {
    final Record record;
    try {
      record = rid.getRecord();
    } catch (final RecordNotFoundException e) {
      throw new CommandExecutionException(brokenLinkMessage(rid, propertyName, "the linked record does not exist"));
    }

    if (!(record instanceof Document document))
      throw new CommandExecutionException(brokenLinkMessage(rid, propertyName, "the linked record has no properties"));

    return TemporalUtil.convertFromStorage(document.get(propertyName));
  }

  /**
   * Keeps the two broken-link failures on one wording. The RID takes the place the generic sibling messages give to the
   * base type's class name: with the record gone, the link itself is the only thing left that identifies what could not
   * be read, and it is what a caller needs to find the property still holding it.
   */
  private static String brokenLinkMessage(final RID rid, final String propertyName, final String reason) {
    return "TypeError: Cannot access property '" + propertyName + "' on " + rid + ": " + reason;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return variableName + "." + propertyName;
  }

  public String getVariableName() {
    return variableName;
  }

  public String getPropertyName() {
    return propertyName;
  }
}
