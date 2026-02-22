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
package com.arcadedb.function.sql.geo;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base for ST_* spatial predicate functions that implement both
 * SQLFunctionAbstract and IndexableSQLFunction.
 * <p>
 * Subclasses provide the exact spatial predicate evaluation via {@link #evaluate(Shape, Shape, Object[])}.
 * The base class wires up query optimizer integration via the {@link IndexableSQLFunction} interface,
 * so that queries using these predicates automatically benefit from geospatial indexes.
 * </p>
 */
public abstract class SQLFunctionST_Predicate extends SQLFunctionAbstract implements IndexableSQLFunction {

  protected SQLFunctionST_Predicate(final String name) {
    super(name);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {
    if (iParams == null || iParams.length < 2 || iParams[0] == null || iParams[1] == null)
      return null;
    final Shape geom1 = GeoUtils.parseGeometry(iParams[0]);
    final Shape geom2 = GeoUtils.parseGeometry(iParams[1]);
    if (geom1 == null || geom2 == null)
      return null;
    return evaluate(geom1, geom2, iParams);
  }

  /**
   * Subclasses override to provide exact spatial predicate evaluation.
   */
  protected abstract Boolean evaluate(Shape geom1, Shape geom2, Object[] params);

  @Override
  public boolean shouldExecuteAfterSearch(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // The index returns a superset; exact predicate check must still run
    return true;
  }

  @Override
  public boolean canExecuteInline(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Always fall back to full scan if no index is available
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 1 || target == null)
      return false;

    // First argument must be a simple field reference (identifier), not a nested function call
    final Expression firstArg = oExpressions[0];
    if (firstArg == null)
      return false;

    final String fieldName = extractFieldName(firstArg);
    if (fieldName == null)
      return false;

    // Determine the type name from the FROM clause
    final String typeName = extractTypeName(target);
    if (typeName == null)
      return false;

    // Check if the type exists in the schema
    final Schema schema = context.getDatabase().getSchema();
    if (!schema.existsType(typeName))
      return false;

    final DocumentType docType = schema.getType(typeName);

    // Look for a GEOSPATIAL index on the field
    for (final TypeIndex typeIndex : docType.getAllIndexes(true)) {
      if (typeIndex.getType() == Schema.INDEX_TYPE.GEOSPATIAL) {
        final List<String> props = typeIndex.getPropertyNames();
        if (props != null && props.contains(fieldName))
          return true;
      }
    }
    return false;
  }

  @Override
  public long estimate(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    // Return -1 to indicate no precise estimate; optimizer will use default heuristics
    return -1;
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 1)
      return List.of();

    final String fieldName = extractFieldName(oExpressions[0]);
    if (fieldName == null)
      return List.of();

    final String typeName = extractTypeName(target);
    if (typeName == null)
      return List.of();

    final Schema schema = context.getDatabase().getSchema();
    if (!schema.existsType(typeName))
      return List.of();

    final DocumentType docType = schema.getType(typeName);

    // Resolve the GEOSPATIAL index on this field
    TypeIndex geoTypeIndex = null;
    for (final TypeIndex typeIndex : docType.getAllIndexes(true)) {
      if (typeIndex.getType() == Schema.INDEX_TYPE.GEOSPATIAL) {
        final List<String> props = typeIndex.getPropertyNames();
        if (props != null && props.contains(fieldName)) {
          geoTypeIndex = typeIndex;
          break;
        }
      }
    }

    if (geoTypeIndex == null)
      return List.of();

    // Parse the search shape from the second expression value
    final Shape searchShape = resolveSearchShape(oExpressions, context);
    if (searchShape == null)
      return List.of();

    // Query each per-bucket geo index and collect the results
    final List<Record> results = new ArrayList<>();
    for (final Index bucketIndex : geoTypeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof final LSMTreeGeoIndex geoIndex) {
        final IndexCursor cursor = geoIndex.get(new Object[] { searchShape }, -1);
        while (cursor.hasNext()) {
          final Identifiable id = cursor.next();
          if (id != null)
            results.add(id.getRecord());
        }
      }
    }
    return results;
  }

  // ---- Private helpers ----

  /**
   * Extracts a simple field name from an expression if it is a plain identifier reference.
   * Returns null if the expression is a complex expression (function call, arithmetic, etc.).
   */
  private static String extractFieldName(final Expression expr) {
    if (expr == null)
      return null;
    // toString() on a plain identifier expression yields the field name
    final String text = expr.toString();
    if (text == null || text.isBlank())
      return null;
    // Reject if this looks like a function call or contains operators
    if (text.contains("(") || text.contains(" ") || text.contains("."))
      return null;
    return text;
  }

  /**
   * Extracts the type name from the FROM clause (e.g. "FROM Location").
   */
  private static String extractTypeName(final FromClause target) {
    if (target == null || target.getItem() == null)
      return null;
    final var identifier = target.getItem().getIdentifier();
    if (identifier == null)
      return null;
    return identifier.getStringValue();
  }

  /**
   * Resolves the search shape from the function expressions. The second argument (index 1)
   * is the shape to search against. It may be a literal WKT string or a nested ST_* function call.
   */
  private static Shape resolveSearchShape(final Expression[] oExpressions, final CommandContext context) {
    if (oExpressions.length < 2 || oExpressions[1] == null)
      return null;
    // Evaluate the second expression in the context of a null record to get the shape value
    final Object value = oExpressions[1].execute((com.arcadedb.database.Identifiable) null, context);
    if (value == null)
      return null;
    return GeoUtils.parseGeometry(value);
  }
}
