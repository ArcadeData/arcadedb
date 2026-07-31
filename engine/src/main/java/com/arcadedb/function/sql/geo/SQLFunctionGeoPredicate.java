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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Abstract base for geo.* spatial predicate functions that implement both
 * SQLFunctionAbstract and IndexableSQLFunction.
 * <p>
 * Subclasses provide the exact spatial predicate evaluation via {@link #evaluate(Shape, Shape, Object[])}.
 * The base class wires up query optimizer integration via the {@link IndexableSQLFunction} interface,
 * so that queries using these predicates automatically benefit from geospatial indexes.
 * </p>
 */
public abstract class SQLFunctionGeoPredicate extends SQLFunctionAbstract implements IndexableSQLFunction {

  protected SQLFunctionGeoPredicate(final String name) {
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

    // #5601: the per-bucket geo cursors are chained LAZILY. Materialising them here loaded every candidate RECORD of
    // every bucket - the full content, not just the RID - before the first row reached the geo.* re-check, so a
    // `LIMIT 10` over a wide-area query paid for the whole candidate set. The index cursor is itself lazy, so the
    // whole chain now streams and a consumer that stops early stops the covering-cell walk with it.
    // Arrays.asList, not List.of: the array comes from a live list and the eager loop this replaced simply skipped a
    // null element (it is not an LSMTreeGeoIndex), so tolerate one here too rather than turning a schema anomaly into
    // an NPE on the query path. It also wraps instead of copying an array that is already freshly allocated.
    final List<Index> bucketIndexes = Arrays.<Index>asList(geoTypeIndex.getIndexesOnBuckets());
    return () -> new GeoCandidateIterator(bucketIndexes, searchShape);
  }

  /**
   * Streams the candidate records of a geo query across the per-bucket geospatial indexes, opening one index cursor at
   * a time. RIDs are unique per bucket, so no cross-bucket deduplication is needed on top of what each cursor already
   * does across the covering cells of the search shape.
   * <p>
   * Package-private rather than private so its lifecycle - exhaustion, early close, a bucket that is not a geospatial
   * index - can be exercised directly instead of only through a planned query.
   */
  static class GeoCandidateIterator implements Iterator<Record>, AutoCloseable {
    private final List<Index>   bucketIndexes;
    private final Shape         searchShape;
    private       int           nextBucket;
    private       IndexCursor   cursor;
    /** Lookahead, named for what it holds rather than for next(): fetchNext() fills it, next() drains it. */
    private       Identifiable  pending;
    private       boolean       closed;

    GeoCandidateIterator(final List<Index> bucketIndexes, final Shape searchShape) {
      this.bucketIndexes = bucketIndexes;
      this.searchShape = searchShape;
    }

    @Override
    public boolean hasNext() {
      if (pending == null)
        fetchNext();
      return pending != null;
    }

    @Override
    public Record next() {
      if (pending == null) {
        fetchNext();
        if (pending == null)
          throw new NoSuchElementException();
      }
      final Identifiable current = pending;
      pending = null;
      return current.getRecord();
    }

    private void fetchNext() {
      if (closed)
        // explicit, so re-entry after close() does not rest on nextBucket having been pushed past the last bucket
        return;

      while (true) {
        if (cursor != null) {
          if (cursor.hasNext()) {
            pending = cursor.next();
            return;
          }
          cursor.close();
          cursor = null;
        }

        if (nextBucket >= bucketIndexes.size())
          return;

        final Index bucketIndex = bucketIndexes.get(nextBucket++);
        if (bucketIndex instanceof final LSMTreeGeoIndex geoIndex)
          cursor = geoIndex.get(new Object[] { searchShape }, -1);
      }
    }

    /**
     * Releases the open index cursor even when the scan did not run to exhaustion (a LIMIT was reached, or the result
     * set was closed early). A compacted-series cursor registers with its file so a full compaction defers dropping
     * it, so an abandoned cursor would keep the retired file alive for the lifetime of the database.
     */
    @Override
    public void close() {
      closed = true;
      if (cursor != null) {
        cursor.close();
        cursor = null;
      }
      pending = null;
    }
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
   * is the shape to search against. It may be a literal WKT string or a nested geo.* function call.
   */
  private static Shape resolveSearchShape(final Expression[] oExpressions, final CommandContext context) {
    if (oExpressions.length < 2 || oExpressions[1] == null)
      return null;
    // Evaluate the second expression in the context of a null record to get the shape value
    final Object value = oExpressions[1].execute((Identifiable) null, context);
    if (value == null)
      return null;
    return GeoUtils.parseGeometry(value);
  }
}
