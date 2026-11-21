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
package com.arcadedb.query.sql.parser;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.serializer.BinaryComparator;

import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

/**
 * Created by luigidellaquila on 06/02/15.
 */
public class OrderByItem {
  public static final String   ASC  = "ASC";
  public static final String   DESC = "DESC";
  protected           String   alias;
  public Modifier    modifier;
  public String      recordAttr;
  public Expression  expression; // For complex ORDER BY expressions (e.g., CASE WHEN)
  protected           String   type = ASC;
  // For parameterized order direction (e.g., ORDER BY field :dir)
  protected           InputParameter directionParameter;

  public String getAlias() {
    return alias;
  }

  public void setAlias(final String alias) {
    this.alias = alias;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getRecordAttr() {
    return recordAttr;
  }

  public void setRecordAttr(final String recordAttr) {
    this.recordAttr = recordAttr;
  }

  public InputParameter getDirectionParameter() {
    return directionParameter;
  }

  public void setDirectionParameter(final InputParameter directionParameter) {
    this.directionParameter = directionParameter;
  }

  // Returns null when only `expression` is set; callers that use List.contains(getName())
  // correctly treat null as "not found" and will generate a computed projection for it.
  public String getName() {
    return alias != null ? alias : recordAttr != null ? recordAttr : null;
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (expression != null) {
      expression.toString(params, builder);
    } else if (alias != null) {
      builder.append(alias);
      if (modifier != null) {
        modifier.toString(params, builder);
      }
    } else if (recordAttr != null) {
      builder.append(recordAttr);
    }
    if (directionParameter != null) {
      builder.append(" ");
      directionParameter.toString(params, builder);
    } else if (type != null) {
      builder.append(" " + type);
    }
  }

  /**
   * The value {@code row} is sorted by for this item, modifier applied. {@link OrderBy#extractKeys} calls it once per row when
   * the row enters the sort, and the sort compares the values with {@link #compareKeys} (issue #12860).
   */
  public Object extractKey(final Result row, final CommandContext context) {
    Object value = null;
    if (recordAttr != null) {
      // a column of the attribute's name the row carries first, a null one included, then the attribute of the row itself, as
      // the projection reads it (issue #11131: only @rid was read back, ORDER BY @type compared nulls and left the rows
      // unordered). Not @this: a whole record has no order of its own
      if (RecordAttribute.isRowAttribute(recordAttr))
        value = RecordAttribute.resolve(row, recordAttr);
      else
        value = row.getProperty(recordAttr);
    } else if (alias != null) {
      value = row.getProperty(alias);
      if (value == null) {
        value = row.getMetadata(alias);
        // On an edge, `out` / `in` with no property of that name are its endpoints, as in the projection (issue #10684). A
        // null here can also be a property present with a null value: the helper asks hasProperty() first and leaves that
        // null alone, so only a name the row does not carry at all is replaced
        if (value == null)
          value = SuffixIdentifier.edgeEndpointIfUnshadowed(row, alias);
      }
    } else if (expression != null)
      value = expression.execute(row, context);

    if (modifier != null)
      value = modifier.execute(row, value, context);
    return value;
  }

  /**
   * Compares two values {@link #extractKey} returned, in ascending order: nulls first, then the typed comparison, then the
   * string fallback. The direction is applied by the caller ({@link #isDescending}).
   *
   * @return -1, 0 or 1
   */
  public int compareKeys(final Object aVal, final Object bVal) {
    if (aVal == null)
      return bVal == null ? 0 : -1;
    if (bVal == null)
      return 1;
    // The commonest numeric keys first, with the answer BinaryComparator gives them at the end of its chain of type tests: the
    // sort compares about 2 * log2(n) pairs per row
    if (aVal instanceof Double aDouble && bVal instanceof Double bDouble)
      return BinaryComparator.compareDoubles(aDouble, bDouble);
    if (aVal instanceof Long aLong && bVal instanceof Long bLong)
      return Long.compare(aLong, bLong);
    if (aVal instanceof Integer anInt && bVal instanceof Integer bInt)
      return Integer.compare(anInt, bInt);
    // Both values are non-null. Attempt a typed comparison first: BinaryComparator also handles some
    // non-Comparable types (e.g. Map). When the values cannot be compared directly (e.g. List,
    // embedded document, custom or mixed types), fall back to a deterministic string comparison so
    // ORDER BY yields a stable order instead of silently leaving the affected rows unordered.
    int result;
    try {
      result = BinaryComparator.compareTo(aVal, bVal);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE,
          "Values of type %s and %s are not directly comparable for ORDER BY, falling back to string comparison",
          aVal.getClass().getName(), bVal.getClass().getName());
      result = aVal.toString().compareTo(bVal.toString());
    }
    return Integer.signum(result);
  }

  /** The sort direction: a direction parameter's value first, then the static type. */
  public boolean isDescending(final CommandContext context) {
    if (directionParameter != null) {
      final Object dirValue = directionParameter.getValue(context.getInputParameters());
      // false = DESC (descending), true/null = ASC (ascending, default)
      if (Boolean.FALSE.equals(dirValue))
        return true;
      if (dirValue instanceof String)
        return DESC.equalsIgnoreCase((String) dirValue);
    }
    return DESC.equals(type);
  }

  /** True when the key is the value of a row's column as is, false when an expression or a modifier computes it. */
  public boolean isColumnKey() {
    return expression == null && modifier == null;
  }

  public OrderByItem copy() {
    final OrderByItem result = new OrderByItem();
    result.alias = alias;
    result.modifier = modifier == null ? null : modifier.copy();
    result.recordAttr = recordAttr;
    result.expression = expression == null ? null : expression.copy();
    result.type = type;
    result.directionParameter = directionParameter == null ? null : directionParameter.copy();
    return result;
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (modifier != null)
      modifier.extractSubQueries(collector);
    if (expression != null)
      expression.extractSubQueries(collector);
  }

  public boolean refersToParent() {
    if ("$parent".equalsIgnoreCase(alias))
      return true;

    if (expression != null && expression.refersToParent())
      return true;

    return modifier != null && modifier.refersToParent();
  }

  public Modifier getModifier() {
    return modifier;
  }

  public void setModifier(final Modifier modifier) {
    this.modifier = modifier;
  }

  /**
   * {@link OrderBy#getIdentityElements()} hands {@link SimpleNode#equals(Object)} the raw {@code OrderByItem}
   * elements to compare with {@link Objects#equals(Object, Object)}, which without this fell back to reference
   * identity: two separate parses of the very same {@code ORDER BY} clause were never equal, and so neither was any
   * statement that carries one (issue #6409, item 3).
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final OrderByItem that = (OrderByItem) o;
    return Objects.equals(alias, that.alias) && Objects.equals(modifier, that.modifier) && Objects.equals(recordAttr, that.recordAttr)
        && Objects.equals(expression, that.expression) && Objects.equals(type, that.type)
        && Objects.equals(directionParameter, that.directionParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, modifier, recordAttr, expression, type, directionParameter);
  }
}
