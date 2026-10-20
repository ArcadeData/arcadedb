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
package com.arcadedb.mongo;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The writes of the MongoDB wire protocol over a TIMESERIES type (issue #10464). Its samples are not records: they have no
 * identity, so the record path of a filtered delete or update, which selects its targets by identity, found none and answered
 * {@code n: 0}. A sample is never updated, and the samples to delete are named by their time and their series, so:
 * <ul>
 *   <li>a {@code deleteMany} is translated into {@code DELETE FROM <type> WHERE ...}, which the engine of the type serves, when its
 *   filter is a conjunction of conditions SQL removes exactly: a range or an equality on the timestamp field and an equality on a
 *   TAG field. Anything else - a FIELD, {@code $or}, {@code $ne}, {@code $in}, a value SQL would coerce where MongoDB would not
 *   match - is refused rather than approximated, since a delete that removed more than its filter says cannot be put back;</li>
 *   <li>a {@code deleteOne} is refused: a time series removes every sample in a range, it cannot pick one;</li>
 *   <li>an update, a replacement and an upsert are refused, filtered or not.</li>
 * </ul>
 * Every refusal names the type. Like SQL's, the delete is not part of the command's transaction: the samples are gone when it
 * returns.
 */
final class MongoTimeSeriesWrites {
  private MongoTimeSeriesWrites() {
  }

  /**
   * The {@code DELETE} statement removing the samples {@code filter} names, its parameters bound into {@code params}.
   *
   * @throws MongoServerError when the filter cannot be applied exactly to the samples of the type
   */
  static String deleteStatement(final TimeSeriesType type, final Document filter, final boolean single,
      final Map<String, Object> params) {
    if (single)
      throw new MongoServerError(ErrorCode.IllegalOperation, "Cannot delete one sample of the TIMESERIES type '" + type.getName()
          + "': a delete removes every sample in a time range of its field '" + type.getTimestampColumn()
          + "' with the given values of its TAG fields. Use deleteMany");

    final List<String> conditions = new ArrayList<>();
    if (filter != null)
      appendConditions(type, filter, conditions, params);

    final StringBuilder sql = new StringBuilder("DELETE FROM ").append(Identifier.quote(type.getName()));
    for (int i = 0; i < conditions.size(); i++)
      sql.append(i == 0 ? " WHERE " : " AND ").append(conditions.get(i));
    return sql.toString();
  }

  /**
   * The refusal of an update, a replacement or an upsert: a sample cannot be changed once written.
   */
  static MongoServerError updateRefusal(final TimeSeriesType type) {
    return new MongoServerError(ErrorCode.IllegalOperation, "Cannot update the samples of the TIMESERIES type '" + type.getName()
        + "': a sample cannot be updated once written. To change one, delete it with deleteMany({" + type.getTimestampColumn()
        + ": <timestamp>, <tag>: <value>, ...}) and insert it again");
  }

  private static void appendConditions(final TimeSeriesType type, final Document filter, final List<String> conditions,
      final Map<String, Object> params) {
    for (final Map.Entry<String, Object> entry : filter.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();

      if ("$and".equals(key)) {
        if (!(value instanceof List<?> items) || items.isEmpty())
          throw unsupported(type, key);
        for (final Object item : items) {
          if (!(item instanceof Document part))
            throw unsupported(type, key);
          appendConditions(type, part, conditions, params);
        }
        continue;
      }

      final ColumnDefinition column = type.getTsColumn(key);
      if (column == null || column.getRole() == ColumnDefinition.ColumnRole.FIELD)
        throw unsupported(type, key);

      if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
        if (value instanceof Document operators) {
          if (operators.isEmpty())
            throw unsupported(type, key);
          for (final Map.Entry<String, Object> op : operators.entrySet())
            conditions.add(condition(key, rangeOperator(type, key, op.getKey()), timestampOf(type, key, op.getValue()), params));
        } else
          conditions.add(condition(key, "=", timestampOf(type, key, value), params));
      } else {
        // a TAG: only an equality, with a value of the tag's own kind. SQL coerces a value of another kind to the tag's type, where
        // MongoDB would match nothing, so the delete would remove samples the filter does not name
        Object operand = value;
        if (value instanceof Document operators) {
          if (operators.size() != 1 || !operators.containsKey("$eq"))
            throw unsupported(type, key);
          operand = operators.get("$eq");
        }
        if (!sameKind(column.getDataType(), operand))
          throw unsupported(type, key);
        conditions.add(condition(key, "=", operand, params));
      }
    }
  }

  private static String condition(final String column, final String operator, final Object value, final Map<String, Object> params) {
    final String param = "p" + params.size();
    params.put(param, value);
    return Identifier.quote(column) + " " + operator + " :" + param;
  }

  private static String rangeOperator(final TimeSeriesType type, final String column, final String operator) {
    return switch (operator) {
      case "$eq" -> "=";
      case "$gt" -> ">";
      case "$gte" -> ">=";
      case "$lt" -> "<";
      case "$lte" -> "<=";
      default -> throw unsupported(type, column);
    };
  }

  /**
   * A timestamp operand in milliseconds: a whole number, or a date when the type is known to store milliseconds.
   */
  private static long timestampOf(final TimeSeriesType type, final String column, final Object value) {
    if (value instanceof Integer || value instanceof Long)
      return ((Number) value).longValue();
    if (value instanceof Double d && d == Math.rint(d) && !Double.isInfinite(d))
      return d.longValue();
    if (type.isPrecisionHonoured()) {
      if (value instanceof Date date)
        return date.getTime();
      if (value instanceof Instant instant)
        return instant.toEpochMilli();
    }
    throw unsupported(type, column);
  }

  private static boolean sameKind(final Type tagType, final Object value) {
    return switch (tagType) {
      case STRING -> value instanceof String;
      case BOOLEAN -> value instanceof Boolean;
      case BYTE, SHORT, INTEGER, LONG -> value instanceof Integer || value instanceof Long
          || value instanceof Double d && d == Math.rint(d) && !Double.isInfinite(d);
      case FLOAT, DOUBLE -> value instanceof Integer || value instanceof Long || value instanceof Double;
      default -> false;
    };
  }

  private static MongoServerError unsupported(final TimeSeriesType type, final String key) {
    return new MongoServerError(ErrorCode.IllegalOperation, "Cannot delete from the TIMESERIES type '" + type.getName()
        + "' with the condition on '" + key + "': a delete removes the samples in a time range of its field '"
        + type.getTimestampColumn() + "' ($eq, $gt, $gte, $lt, $lte with a number of milliseconds or a date) with the given values of"
        + " its TAG fields (an equality with a value of the tag's type), joined by AND. A condition on a FIELD, $or, $ne, $in or any"
        + " other operator is not supported: the samples to remove are named by their time and their series");
  }
}
