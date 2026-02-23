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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousAggregateBuilder {
  private static final Pattern TIME_BUCKET_PATTERN = Pattern.compile(
      "ts\\.timeBucket\\s*\\(\\s*'([^']+)'\\s*,\\s*(\\w+)\\s*\\)",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern ALIAS_PATTERN = Pattern.compile(
      "ts\\.timeBucket\\s*\\([^)]+\\)\\s+(?:AS\\s+)?(\\w+)",
      Pattern.CASE_INSENSITIVE);

  private final DatabaseInternal database;
  private String  name;
  private String  query;
  private boolean ifNotExists = false;

  public ContinuousAggregateBuilder(final DatabaseInternal database) {
    this.database = database;
  }

  public ContinuousAggregateBuilder withName(final String name) {
    this.name = name;
    return this;
  }

  public ContinuousAggregateBuilder withQuery(final String query) {
    this.query = query;
    return this;
  }

  public ContinuousAggregateBuilder withIgnoreIfExists(final boolean ignore) {
    this.ifNotExists = ignore;
    return this;
  }

  public ContinuousAggregate create() {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Continuous aggregate name is required");
    if (name.contains("`"))
      throw new IllegalArgumentException("Continuous aggregate name must not contain backtick characters");
    if (query == null || query.isEmpty())
      throw new IllegalArgumentException("Continuous aggregate query is required");

    final LocalSchema schema = (LocalSchema) database.getSchema();

    if (schema.existsContinuousAggregate(name)) {
      if (ifNotExists)
        return schema.getContinuousAggregate(name);
      throw new SchemaException("Continuous aggregate '" + name + "' already exists");
    }

    if (schema.existsType(name))
      throw new SchemaException("Cannot create continuous aggregate '" + name +
          "': a type with the same name already exists");

    // Parse and validate the query
    final String sourceTypeName = extractSourceType(query);
    if (sourceTypeName == null)
      throw new SchemaException("Continuous aggregate query must SELECT FROM a single type");

    if (!schema.existsType(sourceTypeName))
      throw new SchemaException("Source type '" + sourceTypeName + "' does not exist");

    final DocumentType sourceType = schema.getType(sourceTypeName);
    if (!(sourceType instanceof LocalTimeSeriesType))
      throw new SchemaException("Source type '" + sourceTypeName + "' is not a TimeSeries type. " +
          "Continuous aggregates can only be created on TimeSeries types.");

    // Extract ts.timeBucket parameters
    final Matcher bucketMatcher = TIME_BUCKET_PATTERN.matcher(query);
    if (!bucketMatcher.find())
      throw new SchemaException("Continuous aggregate query must include ts.timeBucket(interval, timestamp) " +
          "in the projection. Example: SELECT ts.timeBucket('1h', ts) AS hour, ...");

    final String intervalStr = bucketMatcher.group(1);
    final String tsColumnInQuery = bucketMatcher.group(2);
    final long bucketIntervalMs = parseInterval(intervalStr);

    // Validate that the timestamp column name doesn't contain backticks (SQL injection prevention)
    if (tsColumnInQuery.contains("`"))
      throw new SchemaException("Timestamp column name must not contain backtick characters: '" + tsColumnInQuery + "'");

    // Extract the alias for the time bucket column
    final Matcher aliasMatcher = ALIAS_PATTERN.matcher(query);
    String bucketAlias = null;
    if (aliasMatcher.find())
      bucketAlias = aliasMatcher.group(1);
    if (bucketAlias == null)
      throw new SchemaException("The ts.timeBucket() projection must have an alias. " +
          "Example: ts.timeBucket('1h', ts) AS hour");
    if (bucketAlias.contains("`"))
      throw new SchemaException("Bucket alias must not contain backtick characters: '" + bucketAlias + "'");

    // Validate GROUP BY is present
    if (!query.toUpperCase().contains("GROUP BY"))
      throw new SchemaException("Continuous aggregate query must include a GROUP BY clause");

    // Validate query structure: buildFilteredQuery uses string manipulation so it cannot
    // handle subqueries, CTEs, or inline comments. Reject unsupported patterns at creation time.
    validateQueryStructure(query);

    final String finalBucketAlias = bucketAlias;
    final String finalTsColumn = tsColumnInQuery;

    return schema.recordFileChanges(() -> {
      // Create backing document type
      schema.buildDocumentType().withName(name).create();

      // Create and register the continuous aggregate
      final ContinuousAggregateImpl ca = new ContinuousAggregateImpl(
          database, name, query, name, sourceTypeName,
          bucketIntervalMs, finalBucketAlias, finalTsColumn);
      ca.setStatus(MaterializedViewStatus.BUILDING);
      schema.continuousAggregates.put(name, ca);
      schema.saveConfiguration();

      // Perform initial full refresh (watermark=0 means all data)
      try {
        ContinuousAggregateRefresher.incrementalRefresh(database, ca);
      } catch (final Exception e) {
        schema.continuousAggregates.remove(name);
        try {
          schema.dropType(name);
        } catch (final Exception dropEx) {
          LogManager.instance().log(ContinuousAggregateBuilder.class, Level.WARNING,
              "Failed to clean up backing type '%s' after continuous aggregate creation failure: %s",
              dropEx, name, dropEx.getMessage());
        }
        throw e;
      }
      schema.saveConfiguration();

      return ca;
    });
  }

  private static void validateQueryStructure(final String query) {
    final String upper = query.toUpperCase().trim();
    // Reject CTEs (WITH ... AS)
    if (upper.startsWith("WITH "))
      throw new SchemaException("Continuous aggregate queries must not use CTEs (WITH ... AS). " +
          "Use a simple SELECT ... FROM ... GROUP BY query.");
    // Reject subqueries in FROM clause
    if (upper.contains("(SELECT ") || upper.contains("( SELECT "))
      throw new SchemaException("Continuous aggregate queries must not contain subqueries. " +
          "Use a simple SELECT ... FROM ... GROUP BY query.");
    // Reject UNION/INTERSECT/EXCEPT
    for (final String keyword : new String[] { " UNION ", " INTERSECT ", " EXCEPT " })
      if (upper.contains(keyword))
        throw new SchemaException("Continuous aggregate queries must not use " + keyword.trim() + ". " +
            "Use a simple SELECT ... FROM ... GROUP BY query.");
  }

  private String extractSourceType(final String sql) {
    final Statement parsed = database.getStatementCache().get(sql);
    if (parsed instanceof SelectStatement select) {
      final FromClause from = select.getTarget();
      if (from != null) {
        final FromItem item = from.getItem();
        if (item != null && item.getIdentifier() != null)
          return item.getIdentifier().getStringValue();
      }
    }
    return null;
  }

  static long parseInterval(final String interval) {
    if (interval == null || interval.isEmpty())
      throw new IllegalArgumentException("Invalid interval: empty");

    int unitStart = 0;
    for (int i = 0; i < interval.length(); i++) {
      if (!Character.isDigit(interval.charAt(i))) {
        unitStart = i;
        break;
      }
    }

    if (unitStart == 0)
      throw new IllegalArgumentException("Invalid interval: '" + interval + "'");

    final long value = Long.parseLong(interval.substring(0, unitStart));
    final String unit = interval.substring(unitStart).trim().toLowerCase();

    return switch (unit) {
      case "s" -> value * 1000L;
      case "m" -> value * 60_000L;
      case "h" -> value * 3_600_000L;
      case "d" -> value * 86_400_000L;
      case "w" -> value * 7 * 86_400_000L;
      default -> throw new IllegalArgumentException("Unknown interval unit: '" + unit + "'. Supported: s, m, h, d, w");
    };
  }
}
