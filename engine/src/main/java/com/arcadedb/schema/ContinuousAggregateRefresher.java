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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;

public class ContinuousAggregateRefresher {

  public static void incrementalRefresh(final Database database, final ContinuousAggregateImpl ca) {
    if (!ca.tryBeginRefresh()) {
      LogManager.instance().log(ContinuousAggregateRefresher.class, Level.FINE,
          "Skipping concurrent refresh for continuous aggregate '%s' — already in progress", null, ca.getName());
      return;
    }
    ca.setStatus(MaterializedViewStatus.BUILDING);
    final long startNs = System.nanoTime();
    try {
      final String backingTypeName = ca.getBackingTypeName();
      final String bucketColumn = ca.getBucketColumn();
      final long watermark = ca.getWatermarkTs();

      // Validate interpolated names to prevent backtick injection
      if (!SAFE_COLUMN_NAME.matcher(backingTypeName).matches())
        throw new IllegalArgumentException("Unsafe backing type name: '" + backingTypeName + "'");
      if (!SAFE_COLUMN_NAME.matcher(bucketColumn).matches())
        throw new IllegalArgumentException("Unsafe bucket column name: '" + bucketColumn + "'");

      database.transaction(() -> {
        // Delete rows in the current (possibly incomplete) bucket and all newer buckets
        if (watermark > 0)
          database.command("sql", "DELETE FROM `" + backingTypeName + "` WHERE `" + bucketColumn + "` >= ?",
              new Date(watermark));

        // Build the filtered query: append WHERE clause with watermark filter on the source timestamp
        final String filteredQuery = buildFilteredQuery(ca, watermark);

        // Execute and insert results
        long maxBucketTs = watermark;
        try (final ResultSet rs = database.query("sql", filteredQuery)) {
          while (rs.hasNext()) {
            final Result result = rs.next();
            final MutableDocument doc = database.newDocument(backingTypeName);
            for (final String prop : result.getPropertyNames()) {
              if (!prop.startsWith("@"))
                doc.set(prop, result.getProperty(prop));
            }
            doc.save();

            // Track maximum bucket timestamp for advancing watermark
            final Object bucketVal = result.getProperty(bucketColumn);
            if (bucketVal != null) {
              final long bucketMs = toEpochMs(bucketVal);
              if (bucketMs > maxBucketTs)
                maxBucketTs = bucketMs;
            }
          }
        }

        // Advance watermark to the max bucket boundary found
        if (maxBucketTs > watermark)
          ca.setWatermarkTs(maxBucketTs);
      });

      final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
      ca.recordRefreshSuccess(durationMs);
      ca.updateLastRefreshTime();
      ca.setStatus(MaterializedViewStatus.VALID);

      // Persist updated watermark only if it actually advanced.
      // If saveConfiguration fails, revert the in-memory watermark to the original value
      // so the next refresh re-processes the same window (delete-first design makes it safe).
      if (ca.getWatermarkTs() > watermark) {
        final LocalSchema schema = (LocalSchema) database.getSchema();
        try {
          schema.saveConfiguration();
        } catch (final Exception saveEx) {
          ca.setWatermarkTs(watermark);
          throw saveEx;
        }
      }

    } catch (final Exception e) {
      ca.recordRefreshError();
      ca.setStatus(MaterializedViewStatus.ERROR);
      LogManager.instance().log(ContinuousAggregateRefresher.class, Level.SEVERE,
          "Error refreshing continuous aggregate '%s': %s", e, ca.getName(), e.getMessage());
      throw e;
    } finally {
      ca.endRefresh();
    }
  }

  // Allows letters, digits, and underscores only — consistent with ArcadeDB identifier rules.
  // Backtick, dot, hyphen, and other injection-enabling characters are excluded.
  private static final java.util.regex.Pattern SAFE_COLUMN_NAME = java.util.regex.Pattern.compile("[A-Za-z0-9_]+");

  static String buildFilteredQuery(final ContinuousAggregateImpl ca, final long watermark) {
    if (watermark <= 0)
      return ca.getQuery();

    final String query = ca.getQuery();
    final String tsColumn = ca.getTimestampColumn();

    // Validate column name to prevent backtick injection
    if (!SAFE_COLUMN_NAME.matcher(tsColumn).matches())
      throw new IllegalArgumentException("Unsafe timestamp column name: '" + tsColumn + "'");

    // Find WHERE clause position at the outermost level (case-insensitive).
    // Note: CTEs and subqueries with their own WHERE clauses are not supported
    // in continuous-aggregate queries.
    final String upperQuery = query.toUpperCase(Locale.ROOT);
    final int whereIdx = findWhereIndex(upperQuery);

    if (whereIdx >= 0) {
      // Insert the watermark filter right after WHERE
      final String before = query.substring(0, whereIdx + 5); // "WHERE" is 5 chars
      final String after = query.substring(whereIdx + 5);
      return before + " `" + tsColumn + "` >= " + watermark + " AND " + after.stripLeading();
    } else {
      // No WHERE clause — insert before GROUP BY, ORDER BY, LIMIT, or at end
      int insertIdx = findKeywordIndex(upperQuery, "GROUP BY");
      if (insertIdx < 0)
        insertIdx = findKeywordIndex(upperQuery, "ORDER BY");
      if (insertIdx < 0)
        insertIdx = findKeywordIndex(upperQuery, "LIMIT");
      if (insertIdx >= 0) {
        final String before = query.substring(0, insertIdx);
        final String after = query.substring(insertIdx);
        return before + "WHERE `" + tsColumn + "` >= " + watermark + " " + after;
      }
      return query + " WHERE `" + tsColumn + "` >= " + watermark;
    }
  }

  private static int findWhereIndex(final String upperQuery) {
    // Find standalone WHERE keyword at the outermost nesting level (depth 0),
    // skipping over string literals (single or double quoted), block comments (/* */),
    // line comments (--), and parenthesized subqueries so that WHERE keywords inside
    // them are not mistaken for the top-level WHERE.
    // E.g.: SELECT func('(foo)') FROM t WHERE ts > 0
    //        SELECT /* WHERE not here */ * FROM t WHERE ts > 0
    int depth = 0;
    int idx = 0;
    final int len = upperQuery.length();
    while (idx < len) {
      final char ch = upperQuery.charAt(idx);
      // Skip over block comments: /* ... */
      if (ch == '/' && idx + 1 < len && upperQuery.charAt(idx + 1) == '*') {
        idx += 2;
        while (idx + 1 < len && !(upperQuery.charAt(idx) == '*' && upperQuery.charAt(idx + 1) == '/'))
          idx++;
        idx += 2; // skip closing */
        continue;
      }
      // Skip over line comments: -- ... \n
      if (ch == '-' && idx + 1 < len && upperQuery.charAt(idx + 1) == '-') {
        idx += 2;
        while (idx < len && upperQuery.charAt(idx) != '\n')
          idx++;
        continue;
      }
      // Skip over quoted string literals to avoid counting parens inside them
      if (ch == '\'' || ch == '"') {
        final char quote = ch;
        idx++;
        while (idx < len) {
          final char c2 = upperQuery.charAt(idx);
          idx++;
          if (c2 == '\\') {
            idx++; // skip escaped character
          } else if (c2 == quote) {
            break;
          }
        }
        continue;
      }
      if (ch == '(') {
        depth++;
        idx++;
        continue;
      }
      if (ch == ')') {
        depth--;
        idx++;
        continue;
      }
      if (depth > 0) {
        idx++;
        continue;
      }
      if (ch == 'W' && upperQuery.startsWith("WHERE", idx)) {
        final boolean leftBound = idx == 0 || !Character.isLetterOrDigit(upperQuery.charAt(idx - 1));
        final boolean rightBound = idx + 5 >= len || !Character.isLetterOrDigit(upperQuery.charAt(idx + 5));
        if (leftBound && rightBound)
          return idx;
        idx += 5;
        continue;
      }
      idx++;
    }
    return -1;
  }

  private static int findKeywordIndex(final String upperQuery, final String keyword) {
    int idx = 0;
    while (idx < upperQuery.length()) {
      final int found = upperQuery.indexOf(keyword, idx);
      if (found < 0)
        return -1;
      final boolean leftBound = found == 0 || !Character.isLetterOrDigit(upperQuery.charAt(found - 1));
      final boolean rightBound = found + keyword.length() >= upperQuery.length()
          || !Character.isLetterOrDigit(upperQuery.charAt(found + keyword.length()));
      if (leftBound && rightBound)
        return found;
      idx = found + keyword.length();
    }
    return -1;
  }

  private static long toEpochMs(final Object value) {
    if (value instanceof Date d)
      return d.getTime();
    if (value instanceof Long l)
      return l;
    if (value instanceof Number n)
      return n.longValue();
    return 0;
  }
}
