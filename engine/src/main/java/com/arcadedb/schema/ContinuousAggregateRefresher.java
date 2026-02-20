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

      // Persist updated watermark
      final LocalSchema schema = (LocalSchema) database.getSchema();
      schema.saveConfiguration();

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

  static String buildFilteredQuery(final ContinuousAggregateImpl ca, final long watermark) {
    if (watermark <= 0)
      return ca.getQuery();

    final String query = ca.getQuery();
    final String tsColumn = ca.getTimestampColumn();

    // Find WHERE clause position (case-insensitive)
    final String upperQuery = query.toUpperCase();
    final int whereIdx = findWhereIndex(upperQuery);

    if (whereIdx >= 0) {
      // Insert the watermark filter right after WHERE
      final String before = query.substring(0, whereIdx + 5); // "WHERE" is 5 chars
      final String after = query.substring(whereIdx + 5);
      return before + " `" + tsColumn + "` >= " + watermark + " AND" + after;
    } else {
      // No WHERE clause — insert before GROUP BY, ORDER BY, or at end
      final int groupByIdx = findKeywordIndex(upperQuery, "GROUP BY");
      if (groupByIdx >= 0) {
        final String before = query.substring(0, groupByIdx);
        final String after = query.substring(groupByIdx);
        return before + "WHERE `" + tsColumn + "` >= " + watermark + " " + after;
      }
      return query + " WHERE `" + tsColumn + "` >= " + watermark;
    }
  }

  private static int findWhereIndex(final String upperQuery) {
    // Find WHERE that's not inside quotes — simple approach: look for standalone WHERE keyword
    int idx = 0;
    while (idx < upperQuery.length()) {
      final int found = upperQuery.indexOf("WHERE", idx);
      if (found < 0)
        return -1;
      // Check it's a standalone word
      final boolean leftBound = found == 0 || !Character.isLetterOrDigit(upperQuery.charAt(found - 1));
      final boolean rightBound = found + 5 >= upperQuery.length() || !Character.isLetterOrDigit(upperQuery.charAt(found + 5));
      if (leftBound && rightBound)
        return found;
      idx = found + 5;
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
