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
import com.arcadedb.utility.DateUtils;

import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class ContinuousAggregateRefresher {
  // Allows letters, digits, and underscores only — consistent with ArcadeDB identifier rules.
  // Backtick, dot, hyphen, and other injection-enabling characters are excluded.
  private static final Pattern SAFE_COLUMN_NAME = Pattern.compile("[A-Za-z0-9_]+");

  /**
   * The clauses a SELECT can carry AFTER its WHERE, in the order {@code SelectStatement.toString()} writes them.
   * The first top-level occurrence of any of them is where the WHERE clause ENDS, which is where the bracket that
   * keeps the caller's own predicate intact has to close (#8156).
   */
  private static final String[] CLAUSES_AFTER_WHERE = { "GROUP BY", "ORDER BY", "UNWIND", "SKIP", "LIMIT", "TIMEOUT" };

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
      // ONE read of the pair: the flag decides whether to delete and the timestamp decides what, so reading them
      // through two separate getters could straddle a concurrent advance (found in review).
      final ContinuousAggregateImpl.Watermark startWatermark = ca.currentWatermark();
      final long watermark = startWatermark.timestamp();
      final boolean watermarkSet = startWatermark.set();

      // Validate interpolated names to prevent backtick injection
      if (!SAFE_COLUMN_NAME.matcher(backingTypeName).matches())
        throw new IllegalArgumentException("Unsafe backing type name: '" + backingTypeName + "'");
      if (!SAFE_COLUMN_NAME.matcher(bucketColumn).matches())
        throw new IllegalArgumentException("Unsafe bucket column name: '" + bucketColumn + "'");

      // Where a completed refresh's new watermark is parked until the transaction has actually COMMITTED (found in
      // review). Advancing it inside the lambda left the watermark AHEAD OF THE DATA whenever the commit itself
      // failed - retry exhaustion, say: the rows were rolled back, nothing restored the watermark, and the next
      // refresh trusted it and skipped the very window that had just been lost.
      final Long[] advancedWatermark = new Long[1];

      database.transaction(() -> {
        // Delete rows in the current (possibly incomplete) bucket and all newer buckets. #8152: the guard used to be
        // `watermark > 0`, which skipped the delete for an aggregate legitimately anchored at the epoch and let that
        // one bucket gain a duplicate on every refresh.
        if (watermarkSet)
          database.command("sql", "DELETE FROM `" + backingTypeName + "` WHERE `" + bucketColumn + "` >= ?",
              new Date(watermark));

        // Build the filtered query: append WHERE clause with watermark filter on the source timestamp
        final String filteredQuery = buildFilteredQuery(ca, watermark, watermarkSet);

        // Execute and insert results
        long maxBucketTs = watermark;
        boolean maxBucketSeen = watermarkSet;
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
              final long bucketMs = toEpochMs(bucketVal, bucketColumn, ca.getName());
              if (!maxBucketSeen || bucketMs > maxBucketTs) {
                maxBucketTs = bucketMs;
                maxBucketSeen = true;
              }
            }
          }
        }

        // The watermark advances to the max bucket boundary found. The FIRST bucket ever seen installs it even when
        // it is not greater than the initial 0 - that is the whole point of tracking "set" separately (#8152).
        if (maxBucketSeen && (!watermarkSet || maxBucketTs > watermark))
          advancedWatermark[0] = maxBucketTs;
      });

      // The transaction committed, so the rows the watermark refers to are durable and it can be installed.
      if (advancedWatermark[0] != null)
        ca.setWatermarkTs(advancedWatermark[0]);

      final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
      ca.recordRefreshSuccess(durationMs);
      ca.updateLastRefreshTime();
      ca.setStatus(MaterializedViewStatus.VALID);

      // Persist updated watermark only if it actually advanced.
      // If saveConfiguration fails, revert the in-memory watermark to the original value
      // so the next refresh re-processes the same window (delete-first design makes it safe).
      if (advancedWatermark[0] != null) {
        final LocalSchema schema = (LocalSchema) database.getSchema();
        try {
          schema.saveConfiguration();
        } catch (final Exception saveEx) {
          // #8152: restore the FLAG too. setWatermarkTs(watermark) would have left the aggregate claiming a
          // watermark of 0 it never had, which the next refresh would then honour by deleting nothing.
          ca.restoreWatermark(watermark, watermarkSet);
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

  /**
   * The defining query with the incremental filter applied, or the query unchanged when no watermark has been set
   * yet. #8152: {@code watermarkSet} is a separate argument and NOT {@code watermark > 0}, because an aggregate
   * anchored at the epoch has a watermark of 0 that must still be filtered on.
   */
  static String buildFilteredQuery(final ContinuousAggregateImpl ca, final long watermark, final boolean watermarkSet) {
    if (!watermarkSet)
      return ca.getQuery();

    final String query = ca.getQuery();
    final String tsColumn = ca.getTimestampColumn();

    // Validate column name to prevent backtick injection
    if (!SAFE_COLUMN_NAME.matcher(tsColumn).matches())
      throw new IllegalArgumentException("Unsafe timestamp column name: '" + tsColumn + "'");

    // Find WHERE clause position at the outermost level (case-insensitive).
    // Note: CTEs and subqueries with their own WHERE clauses are not supported
    // in continuous-aggregate queries.
    final String upperQuery = toUpperCasePreservingLength(query);
    final int whereIdx = findTopLevelKeyword(upperQuery, "WHERE", 0);

    if (whereIdx >= 0) {
      // #8156: BRACKET THE CALLER'S OWN PREDICATE. 'AND' BINDS TIGHTER THAN 'OR', so splicing the watermark filter
      // in as a bare left conjunct rewrote 'WHERE a OR b' into '(ts >= W AND a) OR b' - a DIFFERENT predicate, whose
      // second disjunct escapes the watermark entirely and re-aggregates buckets OLDER than it. Those buckets are
      // not covered by the DELETE that opens the refresh, so their rows survive and the recomputed ones land next to
      // them: one duplicate per such bucket per refresh. The bracket closes at the END OF THE WHERE CLAUSE, not at
      // the end of the string, because everything from GROUP BY onwards is outside it.
      final int whereEnd = findWhereClauseEnd(upperQuery, whereIdx + 5);
      final String before = query.substring(0, whereIdx + 5); // "WHERE" is 5 chars
      final String predicate = query.substring(whereIdx + 5, whereEnd).strip();
      final String after = query.substring(whereEnd);
      return before + " `" + tsColumn + "` >= " + watermark + " AND (" + predicate + ")"
          + (after.isEmpty() ? "" : " " + after.stripLeading());
    } else {
      // No WHERE clause — insert before the first clause that can follow one, or at the end
      final int insertIdx = findWhereClauseEnd(upperQuery, 0);
      if (insertIdx < query.length()) {
        final String before = query.substring(0, insertIdx);
        final String after = query.substring(insertIdx);
        return before + "WHERE `" + tsColumn + "` >= " + watermark + " " + after;
      }
      return query + " WHERE `" + tsColumn + "` >= " + watermark;
    }
  }

  /**
   * Upper-cases for keyword matching WITHOUT changing the length, so that every index found in the mirror addresses
   * the same character in the original. {@code String.toUpperCase} does not promise that - German 'ß' upper-cases to
   * two characters - and every use of the mirror here is a {@code substring} on the original.
   */
  private static String toUpperCasePreservingLength(final String query) {
    final char[] chars = query.toCharArray();
    for (int i = 0; i < chars.length; i++)
      chars[i] = Character.toUpperCase(chars[i]);
    return new String(chars);
  }

  /**
   * Index of the first top-level clause keyword at or after {@code fromIdx} that can follow a WHERE, or the length of
   * the query when there is none - i.e. where the WHERE clause ends.
   */
  private static int findWhereClauseEnd(final String upperQuery, final int fromIdx) {
    int end = upperQuery.length();
    for (final String keyword : CLAUSES_AFTER_WHERE) {
      final int idx = findTopLevelKeyword(upperQuery, keyword, fromIdx);
      if (idx >= 0 && idx < end)
        end = idx;
    }
    return end;
  }

  /**
   * Index of the first standalone occurrence of {@code keyword} at the outermost nesting level (depth 0) at or after
   * {@code fromIdx}, or -1. String literals (single or double quoted), block comments, line comments and
   * parenthesized subqueries are skipped, so a keyword inside any of them is not mistaken for a clause.
   * E.g.: {@code SELECT func('(foo)') FROM t WHERE ts > 0}, {@code SELECT /* WHERE not here *&#47; * FROM t WHERE ts > 0}
   * <p>
   * #8156: this used to scan for WHERE only, and the other branch of {@code buildFilteredQuery} used a plain
   * {@code indexOf} for GROUP BY / ORDER BY / LIMIT that knew nothing about quoting - so a GROUP BY inside a string
   * literal was taken for the clause. One scanner now answers for every clause keyword.
   * <p>
   * The scan always starts at 0 and {@code fromIdx} only filters what it RETURNS: the quote, comment and paren
   * state at {@code fromIdx} can only be known by reading everything before it. Starting the loop at {@code fromIdx}
   * would be faster and wrong. That costs one pass per keyword, on a query string, once per refresh.
   */
  private static int findTopLevelKeyword(final String upperQuery, final String keyword, final int fromIdx) {
    final int keywordLen = keyword.length();
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
      if (ch == keyword.charAt(0) && upperQuery.startsWith(keyword, idx)) {
        final boolean leftBound = idx == 0 || !Character.isLetterOrDigit(upperQuery.charAt(idx - 1));
        final boolean rightBound = idx + keywordLen >= len || !Character.isLetterOrDigit(upperQuery.charAt(idx + keywordLen));
        if (leftBound && rightBound && idx >= fromIdx)
          return idx;
        idx += keywordLen;
        continue;
      }
      idx++;
    }
    return -1;
  }

  /**
   * #8152: this was a private near-copy of the same conversion that lives in {@link DateUtils#toEpochMillis}, and it
   * knew nothing about the {@link java.time.LocalDateTime} that {@code ts.timeBucket()} actually returns - so every
   * bucket read as 0, the watermark never advanced, and each refresh appended another full copy of the aggregate on
   * top of the previous one. THE SILENT {@code return 0} IS WHAT MADE THAT A DATA DEFECT RATHER THAN AN ERROR: 0 is
   * also this class's "no watermark yet", so nothing anywhere could tell the two apart. It now refuses a value it
   * cannot read, which fails the refresh loudly and marks the aggregate ERROR.
   */
  private static long toEpochMs(final Object value, final String bucketColumn, final String aggregateName) {
    try {
      return DateUtils.toEpochMillis(value);
    } catch (final IllegalArgumentException | DateTimeParseException e) {
      throw new IllegalArgumentException("Continuous aggregate '" + aggregateName + "': bucket column '" + bucketColumn
          + "' holds '" + value + "' (" + value.getClass().getName() + "), which is not a timestamp", e);
    }
  }
}
