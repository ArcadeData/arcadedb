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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.IntHashSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class for vector SQL functions providing common utility methods.
 * All vector functions should extend this class to reuse conversion and validation logic.
 * <p>
 * Functions with names starting with "vector." will automatically have an alias
 * generated for backward compatibility. For example, "vector.cosineSimilarity"
 * will also be available as "vectorCosineSimilarity".
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class SQLFunctionVectorAbstract extends SQLFunctionAbstract {
  private static final String VECTOR_PREFIX = "vector.";
  private final String alias;

  protected SQLFunctionVectorAbstract(final String name) {
    super(name);
    // Auto-generate alias for backward compatibility: vector.xxx -> vectorXxx
    if (name.startsWith(VECTOR_PREFIX)) {
      final String suffix = name.substring(VECTOR_PREFIX.length());
      this.alias = "vector" + Character.toUpperCase(suffix.charAt(0)) + suffix.substring(1);
    } else {
      this.alias = null;
    }
  }

  @Override
  public String getAlias() {
    return alias;
  }

  /**
   * Validates that two vectors have the same dimension.
   *
   * @param v1 First vector
   * @param v2 Second vector
   *
   * @throws CommandSQLParsingException if dimensions don't match
   */
  protected void validateSameDimension(final float[] v1, final float[] v2) {
    if (v1.length != v2.length) {
      throw new CommandSQLParsingException("Vectors must have the same dimension, found: " + v1.length + " and " + v2.length);
    }
  }

  /**
   * Validates that a parameter is not null.
   *
   * @param param     Parameter to validate
   * @param paramName Parameter name for error message
   *
   * @throws CommandSQLParsingException if parameter is null
   */
  protected void validateNotNull(final Object param, final String paramName) {
    if (param == null) {
      throw new CommandSQLParsingException(paramName + " cannot be null");
    }
  }

  /**
   * Validates that the number of parameters matches expected count.
   *
   * @param params        Actual parameters
   * @param expectedCount Expected parameter count
   *
   * @throws CommandSQLParsingException if count doesn't match
   */
  protected void validateParameterCount(final Object[] params, final int expectedCount) {
    if (params == null || params.length != expectedCount) {
      throw new CommandSQLParsingException(getSyntax());
    }
  }

  /**
   * Reads a (possibly dotted) property path from a record and returns the leaf value.
   * <p>
   * Supports flat property names ({@code "source"}) and dotted nested paths
   * ({@code "metadata.author"}, {@code "provenance.source.id"}). Each segment after the first
   * descends one level via {@link Map#get(Object)} for {@code Map}-typed values or
   * {@link Document#get(String)} for embedded documents. A missing segment short-circuits to
   * {@code null}; a segment that lands on a non-traversable value (e.g. a primitive in the middle
   * of the path) also returns {@code null}.
   * <p>
   * Used by the {@code groupBy} option on {@code vector.neighbors}, {@code vector.sparseNeighbors},
   * and {@code vector.fuse} (issue #4072).
   *
   * @param record the record to read from; {@code null} returns {@code null}
   * @param path   the property name or dotted path; {@code null} returns {@code null}
   *
   * @return the leaf value, or {@code null} if any segment of the path resolves to {@code null}
   *         or to a non-traversable intermediate value
   */
  protected static Object readNestedField(final Document record, final String path) {
    if (record == null || path == null || path.isEmpty())
      return null;

    final int firstDot = path.indexOf('.');
    if (firstDot < 0)
      return record.get(path);

    final String[] parts = path.split("\\.");
    Object current = record.get(parts[0]);
    for (int i = 1; i < parts.length && current != null; i++) {
      if (current instanceof Document doc)
        current = doc.get(parts[i]);
      else if (current instanceof Map<?, ?> map)
        current = map.get(parts[i]);
      else
        return null;
    }
    return current;
  }

  /**
   * Parses a {@code filter} option (a list of RIDs / Identifiables / RID-string forms) into a
   * {@link Set} of {@link RID} suitable for the post-filter step in vector neighbour searches.
   * Returns {@code null} for null or empty inputs so callers can distinguish "no filter" from
   * "empty filter" with a single null-check.
   *
   * @param items   the {@code filter} option contents, typically the value of
   *                {@code FunctionOptions.getList("filter")}
   * @param functionName the calling function's SQL name, used to build helpful error messages
   * @param context the command context (used to coerce string-form RIDs through
   *                {@link BasicDatabase#newRID})
   *
   * @return a set of allowed RIDs, or {@code null} when no whitelist should apply
   *
   * @throws CommandSQLParsingException if the list contains a value that cannot be coerced to a RID
   */
  /**
   * Mutable accounting state shared by the three vector functions that apply a post-traversal
   * {@code groupBy} / {@code groupSize} filter. Encapsulates the per-group counter map plus the
   * {@code filledGroups} O(1) early-exit counter so the three call sites do not drift in lockstep.
   * <p>
   * Lifetime is one query: instantiate, call {@link #admit(Object)} per candidate row in rank order,
   * call {@link #isFull(int)} to decide whether the loop can stop, discard.
   */
  protected static final class GroupAdmissionState {
    private final java.util.HashMap<Object, Integer> perGroup = new java.util.HashMap<>();
    private final int                                 limit;
    private final int                                 groupSize;
    private       int                                 filledGroups = 0;

    public GroupAdmissionState(final int limit, final int groupSize) {
      this.limit = limit;
      this.groupSize = groupSize;
    }

    /**
     * Decides whether a candidate row with the given group key should be kept. Side-effects the
     * internal counters when admitting. Returns {@code true} if admitted, {@code false} if the row
     * must be skipped (group already full, or this would open a {@code (limit + 1)}-th group).
     */
    public boolean admit(final Object groupKey) {
      final int existing = perGroup.getOrDefault(groupKey, 0);
      if (existing == 0 && perGroup.size() >= limit)
        return false;
      if (existing >= groupSize)
        return false;
      perGroup.put(groupKey, existing + 1);
      if (existing + 1 == groupSize)
        filledGroups++;
      return true;
    }

    /**
     * Returns {@code true} when {@code limit} groups have all reached {@code groupSize}, signalling
     * the caller to break out of its scoring loop. O(1).
     */
    public boolean isFull() {
      return filledGroups >= limit;
    }
  }

  /**
   * Narrows {@code allowedBucketIds} (the per-type bucket allow-list assembled from
   * {@code DocumentType.getBuckets(false)}) to the partition-pruned subset stashed on the
   * {@link CommandContext} by {@code SelectExecutionPlanner.derivePartitionPrunedClusters}.
   * <p>
   * Returns the input unchanged when the planner did not stash a hint, when the hint was
   * derived from a different FROM type than the function's target, or when the hint is empty.
   * Builds a fresh {@link IntHashSet} (rather than mutating the input) so concurrent function
   * calls in the same projection do not stomp on each other's narrowed view.
   * <p>
   * Net effect: a query like {@code SELECT vector.neighbors('Doc[embedding]', ..., k) FROM Doc
   * WHERE tenant_id = 'X'} on a {@code partitioned('tenant_id')} type only enumerates the
   * vector sub-index for {@code X}'s bucket instead of the full N-bucket fan-out. Issue #4087
   * follow-up.
   */
  protected static IntHashSet narrowAllowedBucketIdsByPartitionHint(final IntHashSet allowedBucketIds,
      final String typeName, final CommandContext context) {
    if (allowedBucketIds == null || allowedBucketIds.isEmpty() || typeName == null || context == null)
      return allowedBucketIds;
    final Object hintTypeName = context.getVariable(CommandContext.PARTITION_PRUNED_TYPE_NAME_VAR);
    if (!(hintTypeName instanceof String hintType) || !hintType.equals(typeName))
      return allowedBucketIds;
    final Object hintIds = context.getVariable(CommandContext.PARTITION_PRUNED_BUCKET_FILE_IDS_VAR);
    if (!(hintIds instanceof IntHashSet hintSet) || hintSet.isEmpty())
      return allowedBucketIds;
    final IntHashSet narrowed = new IntHashSet(Math.min(allowedBucketIds.size(), hintSet.size()));
    allowedBucketIds.forEach(id -> {
      if (hintSet.contains(id))
        narrowed.add(id);
    });
    return narrowed;
  }

  /**
   * Reads the {@code @rid} of a candidate row produced by an upstream sparse / dense / fuse
   * vector pipeline. Accepts the three shapes those pipelines emit interchangeably:
   * a {@link Map} with an {@code @rid} or {@code rid} entry, a {@link com.arcadedb.query.sql.executor.Result}
   * with the same fields or with {@code Result#getIdentity()} set, or a bare {@link Identifiable}.
   * Returns {@code null} when no recognisable RID is present so callers can skip the row instead of
   * throwing on an unfamiliar shape.
   * <p>
   * Centralised here so the four reranker functions ({@code vector.mmr}, {@code vector.recommend},
   * {@code vector.discover}, {@code vector.rerank} - and {@code vector.boost} via its source-row
   * iteration) share one canonical implementation. Diverging copies in earlier patches drifted
   * silently (e.g. one variant did not check {@code Result#getIdentity()}); a single helper makes
   * those drifts impossible.
   */
  protected static RID extractRidFromRow(final Object row) {
    if (row == null)
      return null;
    if (row instanceof Map<?, ?> m) {
      Object v = m.get("@rid");
      if (v == null) v = m.get("rid");
      if (v instanceof RID r) return r;
      if (v instanceof Identifiable id) return id.getIdentity();
      return null;
    }
    if (row instanceof com.arcadedb.query.sql.executor.Result r) {
      if (r.getIdentity().isPresent())
        return r.getIdentity().get();
      Object v = r.getProperty("@rid");
      if (v == null) v = r.getProperty("rid");
      if (v instanceof RID rid) return rid;
      if (v instanceof Identifiable id) return id.getIdentity();
      return null;
    }
    if (row instanceof Identifiable id)
      return id.getIdentity();
    return null;
  }

  /**
   * Reads a candidate row's similarity score, with the same auto-flip rules as
   * {@link SQLFunctionVectorFuse#extractScore}: prefer {@code score} or {@code $score}
   * (similarity-shaped, higher = better), fall back to the negated {@code distance} field that
   * {@code vector.neighbors} emits (distance-shaped, lower = better - we flip the sign so the
   * rest of the pipeline can assume one direction). Returns {@link Float#NaN} when no recognisable
   * score field is present so callers can either skip the row or treat NaN as "missing score".
   * <p>
   * Critical: every reranker function that consumes vector function output MUST go through this
   * helper, not its own variant. A bare-{@code score}-only extractor will silently drop every row
   * piped in from {@code vector.neighbors} (which emits {@code distance}) - exactly the
   * silent-data-loss bug the consolidation was meant to prevent.
   */
  protected static float extractScoreFromRow(final Object row) {
    if (row instanceof Map<?, ?> m) {
      final Object score = m.get("score");
      if (score instanceof Number n) return n.floatValue();
      final Object dollar = m.get("$score");
      if (dollar instanceof Number n) return n.floatValue();
      final Object distance = m.get("distance");
      if (distance instanceof Number n) return -n.floatValue();
      return Float.NaN;
    }
    if (row instanceof com.arcadedb.query.sql.executor.Result r) {
      final Object score = r.getProperty("score");
      if (score instanceof Number n) return n.floatValue();
      final Object dollar = r.getProperty("$score");
      if (dollar instanceof Number n) return n.floatValue();
      final Object distance = r.getProperty("distance");
      if (distance instanceof Number n) return -n.floatValue();
      return Float.NaN;
    }
    return Float.NaN;
  }

  protected static Set<RID> parseRidFilter(final List<?> items, final String functionName, final CommandContext context) {
    if (items == null || items.isEmpty())
      return null;

    final BasicDatabase db = context.getDatabase();
    final Set<RID> out = new HashSet<>(items.size());
    for (final Object item : items) {
      if (item == null)
        continue;
      if (item instanceof RID rid)
        out.add(rid);
      else if (item instanceof Identifiable id)
        out.add(id.getIdentity());
      else if (item instanceof String s)
        out.add(db.newRID(s));
      else
        throw new CommandSQLParsingException(
            "Option 'filter' for function '" + functionName + "' must contain RIDs, got: "
                + item.getClass().getSimpleName());
    }
    return out;
  }
}
