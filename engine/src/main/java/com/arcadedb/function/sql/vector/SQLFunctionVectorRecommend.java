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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Qdrant-style recommendation API. Given a list of <i>positive</i> example RIDs and an optional
 * list of <i>negative</i> example RIDs, computes the centroid of each set, takes the difference
 * (positive - negative), and runs a K-NN search against the resulting query vector via
 * {@link SQLFunctionVectorNeighbors}.
 * <p>
 * Implements Qdrant's {@code average_vector} strategy:
 * <pre>
 *   q = mean(positive_vectors) - mean(negative_vectors)
 *   return KNN(q, k)
 * </pre>
 * The product use case is "find more like X and Y, but unlike Z" - common in recsys workloads
 * where the application has product-side IDs in hand instead of an explicit query vector.
 * Without this function callers had to fetch each example's embedding manually, average them
 * client-side, and pass the result to {@code vector.neighbors}; this function does the same
 * thing server-side in one round-trip.
 * <p>
 * Usage:
 * <pre>
 *   SELECT expand(`vector.recommend`('Doc[embedding]',
 *       [#1:1, #1:2],   -- positives (required, non-empty)
 *       [#1:9],         -- negatives (optional, may be empty / null)
 *       10[, options]))
 * </pre>
 * Options are forwarded to {@code vector.neighbors}: {@code efSearch}, {@code groupBy},
 * {@code groupSize}. The {@code filter} option is intentionally <b>not</b> forwarded -
 * {@code vector.neighbors}'s filter is an allow-list, while what recommendation actually wants
 * is the inverse (an exclusion of the example RIDs themselves). Examples are excluded by
 * post-filtering the result set; an explicit filter would compose unsafely with that.
 * <p>
 * Vector source: each example RID's record must carry the same property the index was built on
 * (parsed from the {@code Type[property]} index spec). Records that were deleted between
 * upstream selection and this call are silently dropped; if every positive is dropped, the
 * function returns an empty list rather than running a degenerate KNN with a zero query vector.
 * Embedding dimension mismatch across examples throws.
 * <p>
 * <b>Zero-vector edge case.</b> When the positive and negative centroids cancel out (e.g. the
 * surviving positives and negatives mean to the same point in the embedding space, or the
 * negatives are exactly the additive inverse of the positives), the resulting query vector is
 * zero or near-zero. KNN against a zero vector is well-defined for {@code COSINE} similarity
 * (every candidate has the same similarity, so the order is unstable) but practically useless;
 * the function does not currently detect this case. Callers worried about it should check the
 * dispersion of their positives/negatives upstream, or use {@code vector.discover} (which scores
 * by per-pair margin sum and so degrades gracefully when the centroid signal cancels).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorRecommend extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.recommend";

  /** Forwarded to {@code vector.neighbors}; {@code filter} is intentionally excluded. */
  private static final Set<String> FORWARDED_OPTIONS = Set.of("efSearch", "groupBy", "groupSize");

  public SQLFunctionVectorRecommend() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 4 || params.length > 5)
      throw new CommandSQLParsingException(getSyntax());

    final String indexSpec = params[0].toString();

    final List<RID> positives = parseRidList(params[1], "positives");
    if (positives.isEmpty())
      throw new CommandSQLParsingException(NAME + " requires at least one positive example RID");

    final List<RID> negatives = parseRidList(params[2], "negatives");

    final int k = params[3] instanceof Number n ? n.intValue() : Integer.parseInt(params[3].toString());
    if (k <= 0)
      return new ArrayList<>(0);

    // Index spec must be in {@code Type[property]} form so we know which property to read off
    // each example record. Bare-index-name form (which {@code vector.neighbors} also accepts) is
    // rejected because it does not name a property.
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart < 0 || !indexSpec.endsWith("]"))
      throw new CommandSQLParsingException(
          NAME + " indexSpec must be in 'Type[property]' form, got: " + indexSpec);
    final String propertyName = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);

    final BasicDatabase db = context.getDatabase();
    final float[] posCentroid = centroidFromRids(db, positives, propertyName, "positives");
    if (posCentroid == null)
      // Every positive's record was deleted between selection and this call. KNN against a zero
      // vector is well-defined for cosine but produces noise; returning empty is the contract.
      return new ArrayList<>(0);

    final float[] negCentroid = negatives.isEmpty() ? null : centroidFromRids(db, negatives, propertyName, "negatives");
    if (negCentroid != null && posCentroid.length != negCentroid.length)
      throw new CommandSQLParsingException(
          NAME + " positive/negative centroid dimensions mismatch: " + posCentroid.length + " vs " + negCentroid.length);

    final float[] queryVector;
    if (negCentroid != null) {
      queryVector = new float[posCentroid.length];
      for (int i = 0; i < posCentroid.length; i++)
        queryVector[i] = posCentroid[i] - negCentroid[i];
    } else {
      queryVector = posCentroid;
    }

    // Over-fetch by the example count so post-filtering still has K rows once examples are
    // dropped. Worst case every example sits in the top-K window of the unfiltered result; the
    // over-fetch covers that exactly.
    final int totalExamples = positives.size() + negatives.size();
    final int fetchK = k + totalExamples;

    final LinkedHashMap<String, Object> innerOpts = buildInnerOptions(params.length == 5 ? params[4] : null);

    final SQLFunctionVectorNeighbors neighbors = new SQLFunctionVectorNeighbors();
    final Object rawResult = neighbors.execute(self, currentRecord, currentResult,
        new Object[] { indexSpec, queryVector, fetchK, innerOpts },
        context);
    if (!(rawResult instanceof List<?> rawList))
      return rawResult;

    // Post-filter: drop the example RIDs and truncate to k. {@code vector.neighbors} returns a
    // list of LinkedHashMaps with an {@code @rid} entry; we read it directly rather than
    // resolving the record again to keep this hop cheap.
    final HashSet<RID> exclude = new HashSet<>(totalExamples);
    exclude.addAll(positives);
    exclude.addAll(negatives);

    final List<Object> filtered = new ArrayList<>(Math.min(k, rawList.size()));
    for (final Object row : rawList) {
      if (filtered.size() >= k)
        break;
      if (row instanceof Map<?, ?> m) {
        // RID extends Identifiable, so a single instanceof check covers both the
        // {@code RID r -> exclude.contains(r)} fast path and the rarer
        // {@code Identifiable id -> exclude.contains(id.getIdentity())} indirection. The
        // earlier two-branch version had unreachable second branch on a {@code RID} value.
        if (m.get("@rid") instanceof Identifiable id && exclude.contains(id.getIdentity()))
          continue;
      }
      filtered.add(row);
    }
    return filtered;
  }

  public String getSyntax() {
    // {@code indexSpec} must be in {@code Type[property]} form (NOT a bare index name like
    // {@code vector.neighbors} accepts) because the function needs to know which property to read
    // off each example record. The inconsistency with vector.neighbors's bare-name form is
    // deliberate: recommend has a hard requirement on the property name.
    return NAME + "(<Type[property]>, <positiveRids>, <negativeRids>, <k>[, options])";
  }

  /**
   * Parses a parameter into a list of RIDs. Accepts a single RID, an array, an iterable, or a
   * RID-shaped string. Null returns an empty list. {@code role} is only used in error messages.
   */
  private static List<RID> parseRidList(final Object raw, final String role) {
    if (raw == null)
      return Collections.emptyList();
    final List<RID> out = new ArrayList<>();
    if (raw instanceof RID rid) {
      out.add(rid);
    } else if (raw instanceof Identifiable id) {
      out.add(id.getIdentity());
    } else if (raw instanceof Iterable<?> iter) {
      for (final Object o : iter)
        addOne(out, o, role);
    } else if (raw instanceof Object[] arr) {
      // Java-API callers may hand in an Object[]; the SQL parser produces List/Iterable so the
      // primitive-array reflection path the previous version had is unreachable in practice.
      // Direct cast + loop is faster and avoids the reflection import.
      for (final Object o : arr)
        addOne(out, o, role);
    } else if (raw instanceof String s) {
      out.add(parseStringAsRid(s, role));
    } else {
      throw new CommandSQLParsingException(NAME + " " + role + " must be a RID, list of RIDs, or array, got: "
          + raw.getClass().getSimpleName());
    }
    return out;
  }

  private static void addOne(final List<RID> out, final Object o, final String role) {
    if (o == null)
      return;
    if (o instanceof RID rid)
      out.add(rid);
    else if (o instanceof Identifiable id)
      out.add(id.getIdentity());
    else if (o instanceof String s)
      out.add(parseStringAsRid(s, role));
    else
      throw new CommandSQLParsingException(
          NAME + " " + role + " entry must be a RID or RID-string, got: " + o.getClass().getSimpleName());
  }

  private static RID parseStringAsRid(final String s, final String role) {
    try {
      return new RID(s);
    } catch (final Exception e) {
      throw new CommandSQLParsingException(NAME + " " + role + " entry '" + s + "' is not a valid RID");
    }
  }

  /**
   * Looks up each RID, reads the embedding property, and returns the element-wise mean. Records
   * that fail to look up (deleted between selection and this call) or are missing the property
   * are silently skipped. Returns {@code null} when every RID was dropped - the caller decides
   * whether that condition produces empty results (positives) or falls through (negatives).
   */
  private float[] centroidFromRids(final BasicDatabase db, final List<RID> rids, final String propertyName,
      final String role) {
    if (rids.isEmpty())
      return null;
    float[] sum = null;
    int count = 0;
    int expectedDim = -1;
    for (final RID rid : rids) {
      final Document rec;
      try {
        rec = (Document) db.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        continue;
      }
      final Object raw = rec.get(propertyName);
      if (raw == null)
        continue;
      final float[] v = toFloatArray(raw);
      if (expectedDim == -1) {
        expectedDim = v.length;
        sum = new float[expectedDim];
      } else if (v.length != expectedDim) {
        throw new CommandSQLParsingException(NAME + " " + role + " example " + rid
            + " has embedding dimension " + v.length + ", expected " + expectedDim);
      }
      for (int i = 0; i < expectedDim; i++)
        sum[i] += v[i];
      count++;
    }
    if (count == 0 || sum == null)
      return null;
    final float[] mean = new float[expectedDim];
    for (int i = 0; i < expectedDim; i++)
      mean[i] = sum[i] / count;
    return mean;
  }

  /**
   * Forwards only the options {@code vector.neighbors} understands ({@code efSearch},
   * {@code groupBy}, {@code groupSize}) and rejects everything else with a clear message. The
   * {@code filter} option is intentionally rejected too: {@code vector.neighbors} treats it as an
   * allow-list, but recommendation needs the inverse (the example RIDs must NOT appear in the
   * result), which is handled by the post-filter step in {@link #execute}. Surfacing a typo
   * (e.g. {@code efSerach} for {@code efSearch}) as a parse error catches a class of silent-bug
   * that the pre-fix version dropped on the floor.
   */
  private static LinkedHashMap<String, Object> buildInnerOptions(final Object userOptions) {
    final LinkedHashMap<String, Object> out = new LinkedHashMap<>();
    if (userOptions instanceof Map<?, ?> userMap) {
      for (final var e : userMap.entrySet()) {
        final String key = String.valueOf(e.getKey());
        if (FORWARDED_OPTIONS.contains(key)) {
          out.put(key, e.getValue());
        } else if ("filter".equals(key)) {
          throw new CommandSQLParsingException(
              NAME + " does not accept the 'filter' option (the example RIDs are excluded "
                  + "automatically; use a separate WHERE on the outer query if you need additional "
                  + "exclusions)");
        } else {
          throw new CommandSQLParsingException(
              NAME + " does not recognise option '" + key + "'. Allowed: " + FORWARDED_OPTIONS);
        }
      }
    }
    return out;
  }
}
