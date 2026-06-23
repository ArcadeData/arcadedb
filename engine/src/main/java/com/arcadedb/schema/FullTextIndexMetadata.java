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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata class for full-text indexes, storing Lucene analyzer configuration.
 * <p>
 * Supports configuring:
 * <ul>
 *   <li>Default analyzer for all fields</li>
 *   <li>Separate analyzers for indexing and querying</li>
 *   <li>Per-field analyzer overrides</li>
 *   <li>Query parser options (leading wildcard, default operator)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class FullTextIndexMetadata extends IndexMetadata {

  /**
   * Default analyzer class - Lucene's StandardAnalyzer.
   */
  public static final String DEFAULT_ANALYZER = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  /**
   * BM25 similarity: ranks with term-frequency, inverse document frequency and document-length normalization. Default for newly
   * created full-text indexes.
   */
  public static final String SIMILARITY_BM25 = "BM25";

  /**
   * Legacy term-coordination (match-count) similarity. Default for indexes created before BM25 support, preserving their ranking.
   */
  public static final String SIMILARITY_CLASSIC = "CLASSIC";

  private static final String ANALYZER_SUFFIX = "_analyzer";
  private static final String BOOST_SUFFIX    = "_boost";

  private          String              analyzerClass        = DEFAULT_ANALYZER;
  private          String              indexAnalyzerClass   = null;
  private          String              queryAnalyzerClass   = null;
  private          boolean             allowLeadingWildcard = false;
  private          String              defaultOperator      = "OR";
  // Per-field maps are concurrent: they are populated at index creation but read on the query path (getFieldBoost,
  // getAnalyzerClass) and iterated by writeToJSON on schema save, so a HashMap could throw ConcurrentModificationException.
  private final    Map<String, String> fieldAnalyzers       = new ConcurrentHashMap<>();

  // BM25 SCORING CONFIGURATION
  private String             similarity  = SIMILARITY_BM25;
  private float              bm25K1      = 1.2f;
  private float              bm25B       = 0.75f;
  private final Map<String, Float> fieldBoosts = new ConcurrentHashMap<>();

  // PERSISTED CORPUS STATISTICS FOR avgdl (live document count and sum of document lengths).
  // NOTE (concurrency): concurrent transactions can index documents into the same bucket simultaneously, all mutating this
  // shared per-bucket metadata, so the counters are AtomicLong (and countersValid is volatile) - bare longs would lose updates.
  private final AtomicLong totalDocs     = new AtomicLong(0L);
  private final AtomicLong sumDocLength  = new AtomicLong(0L);
  private volatile boolean countersValid = false;
  // Not persisted (no toJSON/fromJSON): whether the persisted counters have already been checked for staleness against the live
  // data this session. Persisted counters can lag the on-disk data if documents were indexed after the last schema save, so the
  // first BM25 query validates them once (cheap live count) and rebuilds only if they disagree. AtomicBoolean (with CAS) so that
  // concurrent first-queries across the type's shared bucket indexes do not all run the validation/rescan.
  // Intentionally never reset to false after being claimed: the check is a once-per-session guard, not a continuous monitor. The
  // recovery path for counters that are badly stale within a running session (e.g. a heavy rollback burst) is an explicit
  // recomputeBM25Counters() / index rebuild, which also re-marks them consistent.
  private final AtomicBoolean staleChecked = new AtomicBoolean(false);

  /**
   * Creates a new FullTextIndexMetadata instance.
   *
   * @param typeName      the name of the type this index belongs to
   * @param propertyNames the property names indexed
   * @param bucketId      the associated bucket ID
   */
  public FullTextIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);

    if (metadata.has("analyzer"))
      this.analyzerClass = metadata.getString("analyzer");

    if (metadata.has("index_analyzer"))
      this.indexAnalyzerClass = metadata.getString("index_analyzer");

    if (metadata.has("query_analyzer"))
      this.queryAnalyzerClass = metadata.getString("query_analyzer");

    if (metadata.has("allowLeadingWildcard"))
      this.allowLeadingWildcard = metadata.getBoolean("allowLeadingWildcard");

    if (metadata.has("defaultOperator"))
      this.defaultOperator = metadata.getString("defaultOperator");

    // An index persisted before BM25 support has no "similarity" key: keep it on CLASSIC so an upgrade does not silently change
    // ranking. Route an explicit value through the validating setter so an unknown similarity in METADATA {...} is rejected.
    if (metadata.has("similarity"))
      setSimilarity(metadata.getString("similarity"));
    else
      this.similarity = SIMILARITY_CLASSIC;
    // Route through the setters so invalid k1/b in METADATA {...} are rejected at index creation rather than silently scoring wrong.
    setBm25K1(metadata.getFloat("bm25_k1", bm25K1));
    setBm25B(metadata.getFloat("bm25_b", bm25B));
    this.totalDocs.set(metadata.getLong("ft_totalDocs", 0L));
    this.sumDocLength.set(metadata.getLong("ft_sumDocLength", 0L));
    this.countersValid = metadata.getBoolean("ft_countersValid", false);

    // Parse per-field analyzers (pattern: *_analyzer) and per-field boosts (pattern: *_boost)
    for (final String key : metadata.keySet()) {
      if (key.endsWith(ANALYZER_SUFFIX) && !"analyzer".equals(key) && !"index_analyzer".equals(key) && !"query_analyzer".equals(key)) {
        final String fieldName = key.substring(0, key.length() - ANALYZER_SUFFIX.length());
        this.fieldAnalyzers.put(fieldName, metadata.getString(key));
      } else if (key.endsWith(BOOST_SUFFIX)) {
        final String fieldName = key.substring(0, key.length() - BOOST_SUFFIX.length());
        this.fieldBoosts.put(fieldName, metadata.getFloat(key, 1.0f));
      }
    }
  }

  /**
   * Writes the full-text-specific configuration and persisted statistics into the given JSON object, which already carries the
   * common index keys (type, bucket, properties...). Only non-default values are emitted to keep the schema compact, except the
   * corpus counters which are always written when valid.
   *
   * @param metadata the JSON object to populate
   *
   * @return the same JSON object, for chaining
   */
  public JSONObject writeToJSON(final JSONObject metadata) {
    if (!DEFAULT_ANALYZER.equals(analyzerClass))
      metadata.put("analyzer", analyzerClass);
    if (indexAnalyzerClass != null)
      metadata.put("index_analyzer", indexAnalyzerClass);
    if (queryAnalyzerClass != null)
      metadata.put("query_analyzer", queryAnalyzerClass);
    if (allowLeadingWildcard)
      metadata.put("allowLeadingWildcard", true);
    if (!"OR".equalsIgnoreCase(defaultOperator))
      metadata.put("defaultOperator", defaultOperator);

    for (final Map.Entry<String, String> entry : fieldAnalyzers.entrySet())
      metadata.put(entry.getKey() + ANALYZER_SUFFIX, entry.getValue());

    metadata.put("similarity", similarity);
    if (isBM25()) {
      metadata.put("bm25_k1", bm25K1);
      metadata.put("bm25_b", bm25B);
      for (final Map.Entry<String, Float> entry : fieldBoosts.entrySet())
        metadata.put(entry.getKey() + BOOST_SUFFIX, entry.getValue());
    }

    if (countersValid) {
      metadata.put("ft_totalDocs", totalDocs.get());
      metadata.put("ft_sumDocLength", sumDocLength.get());
      metadata.put("ft_countersValid", true);
    }
    return metadata;
  }

  /**
   * Returns the default analyzer class.
   *
   * @return the analyzer class name
   */
  public String getAnalyzerClass() {
    return analyzerClass;
  }

  /**
   * Returns the analyzer class for a specific field.
   * If a field-specific analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @param fieldName the field name
   * @return the analyzer class name for the field
   */
  public String getAnalyzerClass(final String fieldName) {
    return fieldAnalyzers.getOrDefault(fieldName, analyzerClass);
  }

  /**
   * Returns the analyzer class for indexing.
   * If a specific index analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @return the index analyzer class name
   */
  public String getIndexAnalyzerClass() {
    return indexAnalyzerClass != null ? indexAnalyzerClass : analyzerClass;
  }

  /**
   * Returns the analyzer class for querying.
   * If a specific query analyzer is configured, returns that; otherwise returns the default analyzer.
   *
   * @return the query analyzer class name
   */
  public String getQueryAnalyzerClass() {
    return queryAnalyzerClass != null ? queryAnalyzerClass : analyzerClass;
  }

  /**
   * Returns whether leading wildcards are allowed in queries.
   *
   * @return true if leading wildcards are allowed
   */
  public boolean isAllowLeadingWildcard() {
    return allowLeadingWildcard;
  }

  /**
   * Returns the default operator for query parsing.
   *
   * @return "OR" or "AND"
   */
  public String getDefaultOperator() {
    return defaultOperator;
  }

  /**
   * Returns an unmodifiable view of the per-field analyzer map.
   *
   * @return map of field name to analyzer class name
   */
  public Map<String, String> getFieldAnalyzers() {
    return CollectionUtils.immutableMap(fieldAnalyzers);
  }

  /**
   * Sets the default analyzer class.
   *
   * @param analyzerClass the analyzer class name
   */
  public void setAnalyzerClass(final String analyzerClass) {
    this.analyzerClass = analyzerClass;
  }

  /**
   * Sets the index analyzer class.
   *
   * @param indexAnalyzerClass the analyzer class name for indexing, or null to use default
   */
  public void setIndexAnalyzerClass(final String indexAnalyzerClass) {
    this.indexAnalyzerClass = indexAnalyzerClass;
  }

  /**
   * Sets the query analyzer class.
   *
   * @param queryAnalyzerClass the analyzer class name for querying, or null to use default
   */
  public void setQueryAnalyzerClass(final String queryAnalyzerClass) {
    this.queryAnalyzerClass = queryAnalyzerClass;
  }

  /**
   * Sets whether leading wildcards are allowed.
   *
   * @param allowLeadingWildcard true to allow leading wildcards
   */
  public void setAllowLeadingWildcard(final boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  /**
   * Sets the default operator for query parsing.
   *
   * @param defaultOperator "OR" or "AND"
   */
  public void setDefaultOperator(final String defaultOperator) {
    this.defaultOperator = defaultOperator;
  }

  /**
   * Sets an analyzer for a specific field.
   *
   * @param fieldName     the field name
   * @param analyzerClass the analyzer class name
   */
  public void setFieldAnalyzer(final String fieldName, final String analyzerClass) {
    this.fieldAnalyzers.put(fieldName, analyzerClass);
  }

  /**
   * Returns the configured similarity mode ("BM25" or "CLASSIC").
   */
  public String getSimilarity() {
    return similarity;
  }

  /**
   * Sets the similarity mode. Accepts "BM25" or "CLASSIC" (case-insensitive); null resets to the BM25 default.
   *
   * @throws IllegalArgumentException if the name is not a known similarity
   */
  public void setSimilarity(final String similarity) {
    if (similarity == null) {
      this.similarity = SIMILARITY_BM25;
      return;
    }
    final String upper = similarity.toUpperCase();
    if (!SIMILARITY_BM25.equals(upper) && !SIMILARITY_CLASSIC.equals(upper))
      throw new IllegalArgumentException(
          "Unknown full-text similarity '" + similarity + "'. Valid values: " + SIMILARITY_BM25 + ", " + SIMILARITY_CLASSIC);
    this.similarity = upper;
  }

  /**
   * Returns true if this index ranks with BM25 scoring.
   */
  public boolean isBM25() {
    return SIMILARITY_BM25.equalsIgnoreCase(similarity);
  }

  /**
   * Returns the BM25 term-frequency saturation parameter k1.
   */
  public float getBm25K1() {
    return bm25K1;
  }

  /**
   * Sets the BM25 term-frequency saturation parameter k1 (must be &gt;= 0).
   *
   * @throws IllegalArgumentException if k1 is negative
   */
  public void setBm25K1(final float bm25K1) {
    if (bm25K1 < 0)
      throw new IllegalArgumentException("BM25 k1 must be >= 0, but was " + bm25K1);
    this.bm25K1 = bm25K1;
  }

  /**
   * Returns the BM25 document-length normalization parameter b.
   */
  public float getBm25B() {
    return bm25B;
  }

  /**
   * Sets the BM25 document-length normalization parameter b (must be in [0, 1]).
   *
   * @throws IllegalArgumentException if b is outside [0, 1]
   */
  public void setBm25B(final float bm25B) {
    if (bm25B < 0 || bm25B > 1)
      throw new IllegalArgumentException("BM25 b must be in [0, 1], but was " + bm25B);
    this.bm25B = bm25B;
  }

  /**
   * Returns the boost multiplier for a field, or 1.0 when no boost is configured.
   *
   * @param fieldName the field name
   */
  public float getFieldBoost(final String fieldName) {
    return fieldBoosts.getOrDefault(fieldName, 1.0f);
  }

  /**
   * Sets a boost multiplier for a specific field. Boosts greater than 1.0 increase the field's contribution to the BM25 score.
   *
   * @param fieldName the field name
   * @param boost     the multiplier
   */
  public void setFieldBoost(final String fieldName, final float boost) {
    this.fieldBoosts.put(fieldName, boost);
  }

  /**
   * Returns an unmodifiable view of the per-field boost map.
   */
  public Map<String, Float> getFieldBoosts() {
    return CollectionUtils.immutableMap(fieldBoosts);
  }

  /**
   * Returns the persisted live document count used for IDF.
   */
  public long getTotalDocs() {
    return totalDocs.get();
  }

  /**
   * Returns the persisted sum of document lengths used to compute the average document length.
   */
  public long getSumDocLength() {
    return sumDocLength.get();
  }

  /**
   * Returns true when the persisted corpus counters are trustworthy. When false the average document length must be recomputed
   * (e.g. for an index that predates BM25 support).
   */
  public boolean isCountersValid() {
    return countersValid;
  }

  /**
   * Atomically claims the one-per-session staleness check: returns true to exactly one caller (which must then run the
   * live-count validation), false to everyone else. Prevents concurrent first-queries from all rescanning the type.
   */
  public boolean claimStaleCheck() {
    return staleChecked.compareAndSet(false, true);
  }

  /**
   * Marks the persisted corpus counters as valid (or invalid).
   */
  public void setCountersValid(final boolean countersValid) {
    this.countersValid = countersValid;
  }

  /**
   * Sets the persisted corpus counters in one shot, marking them valid.
   *
   * @param totalDocs    live document count
   * @param sumDocLength sum of document lengths
   */
  public void setCounters(final long totalDocs, final long sumDocLength) {
    this.totalDocs.set(totalDocs);
    this.sumDocLength.set(sumDocLength);
    this.countersValid = true;
    this.staleChecked.set(true); // freshly computed counters are by definition consistent with the live data
  }

  /**
   * Records a newly indexed document in the corpus counters. Thread-safe: concurrent indexing transactions may call this on the
   * shared metadata.
   * <p>
   * These counters feed only the average document length (a BM25 length normalizer, robust to small inaccuracies). They are
   * adjusted at index put/remove time, BEFORE the transaction commits, and are NOT reversed on rollback - so a rolled-back batch
   * (or a {@link #removeDocument} whose recomputed length differs from the original, e.g. after an analyzer change) can let the
   * counters drift. The full-text index's {@code recomputeBM25Counters()} rebuilds them exactly when needed.
   *
   * @param docLength number of analyzed tokens of the document
   */
  public void addDocument(final long docLength) {
    totalDocs.incrementAndGet();
    sumDocLength.addAndGet(docLength);
  }

  /**
   * Removes a document from the corpus counters, clamping at zero to stay consistent under at-least-once removals. Thread-safe.
   *
   * @param docLength number of analyzed tokens of the document
   */
  public void removeDocument(final long docLength) {
    totalDocs.updateAndGet(v -> v > 0 ? v - 1 : v);
    sumDocLength.updateAndGet(v -> Math.max(0L, v - docLength));
  }

  /**
   * Returns the average document length across the collection, or 1.0 when no statistics are available.
   * <p>
   * The two counters are read independently (no shared lock), so a concurrent {@link #addDocument}/{@link #removeDocument} can be
   * observed half-applied. The widest skew is one in-flight document: {@code addDocument} bumps {@code totalDocs} before
   * {@code sumDocLength}, so a reader can briefly see {@code n} one too high (or, symmetrically for removal, one too low),
   * yielding an avgdl off by roughly {@code avgdl/n}. With more than a handful of documents this is negligible, and avgdl is only
   * the BM25 length-normalization denominator (further dampened by {@code b}), so a momentary approximation cannot distort
   * ranking materially. An exact value is available on demand via the full-text index's {@code recomputeBM25Counters()}.
   */
  public double avgDocLength() {
    final long n = totalDocs.get();
    return n > 0 ? (double) sumDocLength.get() / n : 1.0;
  }
}
