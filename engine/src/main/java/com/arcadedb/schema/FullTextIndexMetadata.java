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
import java.util.Set;
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

  // These mirror BM25Scorer.DEFAULT_K1 / DEFAULT_B by value. They are intentionally not a reference to that class: BM25Scorer
  // lives in com.arcadedb.index.fulltext and depends on schema, not the other way round, so referencing it here would invert the
  // package dependency. Keep the two pairs in sync if either ever changes.
  /** Default BM25 term-frequency saturation parameter. */
  public static final float  DEFAULT_BM25_K1 = 1.2f;
  /** Default BM25 document-length normalization parameter. */
  public static final float  DEFAULT_BM25_B  = 0.75f;

  private static final String ANALYZER_SUFFIX = "_analyzer";
  private static final String BOOST_SUFFIX    = "_boost";

  /**
   * Fixed keys a user may write in {@code METADATA}. The key space is not closed: {@code <field>_analyzer} and
   * {@code <field>_boost} are per-property, so {@link #isUserMetadataKey(String)} recognises those by shape. The
   * persisted corpus counters ({@code ft_*}) are deliberately absent - they are maintained by the index, not configured.
   */
  private static final Set<String> USER_METADATA_KEYS = Set.of("analyzer", "index_analyzer", "query_analyzer",
      "allowLeadingWildcard", "defaultOperator", "similarity", "bm25_k1", "bm25_b");

  // These scalar fields (analyzers, operator, wildcard flag, similarity, k1/b) are set once at index creation / schema load,
  // before any query-path read, so they need no volatile/synchronization (unlike the live corpus counters and the per-field
  // maps below, which are mutated after construction). Do not "fix" them by adding volatile - the publication is via the
  // happens-before of schema load completing before the index serves queries.
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
  private float              bm25K1      = DEFAULT_BM25_K1;
  private float              bm25B       = DEFAULT_BM25_B;
  private final Map<String, Float> fieldBoosts = new ConcurrentHashMap<>();

  // PERSISTED TYPE-WIDE CORPUS STATISTICS (live document count and sum of document lengths).
  // NOTE (concurrency): every bucket index of a logical TypeIndex shares this metadata instance, so concurrent transactions can
  // update it from different buckets. The counters are AtomicLong (and countersValid is volatile) so bare longs cannot lose
  // updates.
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
   * Creates a new FullTextIndexMetadata instance. Defaults to BM25 similarity (see the {@code similarity} field); the
   * {@link #defaultBM25} factory is just a self-documenting alias of this constructor for the "new index" call site.
   *
   * @param typeName      the name of the type this index belongs to
   * @param propertyNames the property names indexed
   * @param bucketId      the associated bucket ID
   */
  public FullTextIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  /**
   * Creates a metadata instance carrying the BM25 defaults, for a new full-text index created without explicit metadata so it
   * ranks with BM25 out of the box. The constructor already defaults to BM25; this named factory makes that intent explicit at
   * the call site.
   */
  public static FullTextIndexMetadata defaultBM25(final String typeName, final String[] propertyNames, final int bucketId) {
    return new FullTextIndexMetadata(typeName, propertyNames, bucketId);
  }

  /**
   * Carries the analyzers, the query-parser options and the BM25 configuration over, per-field entries included: those
   * are keyed by property name, which a copy keeps.
   * <p>
   * The corpus counters are NOT copied. They describe the documents indexed so far, and the copy is about to index a
   * different set - starting from none - so inheriting them would skew every BM25 score until something recomputed
   * them. A fresh instance starts at {@code countersValid == false}, which is the state that makes the first query
   * validate them against the live data.
   */
  @Override
  public FullTextIndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    final FullTextIndexMetadata copy = copyCommonTo(new FullTextIndexMetadata(typeName, propertyNames, bucketId));
    copy.analyzerClass = analyzerClass;
    copy.indexAnalyzerClass = indexAnalyzerClass;
    copy.queryAnalyzerClass = queryAnalyzerClass;
    copy.allowLeadingWildcard = allowLeadingWildcard;
    copy.defaultOperator = defaultOperator;
    copy.fieldAnalyzers.putAll(fieldAnalyzers);
    copy.similarity = similarity;
    copy.bm25K1 = bm25K1;
    copy.bm25B = bm25B;
    copy.fieldBoosts.putAll(fieldBoosts);
    return copy;
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
    // Route through the setters so invalid k1/b in METADATA {...} are rejected at index creation rather than silently scoring
    // wrong. Default to the DEFAULT_BM25_* constants (not the current field values) so a key absent from the JSON resets to the
    // default rather than carrying a stale value forward if fromJSON is ever called on a recycled instance.
    setBm25K1(metadata.getFloat("bm25_k1", DEFAULT_BM25_K1));
    setBm25B(metadata.getFloat("bm25_b", DEFAULT_BM25_B));
    // Restore the counters by setting the fields directly, NOT via setCounters(): setCounters() consumes the once-per-session
    // stale-check (staleChecked=true), which on a load path would suppress the first-query validation that self-heals counters
    // that lag the on-disk data. Keep this direct so the stale check still runs after a restart.
    this.totalDocs.set(metadata.getLong("ft_totalDocs", 0L));
    this.sumDocLength.set(metadata.getLong("ft_sumDocLength", 0L));
    this.countersValid = metadata.getBoolean("ft_countersValid", false);

    // Parse per-field analyzers (pattern: *_analyzer) and per-field boosts (pattern: *_boost). Clear first so a fromJSON() on an
    // already-populated instance replaces the per-field config rather than merging stale entries into it (these maps are now
    // final/ConcurrentHashMap, so they are no longer swapped out wholesale).
    this.fieldAnalyzers.clear();
    this.fieldBoosts.clear();
    for (final String key : metadata.keySet()) {
      if (key.endsWith(ANALYZER_SUFFIX) && !"analyzer".equals(key) && !"index_analyzer".equals(key) && !"query_analyzer".equals(key)) {
        final String fieldName = key.substring(0, key.length() - ANALYZER_SUFFIX.length());
        this.fieldAnalyzers.put(fieldName, metadata.getString(key));
      } else if (key.endsWith(BOOST_SUFFIX)) {
        final String fieldName = key.substring(0, key.length() - BOOST_SUFFIX.length());
        // Route through the setter so a boost supplied via METADATA {...} gets the same >= 0 validation as the builder path.
        setFieldBoost(fieldName, metadata.getFloat(key, 1.0f));
      }
    }
  }

  @Override
  public Set<String> getUserMetadataKeys() {
    return USER_METADATA_KEYS;
  }

  /**
   * Recognises the per-field keys by shape; {@link #applyUserMetadata} then checks that the field they name is one this
   * index covers, so a typo is reported rather than stored against a property that will never match.
   * <p>
   * Two consequences of a suffix-based key space, both inherent and neither worth a different scheme: a property named
   * {@code index} or {@code query} cannot take a per-field analyzer, because {@code index_analyzer} and
   * {@code query_analyzer} are the reserved keys for the indexing and querying analyzers; and a property whose own name
   * ends in {@code _analyzer} or {@code _boost} would be read as a per-field key for the shorter name. Both are
   * ambiguities of the notation, not of the reader.
   */
  @Override
  protected boolean isUserMetadataKey(final String key) {
    return USER_METADATA_KEYS.contains(key) || key.endsWith(ANALYZER_SUFFIX) || key.endsWith(BOOST_SUFFIX);
  }

  @Override
  protected String describeUserMetadataKeys() {
    return super.describeUserMetadataKeys() + ", <field>" + ANALYZER_SUFFIX + ", <field>" + BOOST_SUFFIX;
  }

  /**
   * Applies the {@code METADATA} clause of {@code CREATE INDEX}. Deliberately not {@link #fromJSON(JSONObject)}: that
   * method reads a PERSISTED definition, where a missing {@code similarity} means an index written before BM25 support
   * and therefore CLASSIC ranking. Here a missing key just means the user did not ask for anything, so an index created
   * with a METADATA clause must keep the same BM25 default as one created without it - it used to silently drop to
   * CLASSIC (issue #5639).
   */
  @Override
  protected void applyUserMetadata(final JSONObject json) {
    if (json.has("analyzer"))
      this.analyzerClass = json.getString("analyzer");

    if (json.has("index_analyzer"))
      this.indexAnalyzerClass = json.getString("index_analyzer");

    if (json.has("query_analyzer"))
      this.queryAnalyzerClass = json.getString("query_analyzer");

    if (json.has("allowLeadingWildcard"))
      this.allowLeadingWildcard = metadataBoolean(json, "allowLeadingWildcard");

    // Through the validating setter: the query parser understands only AND and OR, so anything else was accepted here
    // and then quietly behaved as OR - the last silent-accept left on the full-text clause (issue #5639).
    if (json.has("defaultOperator"))
      setDefaultOperator(json.getString("defaultOperator"));

    // Route through the validating setters so an unknown similarity or an out-of-range k1/b in METADATA {...} is
    // reported at creation rather than silently scoring wrong.
    if (json.has("similarity"))
      setSimilarity(json.getString("similarity"));

    if (json.has("bm25_k1"))
      setBm25K1(metadataFloat(json, "bm25_k1"));

    if (json.has("bm25_b"))
      setBm25B(metadataFloat(json, "bm25_b"));

    for (final String key : json.keySet())
      if (key.endsWith(ANALYZER_SUFFIX) && !USER_METADATA_KEYS.contains(key))
        setFieldAnalyzer(checkIndexedField(key, ANALYZER_SUFFIX), json.getString(key));
      else if (key.endsWith(BOOST_SUFFIX))
        setFieldBoost(checkIndexedField(key, BOOST_SUFFIX), metadataFloat(json, key));
  }

  /**
   * The per-field entries answer from the map rather than from {@code getAnalyzerClass(field)} / the boost getter, which
   * fall back to the index-wide default: a request naming {@code title_analyzer} asks for an analyzer configured FOR
   * that field, and an index that merely inherits the default one does not provide it.
   */
  @Override
  protected Object getUserMetadataValue(final String key) {
    return switch (key) {
      case "analyzer" -> analyzerClass;
      case "index_analyzer" -> indexAnalyzerClass;
      case "query_analyzer" -> queryAnalyzerClass;
      case "allowLeadingWildcard" -> allowLeadingWildcard;
      case "defaultOperator" -> defaultOperator;
      case "similarity" -> similarity;
      case "bm25_k1" -> bm25K1;
      case "bm25_b" -> bm25B;
      default -> {
        if (key.endsWith(ANALYZER_SUFFIX))
          yield fieldAnalyzers.get(key.substring(0, key.length() - ANALYZER_SUFFIX.length()));
        if (key.endsWith(BOOST_SUFFIX))
          yield fieldBoosts.get(key.substring(0, key.length() - BOOST_SUFFIX.length()));
        yield null;
      }
    };
  }

  /**
   * Returns the field name a per-field {@code METADATA} key configures, having checked that this index covers it.
   * <p>
   * An analyzer or a boost for a property the index does not cover is dead configuration: only an indexed field is
   * analyzed, and a boost applies to field-qualified matches, which only an indexed field can produce. Accepting
   * {@code titel_boost} for an index on {@code title} would therefore be the same silently-dropped setting this reader
   * exists to report (issue #5639).
   * <p>
   * Only the user clause is checked. {@link #fromJSON} restores a persisted definition and stays tolerant: an index
   * whose property list changed must still open, and refusing a stale per-field entry there would cost a database its
   * availability to report a setting that is merely inert. The check is also skipped when the property list is not
   * known yet, since there would be nothing to check against.
   */
  private String checkIndexedField(final String key, final String suffix) {
    final String fieldName = key.substring(0, key.length() - suffix.length());
    if (propertyNames != null && !propertyNames.isEmpty() && !propertyNames.contains(fieldName))
      throw new IllegalArgumentException("Full-text metadata key '" + key + "' names '" + fieldName
          + "', which is not one of the indexed properties " + propertyNames
          + ": a per-field analyzer or boost only applies to a property the index covers");
    return fieldName;
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
      // Emit k1/b only when tuned away from the defaults (read back as the defaults when absent), keeping the schema JSON terse.
      // Use an epsilon (not !=): 1.2f/0.75f are not exact in float32, so a value that round-tripped through JSON parsing could
      // differ from the literal by an ULP and be persisted as "non-default" forever.
      if (Math.abs(bm25K1 - DEFAULT_BM25_K1) > 1e-6f)
        metadata.put("bm25_k1", bm25K1);
      if (Math.abs(bm25B - DEFAULT_BM25_B) > 1e-6f)
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
   * Sets the default operator for query parsing: {@code "OR"} (default) or {@code "AND"}, case-insensitive.
   * <p>
   * Validated because the query parser recognises nothing else, so an unrecognised operator would silently behave as
   * OR. {@link #fromJSON} deliberately does NOT route through here: it reads a PERSISTED definition, written by this
   * setter in the first place, and refusing a value there would make a database unopenable rather than report a typo.
   *
   * @param defaultOperator "OR" or "AND"
   *
   * @throws IllegalArgumentException if the operator is neither
   */
  public void setDefaultOperator(final String defaultOperator) {
    if (defaultOperator == null || (!"OR".equalsIgnoreCase(defaultOperator.trim()) && !"AND".equalsIgnoreCase(
        defaultOperator.trim())))
      throw new IllegalArgumentException("Full-text defaultOperator must be AND or OR, got: " + defaultOperator);
    this.defaultOperator = defaultOperator.trim().toUpperCase();
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
   * Sets the similarity mode. Accepts "BM25" or "CLASSIC" (case-insensitive).
   *
   * @throws IllegalArgumentException if the name is null or not a known similarity
   */
  public void setSimilarity(final String similarity) {
    if (similarity == null)
      throw new IllegalArgumentException("Full-text similarity cannot be null. Valid values: " + SIMILARITY_BM25 + ", " + SIMILARITY_CLASSIC);
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
   * Sets the BM25 term-frequency saturation parameter k1 (must be &gt;= 0). Higher values let term frequency keep increasing the
   * score (less saturation). Note the edge case {@code k1 = 0}: it is permitted but degenerates BM25 to a pure IDF (binary
   * presence) model - term frequency stops mattering entirely - which is rarely the intent of "a low k1"; use a small positive
   * value (e.g. 0.5) for strong-but-not-total saturation.
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
   * Sets a boost multiplier for a specific field. Boosts greater than 1.0 increase the field's contribution to the BM25 score;
   * 0.0 disables it. Negative boosts are rejected because they would produce negative term contributions and invert ranking.
   *
   * @param fieldName the field name
   * @param boost     the multiplier (must be >= 0)
   */
  public void setFieldBoost(final String fieldName, final float boost) {
    if (boost < 0)
      throw new IllegalArgumentException("BM25 field boost for '" + fieldName + "' must be >= 0, but was " + boost);
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
   * <p>
   * Also marks the once-per-session staleness check as consumed: the counters have just been recomputed from the live data, so
   * they are exact and there is no point re-validating them later in this session. After this, subsequent maintenance
   * ({@link #addDocument}/{@link #removeDocument}) keeps them current incrementally; the only un-tracked drift source is
   * rollbacks, which the next session's stale check (or another explicit recompute) corrects. Practical implication: if you run a
   * stats recompute and then bulk-import in the same JVM session, no automatic re-validation fires - but the import's incremental
   * updates keep the counters accurate, so that is fine.
   *
   * @param totalDocs    live document count
   * @param sumDocLength sum of document lengths
   */
  public void setCounters(final long totalDocs, final long sumDocLength) {
    this.totalDocs.set(totalDocs);
    this.sumDocLength.set(sumDocLength);
    // The volatile write to countersValid below comes AFTER the two counter writes in program order: by the JMM, a reader that
    // observes countersValid == true is guaranteed to also see these counter values (volatile-write / volatile-read edge).
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
    // Write sumDocLength BEFORE totalDocs (the "published" counter that avgDocLength reads first). By the JMM, a reader that
    // observes the new totalDocs is then guaranteed to observe the new sumDocLength too, so a concurrent reader never sees a
    // half-applied (totalDocs bumped, sumDocLength not) state that would momentarily DEFLATE avgdl and over-penalize. The only
    // possible torn read is sumDocLength-new / totalDocs-old, which inflates avgdl slightly (under-penalizes) - the safe side.
    sumDocLength.addAndGet(docLength);
    totalDocs.incrementAndGet();
  }

  /**
   * Removes a document from the corpus counters, clamping at zero to stay consistent under at-least-once removals. Thread-safe.
   *
   * @param docLength number of analyzed tokens of the document
   */
  public void removeDocument(final long docLength) {
    // Decrement totalDocs before sumDocLength so a concurrent reader's worst torn read is totalDocs-decremented /
    // sumDocLength-not-yet, which inflates avgdl slightly (under-penalizes) rather than deflating it - the safe side, matching
    // addDocument's bias.
    totalDocs.updateAndGet(v -> v > 0 ? v - 1 : v);
    sumDocLength.updateAndGet(v -> Math.max(0L, v - docLength));
  }

  /**
   * Returns the average document length across the collection, or 1.0 when no statistics are available.
   * <p>
   * SCOPE: these counters are TYPE-WIDE because every bucket index of the logical type shares this metadata instance.
   * {@code FullTextSearch} combines them with type-wide document frequencies so scores from different buckets are comparable.
   * A caller addressing one bucket-level index directly still uses that bucket's local N/df with this type-wide average as a
   * length-normalization estimate; cross-bucket callers should use the logical {@code TypeIndex} or {@code FullTextSearch}.
   * <p>
   * The two counters are read independently (no shared lock). This method reads {@code totalDocs} first, then {@code sumDocLength};
   * combined with the write ordering in {@link #addDocument}/{@link #removeDocument} (which publish via {@code totalDocs}), the
   * JMM guarantees a concurrent reader sees either a consistent pair or one biased so avgdl is slightly HIGH (under-penalizing) -
   * never momentarily low/over-penalizing. The widest skew is one in-flight document (~{@code avgdl/n}), negligible beyond a
   * handful of documents and dampened by {@code b}, so it cannot distort ranking materially. An exact value is available on demand
   * via the full-text index's {@code recomputeBM25Counters()}.
   */
  public double avgDocLength() {
    final long n = totalDocs.get();
    return n > 0 ? (double) sumDocLength.get() / n : 1.0;
  }
}
