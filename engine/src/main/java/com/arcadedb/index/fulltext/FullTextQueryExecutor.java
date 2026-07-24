/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.text.TextLevenshteinDistance;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Executes Lucene queries against an LSMTreeFullTextIndex.
 * Translates parsed Lucene Query objects into LSM-Tree lookups.
 * <p>
 * Supports advanced query syntax including:
 * <ul>
 *   <li>Boolean operators: AND (+), OR, NOT (-)</li>
 *   <li>Phrase queries: "java programming"</li>
 *   <li>Wildcards: java* (suffix), *java (prefix if enabled)</li>
 *   <li>Field-specific search: field:value</li>
 * </ul>
 * <p>
 * NOT thread-safe: an instance carries per-query mutable state (the collected scoring tokens, the current caret boost, and the
 * exclusion/tokens-only flags) for the duration of one search. Create a new instance per search and do not share it across
 * threads. {@code resetState()} runs at each public entry point as a defensive guard against accidental sequential reuse on the
 * same thread; it does NOT make the instance safe for concurrent reuse. (The Lucene QueryParser is likewise rebuilt per call.)
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class FullTextQueryExecutor {
  /**
   * The Lucene {@link QueryParser} default field, i.e. the field a query term targets when it is not field-qualified. Internally
   * it means "unqualified": such a term is matched against the unprefixed posting (the only field of a single-property index, or
   * the field-agnostic entry of a multi-property index) and receives no per-field boost.
   * <p>
   * It is a deliberately non-identifier sentinel (not a plausible property name) so it cannot collide with a real field. Whether a
   * parsed query field is unqualified is decided by {@link #isUnqualified(String)}, which is index-aware and so also handles a
   * single-property index whose sole property happens to equal a user's field qualifier - the former {@code "content"} sentinel
   * silently mis-scored a {@code content:term} clause on a multi-property index that owned a real {@code content} field.
   */
  static final String DEFAULT_FIELD = "__arcadedb_default_field__";

  /**
   * Safety cap on the number of distinct scoring tokens a single query may accumulate. Wildcard / prefix / fuzzy clauses expand
   * to one scoring token per matched term, and each token costs one posting-list scan in the BM25 re-rank pass; an extreme
   * expansion (e.g. {@code a*} on a huge vocabulary) could otherwise make a single query arbitrarily expensive. Beyond the cap,
   * matching still proceeds (the result set stays correct) but additional terms no longer contribute to the BM25 score.
   */
  static final int MAX_EXPANDED_SCORING_TERMS = 4096;

  /**
   * Result-universe size above which a pure-negative query (only MUST_NOT clauses, which must materialize the whole index to form
   * the complement) logs a throttled WARNING so operators notice the O(index) cost.
   */
  static final int PURE_NEGATIVE_WARN_THRESHOLD = 100_000;

  private final LSMTreeFullTextIndex   index;
  private final Analyzer               analyzer;
  private final FullTextIndexMetadata  metadata;
  // Cached once: the indexed property names don't change during a query, but isUnqualified() is consulted per matched term.
  private final List<String>           propertyNames;

  // BM25 scoring (issue #4687): the positive scoring tokens collected during matching, mapped to their field boost. Populated
  // only when the index uses BM25 similarity and only for positive clauses (never for MUST_NOT exclusion). A new executor is
  // created per search, so these instance fields are not shared across queries.
  private final Map<String, Float> scoringTokens     = new HashMap<>();
  private       boolean            collectingExclusion = false;
  // When true, matching runs only to capture the scoring tokens (used by EXPLAIN/PROFILE): per-document score accumulation is
  // skipped so no document set is materialized. Token discovery for wildcard/prefix/fuzzy still scans the index as needed.
  private       boolean            tokensOnly          = false;
  // The compounded caret boost (e.g. `title:java^3`) of the BoostQuery currently being descended, multiplied into recorded
  // scoring tokens. Lucene's QueryParser already parses `^` into a BoostQuery; this is where that boost takes effect for BM25.
  private       float              currentBoost        = 1.0f;

  // Throttle for the scoring-token-cap WARNING. A new executor is created per query, so a per-query flag would still log on every
  // execution of a repeated huge wildcard; this static throttle bounds it to once per window across the JVM (matching the 60s
  // saturation-warning pattern used by the engine's thread pools). It is JVM-wide / shared across all indexes: a pathological
  // wildcard on one index can suppress the warning for another for the window - an acceptable trade-off for a diagnostic log.
  private static final long       EXPANSION_WARN_THROTTLE_MS = 60_000L;
  private static final AtomicLong lastExpansionWarnMs        = new AtomicLong(0L);
  // Same 60s throttle for the pure-negative full-index-scan WARNING (also JVM-wide / shared across indexes).
  private static final AtomicLong lastPureNegativeWarnMs     = new AtomicLong(0L);

  /**
   * Bucket-local documents that satisfy a parsed query, plus the positive terms that contribute to BM25 scoring. The match set
   * preserves all Lucene boolean/phrase/wildcard semantics; {@link FullTextSearch} combines these bucket-local matches and
   * re-scores them with type-wide corpus statistics.
   */
  record BM25QueryMatch(Set<RID> candidates, Map<String, Float> scoringTokens) {
  }

  /**
   * Creates a new FullTextQueryExecutor for the given index.
   *
   * @param index the full-text index to search
   */
  public FullTextQueryExecutor(final LSMTreeFullTextIndex index) {
    this.index = index;
    this.analyzer = index.getAnalyzer();
    this.metadata = index.getFullTextMetadata();
    this.propertyNames = index.getPropertyNames();
  }

  /**
   * Creates a new QueryParser configured for this index.
   * QueryParser is not thread-safe, so we create a new instance per search.
   *
   * @return a configured QueryParser
   */
  private QueryParser createQueryParser() {
    final QueryParser parser = new QueryParser(DEFAULT_FIELD, analyzer);
    if (metadata != null) {
      parser.setAllowLeadingWildcard(metadata.isAllowLeadingWildcard());
      if ("AND".equalsIgnoreCase(metadata.getDefaultOperator())) {
        parser.setDefaultOperator(QueryParser.Operator.AND);
      }
    }
    return parser;
  }

  /**
   * Searches the index using Lucene query syntax.
   *
   * @param queryString the query string in Lucene syntax
   * @param limit       maximum number of results to return (-1 for unlimited)
   * @return cursor with matching documents, sorted by score descending
   */
  public IndexCursor search(final String queryString, final int limit) {
    resetState();
    return executeQuery(parseQuery(queryString), limit);
  }

  /**
   * Resets the per-query matching state. An executor is meant to be used for a single search, but resetting at each public entry
   * point guarantees no state leaks between calls even if one is reused.
   */
  private void resetState() {
    scoringTokens.clear();
    collectingExclusion = false;
    tokensOnly = false;
    currentBoost = 1.0f;
  }

  /**
   * Runs only the query's matching logic (no scoring) to collect the scoring tokens, then returns the index's query-level BM25
   * scoring explanation (similarity, k1/b, N, avgdl, and per-term df/idf/boost). Surfaced by {@code EXPLAIN}/{@code PROFILE}.
   *
   * @param queryString the query string in Lucene syntax
   */
  public JSONObject explainScoring(final String queryString) {
    return index.explainScoring(collectScoringTokens(queryString));
  }

  /**
   * Parses a query and discovers its positive scoring terms without materializing matching documents. Package-private so the
   * type-wide coordinator can union wildcard/fuzzy expansions from every bucket before computing global document frequencies.
   */
  Map<String, Float> collectScoringTokens(final String queryString) {
    resetState();
    final Query query = parseQuery(queryString);
    tokensOnly = true;
    try {
      // The score map stays empty in tokens-only mode; we only need the captured scoringTokens.
      collectMatches(query, new HashMap<>(), new HashSet<>());
    } finally {
      tokensOnly = false;
    }
    return Map.copyOf(scoringTokens);
  }

  /**
   * Returns the bucket-local candidate set without applying bucket-local BM25 statistics. Used by type-wide search so matching
   * remains bucket-local while scoring uses one comparable corpus scope across every bucket.
   */
  BM25QueryMatch collectBM25Matches(final String queryString) {
    if (!index.isBM25())
      throw new IllegalStateException("BM25 matching requires a BM25 full-text index");

    resetState();
    final Map<RID, AtomicInteger> matches = collectQueryMatches(parseQuery(queryString));
    return new BM25QueryMatch(Set.copyOf(matches.keySet()), Map.copyOf(scoringTokens));
  }

  private Query parseQuery(final String queryString) {
    try {
      // QueryParser is not thread-safe, so create one per invocation.
      return createQueryParser().parse(queryString);
    } catch (final ParseException e) {
      throw new IndexException("Invalid search query: " + queryString, e);
    }
  }

  private IndexCursor executeQuery(final Query query, final int limit) {
    final Map<RID, AtomicInteger> scoreMap = collectQueryMatches(query);

    // BM25 path: collectMatches above determined WHICH documents match (boolean/phrase/wildcard semantics). Re-rank that
    // candidate set with BM25 using the scoring tokens captured during matching.
    // The result entries carry EMPTY keys (here and in the CLASSIC branch below): a SEARCH_INDEX query is a multi-token Lucene
    // query, not a single index key, so there is no meaningful single key tuple to attach (unlike the direct index.get() path,
    // whose single query keys flow through). The SQL SEARCH_INDEX function reads only RID + $score from these entries, never
    // getKeys(); a direct executor.search() caller should likewise not rely on getKeys() for this path.
    if (index.isBM25())
      return index.scoreCandidatesBM25(scoreMap.keySet(), scoringTokens, new Object[] {}, limit);

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(scoreMap.size());

    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet()) {
      list.add(new IndexCursorEntry(new Object[] {}, entry.getKey(), entry.getValue().get()));
    }

    // Sort by score descending, then by RID for deterministic ordering
    list.sort((o1, o2) -> {
      final int scoreCompare = Integer.compare(o2.score, o1.score);
      if (scoreCompare != 0)
        return scoreCompare;
      return o1.record.getIdentity().compareTo(o2.record.getIdentity());
    });

    if (limit > 0 && list.size() > limit) {
      return new TempIndexCursor(list.subList(0, limit));
    }
    return new TempIndexCursor(list);
  }

  private Map<RID, AtomicInteger> collectQueryMatches(final Query query) {
    final Map<RID, AtomicInteger> scoreMap = new HashMap<>();
    final Set<RID> excluded = new HashSet<>();

    collectMatches(query, scoreMap, excluded);

    // Remove excluded RIDs
    for (final RID rid : excluded) {
      scoreMap.remove(rid);
    }
    return scoreMap;
  }

  private void collectMatches(final Query query, final Map<RID, AtomicInteger> scoreMap,
      final Set<RID> excluded) {

    if (query instanceof final BooleanQuery bq) {
      if (tokensOnly) {
        // Only the positive clauses contribute scoring tokens; recurse to capture them and skip all set intersection/merge work.
        for (final BooleanClause clause : bq.clauses())
          if (clause.occur() != BooleanClause.Occur.MUST_NOT)
            collectMatches(clause.query(), scoreMap, excluded);
        return;
      }

      // First pass: collect MUST_NOT terms and track whether any exist
      boolean hasMustNotClauses = false;
      for (final BooleanClause clause : bq.clauses()) {
        if (clause.occur() == BooleanClause.Occur.MUST_NOT) {
          hasMustNotClauses = true;
          collectTermsForExclusion(clause.query(), excluded);
        }
      }

      // Second pass: process MUST and SHOULD terms
      Map<RID, AtomicInteger> mustResults = null;
      final Map<RID, AtomicInteger> shouldResults = new HashMap<>();

      for (final BooleanClause clause : bq.clauses()) {
        if (clause.occur() == BooleanClause.Occur.MUST) {
          final Map<RID, AtomicInteger> termResults = new HashMap<>();
          collectMatches(clause.query(), termResults, excluded);

          if (mustResults == null) {
            mustResults = termResults;
          } else {
            // Intersection: keep only RIDs that appear in both
            mustResults.keySet().retainAll(termResults.keySet());
            // Add scores
            for (final RID rid : mustResults.keySet()) {
              if (termResults.containsKey(rid)) {
                mustResults.get(rid).addAndGet(termResults.get(rid).get());
              }
            }
          }
        } else if (clause.occur() == BooleanClause.Occur.SHOULD) {
          collectMatches(clause.query(), shouldResults, excluded);
        }
      }

      if (mustResults != null) {
        // MUST clauses exist: only include documents that satisfy all MUST clauses
        // SHOULD clauses add bonus score to documents that already match MUST
        for (final Map.Entry<RID, AtomicInteger> entry : mustResults.entrySet()) {
          final RID rid = entry.getKey();
          int totalScore = entry.getValue().get();
          // Add bonus from SHOULD matches if present
          if (shouldResults.containsKey(rid)) {
            totalScore += shouldResults.get(rid).get();
          }
          scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).addAndGet(totalScore);
        }
      } else if (!shouldResults.isEmpty()) {
        // No MUST clauses: SHOULD results are returned (standard OR behavior)
        for (final Map.Entry<RID, AtomicInteger> entry : shouldResults.entrySet()) {
          scoreMap.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0))
              .addAndGet(entry.getValue().get());
        }
      } else if (hasMustNotClauses) {
        // Pure negative query (only MUST_NOT, no positive clauses): seed with every indexed
        // record so executeQuery() can subtract the excluded set to form the complement.
        collectAllIndexedRids(scoreMap);
      }

    } else if (query instanceof BoostQuery bq) {
      // Caret boost, e.g. `title:java^3`. Compound nested boosts and descend into the wrapped query.
      final float saved = currentBoost;
      currentBoost *= bq.getBoost();
      try {
        collectMatches(bq.getQuery(), scoreMap, excluded);
      } finally {
        currentBoost = saved;
      }
    } else if (query instanceof final TermQuery tq) {
      collectTermMatches(tq, scoreMap);
    } else if (query instanceof final PhraseQuery pq) {
      collectPhraseMatches(pq, scoreMap);
    } else if (query instanceof final PrefixQuery pq) {
      collectPrefixMatches(pq, scoreMap);
    } else if (query instanceof final WildcardQuery wq) {
      collectWildcardMatches(wq, scoreMap);
    } else if (query instanceof final FuzzyQuery fq) {
      collectFuzzyMatches(fq, scoreMap);
    } else if (query instanceof final RegexpQuery rq) {
      collectRegexpMatches(rq, scoreMap);
    }
    // Other Lucene query types (e.g., TermRangeQuery) are intentionally ignored.
  }

  private void collectTermsForExclusion(final Query query, final Set<RID> excluded) {
    if (query instanceof final BooleanQuery bq) {
      for (final BooleanClause clause : bq.clauses()) {
        // A nested MUST_NOT inside an exclusion context is a double negation (e.g. NOT (A AND NOT B) - B should NOT be excluded).
        // Skip such clauses so their terms are not wrongly added to the exclusion set. (Fully representing the positive
        // contribution of a double-negated term would require general boolean evaluation; this at least avoids excluding it.)
        if (clause.occur() == BooleanClause.Occur.MUST_NOT)
          continue;
        collectTermsForExclusion(clause.query(), excluded);
      }
      return;
    }
    // Delegate every other leaf query type to collectMatches so any positive collector
    // automatically has matching exclusion support; the empty excluded set isolates this scratch
    // collection from the caller's accumulator. Tokens matched here are negative (MUST_NOT) and must
    // not contribute to BM25 scoring, so suppress token capture for the duration of the scan.
    final boolean previous = collectingExclusion;
    collectingExclusion = true;
    try {
      final Map<RID, AtomicInteger> tempMap = new HashMap<>();
      collectMatches(query, tempMap, new HashSet<>());
      excluded.addAll(tempMap.keySet());
    } finally {
      collectingExclusion = previous;
    }
  }

  /**
   * Records a positive scoring token (in its stored-key form) and its field boost for the later BM25 re-ranking pass. No-op when
   * the index is not BM25 or while collecting MUST_NOT exclusion terms. When a token is seen more than once the highest boost
   * wins.
   */
  private void recordScoringToken(final String storedKey, final float boost) {
    if (collectingExclusion || !index.isBM25())
      return;
    // Single get(): the cap drops only genuinely new tokens once the limit is reached (a pathological wildcard/fuzzy expansion
    // would otherwise add one posting-list scan per matched term to the re-rank pass); an already-seen token may still raise its
    // boost without growing the map. Matching is unaffected, so the result set stays correct - excess terms just stop scoring.
    final Float current = scoringTokens.get(storedKey);
    if (current == null && scoringTokens.size() >= MAX_EXPANDED_SCORING_TERMS) {
      maybeWarnExpansionCap();
      return;
    }
    // Combine the per-field boost with the caret boost in effect for this part of the query; keep the highest seen.
    final float combined = boost * currentBoost;
    if (current == null || combined > current)
      scoringTokens.put(storedKey, combined);
  }

  /**
   * Logs the scoring-token-cap WARNING at most once per {@link #EXPANSION_WARN_THROTTLE_MS} across the JVM (a new executor per
   * query means a per-query flag would not suppress it for a repeated huge wildcard). The CAS ensures a single winner per window.
   */
  private void maybeWarnExpansionCap() {
    final long now = System.currentTimeMillis();
    final long last = lastExpansionWarnMs.get();
    if (now - last >= EXPANSION_WARN_THROTTLE_MS && lastExpansionWarnMs.compareAndSet(last, now))
      LogManager.instance().log(this, Level.WARNING,
          "Full-text query expanded to more than %d scoring terms on index '%s'; additional terms are matched but not BM25-scored. "
              + "Consider a more specific query (narrower wildcard/fuzzy). (throttled, logged at most once per %d s)",
          MAX_EXPANDED_SCORING_TERMS, index.getName(), EXPANSION_WARN_THROTTLE_MS / 1000);
  }

  /**
   * Logs the pure-negative full-index-scan WARNING, throttled like {@link #maybeWarnExpansionCap()}.
   */
  private void maybeWarnPureNegativeScan(final int universeSize) {
    final long now = System.currentTimeMillis();
    final long last = lastPureNegativeWarnMs.get();
    if (now - last >= EXPANSION_WARN_THROTTLE_MS && lastPureNegativeWarnMs.compareAndSet(last, now))
      LogManager.instance().log(this, Level.WARNING,
          "Pure-negative full-text query on index '%s' materialized %d documents (the whole index) to compute the complement. "
              + "Add a positive clause to bound the scan. (throttled, logged at most once per %d s)",
          index.getName(), universeSize, EXPANSION_WARN_THROTTLE_MS / 1000);
  }

  /**
   * Returns true when a parsed query field denotes the "unqualified" target rather than a real, field-qualified clause. A term is
   * unqualified when the parser left it on the {@link #DEFAULT_FIELD} sentinel, or when it is qualified with the sole property of a
   * single-property index: such an index stores only unprefixed postings, so {@code field:term} and {@code term} are equivalent
   * there. This is what lets a non-colliding sentinel be used without breaking single-property {@code content:term} queries.
   */
  private boolean isUnqualified(final String field) {
    if (field == null || field.isEmpty() || DEFAULT_FIELD.equals(field))
      return true;
    return propertyNames.size() == 1 && propertyNames.get(0).equals(field);
  }

  /**
   * Returns the BM25 field boost for a query field: the configured per-field boost for an explicit field, or 1.0 for an
   * unqualified term.
   */
  private float boostFor(final String field) {
    if (!isUnqualified(field) && metadata != null)
      return metadata.getFieldBoost(field);
    return 1.0f;
  }

  /**
   * Seeds the score map with every record reachable through the underlying index. Used as the
   * candidate universe for pure negative queries so that subtracting the excluded set yields the
   * correct complement. Runs in O(index entries) where one entry exists per (token, RID) pair, not
   * per unique document, so a document indexed under N tokens is visited N times and deduplicated
   * via computeIfAbsent. Each RID is assigned a constant score of 1 because no positive clause
   * contributed to its presence in the result set.
   */
  private void collectAllIndexedRids(final Map<RID, AtomicInteger> scoreMap) {
    final IndexCursor cursor = index.iterateUnderlying(true, null, true);
    while (cursor.hasNext()) {
      // next() may return null after hasNext()==true (deleted/tombstoned entry, issue #5118); skip it.
      final Identifiable record = cursor.next();
      if (record == null)
        continue;
      scoreMap.computeIfAbsent(record.getIdentity(), k -> new AtomicInteger(1));
    }
    // A pure-negative query (only MUST_NOT clauses) has no candidate set, so the whole index must be materialized to subtract the
    // excluded RIDs and form the complement. This is O(index) in time and memory; warn (throttled) when the materialized universe
    // is large so an operator who wrote e.g. `-the` notices the cost and adds a positive clause to bound it.
    if (scoreMap.size() >= PURE_NEGATIVE_WARN_THRESHOLD)
      maybeWarnPureNegativeScan(scoreMap.size());
  }

  private void collectTermMatches(final TermQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getTerm().field();
    final String text = query.getTerm().text();

    // For field-specific queries (e.g., "title:java"), prepend field name
    // Multi-property indexes store tokens as "fieldName:token"
    final String searchKey = isUnqualified(field) ? text : field + ":" + text;

    recordScoringToken(searchKey, boostFor(field));
    if (tokensOnly)
      return;

    // Raw postings lookup: we only need the matching RIDs here. The BM25 score is computed once, later, by scoreCandidatesBM25;
    // going through index.get() would run (and then discard) the full scoring pipeline for every term.
    final IndexCursor cursor = index.getPostings(searchKey);
    while (cursor.hasNext()) {
      // next() may return null after hasNext()==true (deleted/tombstoned entry, issue #5118); skip it.
      final Identifiable record = cursor.next();
      if (record == null)
        continue;
      scoreMap.computeIfAbsent(record.getIdentity(), k -> new AtomicInteger(0)).incrementAndGet();
    }
  }

  private void collectPhraseMatches(final PhraseQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // For phrase queries, all terms must match in the same document
    // Note: We can't verify word order without position indexing, so we just require all terms.
    // Phrase terms are matched/scored against the unprefixed token, so the configured per-field boost is NOT applied to phrase
    // terms (an enclosing caret boost still applies via currentBoost). They are recorded with boost 1.0 accordingly.
    final Term[] terms = query.getTerms();
    if (terms.length == 0)
      return;

    if (tokensOnly) {
      for (final Term term : terms)
        recordScoringToken(term.text(), 1.0f);
      return;
    }

    Map<RID, AtomicInteger> intersection = null;

    for (final Term term : terms) {
      recordScoringToken(term.text(), 1.0f);
      final Map<RID, AtomicInteger> termMatches = new HashMap<>();
      final IndexCursor cursor = index.getPostings(term.text());
      while (cursor.hasNext()) {
        // next() may return null after hasNext()==true (deleted/tombstoned entry, issue #5118); skip it.
        final Identifiable record = cursor.next();
        if (record == null)
          continue;
        termMatches.put(record.getIdentity(), new AtomicInteger(1));
      }

      if (intersection == null) {
        intersection = termMatches;
      } else {
        intersection.keySet().retainAll(termMatches.keySet());
      }
    }

    if (intersection != null) {
      for (final RID rid : intersection.keySet()) {
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).addAndGet(terms.length);
      }
    }
  }

  private void collectPrefixMatches(final PrefixQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getPrefix().field();
    final String prefix = normalizeText(query.getPrefix().text());
    if (prefix.isEmpty())
      return;

    final String searchPrefix = buildSearchKey(field, prefix);
    iterateAndMatch(searchPrefix, key -> key.startsWith(searchPrefix), scoreMap, boostFor(field));
  }

  private void collectWildcardMatches(final WildcardQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getTerm().field();
    final String pattern = normalizeText(query.getTerm().text());
    if (pattern.isEmpty())
      return;

    // Compute the literal prefix (everything up to the first wildcard char)
    final String literalPrefix = extractLiteralPrefix(pattern);
    final String searchPrefix = buildSearchKey(field, literalPrefix);
    final String fieldPrefix = !isUnqualified(field) ? field + ":" : "";
    final Pattern regex = wildcardToRegex(pattern);

    if (literalPrefix.isEmpty()) {
      // Leading wildcard: full scan, then regex match against the unprefixed token portion
      iterateAndMatch(null, key -> {
        if (!key.startsWith(fieldPrefix))
          return false;
        final String token = key.substring(fieldPrefix.length());
        // Skip cross-field tokens for unqualified queries on multi-property indexes
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          return false;
        return regex.matcher(token).matches();
      }, scoreMap, boostFor(field));
    } else {
      // Range scan starting at the literal prefix and stop when keys no longer share it
      iterateAndMatch(searchPrefix, key -> {
        if (!key.startsWith(searchPrefix))
          return false;
        final String token = key.substring(fieldPrefix.length());
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          return false;
        return regex.matcher(token).matches();
      }, scoreMap, boostFor(field));
    }
  }

  private void collectFuzzyMatches(final FuzzyQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getTerm().field();
    final String term = normalizeText(query.getTerm().text());
    if (term.isEmpty())
      return;

    final int maxEdits = query.getMaxEdits();
    final int prefixLen = Math.min(query.getPrefixLength(), term.length());
    final String requiredPrefix = term.substring(0, prefixLen);
    final String fieldPrefix = !isUnqualified(field) ? field + ":" : "";
    final String searchPrefix = fieldPrefix + requiredPrefix;

    iterateAndMatch(searchPrefix.isEmpty() ? null : searchPrefix, key -> {
      if (!key.startsWith(searchPrefix))
        return false;
      final String token = key.substring(fieldPrefix.length());
      if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
        return false;
      return TextLevenshteinDistance.levenshteinDistance(token, term) <= maxEdits;
    }, scoreMap, boostFor(field));
  }

  private void collectRegexpMatches(final RegexpQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getRegexp().field();
    final String regexText = normalizeText(query.getRegexp().text());
    if (regexText.isEmpty())
      return;

    final Pattern regex;
    try {
      regex = Pattern.compile(regexText);
    } catch (final PatternSyntaxException e) {
      return;
    }

    final String fieldPrefix = !isUnqualified(field) ? field + ":" : "";

    iterateAndMatch(null, key -> {
      if (!key.startsWith(fieldPrefix))
        return false;
      final String token = key.substring(fieldPrefix.length());
      if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
        return false;
      return regex.matcher(token).matches();
    }, scoreMap, boostFor(field));
  }

  /**
   * Iterates the underlying index from the given start key and accumulates RIDs whose keys pass the matcher.
   * Stops as soon as the matcher rejects a key when {@code startKey} is non-null (since keys are sorted
   * and a non-prefix means we've left the relevant range). When {@code startKey} is null, performs a full
   * scan and applies the matcher to every key.
   */
  private void iterateAndMatch(final String startKey, final KeyMatcher matcher, final Map<RID, AtomicInteger> scoreMap,
      final float boost) {
    final boolean rangeScan = startKey != null;
    final IndexCursor cursor = index.iterateUnderlying(true,
        rangeScan ? new String[] { startKey } : null, true);
    while (cursor.hasNext()) {
      // LSMTreeIndexCursor.next() can legitimately return null after hasNext()==true (a full-key tombstone / deleted entry it
      // steps over while its internal iterators are still considered alive - issue #5118). Skip it instead of dereferencing.
      final Identifiable record = cursor.next();
      if (record == null)
        continue;
      final RID rid = record.getIdentity();
      final Object[] keys = cursor.getKeys();
      if (keys == null || keys.length == 0 || keys[0] == null)
        continue;
      final String key = keys[0].toString();
      if (matcher.matches(key)) {
        recordScoringToken(key, boost);
        if (!tokensOnly)
          scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      } else if (rangeScan && key.compareTo(startKey) > 0 && !key.startsWith(startKey)) {
        break;
      }
    }
  }

  /**
   * Builds the index search key by prefixing the field name when needed. Multi-property indexes store entries as
   * {@code fieldName:token}; unqualified terms (see {@link #isUnqualified(String)}) target the unprefixed tokens.
   */
  private String buildSearchKey(final String field, final String text) {
    return isUnqualified(field) ? text : field + ":" + text;
  }

  /**
   * Normalizes a wildcard / fuzzy / regex term to lowercase to match how the analyzer stored the tokens.
   * The Lucene QueryParser does not pass these terms through the analyzer; without normalization, queries
   * like {@code Hell*} would never match the lower-cased indexed tokens.
   */
  private static String normalizeText(final String text) {
    return text == null ? "" : text.toLowerCase(Locale.ROOT);
  }

  /**
   * Extracts the longest literal prefix of a wildcard pattern (everything before the first
   * {@code *}, {@code ?}, or escaped char that introduces non-literal matching).
   */
  private static String extractLiteralPrefix(final String pattern) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '*' || c == '?')
        break;
      if (c == '\\' && i + 1 < pattern.length()) {
        sb.append(pattern.charAt(i + 1));
        i++;
        continue;
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Converts a Lucene-style wildcard pattern (using {@code *} and {@code ?}) to a {@link Pattern}.
   * Other regex metacharacters in the input are escaped so they match literally.
   */
  private static Pattern wildcardToRegex(final String wildcard) {
    final StringBuilder sb = new StringBuilder(wildcard.length() + 8);
    sb.append('^');
    for (int i = 0; i < wildcard.length(); i++) {
      final char c = wildcard.charAt(i);
      if (c == '*') {
        sb.append(".*");
      } else if (c == '?') {
        sb.append('.');
      } else if (c == '\\' && i + 1 < wildcard.length()) {
        sb.append(Pattern.quote(String.valueOf(wildcard.charAt(i + 1))));
        i++;
      } else {
        sb.append(Pattern.quote(String.valueOf(c)));
      }
    }
    sb.append('$');
    return Pattern.compile(sb.toString());
  }

  @FunctionalInterface
  private interface KeyMatcher {
    boolean matches(String key);
  }
}
