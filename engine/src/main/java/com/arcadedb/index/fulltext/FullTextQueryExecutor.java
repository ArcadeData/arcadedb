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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.RIDFilter;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.TimeBoundRegex;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
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
  // Stops the posting walks and the score accumulation of a statement that was terminated or ran past its deadline (issue
  // #9894): all of a search's work happens before its first row, so no check of the executor between rows can reach it.
  private final WorkGuard              guard;
  private       int                    guardTicks;
  // Cached once: the indexed property names don't change during a query, but isUnqualified() is consulted per matched term.
  private final List<String>           propertyNames;
  /**
   * The fields an unqualified clause is restricted to, or null to search it across every indexed property (issue #9910).
   */
  private final String[]               restrictedFields;
  /** Decides the matches on their RIDs before they are scored, or null to keep every match (issue #9909). */
  private final RIDFilter              ridFilter;
  /**
   * Per fuzzy clause, the stored keys it expands to, chosen across every bucket of the type by {@link #selectFuzzyExpansions}; or
   * null, and for a clause the map does not hold, the clause chooses them in this bucket alone (issue #12462).
   */
  private       Map<FuzzyQuery, Set<String>> chosenFuzzyExpansions;

  // BM25 scoring (issue #4687): the positive scoring tokens collected during matching, mapped to their field boost. Populated
  // only when the index uses BM25 similarity and only for positive clauses (never for MUST_NOT exclusion). A new executor is
  // created per search, so these instance fields are not shared across queries.
  private final Map<String, Float> scoringTokens       = new HashMap<>();
  // Document frequency of each scoring token within this bucket, counted during the same posting walk that collects the
  // matches. A type-wide search sums these across buckets instead of re-scanning every posting list to derive df.
  private final Map<String, Long>  documentFrequencies = new HashMap<>();
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
  record BM25QueryMatch(Set<RID> candidates, Map<String, Float> scoringTokens, Map<String, Long> documentFrequencies) {
  }

  /**
   * Creates a new FullTextQueryExecutor for the given index.
   *
   * @param index the full-text index to search
   */
  public FullTextQueryExecutor(final LSMTreeFullTextIndex index) {
    this(index, WorkGuard.forCommandDeadline(null));
  }

  /**
   * Creates a new FullTextQueryExecutor for the given index, whose posting walks abort through {@code guard}.
   *
   * @param index the full-text index to search
   * @param guard checked periodically while the postings are walked and the matches combined
   */
  public FullTextQueryExecutor(final LSMTreeFullTextIndex index, final WorkGuard guard) {
    this(index, guard, null, null);
  }

  /**
   * @param restrictedFields the indexed properties an unqualified clause is searched in, each clause becoming an OR of the
   *                         same clause qualified with each of them, or null to search it across every indexed property.
   *                         An explicitly qualified clause keeps its own field (issue #9910)
   * @param ridFilter        decides the matches on their RIDs before they are scored or limited, or null (issue #9909)
   */
  public FullTextQueryExecutor(final LSMTreeFullTextIndex index, final WorkGuard guard, final List<String> restrictedFields,
      final RIDFilter ridFilter) {
    this.index = index;
    this.guard = guard;
    this.analyzer = index.getAnalyzer();
    this.metadata = index.getFullTextMetadata();
    this.propertyNames = index.getPropertyNames();
    this.restrictedFields = restrictedFields != null ? restrictedFields.toArray(new String[0]) : null;
    this.ridFilter = ridFilter;
  }

  /**
   * Makes each fuzzy clause expand to the terms chosen for it across the buckets of the type (see {@link #selectFuzzyExpansions}),
   * rather than to the best ones of this bucket alone: the cap on the expansions is the type's, as it is the whole index's in
   * Lucene (issue #12462).
   *
   * @param expansions per fuzzy clause the stored keys it expands to, or null to choose them in this bucket
   */
  FullTextQueryExecutor withFuzzyExpansions(final Map<FuzzyQuery, Set<String>> expansions) {
    this.chosenFuzzyExpansions = expansions;
    return this;
  }

  /**
   * Creates a new QueryParser configured for this index.
   * QueryParser is not thread-safe, so we create a new instance per search.
   *
   * @return a configured QueryParser
   */
  private QueryParser createQueryParser() {
    // Restricted to some fields, an unqualified clause is expanded by Lucene's own parser into the OR of that clause over
    // each of them - a term, a phrase, a prefix, a wildcard and a fuzzy term alike - which the collectors below then look up
    // under the field-qualified keys the index stores for a multi-property index (issue #9910)
    final QueryParser parser = restrictedFields != null ?
        new MultiFieldQueryParser(restrictedFields, analyzer) :
        new QueryParser(DEFAULT_FIELD, analyzer);
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
    guard.check();
    resetState();
    return executeQuery(parseQuery(queryString), limit);
  }

  /**
   * Resets the per-query matching state. An executor is meant to be used for a single search, but resetting at each public entry
   * point guarantees no state leaks between calls even if one is reused.
   */
  private void resetState() {
    scoringTokens.clear();
    documentFrequencies.clear();
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
    collectTokensOnly(parseQuery(queryString));
    return Map.copyOf(scoringTokens);
  }

  private void collectTokensOnly(final Query query) {
    tokensOnly = true;
    try {
      // The score map stays empty in tokens-only mode; we only need the captured scoringTokens.
      collectMatches(query, new HashMap<>(), new HashSet<>());
    } finally {
      tokensOnly = false;
    }
  }

  /**
   * Decides whether one record matches a query from the stored keys of that record's own postings (issue #11683).
   */
  @FunctionalInterface
  interface RecordMatcher {
    /**
     * @param keys     the stored keys the record has a posting under
     * @param excluded its first cell is set when a MUST_NOT clause matches the record: the search subtracts what such a clause
     *                 matches from the matches of the whole query, wherever the clause sits in it
     */
    boolean matches(Set<String> keys, boolean[] excluded);
  }

  /**
   * A query decided on one record's postings (see {@link #recordQuery}), with the scoring tokens in the order the BM25 pass of
   * this bucket sums them.
   */
  record RecordQuery(RecordMatcher matcher, Map<String, Float> scoringTokens) {
  }

  /**
   * The query as {@link #collectQueryMatches} decides it, for one record at a time, or null when a clause of it is not decided by
   * the record alone (issue #11683): a wildcard, prefix, fuzzy or regexp clause matches the keys of the whole index, and a
   * MUST_NOT clause beside no MUST clause answers the complement of the bucket's other matches.
   * <p>
   * The scoring tokens are this executor's own map in its iteration order, which is the order {@link #executeQuery} hands it to
   * the BM25 pass: the tokens-only walk records the same tokens in the same order as the matching walk, since both visit the
   * positive clauses in order and the matching walk records nothing while it collects the MUST_NOT clauses.
   */
  RecordQuery recordQuery(final String queryString) {
    resetState();
    final Query query = parseQuery(queryString);
    final RecordMatcher matcher = recordMatcher(query);
    if (matcher == null)
      return null;
    collectTokensOnly(query);
    return new RecordQuery(matcher, new LinkedHashMap<>(scoringTokens));
  }

  /** {@link #collectMatches} for one record, or null for a query it cannot decide on the record alone. */
  private RecordMatcher recordMatcher(final Query query) {
    if (query instanceof final BooleanQuery bq) {
      final List<RecordMatcher> exclusions = new ArrayList<>();
      final List<RecordMatcher> musts = new ArrayList<>();
      final List<RecordMatcher> shoulds = new ArrayList<>();
      for (final BooleanClause clause : bq.clauses()) {
        final RecordMatcher matcher = switch (clause.occur()) {
          case MUST_NOT -> exclusionMatcher(clause.query());
          case MUST, SHOULD -> recordMatcher(clause.query());
          // collectMatches ignores a FILTER clause: no parser of ours writes one, so it is left to the index
          case FILTER -> null;
        };
        if (matcher == null)
          return null;
        switch (clause.occur()) {
          case MUST_NOT -> exclusions.add(matcher);
          case MUST -> musts.add(matcher);
          default -> shoulds.add(matcher);
        }
      }
      // Without a MUST clause, MUST_NOT clauses answer the whole bucket minus what they match when no SHOULD clause matches any
      // record of the bucket: a fact about the other records
      if (!exclusions.isEmpty() && musts.isEmpty())
        return null;
      final boolean conjunction = !musts.isEmpty();
      return (keys, excluded) -> {
        for (final RecordMatcher exclusion : exclusions)
          if (exclusion.matches(keys, excluded))
            excluded[0] = true;
        // Every clause is decided, as collectMatches collects every one: a MUST_NOT clause nested in any of them excludes the
        // record from the whole answer
        boolean all = true;
        for (final RecordMatcher must : musts)
          if (!must.matches(keys, excluded))
            all = false;
        boolean any = false;
        for (final RecordMatcher should : shoulds)
          if (should.matches(keys, excluded))
            any = true;
        return conjunction ? all : any;
      };
    }
    if (query instanceof final BoostQuery bq)
      return recordMatcher(bq.getQuery());
    if (query instanceof final TermQuery tq) {
      final String key = buildSearchKey(tq.getTerm().field(), tq.getTerm().text());
      return (keys, excluded) -> keys.contains(key);
    }
    if (query instanceof final PhraseQuery pq) {
      final Term[] terms = pq.getTerms();
      if (terms.length > 1)
        // the positions of the terms are not among the keys of the record's postings (issue #12165)
        return null;
      final String[] phraseKeys = new String[terms.length];
      for (int i = 0; i < terms.length; i++)
        phraseKeys[i] = buildSearchKey(terms[i].field(), terms[i].text());
      return (keys, excluded) -> {
        if (phraseKeys.length == 0)
          return false;
        for (final String key : phraseKeys)
          if (!keys.contains(key))
            return false;
        return true;
      };
    }
    return null;
  }

  /** {@link #collectTermsForExclusion} for one record: whether the record is among what a MUST_NOT clause excludes. */
  private RecordMatcher exclusionMatcher(final Query query) {
    if (query instanceof final BooleanQuery bq) {
      final List<RecordMatcher> parts = new ArrayList<>();
      for (final BooleanClause clause : bq.clauses()) {
        if (clause.occur() == BooleanClause.Occur.MUST_NOT)
          continue;
        final RecordMatcher part = exclusionMatcher(clause.query());
        if (part == null)
          return null;
        parts.add(part);
      }
      return (keys, excluded) -> {
        for (final RecordMatcher part : parts)
          if (part.matches(keys, excluded))
            return true;
        return false;
      };
    }
    final RecordMatcher leaf = recordMatcher(query);
    if (leaf == null)
      return null;
    // collectTermsForExclusion matches a leaf with an exclusion set of its own, which it then drops
    return (keys, excluded) -> leaf.matches(keys, new boolean[1]);
  }

  /**
   * The scoring tokens of a query that is a disjunction of terms, each with its boost, in the order the BM25 pass sums them; or null
   * for any other query (issue #12458). Such a query - terms, caret boosts and OR groups of them, which is also what a restriction to
   * some fields turns an unqualified term into - matches exactly the documents holding one of its tokens and scores each with the
   * tokens it holds, so {@link FullTextTopKSearch} can answer it from the postings alone. A required, prohibited, phrase, prefix,
   * wildcard, fuzzy or regexp clause, or more tokens than {@link #MAX_EXPANDED_SCORING_TERMS} (beyond which a token matches but does
   * not score), keep the general path.
   */
  LinkedHashMap<String, Float> termDisjunction(final String queryText) {
    resetState();
    final Query query = parseQuery(queryText);
    if (!isTermDisjunction(query))
      return null;
    collectTokensOnly(query);
    if (scoringTokens.isEmpty() || scoringTokens.size() >= MAX_EXPANDED_SCORING_TERMS)
      return null;
    return new LinkedHashMap<>(scoringTokens);
  }

  private static boolean isTermDisjunction(final Query query) {
    if (query instanceof TermQuery)
      return true;
    if (query instanceof final BoostQuery bq)
      return isTermDisjunction(bq.getQuery());
    if (query instanceof final BooleanQuery bq) {
      if (bq.clauses().isEmpty() || bq.getMinimumNumberShouldMatch() > 0)
        return false;
      for (final BooleanClause clause : bq.clauses())
        if (clause.occur() != BooleanClause.Occur.SHOULD || !isTermDisjunction(clause.query()))
          return false;
      return true;
    }
    return false;
  }

  /**
   * Returns the bucket-local candidate set without applying bucket-local BM25 statistics. Used by type-wide search so matching
   * remains bucket-local while scoring uses one comparable corpus scope across every bucket.
   */
  BM25QueryMatch collectBM25Matches(final String queryString) {
    if (!index.isBM25())
      throw new IllegalStateException("BM25 matching requires a BM25 full-text index");

    guard.check();
    resetState();
    final Map<RID, AtomicInteger> matches = collectQueryMatches(parseQuery(queryString));
    // The tokens keep this executor's order rather than Map.copyOf's, which is salted per JVM: it is the order every bucket sums
    // a document's contributions in, so a score does not move in its last bit from one run to the next (issue #12458)
    return new BM25QueryMatch(Set.copyOf(matches.keySet()), Collections.unmodifiableMap(new LinkedHashMap<>(scoringTokens)),
        Map.copyOf(documentFrequencies));
  }

  private Query parseQuery(final String queryString) {
    try {
      // QueryParser is not thread-safe, so create one per invocation.
      return createQueryParser().parse(queryString);
    } catch (final ParseException e) {
      throw new FullTextQueryParseException("Invalid search query: " + queryString, e);
    }
  }

  private IndexCursor executeQuery(final Query query, int limit) {
    final Map<RID, AtomicInteger> scoreMap = collectQueryMatches(query);
    if (ridFilter != null) {
      // Before the scoring and the limit: the best matches are the best of those the filter keeps. The scores do not change,
      // the document frequencies were counted on the postings
      final int bucketId = index.getAssociatedBucketId();
      // A predicate on the records is decided through an index listing no longer than the matches (issue #11069)
      ridFilter.listBucket(bucketId, scoreMap.size());
      scoreMap.keySet().removeIf(rid -> !ridFilter.accepts(rid));
      if (!ridFilter.decidesBucket(bucketId))
        // the predicate is left to the records once loaded: the best matches kept here could all fail it
        limit = -1;
    }

    // BM25 path: collectMatches above determined WHICH documents match (boolean/phrase/wildcard semantics). Re-rank that
    // candidate set with BM25 using the scoring tokens captured during matching.
    // The result entries carry EMPTY keys (here and in the CLASSIC branch below): a SEARCH_INDEX query is a multi-token Lucene
    // query, not a single index key, so there is no meaningful single key tuple to attach (unlike the direct index.get() path,
    // whose single query keys flow through). The SQL SEARCH_INDEX function reads only RID + $score from these entries, never
    // getKeys(); a direct executor.search() caller should likewise not rely on getKeys() for this path.
    if (index.isBM25())
      return index.scoreCandidatesBM25(scoreMap.keySet(), scoringTokens, new Object[] {}, limit, guard);

    final ArrayList<IndexCursorEntry> list = new ArrayList<>(scoreMap.size());

    for (final Map.Entry<RID, AtomicInteger> entry : scoreMap.entrySet()) {
      tick();
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
              tick();
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
          tick();
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
          tick();
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
   * Records how many live documents in this bucket contain {@code storedKey}. A single query can walk the same token more than
   * once (a repeated term, or overlapping wildcard ranges such as {@code ap*} and {@code app*}); every such walk counts the whole
   * posting list, so the merge keeps the maximum rather than summing and double-counting.
   */
  private void recordDocumentFrequency(final String storedKey, final long frequency) {
    if (collectingExclusion || !index.isBM25() || frequency < 1)
      return;
    documentFrequencies.merge(storedKey, frequency, Math::max);
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
   * Returns true when a parsed query field denotes the "unqualified" target rather than a real, field-qualified clause. See
   * {@link #isUnqualified(List, String)}, which owns the rule for both query paths.
   */
  private boolean isUnqualified(final String field) {
    return isUnqualified(propertyNames, field);
  }

  /**
   * The one place the "is this qualifier really a field?" rule lives, consulted by both full-text query paths: this
   * Lucene-backed executor and the direct {@code get()} lookup in {@link LSMTreeFullTextIndex#parseQueryTerms}.
   * <p>
   * A term is unqualified when the parser left it on the {@link #DEFAULT_FIELD} sentinel, or when it is qualified with the
   * sole property of a single-property index: such an index stores only unprefixed postings (see
   * {@link LSMTreeFullTextIndex#put}), so {@code field:term} and {@code term} are equivalent there. This is what lets a
   * non-colliding sentinel be used without breaking single-property {@code content:term} queries. The property is compared
   * by its BASE name, since {@code obj.hd} is what a query can write for a property the index calls {@code obj.hd by item}.
   * <p>
   * Issue #6382 existed because the two paths had encoded this rule separately and drifted: the executor had it and the
   * direct path did not, so a single-property index answered nothing for any literal containing a colon. Both now call
   * here, so there is nothing left to drift (issue #6414, item 4).
   *
   * @param propertyNames the indexed property names, in index order
   * @param field         the field a parsed query term is qualified with, or null/empty for none
   */
  static boolean isUnqualified(final List<String> propertyNames, final String field) {
    if (field == null || field.isEmpty() || DEFAULT_FIELD.equals(field))
      return true;
    return propertyNames != null && propertyNames.size() == 1 && Index.basePropertyName(propertyNames.getFirst())
        .equals(field);
  }

  /**
   * Returns the BM25 field boost for a query field: the configured per-field boost for an explicit field, or 1.0 for an
   * unqualified term.
   */
  private float boostFor(final String field) {
    if (!isUnqualified(field) && metadata != null)
      // A per-field boost is declared under the property name the index carries ({@code keywords by item_boost}), the
      // same spelling the direct get() path looks up: the qualifier has to be resolved here too or a BY ITEM property's
      // boost is silently dropped by SEARCH_INDEX while CONTAINSTEXT applies it (issue #7000 review follow-up)
      return metadata.getFieldBoost(storedField(field));
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
    try {
      while (cursor.hasNext()) {
        tick();
        scoreMap.computeIfAbsent(cursor.next().getIdentity(), k -> new AtomicInteger(1));
      }
    } finally {
      // a compacted-series cursor registers with its file, so an abandoned one keeps a retired file alive for the
      // lifetime of the database
      cursor.close();
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

    // For field-specific queries (e.g., "title:java"), prepend the stored field name (see buildSearchKey)
    final String searchKey = buildSearchKey(field, text);

    recordScoringToken(searchKey, boostFor(field));
    if (tokensOnly)
      return;

    // Raw postings lookup: we only need the matching RIDs here. The BM25 score is computed once, later, by scoreCandidatesBM25;
    // going through index.get() would run (and then discard) the full scoring pipeline for every term.
    long df = 0L;
    try (final IndexCursor cursor = index.getPostings(searchKey)) {
      while (cursor.hasNext()) {
        tick();
        final RID rid = cursor.next().getIdentity();
        // Deletion markers carry a negative bucket id: they are not live documents and must not inflate the document frequency.
        if (rid.getBucketId() >= 0)
          ++df;
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }
    recordDocumentFrequency(searchKey, df);
  }

  private void collectPhraseMatches(final PhraseQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // For phrase queries, all terms must match in the same document, and then at the positions the phrase gives them
    // (keepPhraseMatches). Every phrase term is looked up under the SAME key a single-term query would use (buildSearchKey), so
    // a phrase qualified to a field stays within that field and takes that field's boost; looking the terms up by their bare
    // text matched the phrase across every indexed property while the single-term form of the same query was correctly
    // restricted (issue #7000).
    final Term[] terms = query.getTerms();
    if (terms.length == 0)
      return;

    if (tokensOnly) {
      for (final Term term : terms)
        recordScoringToken(buildSearchKey(term.field(), term.text()), boostFor(term.field()));
      return;
    }

    Map<RID, AtomicInteger> intersection = null;

    for (final Term term : terms) {
      final String searchKey = buildSearchKey(term.field(), term.text());
      recordScoringToken(searchKey, boostFor(term.field()));
      final Map<RID, AtomicInteger> termMatches = new HashMap<>();
      long df = 0L;
      try (final IndexCursor cursor = index.getPostings(searchKey)) {
        while (cursor.hasNext()) {
          tick();
          final RID rid = cursor.next().getIdentity();
          // Deletion markers carry a negative bucket id and must not inflate the document frequency.
          if (rid.getBucketId() >= 0)
            ++df;
          termMatches.put(rid, new AtomicInteger(1));
        }
      }
      recordDocumentFrequency(searchKey, df);

      if (intersection == null) {
        intersection = termMatches;
      } else {
        intersection.keySet().retainAll(termMatches.keySet());
      }
    }

    if (terms.length > 1)
      keepPhraseMatches(query, intersection.keySet());

    for (final RID rid : intersection.keySet()) {
      tick();
      scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).addAndGet(terms.length);
    }
  }

  /**
   * Keeps the records of {@code candidates} that hold the phrase: its terms at the positions the query gives them, moved by no more
   * than its slop in all, as Lucene's PhraseQuery matches them (issue #12165). The postings keep no position, so each candidate's
   * indexed values are analyzed again with the index analyzer. A qualified phrase is looked for in its property, an unqualified one in
   * each property of the index on its own: a phrase does not span two properties. The values of a list are one sequence of
   * positions, the analyzer's position increment gap apart, as Lucene indexes a multi-valued field.
   */
  private void keepPhraseMatches(final PhraseQuery query, final Set<RID> candidates) {
    final String field = query.getTerms()[0].field();
    final int property = isUnqualified(field) ? -1 : propertyNames.indexOf(storedField(field));
    if (property < 0 && !isUnqualified(field))
      // a qualifier naming no indexed property: its keys are literal tokens, which the values do not hold as such
      return;

    final PhrasePositions phrase = new PhrasePositions(query);
    final Analyzer indexAnalyzer = index.getIndexAnalyzer();
    final DatabaseInternal database = index.getDatabase();
    candidates.removeIf(rid -> {
      tick();
      if (rid.getBucketId() < 0)
        return false;
      final Record record;
      try {
        record = database.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        return true;
      }
      if (!(record instanceof final Document document))
        return true;
      final boolean[] found = new boolean[1];
      database.getIndexer().forEachIndexKey(index, document, keys -> {
        for (int i = 0; i < keys.length && !found[0]; i++)
          if ((property < 0 || property == i) && keys[i] != null)
            found[0] = phrase.matches(LSMTreeFullTextIndex.tokenPositions(indexAnalyzer, keys[i], phrase.terms()), this::tick);
      });
      return !found[0];
    });
  }

  /**
   * The terms of a phrase, their positions in the query and its slop. A record holds the phrase when each term can be given one of
   * its positions in the value, a term repeated in the phrase a different one each time, so that the positions less the query
   * positions spread over no more than the slop: Lucene's match length, 0 for an exact phrase.
   */
  private static final class PhrasePositions {
    private final String[]    terms;
    private final int[]       offsets;
    private final int         slop;
    private final Set<String> distinct;
    // per term, the index of the previous term of the phrase that is the same token, or -1
    private final int[]       previousSame;

    private PhrasePositions(final PhraseQuery query) {
      final Term[] queryTerms = query.getTerms();
      this.terms = new String[queryTerms.length];
      for (int i = 0; i < queryTerms.length; i++)
        terms[i] = queryTerms[i].text();
      // in ascending order: PhraseQuery takes its positions in order
      this.offsets = query.getPositions();
      this.slop = query.getSlop();
      this.distinct = Set.copyOf(Arrays.asList(terms));
      this.previousSame = new int[terms.length];
      for (int i = 0; i < terms.length; i++) {
        previousSame[i] = -1;
        for (int j = i - 1; j >= 0 && previousSame[i] < 0; j--)
          if (terms[j].equals(terms[i]))
            previousSame[i] = j;
      }
    }

    private Set<String> terms() {
      return distinct;
    }

    /**
     * A placement has a smallest offset (a position less its query position), the offset of one occurrence of one term: each of
     * those is tried as the bottom of a window {@code slop} wide, which holds the phrase when every term has a free position in it.
     * That is a number of windows linear in the occurrences, each decided in one pass over the terms, where trying every placement
     * of a repeated term grew with the factorial of its occurrences (review of #12180).
     *
     * @param positions per term, its count in the first cell and its positions in ascending order after it
     * @param tick      called once per window, so a terminated or timed-out statement stops inside the check
     */
    private boolean matches(final Map<String, int[]> positions, final Runnable tick) {
      final int[][] lists = new int[terms.length][];
      for (int i = 0; i < terms.length; i++) {
        lists[i] = positions.get(terms[i]);
        if (lists[i] == null)
          return false;
      }
      final int[] taken = new int[terms.length];
      for (int i = 0; i < terms.length; i++)
        for (int k = 1; k <= lists[i][0]; k++) {
          tick.run();
          if (fits(lists[i][k] - offsets[i], lists, taken))
            return true;
        }
      return false;
    }

    /**
     * Whether every term has a position whose offset lies between {@code low} and {@code low + slop}, a term repeated in the phrase a
     * later position each time. The terms are taken in query order, each given the first position of its range after the one the
     * same term took last. That loses no placement: the ranges of the occurrences of one term are equally wide and come in query
     * order, so the earliest position never takes one a later occurrence needed, and two occurrences of a term placed the other way
     * round spread no wider once swapped (each offset they would get lies between the two they had).
     */
    private boolean fits(final int low, final int[][] lists, final int[] taken) {
      for (int i = 0; i < terms.length; i++) {
        int from = low + offsets[i];
        if (previousSame[i] >= 0)
          from = Math.max(from, taken[previousSame[i]] + 1);
        final int[] list = lists[i];
        final int k = firstAtLeast(list, list[0], from);
        if (k > list[0] || list[k] > low + offsets[i] + slop)
          return false;
        taken[i] = list[k];
      }
      return true;
    }

    private static int firstAtLeast(final int[] list, final int count, final int value) {
      int low = 1;
      int high = count + 1;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        if (list[mid] < value)
          low = mid + 1;
        else
          high = mid;
      }
      return low;
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
    final String fieldPrefix = fieldPrefix(field);
    final Pattern regex = wildcardToRegex(pattern);
    // A sequence of several *'s (translated to .* by wildcardToRegex, same shape as %  in SQL LIKE) reproduces
    // catastrophic backtracking without needing grouping/alternation/nested quantifiers - issue #5886 follow-up.
    // One shared deadline for the whole scan, same rationale as collectRegexpMatches.
    final long deadline = TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(index.getDatabase()));

    if (literalPrefix.isEmpty()) {
      // Leading wildcard: full scan, then regex match against the unprefixed token portion
      iterateAndMatch(null, key -> {
        if (!key.startsWith(fieldPrefix))
          return false;
        final String token = key.substring(fieldPrefix.length());
        // Skip cross-field tokens for unqualified queries on multi-property indexes
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          return false;
        return TimeBoundRegex.matchesUntil(regex, token, deadline);
      }, scoreMap, boostFor(field));
    } else {
      // Range scan starting at the literal prefix and stop when keys no longer share it
      iterateAndMatch(searchPrefix, key -> {
        if (!key.startsWith(searchPrefix))
          return false;
        final String token = key.substring(fieldPrefix.length());
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          return false;
        return TimeBoundRegex.matchesUntil(regex, token, deadline);
      }, scoreMap, boostFor(field));
    }
  }

  /**
   * Matches the terms a fuzzy clause expands to: at most {@link FullTextIndexMetadata#getFuzzyMaxExpansions()} of them, the best
   * ones as Lucene's FuzzyQuery ranks them (see {@link #BEST_EXPANSION_FIRST}), chosen across the type when the coordinator chose
   * them (see {@link #withFuzzyExpansions}) and in this bucket otherwise (issue #12462).
   */
  private void collectFuzzyMatches(final FuzzyQuery query, final Map<RID, AtomicInteger> scoreMap) {
    Set<String> expansions = chosenFuzzyExpansions != null ? chosenFuzzyExpansions.get(query) : null;
    if (expansions == null)
      expansions = keysOf(fuzzyExpansions(query, maxExpansions()));
    if (expansions.isEmpty())
      return;

    // The walk spans the chosen keys only, and decides each posting with one lookup rather than one edit distance. The span is
    // bounded in the index's own key order (unsigned UTF-8 bytes, i.e. code points), not in String's UTF-16 order
    final TreeSet<String> sorted = new TreeSet<>(FullTextQueryExecutor::compareCodePoints);
    sorted.addAll(expansions);
    iterateAndMatch(sorted.first(), sorted.last(), expansions::contains, scoreMap, boostFor(query.getTerm().field()));
  }

  /** The most terms a fuzzy clause expands to (issue #12462): Lucene's FuzzyQuery default unless the index sets its own. */
  private int maxExpansions() {
    return metadata != null ? metadata.getFuzzyMaxExpansions() : FuzzyQuery.defaultMaxExpansions;
  }

  /**
   * One indexed term a fuzzy clause matches: its stored key, the token the edit distance was measured on (the key without its field
   * prefix), and the boost Lucene's FuzzyTermsEnum gives it, which is what ranks the expansions.
   */
  record FuzzyExpansion(String key, String token, float boost) {
  }

  /**
   * The order Lucene's FuzzyQuery keeps its expansions in (TopTermsRewrite over FuzzyTermsEnum): the higher boost first, and of two
   * terms with the same boost the lower one in term order, which for the UTF-8 bytes Lucene compares is code point order.
   */
  static final Comparator<FuzzyExpansion> BEST_EXPANSION_FIRST = (a, b) -> {
    final int byBoost = Float.compare(b.boost(), a.boost());
    return byBoost != 0 ? byBoost : compareCodePoints(a.token(), b.token());
  };

  /**
   * The boost FuzzyTermsEnum gives a term {@code distance} edits away from a query term of {@code termLength} code points: 1 for
   * the term itself, else {@code 1 - distance / min(token length, termLength)}, so an edit costs more in a shorter term.
   */
  static float fuzzyBoost(final String token, final int distance, final int termLength) {
    if (distance == 0)
      return 1.0f;
    final int minTermLength = Math.min(token.codePointCount(0, token.length()), termLength);
    return 1.0f - (float) distance / (float) minTermLength;
  }

  /** Code point order, which is the order of the UTF-8 bytes both Lucene's terms and the index's keys are sorted by. */
  static int compareCodePoints(final String a, final String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      final int ca = a.codePointAt(i);
      final int cb = b.codePointAt(j);
      if (ca != cb)
        return Integer.compare(ca, cb);
      i += Character.charCount(ca);
      j += Character.charCount(cb);
    }
    return Integer.compare(a.length() - i, b.length() - j);
  }

  private static Set<String> keysOf(final List<FuzzyExpansion> expansions) {
    final Set<String> keys = new HashSet<>(expansions.size() * 2);
    for (final FuzzyExpansion expansion : expansions)
      keys.add(expansion.key());
    return keys;
  }

  /**
   * The best {@code max} terms of this bucket a fuzzy clause expands to, best first (see {@link #BEST_EXPANSION_FIRST}). A term
   * counts once it has a live posting here: a deletion marker alone does not make it an expansion. Each distinct key is measured
   * once, however many postings it has.
   */
  List<FuzzyExpansion> fuzzyExpansions(final FuzzyQuery query, final int max) {
    final String term = normalizeText(query.getTerm().text());
    if (term.isEmpty())
      return List.of();

    final int maxEdits = query.getMaxEdits();
    final boolean transpositions = query.getTranspositions();
    final int termLength = term.codePointCount(0, term.length());
    final int prefixLen = Math.min(query.getPrefixLength(), term.length());
    final String fieldPrefix = fieldPrefix(query.getTerm().field());
    final String searchPrefix = fieldPrefix + term.substring(0, prefixLen);
    final boolean rangeScan = !searchPrefix.isEmpty();

    // The worst of the best kept so far at the head, so a better term replaces it
    final PriorityQueue<FuzzyExpansion> best = new PriorityQueue<>(Math.min(max, 1024) + 1, BEST_EXPANSION_FIRST.reversed());
    final IndexCursor cursor = index.iterateUnderlying(true, rangeScan ? new String[] { searchPrefix } : null, true);
    String lastKey = null;
    try {
      while (cursor.hasNext()) {
        tick();
        final RID rid = cursor.next().getIdentity();
        final Object[] keys = cursor.getKeys();
        if (keys == null || keys.length == 0 || keys[0] == null || rid.getBucketId() < 0)
          continue;
        final String key = keys[0].toString();
        if (key.equals(lastKey))
          continue;
        lastKey = key;
        if (!key.startsWith(searchPrefix)) {
          if (rangeScan && compareCodePoints(key, searchPrefix) > 0)
            break;
          continue;
        }
        final String token = key.substring(fieldPrefix.length());
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          continue;
        final int distance = fuzzyDistance(token, term, transpositions, maxEdits);
        if (distance > maxEdits)
          continue;
        final FuzzyExpansion expansion = new FuzzyExpansion(key, token, fuzzyBoost(token, distance, termLength));
        if (best.size() < max)
          best.add(expansion);
        else if (BEST_EXPANSION_FIRST.compare(expansion, best.peek()) < 0) {
          best.poll();
          best.add(expansion);
        }
      }
    } finally {
      cursor.close();
    }

    final List<FuzzyExpansion> result = new ArrayList<>(best);
    result.sort(BEST_EXPANSION_FIRST);
    return result;
  }

  /**
   * Chooses, for every fuzzy clause of a query, the terms it expands to across all the given buckets of one type (issue #12462).
   * Lucene's FuzzyQuery keeps the best {@code maxExpansions} terms of the whole index; a bucket choosing its own best ones would let
   * a type of n buckets expand to n times as many. The ranking depends on the term alone, so the best ones of the type are among
   * the best ones of each bucket. Answers null when there is nothing to choose across buckets: a single bucket, or no fuzzy clause.
   */
  static Map<FuzzyQuery, Set<String>> selectFuzzyExpansions(final List<LSMTreeFullTextIndex> bucketIndexes,
      final String queryText, final WorkGuard guard, final List<String> restrictedFields) {
    if (bucketIndexes.size() < 2)
      return null;
    final List<FuzzyQuery> fuzzyQueries = new ArrayList<>();
    collectFuzzyQueries(
        new FullTextQueryExecutor(bucketIndexes.getFirst(), guard, restrictedFields, null).parseQuery(queryText), fuzzyQueries);
    if (fuzzyQueries.isEmpty())
      return null;

    final List<FullTextQueryExecutor> executors = new ArrayList<>(bucketIndexes.size());
    for (final LSMTreeFullTextIndex ftIndex : bucketIndexes)
      executors.add(new FullTextQueryExecutor(ftIndex, guard, restrictedFields, null));
    final int max = executors.getFirst().maxExpansions();

    final Map<FuzzyQuery, Set<String>> selected = new HashMap<>();
    for (final FuzzyQuery fuzzy : fuzzyQueries) {
      if (selected.containsKey(fuzzy))
        continue;
      final Map<String, FuzzyExpansion> union = new HashMap<>();
      for (final FullTextQueryExecutor executor : executors)
        for (final FuzzyExpansion expansion : executor.fuzzyExpansions(fuzzy, max))
          union.putIfAbsent(expansion.key(), expansion);
      final List<FuzzyExpansion> ranked = new ArrayList<>(union.values());
      ranked.sort(BEST_EXPANSION_FIRST);
      selected.put(fuzzy, keysOf(ranked.size() > max ? ranked.subList(0, max) : ranked));
    }
    return selected;
  }

  /** Every fuzzy clause of a query, wherever {@link #collectMatches} reaches it, a prohibited one included. */
  private static void collectFuzzyQueries(final Query query, final List<FuzzyQuery> fuzzyQueries) {
    if (query instanceof final BooleanQuery bq) {
      for (final BooleanClause clause : bq.clauses())
        collectFuzzyQueries(clause.query(), fuzzyQueries);
    } else if (query instanceof final BoostQuery bq)
      collectFuzzyQueries(bq.getQuery(), fuzzyQueries);
    else if (query instanceof final FuzzyQuery fq)
      fuzzyQueries.add(fq);
  }

  /**
   * The edit distance Lucene's FuzzyQuery matches by (issue #12165): over code points, with a transposition of two adjacent
   * characters counted as one edit when the query allows transpositions, which it does by default (the optimal string alignment
   * distance), and as two otherwise. Answers {@code maxEdits + 1} at once when the lengths alone differ by more than
   * {@code maxEdits}.
   */
  static int fuzzyDistance(final String token, final String term, final boolean transpositions, final int maxEdits) {
    final int[] s = token.codePoints().toArray();
    final int[] t = term.codePoints().toArray();
    if (Math.abs(s.length - t.length) > maxEdits)
      return maxEdits + 1;
    int[] beforePrevious = new int[t.length + 1];
    int[] previous = new int[t.length + 1];
    int[] current = new int[t.length + 1];
    for (int j = 0; j <= t.length; j++)
      previous[j] = j;
    for (int i = 1; i <= s.length; i++) {
      current[0] = i;
      for (int j = 1; j <= t.length; j++) {
        int distance = Math.min(Math.min(previous[j], current[j - 1]) + 1, previous[j - 1] + (s[i - 1] == t[j - 1] ? 0 : 1));
        if (transpositions && i > 1 && j > 1 && s[i - 1] == t[j - 2] && s[i - 2] == t[j - 1])
          distance = Math.min(distance, beforePrevious[j - 2] + 1);
        current[j] = distance;
      }
      final int[] recycled = beforePrevious;
      beforePrevious = previous;
      previous = current;
      current = recycled;
    }
    return previous[t.length];
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

    final String fieldPrefix = fieldPrefix(field);
    // A user-supplied regexp query (/pattern/ syntax) is matched against every token in what is, absent a literal
    // prefix, a full index scan (issue #5886) - one shared deadline for the whole scan, not a fresh one per token,
    // otherwise a crafted index could still tie the thread up for token count * regexTimeout.
    final long deadline = TimeBoundRegex.newDeadline(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(index.getDatabase()));

    iterateAndMatch(null, key -> {
      if (!key.startsWith(fieldPrefix))
        return false;
      final String token = key.substring(fieldPrefix.length());
      if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
        return false;
      return TimeBoundRegex.matchesUntil(regex, token, deadline);
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
    iterateAndMatch(startKey, null, matcher, scoreMap, boost);
  }

  /**
   * {@link #iterateAndMatch(String, KeyMatcher, Map, float)} that, given a {@code lastKey}, stops past that key rather than past
   * the keys starting with {@code startKey}.
   */
  private void iterateAndMatch(final String startKey, final String lastKey, final KeyMatcher matcher,
      final Map<RID, AtomicInteger> scoreMap, final float boost) {
    final boolean rangeScan = startKey != null;
    final IndexCursor cursor = index.iterateUnderlying(true,
        rangeScan ? new String[] { startKey } : null, true);
    // Per-expanded-key document frequency for this walk, accumulated in a primitive cell (one allocation per distinct key
    // rather than a boxed Long per posting) and flushed once the range has been consumed.
    final Map<String, long[]> walkFrequencies = new HashMap<>();
    try {
      while (cursor.hasNext()) {
        tick();
        final RID rid = cursor.next().getIdentity();
        final Object[] keys = cursor.getKeys();
        if (keys == null || keys.length == 0 || keys[0] == null)
          continue;
        final String key = keys[0].toString();
        if (matcher.matches(key)) {
          recordScoringToken(key, boost);
          // Deletion markers carry a negative bucket id and must not inflate the document frequency.
          if (rid.getBucketId() >= 0)
            ++walkFrequencies.computeIfAbsent(key, k -> new long[1])[0];
          if (!tokensOnly)
            scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
        } else if (lastKey != null ? compareCodePoints(key, lastKey) > 0 :
            rangeScan && key.compareTo(startKey) > 0 && !key.startsWith(startKey)) {
          break;
        }
      }
    } finally {
      // the walk usually stops on the prefix boundary rather than on exhaustion, so this is the common path, not the
      // exceptional one: a compacted-series cursor left registered would keep a retired file alive until restart
      cursor.close();
    }

    for (final Map.Entry<String, long[]> frequency : walkFrequencies.entrySet())
      recordDocumentFrequency(frequency.getKey(), frequency.getValue()[0]);
  }

  /**
   * One unit of matching work: a posting read or a match combined. Checks {@link #guard} every 1024 of them, so a cancelled
   * or timed-out statement stops inside the walk rather than once the whole search is over (issue #9894).
   */
  private void tick() {
    guard.checkPeriodically(++guardTicks);
  }

  /**
   * Builds the index search key by prefixing the field name when needed. Multi-property indexes store entries as
   * {@code fieldName:token}; unqualified terms (see {@link #isUnqualified(String)}) target the unprefixed tokens. The
   * prefix is the field name in the spelling the postings use (see {@link #fieldPrefix(String)}), which is the ONLY way
   * a key may be built on the query side: every collector goes through here or through {@code fieldPrefix}, so the
   * read path cannot derive a key the write path never stored (issue #7000).
   */
  private String buildSearchKey(final String field, final String text) {
    return fieldPrefix(field) + text;
  }

  /**
   * The {@code fieldName:} prefix a qualified clause has to match, or the empty string for an unqualified one. The name
   * is resolved to the spelling {@link LSMTreeFullTextIndex#put} used, which for a property indexed with a modifier is
   * the modifier-qualified one ({@code keywords by item}) rather than the base name a query can write ({@code keywords}),
   * so a {@code BY ITEM} property is reachable by its declared name (issue #7000). A qualifier that names no indexed
   * property is kept verbatim, and matches nothing, as before.
   */
  private String fieldPrefix(final String field) {
    return isUnqualified(field) ? "" : storedField(field) + ":";
  }

  /**
   * The spelling of a qualified field the postings use, or the qualifier itself when it names no indexed property. The
   * one resolution shared by key building and analyzer normalization, so the two cannot disagree on which field a
   * clause is about.
   */
  private String storedField(final String field) {
    final String storedField = storedFieldFor(propertyNames, field);
    return storedField != null ? storedField : field;
  }

  /**
   * Answers the indexed property a query qualifier names, in the spelling the POSTINGS use, or {@code null} when the
   * qualifier names no indexed property. Kept next to {@link #isUnqualified(List, String)} because it is the other half
   * of the same rule, consulted by both full-text query paths: this executor and the direct {@code get()} lookup in
   * {@link LSMTreeFullTextIndex#parseQueryTerms}.
   * <p>
   * The two spellings differ for a property indexed with a modifier: {@link LSMTreeFullTextIndex#put} prefixes tokens
   * with the index's own property name ({@code obj.hd by item}) while a query can only write the base name
   * ({@code obj.hd}) - the modified one carries spaces and would not survive the parser. Matching on the base name and
   * answering with the stored one is what lets a field-qualified lookup reach those postings at all. The direct path
   * had this rule and the executor did not, which is issue #7000's second defect.
   */
  static String storedFieldFor(final List<String> propertyNames, final String qualifier) {
    if (propertyNames == null || qualifier == null)
      return null;
    for (final String property : propertyNames)
      if (property.equals(qualifier) || Index.basePropertyName(property).equals(qualifier))
        return property;
    return null;
  }

  /**
   * Normalizes a wildcard / fuzzy / regexp term the way the analyzer normalizes the tokens it stores, so both sides
   * fold identically whatever the analyzer: lower-casing for the standard analyzer, nothing at all for a
   * case-preserving one such as {@code WhitespaceAnalyzer}, where an unconditional {@code toLowerCase} turned
   * {@code Fo*} into a prefix the stored {@code Foo} could never match (issue #7000). This is also what the Lucene
   * QueryParser itself does for the prefix, wildcard and fuzzy forms; the regexp form it hands over untouched.
   */
  private String normalizeText(final String text) {
    if (text == null || text.isEmpty())
      return "";
    // Normalized under the one field name the write path tokenizes every property under (LSMTreeFullTextIndex.tokenize),
    // not under the query's qualifier: a field-aware analyzer folds by that name, and the stored tokens are what the
    // pattern has to match.
    return analyzer.normalize(LSMTreeFullTextIndex.ANALYZED_FIELD, text).utf8ToString();
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
