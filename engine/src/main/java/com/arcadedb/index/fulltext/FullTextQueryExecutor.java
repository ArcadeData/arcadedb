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

import com.arcadedb.database.RID;
import com.arcadedb.function.text.TextLevenshteinDistance;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
 * This class is thread-safe. QueryParser instances are created per search() invocation.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class FullTextQueryExecutor {
  private final LSMTreeFullTextIndex   index;
  private final Analyzer               analyzer;
  private final FullTextIndexMetadata  metadata;

  /**
   * Creates a new FullTextQueryExecutor for the given index.
   *
   * @param index the full-text index to search
   */
  public FullTextQueryExecutor(final LSMTreeFullTextIndex index) {
    this.index = index;
    this.analyzer = index.getAnalyzer();
    this.metadata = index.getFullTextMetadata();
  }

  /**
   * Creates a new QueryParser configured for this index.
   * QueryParser is not thread-safe, so we create a new instance per search.
   *
   * @return a configured QueryParser
   */
  private QueryParser createQueryParser() {
    final QueryParser parser = new QueryParser("content", analyzer);
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
    try {
      // Create parser per invocation for thread safety
      final QueryParser parser = createQueryParser();
      final Query query = parser.parse(queryString);
      return executeQuery(query, limit);
    } catch (final ParseException e) {
      throw new IndexException("Invalid search query: " + queryString, e);
    }
  }

  private IndexCursor executeQuery(final Query query, final int limit) {
    final Map<RID, AtomicInteger> scoreMap = new HashMap<>();
    final Set<RID> excluded = new HashSet<>();

    collectMatches(query, scoreMap, excluded);

    // Remove excluded RIDs
    for (final RID rid : excluded) {
      scoreMap.remove(rid);
    }

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

  private void collectMatches(final Query query, final Map<RID, AtomicInteger> scoreMap,
      final Set<RID> excluded) {

    if (query instanceof BooleanQuery) {
      final BooleanQuery bq = (BooleanQuery) query;

      // First pass: collect MUST_NOT terms
      for (final BooleanClause clause : bq.clauses()) {
        if (clause.occur() == BooleanClause.Occur.MUST_NOT) {
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
      } else {
        // No MUST clauses: SHOULD results are returned (standard OR behavior)
        for (final Map.Entry<RID, AtomicInteger> entry : shouldResults.entrySet()) {
          scoreMap.computeIfAbsent(entry.getKey(), k -> new AtomicInteger(0))
              .addAndGet(entry.getValue().get());
        }
      }

    } else if (query instanceof TermQuery) {
      collectTermMatches((TermQuery) query, scoreMap);
    } else if (query instanceof PhraseQuery) {
      collectPhraseMatches((PhraseQuery) query, scoreMap);
    } else if (query instanceof PrefixQuery) {
      collectPrefixMatches((PrefixQuery) query, scoreMap);
    } else if (query instanceof WildcardQuery) {
      collectWildcardMatches((WildcardQuery) query, scoreMap);
    } else if (query instanceof FuzzyQuery) {
      collectFuzzyMatches((FuzzyQuery) query, scoreMap);
    } else if (query instanceof RegexpQuery) {
      collectRegexpMatches((RegexpQuery) query, scoreMap);
    }
    // Other Lucene query types (e.g., TermRangeQuery) are intentionally ignored.
  }

  private void collectTermsForExclusion(final Query query, final Set<RID> excluded) {
    if (query instanceof TermQuery) {
      final String term = ((TermQuery) query).getTerm().text();
      final IndexCursor cursor = index.get(new Object[] { term });
      while (cursor.hasNext()) {
        excluded.add(cursor.next().getIdentity());
      }
    } else if (query instanceof BooleanQuery) {
      for (final BooleanClause clause : ((BooleanQuery) query).clauses()) {
        collectTermsForExclusion(clause.query(), excluded);
      }
    }
  }

  private void collectTermMatches(final TermQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getTerm().field();
    final String text = query.getTerm().text();

    // For field-specific queries (e.g., "title:java"), prepend field name
    // Multi-property indexes store tokens as "fieldName:token"
    final String searchKey;
    if (field != null && !field.isEmpty() && !"content".equals(field)) {
      searchKey = field + ":" + text;
    } else {
      searchKey = text;
    }

    final IndexCursor cursor = index.get(new Object[] { searchKey });
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
    }
  }

  private void collectPhraseMatches(final PhraseQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // For phrase queries, all terms must match in the same document
    // Note: We can't verify word order without position indexing, so we just require all terms
    final Term[] terms = query.getTerms();
    if (terms.length == 0)
      return;

    Map<RID, AtomicInteger> intersection = null;

    for (final Term term : terms) {
      final Map<RID, AtomicInteger> termMatches = new HashMap<>();
      final IndexCursor cursor = index.get(new Object[] { term.text() });
      while (cursor.hasNext()) {
        termMatches.put(cursor.next().getIdentity(), new AtomicInteger(1));
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
    iterateAndMatch(searchPrefix, key -> key.startsWith(searchPrefix), scoreMap);
  }

  private void collectWildcardMatches(final WildcardQuery query, final Map<RID, AtomicInteger> scoreMap) {
    final String field = query.getTerm().field();
    final String pattern = normalizeText(query.getTerm().text());
    if (pattern.isEmpty())
      return;

    // Compute the literal prefix (everything up to the first wildcard char)
    final String literalPrefix = extractLiteralPrefix(pattern);
    final String searchPrefix = buildSearchKey(field, literalPrefix);
    final String fieldPrefix = (field != null && !field.isEmpty() && !"content".equals(field)) ? field + ":" : "";
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
      }, scoreMap);
    } else {
      // Range scan starting at the literal prefix and stop when keys no longer share it
      iterateAndMatch(searchPrefix, key -> {
        if (!key.startsWith(searchPrefix))
          return false;
        final String token = key.substring(fieldPrefix.length());
        if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
          return false;
        return regex.matcher(token).matches();
      }, scoreMap);
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
    final String fieldPrefix = (field != null && !field.isEmpty() && !"content".equals(field)) ? field + ":" : "";
    final String searchPrefix = fieldPrefix + requiredPrefix;

    iterateAndMatch(searchPrefix.isEmpty() ? null : searchPrefix, key -> {
      if (!key.startsWith(searchPrefix))
        return false;
      final String token = key.substring(fieldPrefix.length());
      if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
        return false;
      return TextLevenshteinDistance.levenshteinDistance(token, term) <= maxEdits;
    }, scoreMap);
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

    final String fieldPrefix = (field != null && !field.isEmpty() && !"content".equals(field)) ? field + ":" : "";

    iterateAndMatch(null, key -> {
      if (!key.startsWith(fieldPrefix))
        return false;
      final String token = key.substring(fieldPrefix.length());
      if (fieldPrefix.isEmpty() && token.indexOf(':') >= 0)
        return false;
      return regex.matcher(token).matches();
    }, scoreMap);
  }

  /**
   * Iterates the underlying index from the given start key and accumulates RIDs whose keys pass the matcher.
   * Stops as soon as the matcher rejects a key when {@code startKey} is non-null (since keys are sorted
   * and a non-prefix means we've left the relevant range). When {@code startKey} is null, performs a full
   * scan and applies the matcher to every key.
   */
  private void iterateAndMatch(final String startKey, final KeyMatcher matcher, final Map<RID, AtomicInteger> scoreMap) {
    final boolean rangeScan = startKey != null;
    final IndexCursor cursor = index.iterateUnderlying(true,
        rangeScan ? new String[] { startKey } : null, true);
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      final Object[] keys = cursor.getKeys();
      if (keys == null || keys.length == 0 || keys[0] == null)
        continue;
      final String key = keys[0].toString();
      if (matcher.matches(key)) {
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      } else if (rangeScan && startKey != null && key.compareTo(startKey) > 0 && !key.startsWith(startKey)) {
        break;
      }
    }
  }

  /**
   * Builds the index search key by prefixing the field name when needed.
   * Multi-property indexes store entries as {@code fieldName:token}; the default {@code "content"} field
   * (used by the Lucene QueryParser when no field is specified) targets unqualified tokens.
   */
  private static String buildSearchKey(final String field, final String text) {
    if (field != null && !field.isEmpty() && !"content".equals(field))
      return field + ":" + text;
    return text;
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
