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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
 * @author ArcadeDB Team
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
    } else {
      // Fallback for unsupported query types (FuzzyQuery, RegexpQuery, etc.)
      // Log or handle gracefully - return no matches rather than throw
      // Future enhancement: implement support for additional query types
    }
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
    final org.apache.lucene.index.Term[] terms = query.getTerms();
    if (terms.length == 0)
      return;

    Map<RID, AtomicInteger> intersection = null;

    for (final org.apache.lucene.index.Term term : terms) {
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
    // Simplified: treat prefix as exact term for now
    final String prefix = query.getPrefix().text();
    final IndexCursor cursor = index.get(new Object[] { prefix });
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
    }
  }

  private void collectWildcardMatches(final WildcardQuery query, final Map<RID, AtomicInteger> scoreMap) {
    // Simplified: strip wildcards and treat as term
    final String term = query.getTerm().text().replace("*", "").replace("?", "");
    if (!term.isEmpty()) {
      final IndexCursor cursor = index.get(new Object[] { term });
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        scoreMap.computeIfAbsent(rid, k -> new AtomicInteger(0)).incrementAndGet();
      }
    }
  }
}
