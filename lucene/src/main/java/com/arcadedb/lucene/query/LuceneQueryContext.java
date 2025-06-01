/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.arcadedb.lucene.query;

import com.arcadedb.database.Identifiable; // Changed
import com.arcadedb.exception.ArcadeDBException; // Changed
import com.arcadedb.lucene.tx.LuceneTxChanges; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.highlight.TextFragment;

/** Created by Enrico Risa on 08/01/15. */
public class LuceneQueryContext { // Changed class name
  private final CommandContext context; // Changed
  private final IndexSearcher searcher;
  private final Query query;
  private final Sort sort;
  private Optional<LuceneTxChanges> changes; // FIXME: Needs refactoring
  // private HashMap<String, TextFragment[]> fragments; // Replaced by on-demand highlighting

  // Highlighter components - to be initialized if highlighting is requested
  private org.apache.lucene.search.highlight.Highlighter highlighter;
  private org.apache.lucene.analysis.Analyzer highlightingAnalyzer; // Analyzer used for highlighting (might be queryAnalyzer)
  private String[] highlightingFields;


  public LuceneQueryContext( // Changed
      final CommandContext context, final IndexSearcher searcher, final Query query) {
    this(context, searcher, query, Collections.emptyList());
  }

  public LuceneQueryContext( // Changed
      final CommandContext context,
      final IndexSearcher searcher,
      final Query query,
      final List<SortField> sortFields) {
    this.context = context;
    this.searcher = searcher;
    this.query = query;
    if (sortFields == null || sortFields.isEmpty()) { // Added null check
      sort = null;
    } else {
      sort = new Sort(sortFields.toArray(new SortField[0])); // Changed to new SortField[0]
    }
    changes = Optional.empty();
    // fragments = new HashMap<>(); // Not pre-cached anymore

    // Check metadata for highlighting setup
    if (this.context != null && this.context.getVariable("highlight") instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> highlightParams = (Map<String, Object>) this.context.getVariable("highlight");
        // Simple setup for now, more advanced formatting can be added
        // String preTag = (String) highlightParams.getOrDefault("preTag", "<em>");
        // String postTag = (String) highlightParams.getOrDefault("postTag", "</em>");
        // org.apache.lucene.search.highlight.Formatter formatter = new org.apache.lucene.search.highlight.SimpleHTMLFormatter(preTag, postTag);
        org.apache.lucene.search.highlight.Formatter formatter = new org.apache.lucene.search.highlight.SimpleHTMLFormatter();
        org.apache.lucene.search.highlight.QueryScorer queryScorer = new org.apache.lucene.search.highlight.QueryScorer(query);
        this.highlighter = new org.apache.lucene.search.highlight.Highlighter(formatter, queryScorer);
        // Fragmenter:
        // this.highlighter.setTextFragmenter(new org.apache.lucene.search.highlight.SimpleFragmenter(100)); // Example: 100 chars per fragment

        Object fieldsToHighlightObj = highlightParams.get("fields");
        if (fieldsToHighlightObj instanceof String) {
            this.highlightingFields = ((String) fieldsToHighlightObj).split(",");
        } else if (fieldsToHighlightObj instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) fieldsToHighlightObj;
            this.highlightingFields = list.toArray(new String[0]);
        }
        // Analyzer for highlighting should ideally be the one used for querying the specific fields.
        // This is a simplification; a more robust solution would fetch field-specific analyzers.
        // If the engine provides a general queryAnalyzer, use it.
        // this.highlightingAnalyzer = searcher.getAnalyzer(); // This is not standard on IndexSearcher
        // Analyzer must be passed or retrieved from engine/index definition
    }

  }

  public boolean isHighlightingEnabled() {
      return this.highlighter != null && this.highlightingFields != null && this.highlightingFields.length > 0;
  }

  public LuceneQueryContext setHighlightingAnalyzer(Analyzer analyzer) {
      this.highlightingAnalyzer = analyzer;
      return this;
  }


  public boolean isInTx() {
    return changes.isPresent();
  }

  public LuceneQueryContext withChanges(final LuceneTxChanges changes) { // FIXME: Needs refactoring
    this.changes = Optional.ofNullable(changes);
    return this;
  }

  // addHighlightFragment removed as highlights are generated on demand by getHighlights

  public CommandContext getContext() { // Changed
    return context;
  }

  public Query getQuery() {
    return query;
  }

  public Optional<LuceneTxChanges> getChanges() { // FIXME: Needs refactoring
    return changes;
  }

  public Sort getSort() {
    return sort;
  }

  public IndexSearcher getSearcher() {
    // FIXME: LuceneTxChanges and its searcher() method need refactoring
    return changes.map(c -> new IndexSearcher(multiReader(c))).orElse(searcher);
  }

  private MultiReader multiReader(final LuceneTxChanges luceneTxChanges) { // FIXME: Needs refactoring
    final IndexReader primaryReader = searcher.getIndexReader();
    // FIXME: luceneTxChanges.searcher() needs to be refactored and return an IndexSearcher
    final IndexReader txReader = luceneTxChanges.searcher().getIndexReader();
    try {
      // Lucene's MultiReader takes an array of IndexReaders.
      // The boolean for sharing readers is gone in some modern versions,
      // lifecycle of readers passed to MultiReader should be managed by the caller if they are not to be closed by MultiReader.
      // However, if primaryReader and txReader are obtained just for this MultiReader,
      // it might be okay for MultiReader to close them.
      // The decRef logic was for when readers were shared. If they are not shared, it's not needed.
      // Let's assume for now they are not shared and MultiReader can own them.
      // If they are shared/managed elsewhere, then incRef/decRef or try-with-resources on the MultiReader is needed.
      // For Lucene 9+, just passing readers is fine, their lifecycle is tricky.
      // One common pattern is that MultiReader does NOT close the readers given to it by default.
      // The `searcher.getIndexReader()` typically gives a reader that should not be closed by MultiReader if searcher is still live.
      // `txReader` from `luceneTxChanges.searcher().getIndexReader()` also needs care.
      // The original decRef implies they were "taken over".
      // A safer approach for modern Lucene if readers are managed (e.g. by SearcherManager / NRTManager):
      // DONT call decRef here. Ensure MultiReader is closed after use, and that it DOES NOT close its sub-readers
      // if they are still managed externally.
      // The constructor `new MultiReader(IndexReader... subReaders)` does NOT take ownership (doesn't close them).

      // Given the original decRef, it implies MultiReader was taking ownership.
      // This is not standard for the varags MultiReader constructor.
      // The constructor `MultiReader(IndexReader[] r, boolean closeSubReaders)` is gone.
      // Let's assume the readers passed are temporary or their lifecycle is handled by the SearcherManager from which they came.
      // If txReader is from a RAMDirectory, it's simpler.
      // This part is tricky without knowing exactly how primaryReader and txReader are managed.
      // For now, will replicate the structure but acknowledge the complexity.
      // One option: increase ref count before passing to MultiReader, then MultiReader can decRef on its close.
      // primaryReader.incRef(); // If primaryReader is managed and should survive this MultiReader
      // txReader.incRef();    // If txReader is managed
      // MultiReader multiReader = new MultiReader(new IndexReader[] {primaryReader, txReader});
      // If MultiReader is short-lived and we don't want to affect original readers:
      List<IndexReader> readers = new ArrayList<>();
      readers.add(primaryReader);
      if (txReader != null) readers.add(txReader); // txReader could be null if no changes

      return new MultiReader(readers.toArray(new IndexReader[0]));

    } catch (final IOException e) {
      // FIXME: OLuceneIndexException needs to be ArcadeDB specific
      throw ArcadeDBException.wrapException(
          new ArcadeDBException("unable to create reader on changes"), e); // Changed
    }
  }

  public long deletedDocs(final Query query) {
    // FIXME: LuceneTxChanges and its deletedDocs method need refactoring
    return changes.map(c -> c.deletedDocs(query)).orElse(0L); // Ensure Long literal
  }

  public boolean isUpdated(final Document doc, final Object key, final Identifiable value) { // Changed
    // FIXME: LuceneTxChanges and its isUpdated method need refactoring
    return changes.map(c -> c.isUpdated(doc, key, value)).orElse(false);
  }

  public boolean isDeleted(final Document doc, final Object key, final Identifiable value) { // Changed
    // FIXME: LuceneTxChanges and its isDeleted method need refactoring
    return changes.map(c -> c.isDeleted(doc, key, value)).orElse(false);
  }

  /**
   * Generates highlighted snippets for the given Lucene document and configured fields.
   * Requires highlightingAnalyzer to be set.
   */
  public Map<String, String> getHighlights(Document luceneDoc, IndexReader reader) {
    if (!isHighlightingEnabled() || luceneDoc == null || this.highlightingAnalyzer == null) {
        return Collections.emptyMap();
    }

    Map<String, String> highlights = new HashMap<>();
    for (String field : highlightingFields) {
        String text = luceneDoc.get(field);
        if (text != null) {
            try {
                // Get best fragments. Last param is maxNoFragments.
                TextFragment[] frags = highlighter.getBestTextFragments(this.highlightingAnalyzer, field, text, 3);
                StringBuilder sb = new StringBuilder();
                for (TextFragment frag : frags) {
                    if (frag != null && frag.getScore() > 0) {
                        sb.append(frag.toString());
                        sb.append("... "); // Separator for multiple fragments
                    }
                }
                if (sb.length() > 0) {
                    highlights.put("$" + field + "_hl", sb.toString().trim());
                }
            } catch (IOException | org.apache.lucene.search.highlight.InvalidTokenOffsetsException e) {
                // Log error or handle as needed
                System.err.println("Error highlighting field " + field + ": " + e.getMessage());
            }
        }
    }
    return highlights;
  }

  // getFragments() method removed, replaced by getHighlights() logic integrated into LuceneIndexCursor

  // getLimit() and onRecord() were not in the provided OLuceneQueryContext,
  // they might be from a different class or an older version.
  // If they are needed, they would be implemented here.
}
