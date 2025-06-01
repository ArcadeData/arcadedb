/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.arcadedb.common.exception.OException;
import com.arcadedb.lucene.exception.OLuceneIndexException;
import com.arcadedb.lucene.tx.OLuceneTxChanges;
import com.arcadedb.database.OCommandContext;
import com.arcadedb.database.OIdentifiable;
import java.io.IOException;
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
public class OLuceneQueryContext {
  private final OCommandContext context;
  private final IndexSearcher searcher;
  private final Query query;
  private final Sort sort;
  private Optional<OLuceneTxChanges> changes;
  private HashMap<String, TextFragment[]> fragments;

  public OLuceneQueryContext(
      final OCommandContext context, final IndexSearcher searcher, final Query query) {
    this(context, searcher, query, Collections.emptyList());
  }

  public OLuceneQueryContext(
      final OCommandContext context,
      final IndexSearcher searcher,
      final Query query,
      final List<SortField> sortFields) {
    this.context = context;
    this.searcher = searcher;
    this.query = query;
    if (sortFields.isEmpty()) {
      sort = null;
    } else {
      sort = new Sort(sortFields.toArray(new SortField[] {}));
    }
    changes = Optional.empty();
    fragments = new HashMap<>();
  }

  public boolean isInTx() {
    return changes.isPresent();
  }

  public OLuceneQueryContext withChanges(final OLuceneTxChanges changes) {
    this.changes = Optional.ofNullable(changes);
    return this;
  }

  public OLuceneQueryContext addHighlightFragment(
      final String field, final TextFragment[] fieldFragment) {
    fragments.put(field, fieldFragment);
    return this;
  }

  public OCommandContext getContext() {
    return context;
  }

  public Query getQuery() {
    return query;
  }

  public Optional<OLuceneTxChanges> getChanges() {
    return changes;
  }

  public Sort getSort() {
    return sort;
  }

  public IndexSearcher getSearcher() {
    return changes.map(c -> new IndexSearcher(multiReader(c))).orElse(searcher);
  }

  private MultiReader multiReader(final OLuceneTxChanges luceneTxChanges) {
    final IndexReader primaryReader = searcher.getIndexReader();
    final IndexReader txReader = luceneTxChanges.searcher().getIndexReader();
    try {
      // Transfer ownership to the MultiReader so the index searcher can be released transparently.
      // Without this, the primary IndexReader will leak a refcount each time it is wrapped.
      MultiReader multiReader = new MultiReader(new IndexReader[] {primaryReader, txReader}, false);
      primaryReader.decRef();
      txReader.decRef();
      return multiReader;
    } catch (final IOException e) {
      throw OException.wrapException(
          new OLuceneIndexException("unable to create reader on changes"), e);
    }
  }

  public long deletedDocs(final Query query) {
    return changes.map(c -> c.deletedDocs(query)).orElse(0l);
  }

  public boolean isUpdated(final Document doc, final Object key, final OIdentifiable value) {
    return changes.map(c -> c.isUpdated(doc, key, value)).orElse(false);
  }

  public boolean isDeleted(final Document doc, final Object key, final OIdentifiable value) {
    return changes.map(c -> c.isDeleted(doc, key, value)).orElse(false);
  }

  public Map<String, TextFragment[]> getFragments() {
    return fragments;
  }
}
