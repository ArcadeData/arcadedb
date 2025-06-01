/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arcadedb.lucene.index;

import com.arcadedb.lucene.OLuceneCrossClassIndexFactory;
import com.arcadedb.lucene.engine.OLuceneIndexEngine;
import com.arcadedb.database.OIdentifiable;
import com.arcadedb.database.exception.OInvalidIndexEngineIdException;
import com.arcadedb.database.index.OIndexMetadata;
import com.arcadedb.database.storage.OStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

public class ArcadeLuceneFullTextIndex_Old extends OLuceneIndexNotUnique {

  public ArcadeLuceneFullTextIndex_Old(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  public Document buildDocument(final Object key, OIdentifiable identifieable) {

    while (true)
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.buildDocument(key, identifieable);
            });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  public Query buildQuery(final Object query) {
    while (true)
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.buildQuery(query);
            });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  public Analyzer queryAnalyzer() {
    while (true)
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.queryAnalyzer();
            });
      } catch (final OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  public boolean isCollectionIndex() {
    while (true) {
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.isCollectionIndex();
            });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  public Analyzer indexAnalyzer() {
    while (true) {
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.indexAnalyzer();
            });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean isAutomatic() {
    return super.isAutomatic()
        || OLuceneCrossClassIndexFactory.LUCENE_CROSS_CLASS.equals(im.getAlgorithm());
  }
}
