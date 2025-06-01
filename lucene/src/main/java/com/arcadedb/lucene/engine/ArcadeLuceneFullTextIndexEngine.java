/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2014 Orient Technologies.
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

package com.arcadedb.lucene.engine;

import static com.arcadedb.lucene.builder.LuceneQueryBuilder.EMPTY_METADATA; // FIXME: LuceneQueryBuilder needs refactoring

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordId;
import com.arcadedb.database.TransactionContext; // For AtomicOperation
import com.arcadedb.document.Document; // ArcadeDB Document
import com.arcadedb.engine.Storage;
import com.arcadedb.exception.IndexException; // Changed exception
import com.arcadedb.index.CompositeKey;
import com.arcadedb.index.IndexKeyUpdater;
import com.arcadedb.index.IndexMetadata;
import com.arcadedb.index.IndexValuesTransformer;
import com.arcadedb.index.engine.IndexValidator;
import com.arcadedb.lucene.builder.LuceneDocumentBuilder; // FIXME: Needs refactoring
import com.arcadedb.lucene.builder.LuceneQueryBuilder; // FIXME: Needs refactoring
import com.arcadedb.lucene.collections.ArcadeLuceneIndexTransformer; // FIXME: Needs refactoring
import com.arcadedb.lucene.collections.LuceneCompositeKey; // FIXME: Needs refactoring
import com.arcadedb.lucene.collections.LuceneResultSet; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneIndexType;
import com.arcadedb.lucene.query.LuceneKeyAndMetadata; // FIXME: Needs refactoring
import com.arcadedb.lucene.query.LuceneQueryContext;
import com.arcadedb.lucene.tx.LuceneTxChanges;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.schema.Type; // For manual index field creation
import com.arcadedb.utility.Pair; // Changed from ORawPair
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.lucene.document.Document; // Lucene Document
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.store.Directory;

public class ArcadeLuceneFullTextIndexEngine extends OLuceneIndexEngineAbstract implements LuceneIndexEngine { // Changed class, base, and interface
  private static final Logger logger =
      Logger.getLogger(ArcadeLuceneFullTextIndexEngine.class.getName()); // Changed logger

  private final LuceneDocumentBuilder builder; // FIXME: Needs refactoring
  private LuceneQueryBuilder queryBuilder; // FIXME: Needs refactoring
  private final AtomicLong bonsayFileId = new AtomicLong(0); // TODO: Review if bonsayFileId is still relevant in ArcadeDB context

  // Removed 'id' parameter as it's not used by the superclass OLuceneIndexEngineAbstract
  // and not used internally in this class.
  public ArcadeLuceneFullTextIndexEngine(Storage storage, String idxName) { // Changed OStorage
    super(storage, idxName);
    builder = new LuceneDocumentBuilder(); // FIXME: Needs refactoring
  }

  @Override
  public void init(IndexMetadata im) { // Changed OIndexMetadata
    super.init(im.getName(), im.getType(), im.getDefinition(), im.isAutomatic(), im.getMetadata()); // FIXME: super.init might have changed
    // FIXME: getMetadata() on IndexMetadata might be different from OIndexMetadata.getMetadata()
    // queryBuilder = new LuceneQueryBuilder(im.getMetadata()); // FIXME: Needs refactoring and correct metadata access
    if (im.getDefinition() != null && im.getDefinition().getOptions() != null) {
       queryBuilder = new LuceneQueryBuilder(new Document(getDatabase(), im.getDefinition().getOptions())); // FIXME Needs correct metadata Document
    } else {
       queryBuilder = new LuceneQueryBuilder(new Document(getDatabase())); // Empty metadata if not available
    }
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {
    // FIXME: OLuceneIndexWriterFactory needs to be ArcadeLuceneIndexWriterFactory
    // OLuceneIndexWriterFactory fc = new OLuceneIndexWriterFactory();
    // logger.log(Level.FINE, "Creating Lucene index in ''{0}''...", directory);
    // return fc.createIndexWriter(directory, metadata, indexAnalyzer());
    throw new UnsupportedOperationException("ArcadeLuceneIndexWriterFactory not yet implemented");
  }

  @Override
  public void onRecordAddedToResultSet( // Changed parameter types
      final LuceneQueryContext queryContext,
      final RecordId recordId, // Changed OContextualRecordId
      final Document ret, // Lucene Document
      final ScoreDoc score) {
    HashMap<String, Object> data = new HashMap<String, Object>();

    final Map<String, TextFragment[]> frag = queryContext.getFragments();
    frag.forEach(
        (key, fragments) -> {
          final StringBuilder hlField = new StringBuilder();
          for (final TextFragment fragment : fragments) {
            if ((fragment != null) && (fragment.getScore() > 0)) {
              hlField.append(fragment.toString());
            }
          }
          data.put("$" + key + "_hl", hlField.toString());
        });
    data.put("$score", score.score);

    // recordId.setContext(data); // FIXME: RecordId in ArcadeDB does not have setContext. How to pass this data?
                               // This might need a wrapper class or different result handling.
  }

  @Override
  public boolean remove(final TransactionContext atomicOperation, final Object key) { // Changed OAtomicOperation
    return remove(key);
  }

  @Override
  public boolean remove(TransactionContext atomicOperation, Object key, RID value) { // Changed OAtomicOperation, ORID
    return remove(key, value);
  }

  @Override
  public Object get(final Object key) {
    return getInTx(key, null);
  }

  @Override
  public void update( // Changed OAtomicOperation, OIndexKeyUpdater
      final TransactionContext atomicOperation,
      final Object key,
      final IndexKeyUpdater<Object> updater) {
    // FIXME: bonsayFileId might not be relevant. updater.update might change.
    put(atomicOperation, key, updater.update(null, bonsayFileId).getValue());
  }

  @Override
  public void put(final TransactionContext atomicOperation, final Object key, final Object value) { // Changed OAtomicOperation
    updateLastAccess();
    openIfClosed();
    final Document doc = buildDocument(key, (Identifiable) value); // Lucene Document
    addDocument(doc);
  }

  @Override
  public void put(TransactionContext atomicOperation, Object key, RID value) { // Changed OAtomicOperation, ORID
    updateLastAccess();
    openIfClosed();
    final Document doc = buildDocument(key, value); // Lucene Document
    addDocument(doc);
  }

  @Override
  public boolean validatedPut( // Changed OAtomicOperation, ORID, IndexEngineValidator
      TransactionContext atomicOperation,
      Object key,
      RID value,
      IndexValidator<Object, RID> validator) {
    throw new UnsupportedOperationException(
        "Validated put is not supported by ArcadeLuceneFullTextIndexEngine");
  }

  @Override
  public Stream<Pair<Object, RID>> iterateEntriesBetween( // Changed ORawPair, ORID, IndexEngineValuesTransformer
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      IndexValuesTransformer transformer) {
    // FIXME: OLuceneResultSet and LuceneIndexTransformer need refactoring
    return ArcadeLuceneIndexTransformer.transformToStream((LuceneResultSet) get(rangeFrom), rangeFrom);
  }

  private Set<Identifiable> getResults( // Changed OIdentifiable, OCommandContext, OLuceneTxChanges, ODocument
      final Query query,
      final CommandContext context,
      final LuceneTxChanges changes,
      final Document metadata) { // ArcadeDB Document for metadata
    // sort
    // FIXME: OLuceneIndexEngineUtils.buildSortFields needs refactoring
    // final List<SortField> fields = OLuceneIndexEngineUtils.buildSortFields(metadata);
    final List<SortField> fields = null; // Placeholder
    final IndexSearcher luceneSearcher = searcher();
    final LuceneQueryContext queryContext =
        new LuceneQueryContext(context, luceneSearcher, query, fields).withChanges(changes);
    // FIXME: OLuceneResultSet needs refactoring to LuceneResultSet
    return new LuceneResultSet(this, queryContext, metadata);
  }

  @Override
  public Stream<Pair<Object, RID>> iterateEntriesMajor( // Changed ORawPair, ORID, IndexEngineValuesTransformer
      Object fromKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<Pair<Object, RID>> iterateEntriesMinor( // Changed ORawPair, ORID, IndexEngineValuesTransformer
      Object toKey,
      boolean isInclusive,
      boolean ascSortOrder,
      IndexValuesTransformer transformer) {
    return null;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public void updateUniqueIndexVersion(Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(Object key) {
    return 0; // not implemented
  }

  @Override
  public Document buildDocument(Object key, Identifiable value) { // Changed OIdentifiable, Lucene Document
    if (indexDefinition.isAutomatic()) {
      // builder is an instance of LuceneDocumentBuilder
      // LuceneDocumentBuilder.build expects: IndexDefinition, Object key, Identifiable value, Map<String, Boolean> collectionFields, Document metadata
      // collectionFields and metadata are available as protected members from OLuceneIndexEngineAbstract
      return builder.build(indexDefinition, key, value, this.collectionFields, this.metadata);
    } else {
      return putInManualindex(key, value);
    }
  }

  private static Document putInManualindex(Object key, Identifiable oIdentifiable) { // Changed OIdentifiable, Lucene Document
    Document luceneDoc = new Document(); // Lucene Document
    luceneDoc.add(ArcadeLuceneIndexType.createRidField(oIdentifiable));
    // The ID field for manual indexes might store the key itself if simple, or a hash if complex.
    // createIdField might be more about a specific format if needed.
    // For now, let's assume the key itself or its parts are added below with specific field names.
    // If a single "ID" field representing the whole key is desired for searching the key:
    // luceneDoc.add(ArcadeLuceneIndexType.createIdField(oIdentifiable, key));


    if (key instanceof CompositeKey) {
      List<Object> keys = ((CompositeKey) key).getKeys();
      // If this manual index has a definition with field names for composite parts:
      List<String> definedFields = null;
      // Type[] definedTypes = null; // Not directly available for manual index key parts in IndexDefinition easily
      // if (indexDefinition != null) { // indexDefinition is not available in this static context directly
      //    definedFields = indexDefinition.getFields();
      //    // definedTypes = indexDefinition.getTypes(); // This is for the main value, not necessarily for key parts
      // }

      for (int i = 0; i < keys.size(); i++) {
        Object subKey = keys.get(i);
        if (subKey == null) continue;
        String fieldName = (definedFields != null && i < definedFields.size()) ? definedFields.get(i) : "k" + i;
        Type type = Type.getTypeByValue(subKey);
        // For manual keys, typically store and index them. Sorting is less common for manual keys.
        List<Field> fields = ArcadeLuceneIndexType.createFields(fieldName, subKey, Field.Store.YES, false, type);
        for (Field f : fields) {
            luceneDoc.add(f);
        }
      }
    } else if (key instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> keys = (Collection<Object>) key;
      int i = 0;
      for (Object item : keys) {
        if (item == null) continue;
        String fieldName = "k" + i; // Implicit field name for collection items
        Type type = Type.getTypeByValue(item);
        List<Field> fields = ArcadeLuceneIndexType.createFields(fieldName, item, Field.Store.YES, false, type);
         for (Field f : fields) {
            luceneDoc.add(f);
        }
        i++;
      }
    } else if (key != null) {
      // Single key
      // String fieldName = (indexDefinition != null && !indexDefinition.getFields().isEmpty()) ? indexDefinition.getFields().get(0) : "k0";
      String fieldName = "k0"; // Default field name for single manual key
      Type type = Type.getTypeByValue(key);
      // Store.NO was used in original for single key; this means it's indexed but not retrievable from Lucene doc.
      // Let's make it configurable or default to YES for consistency if this key is what user searches.
      // For now, keeping Store.NO to match original hint, but this is questionable.
      // If it's the actual key to be searched, it should likely be YES or its components stored.
      // Given createFields also adds Point fields which are not stored, this might be okay.
      List<Field> fields = ArcadeLuceneIndexType.createFields(fieldName, key, Field.Store.NO, false, type);
       for (Field f : fields) {
            luceneDoc.add(f);
        }
    }
    return luceneDoc;
  }

  @Override
  public Query buildQuery(final Object maybeQuery) {
    try {
      if (maybeQuery instanceof String) {
        // FIXME: queryBuilder (LuceneQueryBuilder) needs refactoring
        return queryBuilder.query(indexDefinition, (String) maybeQuery, new Document(getDatabase()) /*EMPTY_METADATA*/, queryAnalyzer());
      } else {
        LuceneKeyAndMetadata q = (LuceneKeyAndMetadata) maybeQuery; // FIXME: LuceneKeyAndMetadata needs refactoring
        // FIXME: queryBuilder (LuceneQueryBuilder) needs refactoring
        return queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer());
      }
    } catch (final ParseException e) {
      throw new IndexException("Error parsing query", e); // Changed exception
    }
  }

  @Override
  public Set<Identifiable> getInTx(Object key, LuceneTxChanges changes) { // Changed OIdentifiable, OLuceneTxChanges
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof LuceneKeyAndMetadata) { // FIXME: LuceneKeyAndMetadata needs refactoring
        LuceneKeyAndMetadata q = (LuceneKeyAndMetadata) key;
        // FIXME: queryBuilder (LuceneQueryBuilder) needs refactoring
        Query query = queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer());

        CommandContext commandContext = q.key.getContext(); // FIXME: LuceneKeyAndMetadata.key might not have getContext
        return getResults(query, commandContext, changes, q.metadata);

      } else {
        // FIXME: queryBuilder (LuceneQueryBuilder) needs refactoring
        Query query = queryBuilder.query(indexDefinition, key, new Document(getDatabase()) /*EMPTY_METADATA*/, queryAnalyzer());

        CommandContext commandContext = null;
        if (key instanceof LuceneCompositeKey) { // FIXME: LuceneCompositeKey needs refactoring
          commandContext = ((LuceneCompositeKey) key).getContext();
        }
        return getResults(query, commandContext, changes, new Document(getDatabase())/*EMPTY_METADATA*/);
      }
    } catch (ParseException e) {
      throw new IndexException("Error parsing lucene query", e); // Changed exception
    }
  }
}
