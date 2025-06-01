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

  private final LuceneDocumentBuilder builder;
  private LuceneQueryBuilder queryBuilder;
  // bonsayFileId removed as it's not used for standard Lucene updates.
  // If a specific versioning or optimistic locking mechanism is needed for index entries,
  // it would require a different design, possibly involving specific fields in Lucene documents.

  public ArcadeLuceneFullTextIndexEngine(Storage storage, String idxName) {
    super(storage, idxName);
    builder = new LuceneDocumentBuilder();
  }

  @Override
  public void init(IndexMetadata indexMetadata) {
    // The super.init in OLuceneIndexEngineAbstract expects:
    // (String indexName, String indexType, IndexDefinition indexDefinition, boolean isAutomatic, Document metadata)
    // IndexMetadata (ArcadeDB) has: name, typeName (of Schema Type), algorithm, propertyNames, keyTypes, options, unique, automatic, associatedToBucket, nullStrategy.
    // It does not directly have a single "indexType" string in the sense of "LUCENE" or "FULLTEXT" - that's algorithm.
    // The "metadata" Document for super.init should be created from indexMetadata.getOptions().

    com.arcadedb.document.Document engineInitMetadata = new com.arcadedb.document.Document(getDatabase());
    if (indexMetadata.getOptions() != null) {
        engineInitMetadata.fromMap(indexMetadata.getOptions());
    }

    super.init(indexMetadata.getName(),
               indexMetadata.getAlgorithm(), // Pass algorithm as indexType
               indexMetadata, // Pass the whole IndexMetadata as IndexDefinition (it implements it)
               indexMetadata.isAutomatic(),
               engineInitMetadata);

    // queryBuilder uses the same options Document
    queryBuilder = new LuceneQueryBuilder(engineInitMetadata);
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
      final org.apache.lucene.document.Document ret, // Lucene Document
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
  public void update(
      final TransactionContext txContext, // Changed parameter name for clarity
      final Object key,
      final IndexKeyUpdater<Object> updater) {
    // A Lucene update is typically a delete followed by an add.
    // The 'key' here is what identifies the document(s) to be updated.
    // The 'updater' provides the new value(s)/Identifiable(s).

    // 1. Determine the new Identifiable that results from the update.
    // The updater.update(oldValue, ...) is meant to get the new value.
    // 'oldValue' for an index is usually the set of RIDs mapped to the key.
    // Since this is a full-text index, the 'key' itself might be complex.
    // For simplicity, if we assume the updater gives the *new complete Identifiable* to index:
    Object newValue = updater.update(null, null).getValue(); // Passing null for oldValue and bonsayFileId.

    if (!(newValue instanceof Identifiable)) {
        throw new IndexException("Updater did not provide an Identifiable value for Lucene index update. Key: " + key);
    }
    Identifiable newIdentifiable = (Identifiable) newValue;

    // 2. Delete old document(s) associated with the key.
    // This requires a query that uniquely identifies the old document(s) for this key.
    // If the key is the RID itself (e.g. auto index on @rid), then it's simple.
    // If the key is field values, and these values *might have changed*, then deleting by
    // the *old* key is important. The current `key` parameter should represent the old key.
    // However, IndexKeyUpdater is often used when the key itself doesn't change, but the RID does (e.g. unique index).
    // Or when the indexed content of the RID changes, but the RID (and key) remains the same.

    // Let's assume 'key' can identify the old document(s) and 'newIdentifiable' is the new state to index.
    // If the RID is constant and only content changes:
    // We need to re-build the Lucene document for newIdentifiable and use Lucene's updateDocument.

    // Simplest approach for now: delete by key, then put new document.
    // This assumes 'key' can uniquely identify the document via a query.
    // If 'key' is the set of indexed fields from the *old* version of the document:
    if (key != null) {
        Query deleteByOldKeyQuery = this.queryBuilder.query(this.indexDefinition, key, EMPTY_METADATA, this.queryAnalyzer(), getDatabase());
        try {
            this.deleteDocument(deleteByOldKeyQuery); // From OLuceneIndexEngineAbstract
        } catch (IOException e) {
            throw new IndexException("Error deleting old document during update for key: " + key, e);
        }
    } else if (newIdentifiable != null && newIdentifiable.getIdentity() != null) {
        // If key is null, but we have the new Identifiable's RID, try to delete by RID.
        // This is only safe if we are sure this RID was previously indexed and this is a true update.
        Query deleteByRidQuery = ArcadeLuceneIndexType.createQueryId(newIdentifiable);
         try {
            this.deleteDocument(deleteByRidQuery);
        } catch (IOException e) {
            throw new IndexException("Error deleting old document by RID during update for: " + newIdentifiable.getIdentity(), e);
        }
    } else {
         throw new IndexException("Cannot determine document to update for Lucene index. Key and new Identifiable are null.");
    }

    // 3. Put the new document state
    // The 'key' for put should be derived from the newIdentifiable's fields if it's an automatic index.
    // If it's a manual index, the 'key' might remain the same or be derived.
    // For now, assuming the 'key' parameter to 'update' is what we use to identify the document,
    // and the new content comes from 'newIdentifiable'.
    // The 'put' method will call buildDocument(key, newIdentifiable).
    put(txContext, key, newIdentifiable); // Pass the original key for now
  }

  @Override
  public void put(final TransactionContext atomicOperation, final Object key, final Object value) { // Changed OAtomicOperation
    updateLastAccess();
    openIfClosed();
    final org.apache.lucene.document.Document doc = buildDocument(key, (Identifiable) value); // Lucene Document
    addDocument(doc);
  }

  @Override
  public void put(TransactionContext atomicOperation, Object key, RID value) { // Changed OAtomicOperation, ORID
    updateLastAccess();
    openIfClosed();
    final org.apache.lucene.document.Document doc = buildDocument(key, value); // Lucene Document
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
  public org.apache.lucene.document.Document buildDocument(Object key, Identifiable value) { // Changed OIdentifiable, Lucene Document
    if (indexDefinition.isAutomatic()) {
      // builder is an instance of LuceneDocumentBuilder
      // LuceneDocumentBuilder.build expects: IndexDefinition, Object key, Identifiable value, Map<String, Boolean> collectionFields, Document metadata
      // collectionFields and metadata are available as protected members from OLuceneIndexEngineAbstract
      return builder.build(indexDefinition, key, value, this.collectionFields, this.metadata);
    } else {
      return putInManualindex(key, value);
    }
  }

  private static org.apache.lucene.document.Document putInManualindex(Object key, Identifiable oIdentifiable) { // Changed OIdentifiable, Lucene Document
    org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document(); // Lucene Document
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
        return queryBuilder.query(indexDefinition, (String) maybeQuery, new com.arcadedb.document.Document(getDatabase()) /*EMPTY_METADATA*/, queryAnalyzer(), getDatabase());
      } else {
        LuceneKeyAndMetadata q = (LuceneKeyAndMetadata) maybeQuery; // FIXME: LuceneKeyAndMetadata needs refactoring
        return queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer(), getDatabase());
      }
    } catch (final ParseException e) {
      throw new IndexException("Error parsing query for index '" + name + "'", e); // Changed exception
    }
  }

  @Override
  public Set<Identifiable> getInTx(Object key, LuceneTxChanges changes) { // Changed OIdentifiable, OLuceneTxChanges
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof LuceneKeyAndMetadata) { // FIXME: LuceneKeyAndMetadata needs refactoring
        LuceneKeyAndMetadata q = (LuceneKeyAndMetadata) key;
        Query luceneQuery = queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer(), getDatabase());

        CommandContext commandContext = q.getContext(); // LuceneKeyAndMetadata now has getContext()
        return getResults(luceneQuery, commandContext, changes, q.metadata);

      } else {
        Query luceneQuery = queryBuilder.query(indexDefinition, key, new com.arcadedb.document.Document(getDatabase()) /*EMPTY_METADATA*/, queryAnalyzer(), getDatabase());

        CommandContext commandContext = null;
        if (key instanceof LuceneCompositeKey) { // FIXME: LuceneCompositeKey needs refactoring
          commandContext = ((LuceneCompositeKey) key).getContext(); // Assuming LuceneCompositeKey might have a context
        }
        return getResults(luceneQuery, commandContext, changes, new com.arcadedb.document.Document(getDatabase())/*EMPTY_METADATA*/);
      }
    } catch (ParseException e) {
      throw new IndexException("Error parsing lucene query for index '" + name + "'", e); // Changed exception
    }
  }
}
