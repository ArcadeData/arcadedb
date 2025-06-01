/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  * Copyright 2023 Arcade Data Ltd
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

package com.arcadedb.lucene.tx;

import com.arcadedb.database.Identifiable; // Changed
import com.arcadedb.exception.ArcadeDBException; // Changed
import com.arcadedb.lucene.engine.LuceneIndexEngine; // Changed
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer; // For isDeleted/isUpdated with Analyzer
import org.apache.lucene.analysis.core.KeywordAnalyzer; // For MemoryIndex in isDeleted
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex; // For isDeleted
import org.apache.lucene.search.Query;

/** Created by Enrico Risa on 15/09/15. */
public class LuceneTxChangesMultiRid extends LuceneTxChangesAbstract { // Changed class name and base class
  // Stores RID string to a list of associated keys that were part of a delete operation for that RID.
  private final Map<String, List<Object>> deletedRidToKeys = new HashMap<>();

  // To support new interface methods
  private final List<Document> addedDocuments      = new ArrayList<>();
  // For MultiRid, an "update" is typically a delete of an old key-RID pair (doc) and an add of a new one.
  // Tracking specific "updates" as Query->Document is complex here if not just delete+add.
  private final Map<Query, Document> updatedDocumentsMap = new HashMap<>();
  private final Set<Query> deletedQueries        = new HashSet<>();


  public LuceneTxChangesMultiRid( // Changed
      final LuceneIndexEngine engine, final IndexWriter writer, final IndexWriter deletesExecutor) {
    super(engine, writer, deletesExecutor);
  }

  @Override
  public void put(final Object key, final Identifiable value, final Document doc) {
    try {
      super.addDocument(doc); // Use base class to add to writer
      addedDocuments.add(doc); // Track for getAddedDocuments()
    } catch (IOException e) {
      throw ArcadeDBException.wrapException( // Changed
          new ArcadeDBException("Unable to add document to transactional Lucene index for multi-RID"), e); // Changed
    }
  }

  @Override
  public void remove(final Object key, final Identifiable value) {
    Query deleteQuery;
    if (value == null) { // Delete by key - affects all RIDs for this key
        deleteQuery = engine.deleteQuery(key, null);
    } else { // Delete a specific key-RID association
        deleteQuery = engine.deleteQuery(key, value);
    }

    try {
      super.deleteDocument(deleteQuery); // Apply to current transaction's writer
      deletedQueries.add(deleteQuery); // Track query for getDeletedDocuments()

      if (value != null && value.getIdentity() != null && !value.getIdentity().isNew()) {
        // Track that this RID was involved in a delete operation with this key
        String ridString = value.getIdentity().toString();
        deletedRidToKeys.computeIfAbsent(ridString, k -> new ArrayList<>()).add(key);

        // Original logic added the specific doc to deletedIdx (deletesExecutor).
        // This implies deletesExecutor might track full documents to be deleted from the main index.
        // If super.deleteDocument also routes to deletesExecutor based on query, this might be redundant
        // or requires deletesExecutor to handle full document additions for its own logic.
        // For now, let's assume super.deleteDocument(query) is sufficient for deletesExecutor if it's configured for queries.
        // If deletesExecutor *must* have the full doc:
        // final Document docToDelete = engine.buildDocument(key, value); // FIXME: engine.buildDocument dependency
        // if (deletesExecutor != null) deletesExecutor.addDocument(docToDelete);
      }
    } catch (final IOException e) {
      throw ArcadeDBException.wrapException( // Changed
          new ArcadeDBException("Error while deleting documents in transaction from Lucene index (multi-RID)"), e); // Changed
    }
  }

  @Override
  public int numDocs() {
    // The base class numDocs() provides NRT view of `writer`.
    // Original OLuceneTxChangesMultiRid subtracted deletedDocs.size().
    // `deletedDocs` (now represented by deletedQueries or deletedRidToKeys) refers to deletions
    // that will be applied to the main index.
    // A precise count is complex. For now, relying on base class numDocs which reflects writer's current state.
    // A more accurate count of "net new documents in this TX" would be addedDocuments.size() minus
    // documents that were added then deleted within the same TX (if tracked).
    // If numDocs should reflect the final state after commit, it's more complex.
    // Let's return the NRT view of the current writer.
    return super.numDocs();
  }

  @Override
  public Set<Document> getDeletedLuceneDocs() {
    // The original stored actual Document objects that were deleted.
    // This is hard to reconstruct if we only store queries or (RID,Key) pairs.
    // FIXME: If this exact Set<Document> is needed, logic in remove() must re-build and store them.
    // For now, returning empty as per LuceneTxChangesSingleRid refactoring.
    return Collections.emptySet();
  }

  @Override
  public boolean isDeleted(final Document document, final Object key, final Identifiable value) { // Changed
    if (value == null || value.getIdentity() == null) return false;

    final List<Object> associatedKeys = deletedRidToKeys.get(value.getIdentity().toString());
    if (associatedKeys != null) {
      // Check if the provided 'key' (or a general match for the document) is among those deleted for this RID
      if (associatedKeys.contains(key)) return true; // Exact key match

      // More complex check: does the 'document' match any of the delete operations for this RID?
      // This matches the original MemoryIndex check.
      final MemoryIndex memoryIndex = new MemoryIndex();
      // Populate memoryIndex with the fields of the 'document' parameter
      for (final IndexableField field : document.getFields()) {
        // TODO: This needs proper handling for different field types.
        // stringValue() might not be universally appropriate.
        // Using KeywordAnalyzer, so it's mostly for exact term matching.
        // This part is tricky and might need to use the actual field value from IndexableField.
        // For now, assuming stringValue is a simplified placeholder.
         if (field.stringValue() != null) { // MemoryIndex cannot add null values
            memoryIndex.addField(field.name(), field.stringValue(), new KeywordAnalyzer());
         }
      }

      for (final Object deletedKey : associatedKeys) {
        // engine.deleteQuery should generate a query that identifies the specific key-RID pair
        final Query q = engine.deleteQuery(deletedKey, value); // Query for specific key-RID pair
        if (memoryIndex.search(q) > 0.0f) {
          return true; // The document matches one of the delete operations for this RID
        }
      }
    }
    return false;
  }

  @Override
  public boolean isUpdated(final Document document, final Object key, final Identifiable value) { // Changed
    // For MultiRid, an update is typically a delete of an old association and an add of a new one.
    // This class doesn't explicitly track "updates" in a separate set like SingleRid did.
    // One could argue an entry is "updated" if it was deleted and then re-added with the same RID but different key/doc.
    // However, without more state, this is hard to determine accurately here.
    // The original returned false.
    return false;
  }

  // Implementations for new methods from LuceneTxChanges interface
  @Override
  public List<Document> getAddedDocuments() {
      return Collections.unmodifiableList(addedDocuments);
  }

  @Override
  public Set<Query> getDeletedDocuments() {
      return Collections.unmodifiableSet(deletedQueries);
  }

  @Override
  public Map<Query, Document> getUpdatedDocuments() {
      // Updates are not explicitly tracked as Query->Document in this multi-value implementation.
      // An update is a delete of one Lucene document and an add of another.
      // To fulfill this, one might need to capture the delete query and the newly added document
      // if a "key" conceptually remains the same but its associated RIDs change.
      // For now, returning empty, as this requires more specific tracking.
      return Collections.unmodifiableMap(updatedDocumentsMap);
  }
}
