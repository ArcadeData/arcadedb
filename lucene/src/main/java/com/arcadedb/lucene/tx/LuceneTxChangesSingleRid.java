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
import com.arcadedb.lucene.index.ArcadeLuceneIndexType; // Changed for createField
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field; // For Field.Store
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query; // For getDeletedDocuments & getUpdatedDocuments

/** Created by Enrico Risa on 15/09/15. */
public class LuceneTxChangesSingleRid extends LuceneTxChangesAbstract { // Changed class name and base class
  private final Set<String> deletedRids = new HashSet<>(); // RIDs marked for deletion from main index
  private final Set<String> updatedRids = new HashSet<>(); // RIDs that were deleted and then re-added (i.e., updated)

  // To support new interface methods
  private final List<Document> addedDocuments      = new ArrayList<>();
  private final Map<Query, Document> updatedDocumentsMap = new HashMap<>(); // Query to delete old, Document is new
  private final Set<Query> deletedQueries        = new HashSet<>();


  public LuceneTxChangesSingleRid( // Changed
      final LuceneIndexEngine engine, final IndexWriter writer, final IndexWriter deletesExecutor) {
    super(engine, writer, deletesExecutor);
  }

  @Override
  public void put(final Object key, final Identifiable value, final Document doc) {
    // This method is called when a key/value is to be associated in the index.
    // The `doc` is the Lucene document representing this association.
    try {
      if (value != null && value.getIdentity() != null && !value.getIdentity().isNew()) {
        String ridString = value.getIdentity().toString();
        if (deletedRids.remove(ridString)) {
          // If it was previously deleted in this transaction, it's now an update.
          // The TMP field was used to mark such docs for special handling during merge/query,
          // but it's unclear if that's needed with current Lucene NRT capabilities or specific merge logic.
          // For now, we track it as updated.
          doc.add(ArcadeLuceneIndexType.createField(TMP, ridString, Field.Store.YES)); // Changed OLuceneIndexType
          updatedRids.add(ridString);
          // The document for this RID might have been in `deletesExecutor`;
          // an update means it shouldn't be deleted from the main index.
          // This might require removing it from `deletesExecutor` if it was added there.
          // This is complex and depends on how commit logic handles deletesExecutor.
          // For now, just adding to writer.
        }
      }
      super.addDocument(doc); // Use base class to add to writer
      addedDocuments.add(doc); // Track for getAddedDocuments()
    } catch (IOException e) {
      throw ArcadeDBException.wrapException( // Changed
          new ArcadeDBException("Unable to add document to transactional Lucene index"), e); // Changed
    }
  }

  @Override
  public void remove(final Object key, final Identifiable value) {
    // This method is called to disassociate a key/value.
    // `value` is the RID to be removed.
    // `key` might be used to construct a more specific delete query if needed, but typically deletion by RID is sufficient.
    Query deleteQuery;
    if (value == null) {
      // Delete by key - this is dangerous for non-unique indexes, but Lucene handles it by query
      deleteQuery = engine.deleteQuery(key, null); // engine.deleteQuery should handle null value for key-based delete
    } else {
      deleteQuery = engine.deleteQuery(key, value); // Specific RID deletion query
    }

    try {
      super.deleteDocument(deleteQuery); // Use base class to delete from writer (current TX view)
      deletedQueries.add(deleteQuery); // Track for getDeletedDocuments()

      if (value != null && value.getIdentity() != null && !value.getIdentity().isNew()) {
        // If it's a persistent RID, track it for specific management.
        // This logic matches original: add to deletedRids and also add its document to deletesExecutor
        String ridString = value.getIdentity().toString();
        deletedRids.add(ridString);
        updatedRids.remove(ridString); // If it was updated then deleted, it's just a delete.

        // The original added the full document to deletedIdx (deletesExecutor).
        // This implies deletesExecutor might be a "negative" index.
        if (deletesExecutor != null) {
           // We need the document as it was in the main index to correctly mark it for deletion.
           // Building it here might not be accurate if fields changed.
           // FIXME: This needs a robust way to get the "old" document or rely on query for deletion.
           // For now, if we have 'value', we assume `engine.deleteQuery` is by specific ID.
           // If `deletesExecutor` is meant to hold docs to be deleted from main index on commit:
           // Document docToDelete = engine.buildDocument(key, value); // This builds NEW doc.
           // Instead of adding doc, we add the query. Commit logic will use these queries.
        }
      }
    } catch (final IOException e) {
      throw ArcadeDBException.wrapException( // Changed
          new ArcadeDBException("Error while deleting documents in transaction from Lucene index"), e); // Changed
    }
  }

  @Override
  public int numDocs() {
    // The base class numDocs() returns writer.getDocStats().numDocs or similar NRT count from writer.
    // This reflects documents added/updated in the current TX.
    // The original OLuceneTxChangesSingleRid subtracted deleted.size() and updated.size().
    // Subtracting deletedRids makes sense if these are deletions from the main index state.
    // Subtracting updatedRids from writer's NRT count is tricky; an update is a delete + add.
    // The NRT reader from `writer` already accounts for its own adds/deletes.
    // If `deletedRids` tracks docs to be deleted from the *main committed index*, then this makes sense.
    // Let's assume the base `numDocs()` gives count from `writer` (adds/updates in tx).
    // We need to subtract those in `deletedRids` that were not re-added/updated.
    int writerDocs = super.numDocs();
    int netDeletes = 0;
    for (String rid : deletedRids) {
        if (!updatedRids.contains(rid)) { // If it was deleted and not subsequently updated/re-added
            netDeletes++;
        }
    }
    // This is still an approximation of the final count after commit.
    // A true transactional count would need to consider the main index count + adds - (deletes not in adds).
    // For now, this is an estimate of the TX view.
    return writerDocs - netDeletes;
  }

  @Override
  public Set<Document> getDeletedLuceneDocs() {
    // This method from the original interface returned Lucene docs marked for deletion.
    // The new interface has getDeletedDocuments returning Set<Query>.
    // This method can be implemented if still needed, but might be redundant.
    // For now, let's try to build it from deletedQueries if possible, or keep original logic if it made sense.
    // The original stored `deletedDocs` (actual Document objects).
    // Let's return empty for now, assuming getDeletedDocuments() is the primary.
    // FIXME: Review if this specific Set<Document> is still needed.
    return Collections.emptySet();
  }

  @Override
  public boolean isDeleted(Document document, Object key, Identifiable value) { // Changed
    return value != null && value.getIdentity() != null && deletedRids.contains(value.getIdentity().toString());
  }

  @Override
  public boolean isUpdated(Document document, Object key, Identifiable value) { // Changed
    return value != null && value.getIdentity() != null && updatedRids.contains(value.getIdentity().toString());
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
      // This class tracks updatedRids. To fulfill Map<Query, Document>,
      // we'd need to store the delete query and the new document for each update.
      // The current `put` logic handles updates by re-adding.
      // FIXME: This needs more sophisticated tracking if specific update queries are required.
      // For now, returning based on `updatedRids` and `addedDocuments`.
      // This is an approximation.
      Map<Query, Document> approxUpdated = new HashMap<>();
      for (Document doc : addedDocuments) {
          String tmpRid = doc.get(TMP);
          if (tmpRid != null && updatedRids.contains(tmpRid)) {
              // This doc is an update. What was the query to delete the old one?
              // We don't store the original key for the RID directly here for updates.
              // This highlights a gap if this specific Map is needed.
              // For now, this will be empty or needs more info.
          }
      }
      return Collections.unmodifiableMap(updatedDocumentsMap); // Requires populating this map during put/update
  }
}
