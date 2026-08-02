/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.File;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Builder class for manual indexes.
 * <p>
 * A manual index is not bound to any type or bucket: nothing populates it, and nothing can rebuild it. Its entries are
 * whatever the caller put in it, and every difference from a type index below follows from that one fact - which index
 * kinds can be built at all, and the refusal to replace an existing index implicitly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ManualIndexBuilder extends IndexBuilder<Index> {
  /**
   * The index kinds that can be created without a type. The other kinds read the indexed type and property names off
   * the {@link IndexMetadata} the type/bucket builders fill in ({@code FULL_TEXT}, {@code GEOSPATIAL}) or downcast the
   * builder to their own bucket-level subclass ({@code LSM_VECTOR}, {@code LSM_SPARSE_VECTOR}), neither of which the
   * manual path has. Refused here so the caller is told what is unsupported, instead of meeting a NullPointerException
   * or a ClassCastException from inside the index factory.
   */
  private static final EnumSet<Schema.INDEX_TYPE> SUPPORTED_INDEX_TYPES = EnumSet.of(Schema.INDEX_TYPE.LSM_TREE,
      Schema.INDEX_TYPE.HASH);

  protected ManualIndexBuilder(final DatabaseInternal database, final String indexName, final Type[] keyTypes) {
    super(database, Index.class);
    this.indexName = indexName;
    this.keyTypes = keyTypes;
  }

  /**
   * Never allowed on a manual index, and refused here rather than at {@link #create()} so the caller finds out at the
   * call that expresses the intent.
   * <p>
   * On a type index the replacement is a REBUILD: the entries are derived from the type's records, so the new index
   * ends up carrying the same data under the new definition. A manual index has no records behind it - its entries are
   * the only copy - so the same operation is an unrecoverable delete of user data wearing the name of a schema change.
   * An explicit {@code dropIndex} followed by a create says the same thing out loud.
   */
  @Override
  public IndexBuilder<Index> withReplaceIfIncompatible(final boolean replaceIfIncompatible) {
    if (replaceIfIncompatible)
      throw new UnsupportedOperationException(
          "Cannot replace the manual index '" + indexName + "' implicitly: unlike a type index it is not derived from "
              + "any record, so the replacement would drop its entries with nothing to rebuild them from. Drop the "
              + "index explicitly if the definition has to change");
    return this;
  }

  public Index create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // Both checked before the existing-index lookup below, which needs the requested index kind to decide whether the
    // index already carrying this name covers the request.
    if (indexType == null)
      throw new DatabaseMetadataException(
          "Cannot create the manual index '" + indexName + "' because indexType was not specified");

    if (!SUPPORTED_INDEX_TYPES.contains(indexType))
      throw new IllegalArgumentException(
          "Cannot create the manual index '" + indexName + "' as " + indexType + ": a manual index is not bound to a "
              + "type, and only " + SUPPORTED_INDEX_TYPES + " can be built without one");

    // Wait for any running async tasks (e.g., compaction) to complete before creating new index
    // This prevents NeedRetryException when creating multiple indexes sequentially on large datasets
    while (database.isAsyncProcessing())
      database.async().waitCompletion();

    final LocalSchema schema = database.getSchema().getEmbedded();

    final IndexInternal existing = schema.indexMap.get(indexName);
    if (existing != null) {
      if (!ignoreIfExists)
        throw new SchemaException("Cannot create index '" + indexName + "' because already exists");

      // "Ignore if exists" means the caller can live with what is already there - but only when what is already there
      // provides what was asked for. This branch used to compare the uniqueness alone (behind a dead
      // `x != null && x == null` null-strategy test) and DROP the existing index on a mismatch, taking its entries
      // with it, while a request for a different index kind was answered with whatever index carried the name
      // (issue #5765, the manual twin of #5675).
      if (satisfiesRequest(existing, indexType, unique))
        return existing;

      throw conflictWithExistingManualIndex(existing, indexType, unique);
    }

    return schema.recordFileChanges(() -> {
      final AtomicReference<IndexInternal> result = new AtomicReference<>();
      database.transaction(() -> {

        filePath = database.getDatabasePath() + File.separator + indexName;

        final IndexInternal index = schema.indexFactory.createIndex(this);

        result.set(index);

        // The index's PaginatedComponent has to be registered under the file id its pages are allocated against. The
        // test this replaced - `index instanceof PaginatedComponent` - never matched: both LSMTreeIndex and HashIndex
        // WRAP their component rather than being one. The file therefore stayed unknown to the schema and the commit
        // below failed resolving it, AFTER the WAL append, which fenced the whole database (issue #5765). Same
        // accessor the type-index path uses, so there is one answer to "which file does this index own".
        schema.registerFile(index.getComponent());

        schema.indexMap.put(indexName, index);

      }, false, 1, null, error -> {
        final IndexInternal indexToRemove = result.get();
        if (indexToRemove != null) {
          // Best-effort cleanup, and it must not throw: this callback runs on the way out of the failed transaction,
          // so an exception raised here REPLACES the failure the caller needs to see - which is how the unregistered
          // file above surfaced as a bare fence error with no cause attached.
          schema.indexMap.remove(indexName);
          try {
            indexToRemove.drop();
          } catch (final Exception e) {
            LogManager.instance()
                .log(this, Level.WARNING, "Error on dropping the partially created manual index '%s'", e, indexName);
          }
        }
      });

      return result.get();
    });
  }

  /**
   * Builds the error reported when an index already carries the requested name but does not provide what was asked for.
   * Separate from {@link #conflictWithExistingIndex} because a manual index is identified by its name rather than by a
   * type and a property set, and because the reason it is not replaced implicitly is the stronger one.
   */
  private static IllegalArgumentException conflictWithExistingManualIndex(final Index existing,
      final Schema.INDEX_TYPE requestedType, final boolean requestedUnique) {
    return new IllegalArgumentException(
        "Cannot create the manual index '" + existing.getName() + "' as " + requestedType + " (unique=" + requestedUnique
            + ") because an index with that name already exists as " + existing.getType() + " (unique="
            + existing.isUnique()
            + "). Drop the existing index first: it is not replaced implicitly because a manual index holds the only "
            + "copy of its entries - there are no records to rebuild them from");
  }
}
