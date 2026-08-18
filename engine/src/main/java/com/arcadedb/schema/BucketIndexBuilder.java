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
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builder class for bucket indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BucketIndexBuilder extends IndexBuilder<Index> {
  final String   typeName;
  final String   bucketName;
  final String[] propertyNames;

  protected BucketIndexBuilder(final DatabaseInternal database, final String typeName, final String bucketName,
      final String[] propertyNames) {
    super(database, Index.class);
    this.typeName = typeName;
    this.bucketName = bucketName;
    this.propertyNames = propertyNames;
  }

  @Override
  public IndexBuilder<Index> withType(Schema.INDEX_TYPE indexType) {
    if (indexType == Schema.INDEX_TYPE.LSM_VECTOR && !(this instanceof BucketLSMVectorIndexBuilder))
      return new BucketLSMVectorIndexBuilder(this);
    return super.withType(indexType);
  }

  public String[] getPropertyNames() {
    return propertyNames;
  }

  public String getTypeName() {
    return typeName;
  }

  /** Creates the sub-index on this builder's bucket, resolving the bucket from the type by name. */
  private Index createIndexOnBucket(final LocalSchema schema, final LocalDocumentType type, final Type[] keyTypes,
      final boolean build) {
    Bucket bucket = null;
    for (final Bucket b : type.getBuckets(true)) {
      if (bucketName.equals(b.getName())) {
        bucket = b;
        break;
      }
    }

    return schema.createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy,
        callback, propertyNames, null, batchSize, metadata, build);
  }

  @Override
  public Index create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // The FOURTH way into a bucket scan, and the one issue #6281's first pass missed. TypeIndexBuilder holds the same
    // quiescence before delegating to LocalSchema, and so does REBUILD INDEX - but this builder is reachable on its
    // own through the public Schema.buildBucketIndex(...), which is how CHECK DATABASE ... FIX rebuilds an index it
    // found damaged (DatabaseChecker). Reached that way it would scan a bucket that does not yet contain whatever an
    // async worker is still holding in its open batch, and produce the same silently incomplete index #6281 is about.
    // Holding it twice on the paths that already do is free: quiescence is reentrant per thread.
    //
    // A QUIESCENCE AND NOT JUST THE BARRIER OF #6281 (issue #6303, item 2). The barrier answers about the past, and
    // that is only half of what a build needs: the other half is that nothing writes DURING the scan. This method
    // used to reach for that half with a pause task per worker, scheduled and forgotten - the boolean scheduleTask
    // returns was discarded and nothing waited for a worker to actually reach the pause - so a task already queued
    // ahead of the pause could still be writing while the scan ran, and a worker could park holding an uncommitted
    // batch. quiesceAsync() commits each worker's batch and does not return until every one of them has confirmed it
    // is parked.
    //
    // Note it does NOT call database.async(), which CREATES the executor: the old code did, so building an index on a
    // database that had never touched the async API started a full set of worker threads only to park them.
    try (final AsyncQuiesce asyncPaused = database.quiesceAsync()) {

      final LocalSchema schema = database.getSchema().getEmbedded();

      if (propertyNames.length == 0)
        throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

      final LocalDocumentType type = schema.getType(typeName);

      // CHECK ALL THE PROPERTIES EXIST
      final Type[] keyTypes = new Type[propertyNames.length];
      int i = 0;

      for (final String propertyName : propertyNames) {
        if (type instanceof LocalEdgeType && (Property.OUT_PROPERTY.equals(propertyName) || Property.IN_PROPERTY.equals(propertyName))) {
          keyTypes[i++] = Type.LINK;
          continue;
        }

        final boolean isByItem = propertyName.endsWith(" by item");
        final String actualPropertyName = isByItem ? propertyName.substring(0, propertyName.length() - 8) : propertyName;

        final Property property = type.getPolymorphicPropertyIfExists(actualPropertyName);
        if (property == null)
          throw new SchemaException(
              "Cannot create the index on type '" + typeName + "." + actualPropertyName + "' because the property does not exist");

        keyTypes[i++] = isByItem ? Type.STRING : property.getType();
      }

      // Carry a caller-supplied logical name onto the metadata (mirrors TypeIndexBuilder#create): a bucket sub-index
      // rebuilt while it is the LAST sub-index of its TypeIndex drops the wrapper along with it (LocalSchema.dropIndex
      // removes a TypeIndex once it has no sub-index left), so LocalDocumentType#addIndexInternal cannot find an
      // existing TypeIndex to reattach to and mints a new one - which, without this, always takes the auto-derived
      // "typeName[properties]" form even for an explicitly-named index (issue #5791). Single-bucket types hit this on
      // every REBUILD, since their one sub-index is always the last one.
      if (indexName != null && !indexName.isEmpty()) {
        if (metadata == null)
          metadata = new IndexMetadata(typeName, propertyNames, -1);
        metadata.typeIndexName = indexName;
      }

      // Asked before the transactions below, while the answer is still about the transaction that was already there
      // (issue #6324, item 1). See IndexBuilder#buildSharesCallerTransaction.
      final boolean sharesCallerTransaction = buildSharesCallerTransaction();

      return schema.recordFileChanges(() -> {
        final AtomicReference<Index> result1 = new AtomicReference<>();

        if (!sharesCallerTransaction) {
          // ONE TRANSACTION, exactly as before issue #6324: with nothing of anybody else's to see, creating and
          // building in one go is both simpler and one commit cheaper.
          database.transaction(() -> {
            result1.set(createIndexOnBucket(schema, type, keyTypes, true));
            schema.saveConfiguration();
          }, false, maxAttempts, null, error -> dropPartiallyBuiltIndex(schema, result1.get()));
          return result1.get();
        }

        // TWO TRANSACTIONS, for the reasons spelled out at the same split in TypeIndexBuilder#create: the component
        // is created and COMMITTED on its own, because the schema entry that names it is written regardless of what
        // the caller's transaction does; the build then joins the caller's transaction, because a scan reads the
        // transaction it runs in and that is the only way it sees records the caller has written and not committed.
        database.transaction(() -> {
          result1.set(createIndexOnBucket(schema, type, keyTypes, false));
          schema.saveConfiguration();
        }, false, maxAttempts, null, error -> dropPartiallyBuiltIndex(schema, result1.get()));

        // Cleaned up from a try/catch of its own rather than from the transaction's error callback, because that
        // callback is NOT reached on every failure: a NeedRetryException or a DuplicatedKeyException raised inside a
        // JOINED transaction is rethrown immediately by LocalDatabase.transaction (issue #661 - retrying would roll
        // back a transaction it does not own), skipping the callback entirely. And a duplicate is exactly what this
        // build can now hit, since it sees the caller's own pending writes. buildCreatedIndex has already flipped the
        // index to AVAILABLE by then, so leaving it behind would register a half-built index that answers queries.
        try {
          database.transaction(() -> buildCreatedIndex(result1.get(), true), true, maxAttempts, null, null);
        } catch (final RuntimeException e) {
          dropPartiallyBuiltIndex(schema, result1.get());
          throw e;
        }

        return result1.get();
      });
    }
  }
}
