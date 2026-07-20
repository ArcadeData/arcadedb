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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

/**
 * Builder class for schema types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeBuilder<T> {
  final DatabaseInternal database;
  final Class<T>         type;
  boolean                 ignoreIfExists    = false;
  String                  typeName;
  List<LocalDocumentType> superTypes;
  List<Bucket>            bucketInstances   = Collections.emptyList();
  int                     buckets;
  int                     pageSize;
  boolean                 edgeBidirectional = true;

  protected TypeBuilder(final DatabaseInternal database, final Class<T> type) {
    this.database = database;
    this.type = type;
    this.buckets = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    this.pageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
  }

  public T create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    final LocalSchema schema = database.getSchema().getEmbedded();

    // INVARIANT: creating a type is a check-then-act (look it up, then add the missing buckets and super
    // types) and must be atomic, otherwise two callers racing on the same name both observe the type as
    // incomplete and both try to associate the same bucket, the loser failing with "the bucket is already
    // associated to the type". The lookup runs under the shared lock so a type being built by another
    // thread - which publishes itself into schema.types BEFORE its buckets are added - is never observed
    // half-populated; anything that mutates re-checks under the exclusive lock. The fast path can also throw,
    // for the type-already-exists and wrong-type cases, exactly as the locked path does.
    final T alreadyComplete = database.executeInReadLock(() -> {
      final LocalDocumentType existing = schema.types.get(typeName);
      return existing != null && isComplete(existing) ? (T) checkExistingIsCompatible(existing) : null;
    });
    if (alreadyComplete != null)
      return alreadyComplete;

    return database.executeInWriteLock(() -> createInternal(schema));
  }

  /**
   * A type needs no further work from this builder when it already carries at least the requested number
   * of buckets and every requested super type.
   */
  private boolean isComplete(final LocalDocumentType t) {
    if (t.buckets.size() < buckets)
      return false;
    if (superTypes != null)
      for (final LocalDocumentType sup : superTypes)
        if (!t.getSuperTypes().contains(sup))
          return false;
    return true;
  }

  private LocalDocumentType checkExistingIsCompatible(final LocalDocumentType t) {
    if (!ignoreIfExists)
      throw new SchemaException("Cannot create type '" + typeName + "' because already exists");

    if (!type.isAssignableFrom(t.getClass())) {
      final String expectedType = type.isAssignableFrom(VertexType.class) ?
          "vertex" :
          type.isAssignableFrom(EdgeType.class) ? "edge" : "document";
      throw new SchemaException("Type '" + typeName + "' is not a " + expectedType + " type");
    }

    return t;
  }

  private T createInternal(final LocalSchema schema) {
    final LocalDocumentType t = schema.types.get(typeName);
    if (t != null) {
      checkExistingIsCompatible(t);

      if (t.buckets.size() < buckets) {
        // CREATE MISSING BUCKETS
        for (int i = t.buckets.size(); i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, schema.getEncoding()) + "_" + i;
          if (schema.existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            t.addBucket(schema.getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            t.addBucket(schema.createBucket(bucketName, pageSize));
        }
      }

      boolean modified = false;
      if (superTypes != null)
        for (LocalDocumentType sup : superTypes) {
          if (!t.getSuperTypes().contains(sup)) {
            t.addSuperType(sup);
            modified = true;
          }
        }

      if (modified) {
        schema.saveConfiguration();
        schema.updateSecurity();
      }

      return (T) t;
    }

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

    return schema.recordFileChanges(() -> {
      final LocalDocumentType c;
      if (type.equals(VertexType.class))
        c = new LocalVertexType(schema, typeName);
      else if (type.equals(EdgeType.class))
        c = new LocalEdgeType(schema, typeName, edgeBidirectional);
      else {
        c = new LocalDocumentType(schema, typeName);

        // CREATE ENTRY IN DICTIONARY IF NEEDED. THIS IS USED BY EMBEDDED DOCUMENT WHERE THE DICTIONARY ID IS SAVED
        schema.getDictionary().getIdByName(typeName, true);
      }

      schema.types.put(typeName, c);

      if (bucketInstances.isEmpty()) {
        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, schema.getEncoding()) + "_" + i;
          if (schema.existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(schema.getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(schema.createBucket(bucketName, pageSize));
        }
      } else {
        for (final Bucket bucket : bucketInstances)
          c.addBucket(bucket);
      }

      if (superTypes != null)
        for (LocalDocumentType sup : superTypes)
          c.addSuperType(sup);

      schema.saveConfiguration();
      schema.updateSecurity();

      return c;
    });
  }

  public TypeBuilder<T> withName(final String typeName) {
    this.typeName = typeName;
    return this;
  }

  public TypeBuilder<T> withSuperType(final String superType) {
    if (superTypes == null)
      superTypes = new ArrayList<>();
    superTypes.add((LocalDocumentType) database.getSchema().getType(superType));
    return this;
  }

  public TypeBuilder<T> withIgnoreIfExists(final boolean ignoreIfExists) {
    this.ignoreIfExists = ignoreIfExists;
    return this;
  }

  /**
   * Sets to zero to have a type that can only be embedded or abstract (no records of this direct type, but subtypes can)
   *
   * @param buckets Number of buckets to use. By default the {@link GlobalConfiguration:TYPE_DEFAULT_BUCKETS} configuration is used.
   */
  public TypeBuilder<T> withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    return this;
  }

  public TypeBuilder<T> withBuckets(final List<Bucket> bucketInstances) {
    this.bucketInstances = bucketInstances;
    this.buckets = 0;
    return this;
  }

  public TypeBuilder<T> withBidirectional(final boolean edgeBidirectional) {
    if (!type.isAssignableFrom(EdgeType.class))
      throw new UnsupportedOperationException("withBidirectional() on non edge type");
    this.edgeBidirectional = edgeBidirectional;
    return this;
  }

  public TypeBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }
}
