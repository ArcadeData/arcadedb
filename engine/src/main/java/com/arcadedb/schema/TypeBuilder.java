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

import java.util.*;
import java.util.logging.*;

/**
 * Builder class for schema types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeBuilder<T> {
  final DatabaseInternal database;
  final Class<T>         type;
  boolean                    ignoreIfExists  = false;
  String                  typeName;
  List<LocalDocumentType> superTypes;
  List<Bucket>            bucketInstances = Collections.emptyList();
  int                        buckets;
  int                        pageSize;

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

    final LocalDocumentType t = schema.types.get(typeName);
    if (t != null) {
      if (type.isAssignableFrom(t.getClass()))
        return (T) t;

      final String expectedType = type.isAssignableFrom(VertexType.class) ?
          "vertex" :
          type.isAssignableFrom(EdgeType.class) ? "edge" : "document";
      throw new SchemaException("Type '" + typeName + "' is not a " + expectedType + " type");
    }

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

    if (schema.types.containsKey(typeName))
      throw new SchemaException("Type '" + typeName + "' already exists");

    return schema.recordFileChanges(() -> {
      final LocalDocumentType c;
      if (type.equals(VertexType.class))
        c = new LocalVertexType(schema, typeName);
      else if (type.equals(EdgeType.class))
        c = new LocalEdgeType(schema, typeName);
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

  public TypeBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }
}
