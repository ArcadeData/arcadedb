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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LocalVertexType extends LocalDocumentType implements VertexType {
  private final List<Bucket> additionalBuckets = new ArrayList<>();

  public LocalVertexType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  @Override
  public MutableVertex newRecord() {
    return schema.getDatabase().newVertex(name);
  }

  @Override
  public void rename(final String newName) {
    final String oldName = name;

    super.rename(newName);

    final List<Bucket> removedBuckets = new ArrayList<>();

    try {
      for (Bucket bucket : additionalBuckets) {
        final String oldBucketName = bucket.getName();

        if (!InternalBucketNaming.isEdgeListBucketName(oldBucketName))
          throw new SchemaException(
              "Cannot rename bucket '" + oldBucketName + "' because it does not follow the naming convention");

        // The bucket index and the edge marker both live in the suffix, which is carried over untouched.
        final String newBucketName = LocalSchema.rebaseComponentName(oldBucketName, oldName, newName, schema.getEncoding());
        if (newBucketName == null)
          // Edge bucket of a bucket attached with addBucket(): named after that bucket, not after the type.
          continue;

        ((LocalBucket) bucket).rename(newBucketName);

        removedBuckets.add(bucket);

        rekeyBucket(bucket, oldBucketName);
      }

      // SchemaException too: it is a RuntimeException, and letting it past this catch would leave the edge buckets
      // already renamed on disk with a schema.json that still names the old files.
    } catch (IOException | SchemaException e) {
      super.rename(oldName);

      boolean corrupted = false;
      for (Bucket bucket : removedBuckets) {
        final String renamedBucketName = bucket.getName();
        try {
          final String restoredName = LocalSchema.rebaseComponentName(renamedBucketName, newName, oldName,
              schema.getEncoding());
          if (restoredName == null)
            corrupted = true;
          else
            ((LocalBucket) bucket).rename(restoredName);
        } catch (IOException ex) {
          corrupted = true;
        } finally {
          rekeyBucket(bucket, renamedBucketName);
        }
      }

      if (corrupted)
        throw new SchemaException("Error on renaming type '" + oldName + "' in '" + newName
            + "'. The database schema is corrupted, check single file names for buckets " + removedBuckets, e);

      throw new SchemaException("Error on renaming type '" + oldName + "' in '" + newName + "'", e);
    }
  }

  @Override
  public List<Bucket> getInvolvedBuckets() {
    final ArrayList<Bucket> result = new ArrayList<>(super.getInvolvedBuckets());
    result.addAll(additionalBuckets);
    return result;
  }

  @Override
  protected void addBucketInternal(final Bucket bucket) {
    super.addBucketInternal(bucket);
    additionalBuckets.addAll(
        ((DatabaseInternal) schema.getDatabase()).getGraphEngine().createVertexAdditionalBuckets((LocalBucket) bucket));
  }
}
