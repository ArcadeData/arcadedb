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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripedEdgeList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalVertexType extends LocalDocumentType implements VertexType {
  // grown in place when a bucket is added to the type and read lock-free by the bucket listings: copy-on-write (#9989)
  private final    List<Bucket> additionalBuckets = new CopyOnWriteArrayList<>();
  // The name this type had while a rename is moving its super-node stripe pool, else null. The pool is found by name, and
  // the per-file maps are rebuilt in the middle of a rename, when the pool may still answer to the old name only: listing
  // both names' pools until the move is over keeps it in the permission map and the explicit lock throughout (issue #9780).
  private volatile String       renamingFrom;

  public LocalVertexType(final LocalSchema schema, final String name) {
    super(schema, name);
  }

  @Override
  public MutableVertex newRecord() {
    return schema.getDatabase().newVertex(name);
  }

  /**
   * The vertex type's renaming steps. Called by {@link LocalDocumentType#rename} with {@link #renameLock} held, so the
   * edge-bucket loop and both rollbacks never interleave with a second rename of this type (issue #7937), and published
   * once with everything they moved (issue #9762). The document part goes through {@link #renameDocumentPart}, which
   * publishes nothing of its own (issue #9891).
   */
  @Override
  protected void renameFiles(final String newName) {
    renameInternal(newName);
  }

  private void renameInternal(final String newName) {
    final String oldName = name;

    renamingFrom = oldName;
    try {
      renameInternal(oldName, newName);
    } finally {
      renamingFrom = null;
    }
  }

  private void renameInternal(final String oldName, final String newName) {
    // Refused before anything moves: a stripe name already taken would otherwise be found only after the type and its edge
    // buckets were renamed, and all of it rolled back
    checkStripePoolRenameTargets(oldName, newName);

    renameDocumentPart(newName);

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

      renameSuperNodeStripePool(oldName, newName, removedBuckets);

      // Any RuntimeException too, SchemaException included: letting one past this catch would leave the edge buckets and
      // the stripe pool already renamed on disk with a schema.json that still names the old files.
    } catch (IOException | RuntimeException e) {
      renameDocumentPart(oldName);

      boolean corrupted = false;
      for (Bucket bucket : removedBuckets) {
        final String renamedBucketName = bucket.getName();
        try {
          final String restoredName = restoredBucketName(renamedBucketName, oldName, newName);
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

  /**
   * The type's own buckets, their edge-list buckets and their paired external buckets, plus the super-node stripe pool the
   * type's promoted vertices keep their edge chunks in (issue #9780). The per-file permission map and the explicit type
   * lock are both built from this list: a pool left out of it was default-allow for every grant, and an explicitly locked
   * append to a promoted vertex wrote a file it had not locked and failed the commit.
   */
  @Override
  public List<Bucket> getInvolvedBuckets() {
    final List<Bucket> own = super.getInvolvedBuckets();
    final Bucket[] stripePool = getSuperNodeStripePool(name);
    final String previousName = renamingFrom;
    final Bucket[] previousStripePool = previousName != null && !previousName.equals(name) ?
        getSuperNodeStripePool(previousName) :
        null;

    final ArrayList<Bucket> result = new ArrayList<>(
        own.size() + additionalBuckets.size() + stripePool.length + (previousStripePool != null ? previousStripePool.length : 0));
    result.addAll(own);
    result.addAll(additionalBuckets);
    addStripes(result, stripePool);
    if (previousStripePool != null)
      addStripes(result, previousStripePool);
    return result;
  }

  private static void addStripes(final List<Bucket> result, final Bucket[] stripePool) {
    for (final Bucket stripe : stripePool)
      if (stripe != null)
        result.add(stripe);
  }

  /** The stripe pool named after {@code typeName}, indexed by slot: see {@link StripedEdgeList#getStripePool}. */
  private Bucket[] getSuperNodeStripePool(final String typeName) {
    return StripedEdgeList.getStripePool(schema, typeName,
        schema.getDatabase().getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES));
  }

  /**
   * Moves the super-node stripe pool under the new type name (issue #9780). The pool is found, dropped and listed among the
   * type's involved buckets by its name alone, so one left under the old name would fall out of the type's permission map
   * and explicit lock again, be orphaned when the type is dropped, and be adopted by the next type created under the old
   * name. Every moved bucket is added to {@code renamedBuckets} so a failure further on puts it back. The pool is walked as
   * {@link StripedEdgeList#getStripePool} walks it for the drop and the checker, under the live stripe count.
   */
  private void renameSuperNodeStripePool(final String oldName, final String newName, final List<Bucket> renamedBuckets)
      throws IOException {
    final Bucket[] pool = getSuperNodeStripePool(oldName);
    for (int slot = 0; slot < pool.length; slot++) {
      final Bucket bucket = pool[slot];
      if (bucket == null)
        continue;
      final String oldBucketName = bucket.getName();
      ((LocalBucket) bucket).rename(StripedEdgeList.stripeBucketName(newName, slot));
      renamedBuckets.add(bucket);
      rekeyBucket(bucket, oldBucketName);
    }

    if (pool.length > 0)
      // The per-file maps were rebuilt by the type rename, when the pool still answered to the old name only
      schema.saveConfiguration();
  }

  private void checkStripePoolRenameTargets(final String oldName, final String newName) {
    final Bucket[] pool = getSuperNodeStripePool(oldName);
    for (int slot = 0; slot < pool.length; slot++) {
      final String target = StripedEdgeList.stripeBucketName(newName, slot);
      if (pool[slot] != null && schema.existsBucket(target))
        throw new AlreadyExistsException(AlreadyExistsException.Kind.BUCKET,
            "Cannot rename the super-node stripe bucket '" + pool[slot].getName() + "': a bucket named '" + target + "' already exists");
    }
  }

  /** The name a bucket this rename moved goes back to when the rename is rolled back, or {@code null} if none can be told. */
  private String restoredBucketName(final String renamedBucketName, final String oldName, final String newName) {
    final int slot = StripedEdgeList.stripeSlotOf(newName, renamedBucketName);
    if (slot > -1)
      return StripedEdgeList.stripeBucketName(oldName, slot);
    return LocalSchema.rebaseComponentName(renamedBucketName, newName, oldName, schema.getEncoding());
  }

  @Override
  protected void addBucketInternal(final Bucket bucket) {
    super.addBucketInternal(bucket);
    additionalBuckets.addAll(
        ((DatabaseInternal) schema.getDatabase()).getGraphEngine().createVertexAdditionalBuckets((LocalBucket) bucket));
  }
}
