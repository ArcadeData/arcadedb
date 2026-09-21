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
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class LocalDocumentType implements DocumentType {
  // Reassigned by rename() under the schema write lock (with a rollback assignment on failure) and read lock-free by
  // getName() and by instanceOf(String), which openCypher's Labels calls during query planning while holding no
  // database lock. Volatile for the same reason as the copy-on-write members below: without it a planning thread has
  // no happens-before edge against a concurrent ALTER TYPE ... NAME and can match on either spelling indefinitely
  // (issues #6678, #7033, #7119, #7299).
  protected volatile String                         name;
  protected final LocalSchema                       schema;
  protected final List<LocalDocumentType>           superTypes                   = new ArrayList<>();
  protected final List<LocalDocumentType>           subTypes                     = new ArrayList<>();
  // Sixth member of the copy-on-write family: reassigned by setAliases under the schema mutation lock and read
  // lock-free by instanceOf(String) from openCypher label resolution during query planning. Volatile for the
  // publication edge, and always assigned an unmodifiable COPY: setAliases used to store the caller's set by
  // reference, so a caller that kept its set and mutated it afterwards was editing live schema state that a
  // lock-free reader is walking (issue #7299).
  private volatile Set<String>                      aliases                      = Set.of();
  // Mutated by CREATE/DROP PROPERTY under the schema write lock. Record creation reads it under the read lock and is
  // therefore excluded, but two readers are not: query planning, and toJSON() - which LocalSchema.recordFileChanges
  // calls to save schema.json AFTER the write lock is released, so a save running alongside another thread's DDL threw
  // ConcurrentModificationException straight out of a plain HashMap. Concurrent, so a reader crossing an in-flight
  // mutation sees a weakly consistent view of it rather than a corrupt one (#6799).
  protected final Map<String, Property>             properties                   = new ConcurrentHashMap<>();
  protected final Map<Integer, List<IndexInternal>> bucketIndexesByBucket        = new HashMap<>();
  protected final Map<List<String>, TypeIndex>      indexesByProperties          = new HashMap<>();
  protected final RecordEventsRegistry              events                       = new RecordEventsRegistry();
  protected final Map<String, Object>               custom                       = new HashMap<>();
  // The four bucket lists are copy-on-write: reassigned under the schema mutation lock and read lock-free by query
  // planning through getBuckets(polymorphic)/getBucketIds(polymorphic), both branches of the ternary - the
  // non-polymorphic pair feeds SelectExecutionPlanner's partition pruning and FetchFromSchemaTypesStep exactly as the
  // polymorphic pair does. All four are volatile so a planning thread has a happens-before edge against a concurrent
  // ALTER TYPE ... BUCKET, matching the LocalSchema.bucketId2TypeMap publication pattern (issues #6678 and #7033).
  protected volatile List<Bucket>                   buckets                      = new ArrayList<>();
  protected volatile List<Bucket>                   cachedPolymorphicBuckets     = new ArrayList<>(); // PRE COMPILED LIST TO SPEED UP RUN-TIME OPERATIONS
  protected volatile List<Integer>                  bucketIds                    = new ArrayList<>();
  protected volatile List<Integer>                  cachedPolymorphicBucketIds   = new ArrayList<>(); // PRE COMPILED LIST TO SPEED UP RUN-TIME OPERATIONS
  // Fifth member of the same copy-on-write family: reassigned by setBucketSelectionStrategy, read lock-free by
  // getBucketIdByRecord/getBucketIndexByKeys on the record-write path and by the planner's partition pruning through
  // getBucketSelectionStrategy(), so it is volatile for the same reason as the four lists above (issue #7119).
  protected volatile BucketSelectionStrategy        bucketSelectionStrategy      = new RoundRobinBucketSelectionStrategy();
  // Names of the OWN properties that declare a DEFAULT. A cache: the authority is the per-property default value, but
  // record creation would otherwise pay an O(properties) scan to find the (usually empty) subset that has one.
  // Copy-on-write, and read through getPolymorphicPropertiesWithDefaultDefined() by ApplyDefaultsStep (the SQL insert
  // and UPDATE ... APPLY DEFAULTS plans), which holds no database lock - LocalDatabase.createRecord does take the read
  // lock, so that path is already excluded against DDL. The replacement therefore has to be published atomically or a
  // lock-free reader could observe a half-built set, the same publication requirement documented on
  // cachedPolymorphicBuckets above.
  // An AtomicReference rather than a plain volatile field, unlike those siblings, because the update is a
  // read-copy-write and it must stay correct where its writers are NOT serialized: setDefaultValue publishes inside
  // recordFileChanges, but that bypasses the write lock entirely while the schema is being read from file, and the
  // CAS is what makes setPropertyHasDefault safe on its own terms rather than only as long as every future caller
  // remembers to hold the lock. Without it, two writers copying the same snapshot would lose one of the two names.
  // Maintained in exactly one place, {@link #setPropertyHasDefault}, so it cannot drift from the properties map
  // again (issue #6799).
  private final AtomicReference<Set<String>>        propertiesWithDefaultDefined = new AtomicReference<>(
      Collections.emptySet());
  // Map: primary bucket id -> external bucket id. Populated lazily when the first EXTERNAL property is
  // set on the type, and persisted in schema.json under the per-type "externalBuckets" key.
  protected final Map<Integer, Integer>             externalBucketIdByPrimaryBucketId = new ConcurrentHashMap<>();
  // Cached count of OWN properties (not inherited) currently flagged EXTERNAL. Avoids O(N) scans of
  // getPolymorphicProperties() on hot paths (cascadeDeleteExternalValues, addBucketInternal, addSuperType).
  // Maintained by LocalProperty.setExternal, dropProperty, and the schema-load path.
  final AtomicInteger                               ownExternalPropertyCount          = new AtomicInteger(0);
  // Tracks whether the partition mapping is currently trustworthy. Set to {@code true} when a
  // schema mutation (bucket add / drop, or strategy change between two partition shapes on a
  // populated type) invalidates the {@code hash(propertyValue) % bucketCount} mapping for some
  // existing records. While {@code true}, the partition-pruning planner rule must NOT fire -
  // queries fall back to scanning every bucket and stay correct, just lose the optimization.
  // Cleared by a successful {@code REBUILD TYPE <name> WITH repartition = true}. Persisted in
  // {@code schema.json} only when {@code true} so the default case writes nothing extra.
  // Private so subclasses (LocalVertexType, LocalEdgeType, LocalTimeSeriesType) cannot bypass
  // {@link #setNeedsRepartition(boolean)} and skip the schema.saveConfiguration() that the
  // setter triggers on every transition. Read via {@link #isNeedsRepartition()}.
  // {@link AtomicBoolean} (rather than {@code volatile boolean}) so the transition guard in
  // {@link #setNeedsRepartition} is atomic: under concurrent DDL (two parallel
  // {@code ALTER TYPE ... BUCKET +x} commands) the volatile-field read-check-write let both
  // threads pass the guard and call {@code schema.saveConfiguration()} twice. CAS guarantees
  // exactly one transition fires the save.
  private final AtomicBoolean                       needsRepartition                  = new AtomicBoolean(false);
  // Throttle for the per-type query-time WARNING emitted when a query plan touches a type whose
  // {@code needsRepartition} is {@code true}. Same shape as the saturation throttles on
  // {@link com.arcadedb.query.QueryEngineManager} and
  // {@link com.arcadedb.index.sparsevector.SparseVectorScoringPool}: at most one entry per
  // 60-second window per type, initialised to 0L (not {@code Long.MIN_VALUE}) so the
  // {@code now - last} subtraction does not overflow on the first call.
  private static final long                         REPARTITION_WARN_INTERVAL_MS      = 60_000L;
  private final AtomicLong                          lastRepartitionWarnMs             = new AtomicLong(0L);

  public LocalDocumentType(final LocalSchema schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public MutableDocument newRecord() {
    return schema.getDatabase().newDocument(name);
  }

  @Override
  public RecordEvents getEvents() {
    return events;
  }

  @Override
  public Set<String> getPolymorphicPropertiesWithDefaultDefined() {
    final Set<String> own = propertiesWithDefaultDefined.get();
    if (superTypes.isEmpty())
      return own;

    final HashSet<String> set = new HashSet<>(own);
    for (final LocalDocumentType superType : superTypes)
      set.addAll(superType.getPolymorphicPropertiesWithDefaultDefined());
    return set;
  }

  /**
   * Enforces the UPDATE_SCHEMA permission for any schema-mutating operation. No-op in embedded mode or when no
   * current user is bound to the thread (e.g. schema load at startup, replication apply).
   */
  protected void checkForSchemaMutation() {
    ((DatabaseInternal) schema.getDatabase()).checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
  }

  @Override
  public DocumentType addSuperType(final String superName) {
    return addSuperType(schema.getType(superName));
  }

  @Override
  public DocumentType addSuperType(final DocumentType superType) {
    return addSuperType(superType, true);
  }

  /**
   * Removes a super type (by its name) from the current type.
   *
   * @see #removeSuperType(DocumentType)
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public DocumentType removeSuperType(final String superTypeName) {
    removeSuperType(schema.getType(superTypeName));
    return this;
  }

  /**
   * Removes a super type from the current type.
   *
   * @see #removeSuperType(String)
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public DocumentType removeSuperType(final DocumentType superType) {
    checkForSchemaMutation();
    recordFileChanges(() -> {
      if (!superTypes.contains(superType))
        // ALREADY REMOVED SUPER TYPE
        return null;

      unlinkSuperType((LocalDocumentType) superType);
      dropSubIndexesNoLongerCovered((LocalDocumentType) superType);
      return null;
    });
    return this;
  }

  /**
   * Drops every sub-index that the linkage created over a bucket the former super type - or an ancestor of it - no
   * longer reaches, which is the INDEX half of what {@link #removeSuperType(DocumentType)} has to undo.
   * <p>
   * Linking propagates the super type's indexes (its own AND the ones it inherits, {@code getAllIndexes(true)}) over
   * this type's buckets, and every one of those components is attached to the ANCESTOR's {@link TypeIndex} by
   * {@link #addIndexInternal}. Unlinking used to undo only the polymorphic BUCKET side (issue #6935), so the
   * ancestor's wrapper kept fanning out over the detached subtree: {@code lookupByKey} handed back records of a
   * foreign type, {@code countEntries()} counted them, and the ancestor's UNIQUE constraint stayed enforced across
   * them, refusing a key that was genuinely free. {@code SELECT} was spared only because the planner filters index
   * results by type downstream (issue #7892).
   * <p>
   * Dropped rather than merely detached, which is what makes it the exact mirror of {@link #addSuperType}: the
   * components exist BECAUSE of the linkage, they index a property the subtype no longer even has, and a detachment
   * alone would leave them on disk, invisible to the schema and resurrected by the next reload - which re-reads the
   * attachment from {@code schema.json} and would put the ancestor's wrapper straight back over them.
   * <p>
   * Which buckets are "no longer reached" is read from the polymorphic caches {@link #unlinkSuperType} has just
   * recomputed, never subtracted from the detached subtree. That is the same rule and the same reason as there: in a
   * diamond, a bucket the former super type still reaches through another surviving path must keep its sub-index.
   */
  private void dropSubIndexesNoLongerCovered(final LocalDocumentType formerSuperType) {
    if (schema.isTypeBeingDropped())
      // A dropType cascade severs this link only to re-parent the surviving sub types onto the same super type a
      // few lines later, re-linking them with createIndexes=false because their components are still attached. The
      // relationship is mid-rewrite, so "no longer reached" is not yet true of anything; see LocalSchema
      // #isTypeBeingDropped. The doomed type's own components are dropped by that cascade itself.
      return;

    final List<String> orphans = new ArrayList<>();
    // BY IDENTITY: TypeIndex.equals() is content-based and asks an empty wrapper for its property names, which is
    // exactly the state the wrappers in here are about to be left in.
    final Set<TypeIndex> affectedWrappers = Collections.newSetFromMap(new IdentityHashMap<>());
    collectSubIndexesNoLongerCovered(formerSuperType, new HashSet<>(), orphans, affectedWrappers);

    for (final String indexName : orphans)
      schema.dropIndex(indexName);

    // A wrapper whose LAST sub-index was one of the orphans has to leave its owner's index list too, or the next
    // schema serialization asks an empty TypeIndex for its property names and fails. LocalSchema's own leaf-drop
    // cleanup cannot do it here: it looks the wrapper up from the SUB-type the component belonged to and walks that
    // type's super types, and the link that would have led it to the owner is exactly the one just severed.
    for (final TypeIndex wrapper : affectedWrappers)
      if (wrapper.countIndexesOnBuckets() == 0) {
        final LocalDocumentType owner = schema.getType(wrapper.getTypeName());
        owner.removeTypeIndexInternal(wrapper);
        schema.removeIndexDuringLoad(wrapper.getName());
      }
  }

  /** Walks {@code type} and its super types, collecting the sub-indexes sitting on buckets the type no longer reaches. */
  private static void collectSubIndexesNoLongerCovered(final LocalDocumentType type, final Set<String> visited,
      final List<String> orphans, final Set<TypeIndex> affectedWrappers) {
    if (!visited.add(type.getName()))
      // A DIAMOND REACHES THE SAME ANCESTOR THROUGH MORE THAN ONE PATH
      return;

    final Set<Integer> stillCovered = new HashSet<>(type.getBucketIds(true));
    for (final TypeIndex typeIndex : type.indexesByProperties.values())
      for (final IndexInternal subIndex : typeIndex.getIndexesOnBuckets())
        if (!stillCovered.contains(subIndex.getAssociatedBucketId())) {
          orphans.add(subIndex.getName());
          affectedWrappers.add(typeIndex);
        }

    for (final LocalDocumentType superType : type.superTypes)
      collectSubIndexesNoLongerCovered(superType, visited, orphans, affectedWrappers);
  }

  /**
   * Renames the type, its buckets and its indexes.
   * <p>
   * The name-uniqueness check below is a fast, friendly refusal only, and it earns its place by running BEFORE
   * the bucket renames: the common "that name is taken" case is answered without moving a single file. The
   * AUTHORITATIVE check is the atomic {@code putIfAbsent} further down, next to the mutation it guards. A check up here on its own is a
   * time-of-check to time-of-use hole - two concurrent renames onto the same target both pass it and the second
   * silently overwrites the first's map entry (issue #7918) - and it cannot be closed by moving the whole method
   * under {@link #recordFileChanges}: every bucket rename in the loop below waits for the WHOLE database's page
   * flush queue to drain ({@link com.arcadedb.engine.PaginatedComponent#rename}), which is the long part this
   * engine deliberately keeps outside every lock. So only the reservation takes the lock, and it is the map
   * operations alone.
   */
  public void rename(final String newName) {
    checkForSchemaMutation();
    if (schema.existsType(newName))
      throw new IllegalArgumentException("Type with name '" + newName + "' already exists");

    final String oldName = name;

    final List<Bucket> removedBuckets = new ArrayList<>();
    final List<TypeIndex> renamedIndexes = new ArrayList<>();

    try {
      for (Bucket bucket : buckets) {
        final String oldBucketName = bucket.getName();

        final String newBucketName = LocalSchema.rebaseComponentName(oldBucketName, oldName, newName, schema.getEncoding());
        if (newBucketName == null)
          // Bucket attached with addBucket() under a name of its own: it does not follow the type name.
          continue;

        ((LocalBucket) bucket).rename(newBucketName);

        removedBuckets.add(bucket);

        rekeyBucket(bucket, oldBucketName);
      }

      // ATOMIC, and the only check that decides (#7918). Two things make it so: putIfAbsent on the
      // ConcurrentHashMap, so the reservation cannot silently evict whoever already holds the name - a plain
      // put() would leave two types answering to one - and the database WRITE LOCK around it, which is the lock
      // TypeBuilder.createInternal does its own check-then-put under, so a concurrent CREATE TYPE either loses
      // the name to us or is refused by it. Everything expensive stays outside: the bucket renames above each
      // wait for the whole database's page flush queue to drain. Refusing here unwinds through the catch below,
      // which puts the buckets renamed above back under their old names.
      ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance().executeInWriteLock(() -> {
        if (schema.typeMap().putIfAbsent(newName, this) != null)
          throw new SchemaException("Type with name '" + newName + "' already exists");

        name = newName;
        return null;
      });

      // Registered before the call, not after: updateTypeName() walks the index's own per-bucket sub-indexes, so a
      // failure part way through leaves that index half renamed and it has to be rolled back too.
      for (TypeIndex idx : getAllIndexes(false)) {
        renamedIndexes.add(idx);
        idx.updateTypeName(newName);
      }

      schema.saveConfiguration();

      // OLD NAME RELEASED ONLY HERE, once nothing left can fail and send us to the catch below (found by
      // CodeRabbit on PR #7935). Releasing it at the reservation instead opened a window in which the name was
      // free while this rename could still roll back: a concurrent CREATE TYPE could take it - legitimately, and
      // under the very write lock the reservation uses - and the rollback's restore would then have evicted that
      // type and left it answering to no name at all. Held across the index renames and the save, the window
      // cannot open, so the rollback has nothing to restore and nothing to overwrite.
      //
      // The cost is that the type answers to BOTH names in between, which is the conservative direction: a
      // concurrent CREATE TYPE on the old name is refused while the rename may still come back, and a reader
      // resolving the old name gets this type rather than nothing. schema.json is unaffected either way -
      // saveConfiguration() keys each entry by t.getName(), so two keys onto one type collapse into one entry.
      ((DatabaseInternal) schema.getDatabase()).getWrappedDatabaseInstance()
          .executeInWriteLock(() -> schema.typeMap().remove(oldName, this));

      // SchemaException too: it is a RuntimeException, and letting it past this catch would leave the buckets
      // already renamed on disk with a schema.json that still names the old files.
    } catch (IOException | SchemaException e) {
      name = oldName;
      // ONLY OUR RESERVATION GOES, and nothing is restored: the old name was never released above, so it still
      // maps to this type. The two-argument remove() is what keeps a refusal - the putIfAbsent losing to whoever
      // already holds the new name - from evicting that winner's entry (#7918).
      schema.typeMap().remove(newName, this);

      boolean corrupted = false;

      // Unwound in reverse: indexes were renamed last, so they are restored first. 'name' is already back to the
      // old value above, which is what TypeIndex.updateTypeName() recomputes its logic name from.
      for (TypeIndex idx : renamedIndexes) {
        try {
          idx.updateTypeName(oldName);
        } catch (Exception ex) {
          corrupted = true;
        }
      }

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

  /**
   * Moves the schema's bucket-map entry from {@code previousKey} to the name the bucket now reports.
   * <p>
   * The map is keyed by name and the key is not derived from the component on lookup, so renaming a bucket without
   * re-keying leaves the bucket unreachable under its own name while the stale key resolves to a component that
   * answers to a different one. Everything name-based then disagrees with the schema: {@code existsBucket()},
   * {@code getBucketByName()} (hence {@code SELECT FROM BUCKET:x}), the duplicate-name guard in
   * {@code LocalSchema.createBucket()}, and the keys of the statistics file.
   * <p>
   * Called from both the forward rename loop and its rollback: a rollback that restores the file but not the key is
   * the same inconsistency in the opposite direction. In the rollback it runs even when the restore failed, so the
   * map always agrees with whatever name the component ended up with; that case is reported separately through the
   * {@code corrupted} flag.
   * <p>
   * The guarantee is only that the key agrees with the component, not that the component agrees with the disk:
   * {@link com.arcadedb.engine.PaginatedComponent#rename} moves the file and updates the {@code FileManager} before
   * it assigns {@code componentName}, so a failure between those steps leaves the file under the new name while the
   * component - and therefore this map - keeps the old one. That window predates this method and is not closed by
   * it.
   */
  protected void rekeyBucket(final Bucket bucket, final String previousKey) {
    final String currentName = bucket.getName();
    if (previousKey.equals(currentName))
      return;

    schema.bucketMap.remove(previousKey, bucket);
    schema.bucketMap.put(currentName, (LocalBucket) bucket);
  }

  /**
   * Returns true if the current type is the same or a subtype of `type` parameter.
   *
   * @param type the type name to check
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public boolean instanceOf(final String type) {
    if (name.equals(type))
      return true;

    if (aliases.contains(type))
      return true;

    for (final DocumentType t : superTypes) {
      if (t.instanceOf(type))
        return true;
    }

    return false;
  }

  /**
   * Returns the list of super types if any, otherwise an empty collection.
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public List<DocumentType> getSuperTypes() {
    return new ArrayList<>(superTypes);
  }

  /**
   * Set the type super types. Any previous configuration about supertypes will be replaced with this new list.
   *
   * @param newSuperTypes List of super types to assign
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public DocumentType setSuperTypes(List<DocumentType> newSuperTypes) {
    checkForSchemaMutation();
    if (newSuperTypes == null)
      newSuperTypes = Collections.emptyList();

    final List<DocumentType> commonSuperTypes = new ArrayList<>(superTypes);
    commonSuperTypes.retainAll(newSuperTypes);

    final List<DocumentType> toRemove = new ArrayList<>(superTypes);
    toRemove.removeAll(commonSuperTypes);
    toRemove.forEach(this::removeSuperType);

    final List<DocumentType> toAdd = new ArrayList<>(newSuperTypes);
    toAdd.removeAll(commonSuperTypes);
    toAdd.forEach(this::addSuperType);

    return this;
  }

  /**
   * Returns the list of subtypes, in any, or an empty list in case the type has not subtypes defined.
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  @Override
  public List<DocumentType> getSubTypes() {
    return new ArrayList<>(subTypes);
  }

  /**
   * Returns the list of aliases defined for the type.
   */
  public Set<String> getAliases() {
    return this.aliases;
  }

  /**
   * Sets the list of aliases for the type. Any previous configuration will be lost.
   * <p>
   * Every check and both map passes run inside the single {@link #recordFileChanges} callback, i.e. under the
   * database write lock, for the same two reasons {@link #createProperty} and {@link #dropProperty} spell out and
   * this method used to violate (issue #8064). {@code checkForSchemaMutation()} is a precondition check, not a
   * lock, and it was the only thing here:
   * <ul>
   *   <li><b>Check-then-put.</b> The refusal consulted {@code schema.existsType(alias)} and the install happened
   *   several statements later, so two concurrent {@code ALTER TYPE ... ALIASES} on different types could both
   *   pass the check and the second {@code put} silently won, leaving two types believing they owned one name.
   *   The reservation is now an atomic {@code putIfAbsent} under the write lock, the same shape {@link #rename}
   *   uses, so the check and the install are one step.</li>
   *   <li><b>Unconditional deregistration.</b> EVERY previous alias was removed from the type map before the new
   *   set was installed, so a concurrent reader resolving a name the new set still carries saw it disappear. Only
   *   the aliases the new set drops are removed now, and the install runs first, so a surviving name never leaves
   *   the map at all.</li>
   * </ul>
   * A refusal part way through unwinds what this call had already reserved: the aliases are all-or-nothing, never
   * a half-installed set left behind by a rejected {@code ALTER TYPE}.
   * <p>
   * {@code recordFileChanges} saves {@code schema.json} itself, which is why the explicit
   * {@code schema.saveConfiguration()} this method used to end with is gone.
   */
  public LocalDocumentType setAliases(final Set<String> aliases) {
    checkForSchemaMutation();

    return recordFileChanges(() -> {
      final Set<String> previousAliases = this.aliases;

      // ONLY THE GENUINELY NEW NAMES ARE RESERVED: AN ALIAS THIS TYPE ALREADY ANSWERS TO IS ALREADY IN THE MAP
      // POINTING AT US, AND putIfAbsent WOULD REPORT IT AS TAKEN - BY OURSELVES
      final Set<String> addedAliases = new HashSet<>(aliases);
      addedAliases.removeAll(previousAliases);

      final List<String> reserved = new ArrayList<>(addedAliases.size());
      try {
        for (final String alias : addedAliases) {
          final LocalDocumentType owner = schema.typeMap().putIfAbsent(alias, this);
          if (owner != null)
            // OWNED BY US MEANS IT IS THIS TYPE'S OWN NAME: EVERY ALIAS IT ALREADY CARRIES WAS FILTERED OUT ABOVE.
            // SAYING "ALREADY USED BY TYPE 'X'" WITH X NAMED TWICE READS AS AN ENGINE BUG RATHER THAN AS THE
            // REFUSAL IT IS, SO SAY WHICH OF THE TWO REFUSALS THIS IS
            throw new SchemaException(owner == this ?
                "Cannot set alias '" + alias + "' for type '" + name + "' because it is the name of the type itself" :
                "Cannot set alias '" + alias + "' for type '" + name + "' because it is already used by type '"
                    + owner.getName() + "'");
          reserved.add(alias);
        }
      } catch (final RuntimeException e) {
        // UNWIND ONLY WHAT THIS CALL RESERVED, AND ONLY WHILE IT STILL POINTS AT US
        for (final String alias : reserved)
          schema.typeMap().remove(alias, this);
        throw e;
      }

      // DEREGISTER ONLY THE PREVIOUS ALIASES THE NEW SET NO LONGER CARRIES, AFTER THE NEW ONES ARE IN
      for (final String alias : previousAliases)
        if (!aliases.contains(alias))
          schema.typeMap().remove(alias, this);

      // A copy, and an unmodifiable one: the parameter belongs to the caller. Published last so a lock-free
      // instanceOf() either sees the whole previous set or the whole new one.
      this.aliases = Set.copyOf(aliases);
      return this;
    });
  }

  /**
   * Returns all the properties defined in the type, not considering the ones inherited from subtypes.
   *
   * @return Set containing all the names
   *
   * @see #getPolymorphicPropertyNames()
   */
  @Override
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  @Override
  public Collection<? extends Property> getProperties() {
    return properties.values();
  }

  /**
   * Returns all the properties defined in the type and subtypes.
   *
   * @see {@link #getPolymorphicPropertyNames()}, {@link #getProperties()}
   */
  @Override
  public Collection<? extends Property> getPolymorphicProperties() {
    if (superTypes.isEmpty())
      return getProperties();

    final Set<Property> allProperties = new HashSet<>(getProperties());
    for (final DocumentType p : superTypes)
      allProperties.addAll(p.getPolymorphicProperties());
    return allProperties;
  }

  /**
   * Returns all the properties defined in the type and subtypes.
   *
   * @return Set containing all the names
   *
   * @see #getPropertyNames()
   */
  @Override
  public Set<String> getPolymorphicPropertyNames() {
    if (superTypes.isEmpty())
      return getPropertyNames();

    final Set<String> allProperties = new HashSet<>(getPropertyNames());
    for (final DocumentType p : superTypes)
      allProperties.addAll(p.getPolymorphicPropertyNames());
    return allProperties;
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type by type name @{@link String}
   */
  @Override
  public LocalProperty createProperty(final String propertyName, final String propertyType) {
    return createProperty(propertyName, Type.getTypeByName(propertyType));
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as Java @{@link Class}
   */
  @Override
  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    return createProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as @{@link Type}
   */
  @Override
  public LocalProperty createProperty(final String propertyName, final Type propertyType) {
    return createProperty(propertyName, propertyType, null);
  }

  /**
   * Refuses a property that a TIMESERIES type could never store.
   * <p>
   * A TIMESERIES type keeps its columns in {@code LocalTimeSeriesType.tsColumns}, filled once by
   * {@code CREATE TIMESERIES TYPE}, and the write path reads the document under those names and no others. A
   * property created afterwards lands in {@link #properties} instead, which is what the schema listing renders: the
   * column looks declared, every write drops its value and nothing reports it (issue #7567). The declared columns
   * themselves reach this method too - {@code TimeSeriesTypeBuilder.create()} registers each one as a property right
   * after filling {@code tsColumns} - and pass, because by then the name is declared.
   * <p>
   * A database written before this rule may already carry such a property. Refusing it at schema load would make
   * that database unopenable, so the load path only warns; {@code DROP PROPERTY} on the stray name is the remedy and
   * stays allowed.
   *
   * @param propertyName the property about to be created
   */
  private void checkTimeSeriesColumnDeclared(final String propertyName) {
    if (!(this instanceof LocalTimeSeriesType tsType) || tsType.isDeclaredColumn(propertyName))
      return;

    if (schema.isReadingFromFile()) {
      LogManager.instance().log(this, Level.WARNING,
          "Property '%s.%s' is not a declared TIMESERIES column (declared: %s): it was added to this database before "
              + "issue #7567 was fixed and no write will ever populate it. Remove it with DROP PROPERTY `%s`.`%s`",
          name, propertyName, tsType.getTsColumnNames(), name, propertyName);
      return;
    }

    throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name
        + "' because the type is a TIMESERIES type and '" + propertyName + "' is not one of its declared columns "
        + tsType.getTsColumnNames()
        + ". A TIMESERIES type stores only the TIMESTAMP, TAGS and FIELDS named in CREATE TIMESERIES TYPE, so a "
        + "property added afterwards would be silently ignored by every write");
  }

  /**
   * Refuses a super type relationship that would let a TIMESERIES type - {@code this}, {@code superType}, or a
   * TIMESERIES type sitting anywhere in {@code this}'s existing descendant subtree - inherit a polymorphic
   * property it cannot store.
   * <p>
   * The same shape {@link #checkTimeSeriesColumnDeclared} refuses for a property created directly on the type, one
   * hop further: {@code LocalTimeSeriesType.tsColumns} is filled once by {@code TimeSeriesTypeBuilder.create()} and
   * {@code addTsColumn} has no other caller, so a super type linked afterwards cannot extend it. A polymorphic
   * property inherited across the hierarchy - whichever end declares it - lands in {@link #properties} and is
   * listed as part of the type, but the time-series write path ({@code SaveElementStep#saveToTimeSeries}) reads the
   * document under the declared column names only and silently drops everything else (issue #7581).
   * <p>
   * The descendant walk closes a gap the direct {@code this}/{@code superType} check alone leaves open: a legacy
   * database can already have an ordinary type with a TIMESERIES type somewhere below it in the subtree (loaded
   * tolerantly, warning-only, by the branch below). Linking a new, entirely ordinary super type onto {@code this}
   * does not touch either end of THAT link, but the new super type's properties still flow down through {@code
   * this} to every one of its subtypes, TIMESERIES ones included.
   * <p>
   * A database written before this rule may already carry such a hierarchy. Refusing it at schema load would make
   * that database unopenable, so the load path only warns and leaves the hierarchy standing.
   *
   * @param superType the super type about to be linked
   */
  private void checkTimeSeriesHierarchy(final DocumentType superType) {
    final LocalTimeSeriesType affected = findTimeSeriesInSubtree(this);
    if (affected == null && !(superType instanceof LocalTimeSeriesType))
      return;

    if (schema.isReadingFromFile()) {
      LogManager.instance().log(this, Level.WARNING,
          "Type '%s' has super type '%s': a TIMESERIES type (%s) is involved in the hierarchy and this link was "
              + "created before issue #7581 was fixed. A polymorphic property inherited across it is silently "
              + "dropped by the time-series write path. Remove the SUPERTYPE relationship with ALTER TYPE to fix this",
          name, superType.getName(), affected != null ? affected.getName() : superType.getName());
      return;
    }

    throw new SchemaException("Cannot add super type '" + superType.getName() + "' to type '" + name
        + "' because a TIMESERIES type ("
        + (affected != null ? affected.getName() : superType.getName())
        + ") only stores the TIMESTAMP, TAGS and FIELDS columns declared in CREATE TIMESERIES TYPE, whether it is "
        + "an end of this link or sits below it in the hierarchy: a polymorphic property inherited across it would "
        + "be silently dropped by every write");
  }

  /**
   * The TIMESERIES type at or below {@code type} in its subtype tree, or {@code null} if there is none. {@code
   * type} itself is checked first, so a direct TIMESERIES/TIMESERIES link is reported the same way a transitive
   * one is.
   */
  private static LocalTimeSeriesType findTimeSeriesInSubtree(final LocalDocumentType type) {
    if (type instanceof LocalTimeSeriesType tsType)
      return tsType;

    for (final LocalDocumentType subType : type.subTypes) {
      final LocalTimeSeriesType found = findTimeSeriesInSubtree(subType);
      if (found != null)
        return found;
    }
    return null;
  }

  /**
   * Creates a new property with type `propertyType`.
   * <p>
   * Every check below, and the mutation itself, run inside the single {@link #recordFileChanges} callback, for the
   * two reasons {@link #dropProperty} and {@link #renameProperty} spell out and this method used to violate (issue
   * #7918) - the mirror image of #7672, walking UP the hierarchy instead of down. First,
   * {@link #getPolymorphicPropertyNames()} recurses over the {@link #superTypes} list of this type and of every
   * type above it, and those lists are plain {@link ArrayList}s structurally modified by {@code linkSuperType}/
   * {@code unlinkSuperType} - which run inside {@code recordFileChanges} themselves, so only a walk that runs
   * there too is serialised against them rather than racing a concurrent {@code CREATE TYPE ... EXTENDS} into a
   * {@link java.util.ConcurrentModificationException}. Second, validating outside and mutating inside would let a
   * concurrent {@code CREATE TYPE ... EXTENDS}, {@code ALTER TYPE ... SUPERTYPE} or {@code CREATE PROPERTY} on a
   * super type land between the two, so both would validate against the pre-link picture and both apply, leaving
   * the subtype shadowing a super type's property - exactly what the upward walk exists to prevent.
   * <p>
   * {@code checkForSchemaMutation()} stays outside: it refuses the mutation in the wrong context and takes no
   * lock, so it is not part of what has to be serialised.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as @{@link Type}
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  @Override
  public LocalProperty createProperty(final String propertyName, final Type propertyType, final String ofType) {
    checkForSchemaMutation();

    return recordFileChanges(() -> {
      if (this instanceof LocalEdgeType edgeType && edgeType.isLightweight())
        // A lightweight edge is a pair of pointers inside the two vertices: there is no record to hold a value, so a
        // declared property could never be written. Rejecting it here keeps the contract structural rather than a
        // convention nobody reads, and stops mandatory/default-valued properties from being declared on a type whose
        // creation path can never satisfy them.
        throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name
            + "' because the type is declared LIGHTWEIGHT and its edges cannot have properties");

      checkTimeSeriesColumnDeclared(propertyName);

      if (properties.containsKey(propertyName))
        throw new SchemaException(
            "Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

      if (getPolymorphicPropertyNames().contains(propertyName))
        throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name
            + "' because it was already defined in a super type");

      // The construction stays HERE, after the checks and inside the callback, although the constructor reaches
      // Dictionary.getIdByName(name, true) - which, for a name the dictionary has not seen, opens and commits a
      // transaction of its own, so the write lock is held across that commit and the schema save it triggers.
      // Hoisting it out to shorten the hold would allocate a dictionary id for a create that is then REFUSED, and
      // the dictionary is append-only: a loop of failing CREATE PROPERTY would grow it for names no type declares.
      // A transaction inside recordFileChanges is established here anyway - TimeSeriesTypeBuilder opens one, and
      // addSuperTypeInternal's index propagation commits several - and CREATE PROPERTY is rare DDL, so the longer
      // hold is the cheaper of the two.
      final LocalProperty property = new LocalProperty(this, propertyName, propertyType);

      if (ofType != null)
        property.setOfType(ofType);

      properties.put(propertyName, property);
      return property;
    });
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, by type name @{@link String}, to use in case the property does not exist and will be created
   */
  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType), null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, by type name @{@link String}, to use in case the property does not exist and will be created
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType, final String ofType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType), ofType);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as Java @{@link Class}, to use in case the property does not exist and will be created
   */
  @Override
  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByClass(propertyType), null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as @{@link Type}, to use in case the property does not exist and will be created
   */
  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    return getOrCreateProperty(propertyName, propertyType, null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as @{@link Type}, to use in case the property does not exist and will be created
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType, final String ofType) {
    final Property p = getPolymorphicPropertyIfExists(propertyName);
    if (p != null) {
      if (p.getType().equals(propertyType) && Objects.equals(p.getOfType(), ofType))
        return p;

      // DIFFERENT TYPE: DROP THE PROPERTY AND CREATE A NEW ONE
      dropProperty(propertyName);
    }
    return createProperty(propertyName, propertyType, ofType);
  }

  /**
   * The index, if any, anywhere in the type hierarchy that names {@code propertyName} - this type's own indexes,
   * a super type's (both via {@link #getAllIndexes(boolean)}, which only ever walks up), and a SUBTYPE's own index
   * on the inherited property, which {@code getAllIndexes} alone misses. {@link #dropProperty} and {@link
   * #renameProperty} both need the full picture: an index a subtype declared on a property this type owns would
   * otherwise survive a rename or a drop, left pointing at a name the type no longer has under that meaning.
   */
  private TypeIndex findIndexOnProperty(final String propertyName) {
    for (final TypeIndex index : getAllIndexes(true))
      if (index.getPropertyNames().contains(propertyName))
        return index;
    return findDescendantIndexOnProperty(propertyName);
  }

  private TypeIndex findDescendantIndexOnProperty(final String propertyName) {
    for (final LocalDocumentType subType : subTypes) {
      if (subType.getPropertyIfExists(propertyName) != null)
        // addSuperType lets a subtype declare its own property under a name a super type already uses - a
        // conflict it only warns about, never refuses. From here down, propertyName resolves to THIS subtype's
        // own, independently-declared property, not the one above being renamed or dropped: an index in this
        // branch belongs to that shadowing property, so it is not a reason to refuse the change higher up.
        continue;

      for (final TypeIndex index : subType.getAllIndexes(false))
        if (index.getPropertyNames().contains(propertyName))
          return index;

      final TypeIndex foundDeeper = subType.findDescendantIndexOnProperty(propertyName);
      if (foundDeeper != null)
        return foundDeeper;
    }
    return null;
  }

  /**
   * Drops a property from the type. If there is any index on the property, anywhere in the hierarchy, a
   * {@link SchemaException} is thrown and nothing changes.
   * <p>
   * Every check below, and the mutation itself, runs inside the single {@link #recordFileChanges} callback, for the
   * two reasons {@link #renameProperty} spells out and this method used to violate (issue #7672). First,
   * {@link #findIndexOnProperty} walks the {@link #subTypes} list of this type and of every type below it, and those
   * lists are plain {@link ArrayList}s structurally modified by {@code linkSuperType}/{@code unlinkSuperType} - which
   * run inside {@code recordFileChanges} themselves, so only a walk that runs there too is serialised against them
   * rather than racing into a {@link java.util.ConcurrentModificationException}. Second, validating outside and
   * mutating inside would let a concurrent {@code CREATE INDEX}/{@code CREATE TYPE ... EXTENDS} land between the two,
   * so the drop would commit against a pre-create picture and leave an index naming a property the type no longer
   * declares - exactly what the descendant walk exists to prevent.
   *
   * @param propertyName Property name to remove
   *
   * @return the property dropped if found, {@code null} if the type does not declare it
   */
  @Override
  public Property dropProperty(final String propertyName) {
    checkForSchemaMutation();

    return recordFileChanges(() -> {
      if (this instanceof LocalTimeSeriesType tsType && tsType.isDeclaredColumn(propertyName))
        // The column stays in the type's tsColumns list whatever happens to the schema property - nothing removes an
        // entry from it - so the engine would keep storing and returning the column while the type stopped declaring
        // it. Refusing here keeps the two descriptions of a TIMESERIES type from drifting apart (issue #7567). A
        // property that is NOT a declared column is still droppable, which is how a database written before that
        // issue gets rid of the stray one it may already carry.
        throw new SchemaException("Cannot drop the property '" + propertyName + "' from type '" + name
            + "' because it is a declared TIMESERIES column: the storage engine keeps reading and writing it. Drop the "
            + "whole type to remove the column");

      final TypeIndex indexOnProperty = findIndexOnProperty(propertyName);
      if (indexOnProperty != null)
        throw new SchemaException(
            "Error on dropping property '" + propertyName + "' because used by index '" + indexOnProperty.getName() + "'");

      final Property removed = properties.remove(propertyName);
      if (removed != null) {
        // Keep the EXTERNAL counter consistent so hasExternalProperties() stays O(1).
        if (removed.isExternal())
          ownExternalPropertyCount.decrementAndGet();
        // Issue #6799: and the same for the default-property cache. A dropped property must stop participating in
        // default-value processing at once, or the next record create looks the name up with getPolymorphicProperty()
        // and fails with "Cannot find property '<name>' in type '<type>'". Only the in-memory view was ever wrong -
        // schema.json holds the default on the property itself, so a reload rebuilt the set correctly - which is why
        // the failure looked like it healed on restart.
        setPropertyHasDefault(propertyName, false);
      }
      return removed;
    });
  }

  /**
   * Renames a property in place: only this type's own record of the property's name changes ({@link #properties}
   * and, through {@link #recordFileChanges}, {@code schema.json}). See {@link Property#rename(String)} for the
   * full contract - in particular, existing documents are not touched or revisited: a value already written under
   * {@code propertyName} keeps reading back under that name, and only a write made after this call lands under
   * {@code newPropertyName} (issue #7589).
   * <p>
   * {@code name} is one of {@link AbstractProperty}'s final fields, so the rename is a swap: a new {@link
   * LocalProperty} is built under the new name with every other attribute copied across, and it replaces the old
   * one in {@link #properties}. The old {@code Property} handle is stale from this point on, the same way a
   * dropped property's handle already is.
   * <p>
   * Refused, mirroring {@link #dropProperty}, when an index stands on the property anywhere in the type hierarchy
   * - a super type's own index, or one a SUBTYPE declared on this (inherited) property, which {@link
   * #getAllIndexes(boolean)} alone would miss (it only ever walks up): the index's own definition names the
   * property by the old name, and propagating the rename into every index type/file naming scheme is out of scope
   * here - drop the index, rename, then recreate it on the new name. Also refused when the property is a declared
   * TIMESERIES column, for the same reason {@link #dropProperty} refuses one: the write path resolves those by
   * the fixed name in {@code LocalTimeSeriesType.tsColumns}, which this method does not touch.
   * <p>
   * Every check above, and the mutation itself, runs inside the single {@link #recordFileChanges} callback below:
   * validating outside it and mutating inside would let two concurrent renames both validate against the
   * pre-rename state and then both apply, since only the mutation - not the read that preceded it - is serialised
   * by the database write lock {@code recordFileChanges} takes.
   *
   * @param propertyName    the property's current name
   * @param newPropertyName the name it should have from now on
   *
   * @return the renamed property, under its new name
   */
  @Override
  public Property renameProperty(final String propertyName, final String newPropertyName) {
    checkForSchemaMutation();

    return recordFileChanges(() -> {
      final LocalProperty property = (LocalProperty) properties.get(propertyName);
      if (property == null)
        throw new SchemaException("Cannot rename the property '" + propertyName + "' in type '" + name + "' because it does not exist");

      if (propertyName.equals(newPropertyName))
        return property;

      if (this instanceof LocalTimeSeriesType tsType && tsType.isDeclaredColumn(propertyName))
        throw new SchemaException("Cannot rename the property '" + propertyName + "' in type '" + name
            + "' because it is a declared TIMESERIES column: the storage engine keeps reading and writing it under its "
            + "declared name. Drop the whole type to change the column");

      if (properties.containsKey(newPropertyName))
        throw new SchemaException("Cannot rename the property '" + propertyName + "' in type '" + name + "' to '"
            + newPropertyName + "' because a property with that name already exists");

      if (getPolymorphicPropertyNames().contains(newPropertyName))
        throw new SchemaException("Cannot rename the property '" + propertyName + "' in type '" + name + "' to '"
            + newPropertyName + "' because it is already defined in a super type");

      final TypeIndex indexOnProperty = findIndexOnProperty(propertyName);
      if (indexOnProperty != null)
        throw new SchemaException("Cannot rename the property '" + propertyName + "' in type '" + name
            + "' because it is used by index '" + indexOnProperty.getName()
            + "'. Drop the index first, rename the property, then recreate the index on the new name");

      final LocalProperty renamed = property.copyWithName(newPropertyName);

      properties.remove(propertyName);
      properties.put(newPropertyName, renamed);
      if (propertiesWithDefaultDefined.get().contains(propertyName)) {
        setPropertyHasDefault(propertyName, false);
        setPropertyHasDefault(newPropertyName, true);
      }
      return renamed;
    });
  }

  /**
   * The single point of maintenance for {@link #propertiesWithDefaultDefined}. Every mutation that can change whether
   * an own property declares a DEFAULT routes here: {@link LocalProperty#setDefaultValue} when one is set, changed or
   * cleared, and {@link #dropProperty} when the property itself goes away.
   * <p>
   * Copy-on-write: the set is published as an immutable snapshot, never mutated in place, so the lock-free readers on
   * the record-create path always see one consistent version of it. The membership check up front keeps the common
   * case (a type with no defaults at all, or a re-set that does not change membership) allocation-free.
   */
  void setPropertyHasDefault(final String propertyName, final boolean hasDefault) {
    // The common case - a re-set that does not change membership, or a type with no defaults at all - reads the
    // reference once and neither allocates nor writes. Deliberately a duplicate of the check inside updateAndGet
    // below, not a substitute for it: this one skips the closure and the copy, that one re-decides against whatever
    // the CAS actually saw. Removing either is a regression - one in allocation, the other in correctness.
    if (hasDefault == propertiesWithDefaultDefined.get().contains(propertyName))
      return;

    propertiesWithDefaultDefined.updateAndGet(current -> {
      if (hasDefault == current.contains(propertyName))
        return current;

      final Set<String> updated = new HashSet<>(current);
      if (hasDefault)
        updated.add(propertyName);
      else
        updated.remove(propertyName);

      return updated.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(updated);
    });
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).create();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize).create();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).create();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withIgnoreIfExists(true).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  public List<Bucket> getInvolvedBuckets() {
    return getBuckets(false);
  }

  @Override
  public List<Bucket> getBuckets(final boolean polymorphic) {
    return polymorphic ? cachedPolymorphicBuckets : buckets;
  }

  @Override
  public List<Integer> getBucketIds(final boolean polymorphic) {
    return polymorphic ? cachedPolymorphicBucketIds : bucketIds;
  }

  @Override
  public DocumentType addBucket(final Bucket bucket) {
    checkForSchemaMutation();
    recordFileChanges(() -> {
      addBucketInternal(bucket);
      return null;
    });
    return this;
  }

  @Override
  public DocumentType removeBucket(final Bucket bucket) {
    checkForSchemaMutation();
    recordFileChanges(() -> {
      removeBucketInternal(bucket);
      return null;
    });
    return this;
  }

  @Override
  public Bucket getBucketIdByRecord(final Document record, final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException("Cannot retrieve a bucket for type '" + name + "' because there are no buckets associated");
    return buckets.get(bucketSelectionStrategy.getBucketIdByRecord(record, async));
  }

  @Override
  public int getBucketIndexByKeys(final List<String> propertyNames, final Object[] keys, final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException(
          "Cannot retrieve a bucket for keys '" + Arrays.toString(keys) + "' because there are no buckets associated");
    return bucketSelectionStrategy.getBucketIdByKeys(propertyNames, keys, async);
  }

  @Override
  public BucketSelectionStrategy getBucketSelectionStrategy() {
    return bucketSelectionStrategy;
  }

  /**
   * Returns {@code true} when this type's partition mapping is stale - i.e. some existing records
   * may not be in the bucket their current partition strategy would route them to. Set by schema
   * mutations that change the partitioning surface (bucket add/drop on a partitioned type, or a
   * strategy change between partitioned shapes on a populated type) and cleared by a successful
   * {@code REBUILD TYPE &lt;name&gt; WITH repartition = true}. The query planner reads this flag
   * before applying the partition-pruning rule: when {@code true} the rule does not fire and the
   * query fans out across every bucket, staying correct at the cost of losing the optimisation
   * until a rebuild runs.
   */
  @Override
  public boolean isNeedsRepartition() {
    return needsRepartition.get();
  }

  /**
   * Sets the {@code needsRepartition} flag. Intended to be called from schema-mutation paths and
   * from the rebuild command. Public so tests and the rebuild executor can manipulate it; not part
   * of the public {@link com.arcadedb.schema.DocumentType} interface so user-level SQL can't reach
   * it directly. Persisted to {@code schema.json} only when {@code true}.
   * <p>
   * When the value transitions, {@link LocalSchema#saveConfiguration} is invoked so the change
   * survives a server restart. During schema load that call is a no-op (the schema's own
   * {@code readingFromFile} guard postpones the write), so this is safe to call regardless of
   * the load state.
   *
   * @return {@code true} when the value transitioned and the schema was therefore saved. Lets a caller that was
   * going to save anyway skip a second full rewrite of {@code schema.json} in the same call; every other caller is
   * free to ignore it.
   */
  public boolean setNeedsRepartition(final boolean needsRepartition) {
    // CAS guarantees that the save fires exactly once per real transition: only the thread that
    // observes the opposite value and successfully flips it proceeds; any concurrent caller
    // requesting the same target value either loses the race (CAS returns false) or finds the
    // value already at the target. Both cases skip the {@code schema.saveConfiguration()} call.
    if (!this.needsRepartition.compareAndSet(!needsRepartition, needsRepartition))
      return false;
    schema.saveConfiguration();
    return true;
  }

  /**
   * Throttled because the planner calls this on every prunable query against a stale type;
   * without throttling a single workload can produce thousands of duplicate WARNING lines/sec.
   */
  public void warnIfNeedsRepartition() {
    if (!needsRepartition.get())
      return;
    final long now = System.currentTimeMillis();
    final long last = lastRepartitionWarnMs.get();
    if (now - last <= REPARTITION_WARN_INTERVAL_MS)
      return;
    if (!lastRepartitionWarnMs.compareAndSet(last, now))
      return;
    LogManager.instance().log(this, Level.WARNING,
        """
        Type '%s' has needsRepartition=true; partition-aware bucket pruning is disabled until \
        `REBUILD TYPE %s WITH repartition = true` runs. Queries continue to return correct \
        results but fan out across all %d buckets.""",
        null, name, name, buckets.size());
  }

  // Package-private accessors for the throttle timestamp used by the schema tests to pin the
  // throttle-window contract without sleeping or scraping log output.
  long lastRepartitionWarnMsForTesting() {
    return lastRepartitionWarnMs.get();
  }

  void setLastRepartitionWarnMsForTesting(final long value) {
    lastRepartitionWarnMs.set(value);
  }

  @Override
  public DocumentType setBucketSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    return setBucketSelectionStrategy(selectionStrategy, true);
  }

  /**
   * @param persistOnItsOwn {@code false} for a caller that is already inside a {@link LocalSchema#recordFileChanges}
   *                        block, which saves the schema when it completes. That save carries the strategy just as
   *                        this method's own would - the field is assigned before either runs - so persisting here
   *                        as well would rewrite {@code schema.json} twice for one operation. Every caller that is
   *                        the whole operation passes {@code true}: the point of issue #5637 is that this mutator
   *                        must not depend on somebody else writing for it.
   */
  private DocumentType setBucketSelectionStrategy(final BucketSelectionStrategy selectionStrategy,
      final boolean persistOnItsOwn) {
    checkForSchemaMutation();
    // Bind and vet the strategy BEFORE publishing it (issue #7119). The field is read lock-free by
    // getBucketIdByRecord/getBucketIndexByKeys, so assigning first would let a concurrent insert reach a strategy
    // whose bucket count is still 0 (ThreadBucketSelectionStrategy divides by it) or whose type is still null
    // (PartitionedBucketSelectionStrategy dereferences it). Binding only reads the bucket lists, never this field,
    // so nothing here needs the assignment to have happened - and a refusal from the suitability check now leaves
    // the field untouched instead of needing a rollback. Binding itself no longer validates anything (see
    // PartitionedBucketSelectionStrategy.setType); every refusal comes out of the suitability check, which is also
    // what makes the reaction depend on how we got here.
    selectionStrategy.setType(this);
    if (selectionStrategy instanceof PartitionedBucketSelectionStrategy partitioned)
      reportPartitionSuitability(partitioned,
          schema.isReadingFromFile() ? PartitionReport.RELOAD : PartitionReport.ASSIGNMENT);
    final BucketSelectionStrategy previous = this.bucketSelectionStrategy;
    this.bucketSelectionStrategy = selectionStrategy;
    // Strategy-change flag flip (issue #4087). Switching the bucket-selection strategy on a
    // populated type can leave existing records in buckets that no longer match the new
    // strategy's hash. Two cases set the flag:
    //   - non-partitioned -> partitioned: every existing record was placed by the old strategy
    //     (round-robin / thread) without regard for the partition hash.
    //   - partitioned -> partitioned with a different property set: the modulus is the same but
    //     the hashed input differs, so most records will be in the wrong bucket.
    // Skipped when the type has zero records (the partition mapping is trivially correct over
    // an empty set). Also skipped when the new strategy is non-partitioned: round-robin and
    // thread don't have a modulus invariant the planner can prune by, so no rebuild is needed.
    // <p>
    // Skipped during schema load: the persisted needsRepartition value gets re-applied by
    // LocalSchema right after this call, and {@code hasAnyRecord()} would walk every bucket
    // and call {@code count()} (per-bucket I/O) producing a value that's about to be
    // overwritten anyway.
    boolean alreadySaved = false;
    if (!schema.isReadingFromFile()
        && selectionStrategy instanceof PartitionedBucketSelectionStrategy newPartitioned
        && partitionShapeChanged(previous, newPartitioned)
        && hasAnyRecord())
      // Use the typed setter so the schema-save-on-transition contract fires consistently with
      // every other site that mutates this flag. Direct field assignment would only work because
      // the wrapping DDL happens to save the schema afterward; relying on that is brittle.
      // Its answer says whether that contract already wrote the file, which is the same write the
      // block below would make - the strategy field was assigned before this, so the flag's save
      // carries it. Only a real transition saves, so this stays false when the flag was already set.
      alreadySaved = setNeedsRepartition(true);

    // Persist the strategy itself (issue #5637). This is the one schema mutator that used to leave the write to
    // somebody else - every sibling either calls saveConfiguration() directly (setAliases, LocalProperty's setters)
    // or goes through recordFileChanges, which calls it - so a type partitioned by `ALTER TYPE ...
    // BucketSelectionStrategy` and nothing else reopened as round-robin. That is worse than an unsuitable partition
    // being accepted: a correctly configured type lost its pruning across a restart AND started placing new records
    // round-robin among rows the partition hash had placed, with nothing said anywhere. The flag ABOUT the
    // partitioning persisted (see setNeedsRepartition above) while the partitioning did not.
    //
    // Skipped while the schema is being read back, where the value being assigned is the one just read from
    // schema.json: saveConfiguration() would only mark the schema dirty, and LocalSchema flushes that at the end of
    // load, rewriting an identical file on every single open of any partitioned database.
    //
    // Deliberately NOT also skipped when the new strategy is equivalent to the previous one, which several sibling
    // mutators do guard on. Re-issuing the same `ALTER TYPE ... BucketSelectionStrategy` is the operator's way of
    // forcing the in-memory strategy back onto disk, and the two can genuinely diverge: saveConfiguration() reports
    // an IOException by logging SEVERE and returning, so a transient disk failure leaves a type partitioned in
    // memory and round-robin in schema.json - exactly the shape this issue is about. A skip-if-unchanged guard would
    // read the in-memory value, conclude nothing changed, and turn that repair into a silent no-op. The cost of not
    // guarding is one rewrite of schema.json per redundant DDL statement, on a path measured in statements per
    // database lifetime. What IS skipped are two writes that would be redundant within one operation rather than
    // across two DDL statements: the one the needsRepartition flip just made in this same call, and the one an
    // enclosing recordFileChanges block is going to make on its way out (see persistOnItsOwn).
    if (!schema.isReadingFromFile() && !alreadySaved && persistOnItsOwn)
      schema.saveConfiguration();

    return this;
  }

  /**
   * How the caller wants a partition diagnosis reacted to. The diagnosis itself is identical in all three cases -
   * {@link PartitionedBucketSelectionStrategy#checkSuitability()} is the single source - and only the reaction moves.
   */
  private enum PartitionReport {
    /**
     * The user is choosing the strategy right now. A blocker is refused outright and warnings are advice worth one
     * line at the moment the shape is chosen.
     */
    ASSIGNMENT,
    /**
     * The strategy is being read back out of {@code schema.json}. A refusal here would turn a slow database into an
     * unopenable one, so a blocker is only logged; warnings are suppressed, because they describe a schema that was
     * accepted and is working as designed and would otherwise put a WARNING on every startup, forever - the kind of
     * line operators learn to filter out, taking the blockers with it.
     */
    RELOAD,
    /**
     * A later schema change (an index created on an already-partitioned type) has just re-decided the answer
     * (issue #5637). Everything is reported and nothing is refused: the index is what the user asked for and it is
     * useful, whereas at assignment time the strategy was what was asked for and a blocked one is pure cost.
     * <p>
     * <b>The whole current picture is reported, not the delta.</b> A type already carrying a fan-out advisory for an
     * index on {@code code} draws it again when a third index is created, even though that DDL did not cause it. The
     * alternative - work out which lines the new index is responsible for - would mean either matching on message
     * text or teaching {@code checkSuitability()} to attribute each finding to an index, and it would report a state
     * as partly acceptable while the enclosing paragraph says it is not. This says the same thing every time it is
     * asked, which is what makes the answer worth reading; a {@code CREATE INDEX} is rare, deliberate DDL, and the
     * line count is bounded by the number of indexes on the type.
     */
    SCHEMA_CHANGE
  }

  /**
   * Reports what the partition configuration will actually do, at the moment it is decided rather than at the first
   * query that comes up short (issue #5603).
   * <p>
   * Both #5589 and #5595 were the same story from the user's side: the strategy attached without complaint and the
   * damage - missing rows, duplicates in a UNIQUE index, an unexplained slowdown - surfaced much later at read time,
   * with nothing tying it back to the {@code ALTER TYPE}. The read side is fixed; this is the other half, saying so
   * up front.
   * <p>
   * <b>The moment is not only the assignment.</b> A partition's suitability is a fact about the type AND its indexes,
   * so a {@code CREATE INDEX} that lands afterwards can flip the answer - recollating the partition index {@code
   * COLLATE CI} makes it unprunable, and an index on other properties adds a lookup that fans out. That reordering
   * of the same DDL walked straight past the assignment-time check, which is why {@link TypeIndexBuilder} calls this
   * too (issue #5637).
   * <p>
   * Blockers repeat on every open, deliberately: those describe a database that is still paying for a strategy it
   * cannot use, and that stays worth saying until somebody acts on it. Warnings never refuse anywhere - a second
   * index on non-partition properties is a perfectly reasonable schema, it just does not benefit from the
   * partitioning - and they are reported last, so a configuration that is about to be refused outright does not
   * first draw advice on how to speed it up.
   *
   * @param mode what to do with the diagnosis; see {@link PartitionReport}
   */
  private void reportPartitionSuitability(final PartitionedBucketSelectionStrategy partitioned,
      final PartitionReport mode) {
    final PartitionedBucketSelectionStrategy.Suitability suitability = partitioned.checkSuitability();

    if (!suitability.isUsable()) {
      final String reasons = String.join("; ", suitability.blockers());

      if (mode == PartitionReport.ASSIGNMENT)
        throw new SchemaException(
            "Cannot use the partitioned bucket selection strategy on " + partitioned.getProperties() + " for type '"
                + name + "': " + reasons
                + ". No lookup would ever be pruned to one bucket, so the partitioning would cost the placement "
                + "constraints and return nothing. Use `round-robin` instead, or fix the partition key or its index.");

      LogManager.instance().log(this, Level.WARNING, """
          Type '%s' is configured with the partitioned bucket selection strategy on %s, but no lookup can be pruned \
          to one bucket because %s. Queries stay correct - every lookup fans out across all %d buckets - but the \
          partitioning buys nothing. Switch the type back to `round-robin`, or fix the configuration and run \
          `REBUILD TYPE %s WITH repartition = true`.""", null, name, partitioned.getProperties(), reasons,
          buckets.size(), name);
    }

    if (mode == PartitionReport.RELOAD)
      return;

    for (final String warning : suitability.warnings())
      LogManager.instance().log(this, Level.WARNING,
          "Type '%s' uses the partitioned bucket selection strategy on %s, but %s", null, name,
          partitioned.getProperties(), warning);
  }

  /**
   * Re-runs the partition diagnosis after a schema change that can have altered it, reporting everything and
   * refusing nothing (issue #5637). A no-op on a type that is not partitioned.
   * <p>
   * Called from {@link TypeIndexBuilder} once per {@code CREATE INDEX}, and from {@link LocalSchema#dropIndexInternal}
   * once per {@code DROP INDEX} (issue #5646). The obvious hook, {@link #addIndexInternal}, runs once per BUCKET, so
   * it would multiply every line by the bucket count and fire during schema reload as well.
   * <p>
   * <b>Deferred to the end of the enclosing transaction, when one is active.</b> Recollating an index is a
   * {@code DROP INDEX} followed by a {@code CREATE INDEX}, usually in one transaction; reporting synchronously on the
   * drop would announce "there is no unique automatic index on the partition properties" in the middle of a sequence
   * that is about to put one back - a line that is true for the instant it is printed, misleading by the time it is
   * read, and printed ahead of the accurate one the create emits. Deferring the diagnosis to commit, keyed per type
   * name via {@link TransactionContext#addAfterCommitCallbackIfAbsent}, means a {@code DROP}-then-{@code CREATE} (or
   * any other run of index-surface edits on the same type within one transaction) is diagnosed once against the
   * state the transaction settled on, whichever statement caused it.
   * <p>
   * Outside a transaction - the auto-commit / single-statement path - there is no commit to defer to, so the
   * diagnosis runs immediately, which is also what kept the pre-#5646 {@code CREATE INDEX} behaviour for that case.
   * <p>
   * <b>Skipped entirely for a type that is itself being dropped.</b> {@link LocalSchema#dropType} drops every one of
   * a type's indexes (via {@link LocalSchema#dropIndexInternal}) before removing the type from the schema, so without
   * this check a {@code DROP TYPE} on a partitioned type would report a spurious "no unique automatic index" blocker
   * for a type that no longer exists by the time the report is read - immediately if outside a transaction, or via
   * the deferred callback otherwise, since {@code dropType} itself runs inside one transaction.
   */
  void reportPartitionSuitabilityAfterSchemaChange() {
    if (!(bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy) || !schema.existsType(name))
      return;

    final DatabaseInternal database = (DatabaseInternal) schema.getDatabase();
    if (database.isTransactionActive())
      database.getTransaction().addAfterCommitCallbackIfAbsent("partition-suitability:" + name,
          this::reportPartitionSuitabilityNow);
    else
      reportPartitionSuitabilityNow();
  }

  /**
   * The actual diagnosis, run either immediately or from the deferred callback {@link #reportPartitionSuitabilityAfterSchemaChange}
   * registers. The callback is a method reference bound to {@code this} at scheduling time, so if a
   * {@code DROP TYPE}-then-{@code CREATE TYPE} of the same name happened before it fires, {@code this} is a stale,
   * no-longer-registered instance - {@code schema.existsType(name)} alone would come back {@code true} off the
   * *new* type sharing the name, while every field read through {@code this} would still be the dropped instance's.
   * Re-resolving the live type by name and reading/reporting through it instead of {@code this} sidesteps that: a
   * type reverted to round-robin, dropped outright, or dropped-and-recreated between scheduling and commit is
   * correctly diagnosed against whatever is actually registered under the name now, or not diagnosed at all if
   * nothing is (issue #5646 review follow-up on PR #5946).
   */
  private void reportPartitionSuitabilityNow() {
    if (!schema.existsType(name))
      return;
    final LocalDocumentType live = schema.getType(name);
    if (live.bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy partitioned)
      live.reportPartitionSuitability(partitioned, PartitionReport.SCHEMA_CHANGE);
  }

  /**
   * True if the type has at least one record across any of its (non-polymorphic) buckets.
   * <p>
   * <b>Performance.</b> Two-phase lookup. Phase 1 reads {@link LocalBucket#getCachedRecordCount}
   * (a plain {@link AtomicLong} read, no I/O) on each bucket and short-circuits on the first
   * positive count. The cache is populated by {@link Bucket#count}, transaction commits, and
   * WAL replay - hot in steady state, so a populated type returns {@code true} in O(buckets)
   * memory ops with no page-cache access. Phase 2 falls back to {@link Bucket#count} only when
   * no cached value yielded a positive answer (every cache is {@code -1}, every cache is
   * {@code 0}, or a mix of the two). That handles the cold-cache case AND the in-transaction
   * delta case (e.g. a single TX that inserts records and then alters the schema), since
   * {@link Bucket#count} folds the active transaction's delta into the returned value. The
   * fallback also warms the cache, so subsequent DDL calls on the same type stay on Phase 1.
   * <p>
   * Called from {@link #setBucketSelectionStrategy(BucketSelectionStrategy)},
   * {@link #addBucketInternal}, and {@link #removeBucketInternal} to gate the
   * {@code needsRepartition} flag flip; a per-type record counter (along the lines of
   * {@link #ownExternalPropertyCount}) would make this strictly O(1) but requires hooks on
   * every record save / delete - tracked as a follow-up if the DDL hot path becomes a bottleneck
   * on types with many buckets where Phase 1 routinely misses.
   */
  private boolean hasAnyRecord() {
    // Phase 1: cheap cache-only check.
    for (final Bucket b : buckets) {
      if (b instanceof LocalBucket lb && lb.getCachedRecordCount() > 0L)
        return true;
    }
    // Phase 2: cache miss or all-zero. Delegate to count() to cover cold caches and
    // in-transaction deltas; the call warms the cache for next time.
    for (final Bucket b : buckets) {
      if (b.count() > 0L)
        return true;
    }
    return false;
  }

  /**
   * Returns {@code true} when transitioning from {@code previous} to {@code newPartitioned}
   * changes the partition shape - i.e. either the previous strategy was not partitioned at
   * all, or it was partitioned on a different property set. Same property set on both sides
   * means the hash inputs are identical and existing records are still correctly placed.
   */
  private static boolean partitionShapeChanged(final BucketSelectionStrategy previous,
      final PartitionedBucketSelectionStrategy newPartitioned) {
    if (!(previous instanceof PartitionedBucketSelectionStrategy oldPartitioned))
      return true;
    return !Objects.equals(oldPartitioned.getProperties(), newPartitioned.getProperties());
  }

  @Override
  public DocumentType setBucketSelectionStrategy(final String selectionStrategyName, final Object... args) {
    BucketSelectionStrategy selectionStrategy;
    if ("thread".equalsIgnoreCase(selectionStrategyName))
      selectionStrategy = new ThreadBucketSelectionStrategy();
    else if ("round-robin".equalsIgnoreCase(selectionStrategyName))
      selectionStrategy = new RoundRobinBucketSelectionStrategy();
    else if ("partitioned".equalsIgnoreCase(selectionStrategyName)) {
      final List<String> convertedParams = new ArrayList<>(args.length);
      for (int i = 0; i < args.length; i++)
        convertedParams.add(FileUtils.getStringContent(args[i]));

      selectionStrategy = new PartitionedBucketSelectionStrategy(convertedParams);
    } else if (selectionStrategyName.startsWith("partitioned(") && selectionStrategyName.endsWith(")")) {
      final String[] params = selectionStrategyName.substring("partitioned(".length(), selectionStrategyName.length() - 1)
          .split(",");
      final List<String> convertedParams = new ArrayList<>(params.length);
      for (int i = 0; i < params.length; i++)
        convertedParams.add(FileUtils.getStringContent(params[i]));

      selectionStrategy = new PartitionedBucketSelectionStrategy(convertedParams);
    } else {
      // GET THE VALUE AS FULL-CLASS-NAME
      try {
        selectionStrategy = (BucketSelectionStrategy) Class.forName(selectionStrategyName).getConstructor().newInstance();
      } catch (Exception e) {
        throw new SchemaException("Cannot find bucket selection strategy class '" + selectionStrategyName + "'", e);
      }
    }

    // Delegate to the typed setter so the needsRepartition flag-flip hook fires uniformly.
    return setBucketSelectionStrategy(selectionStrategy);
  }

  @Override
  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  @Override
  public boolean existsPolymorphicProperty(final String propertyName) {
    return getPolymorphicPropertyNames().contains(propertyName);
  }

  @Override
  public Property getPropertyIfExists(final String propertyName) {
    return properties.get(propertyName);
  }

  @Override
  public Collection<TypeIndex> getAllIndexes(final boolean polymorphic) {
    if (!polymorphic || superTypes.isEmpty())
      return Collections.unmodifiableCollection(indexesByProperties.values());

    final Set<TypeIndex> set = new HashSet<>(indexesByProperties.values());

    for (final DocumentType t : superTypes)
      set.addAll(t.getAllIndexes(true));

    return Collections.unmodifiableSet(set);
  }

  @Override
  public List<IndexInternal> getPolymorphicBucketIndexByBucketId(final int bucketId, final List<String> filterByProperties) {
    List<IndexInternal> r = bucketIndexesByBucket.get(bucketId);

    if (r != null && filterByProperties != null) {
      // FILTER BY PROPERTY NAMES
      r = new ArrayList<>(r);
      r.removeIf(idx -> !idx.getPropertyNames().equals(filterByProperties));
    }

    if (superTypes.isEmpty()) {
      // MOST COMMON CASES, OPTIMIZATION AVOIDING CREATING NEW LISTS
      if (r == null)
        return Collections.emptyList();
      else
        // MOST COMMON CASE, SAVE CREATING AND COPYING TO A NEW ARRAY
        return Collections.unmodifiableList(r);
    }

    final List<IndexInternal> result = r != null ? new ArrayList<>(r) : new ArrayList<>();
    for (final DocumentType t : superTypes)
      result.addAll(t.getPolymorphicBucketIndexByBucketId(bucketId, filterByProperties));

    return result;
  }

  @Override
  public List<TypeIndex> getIndexesByProperties(final String property1, final String... propertiesN) {
    final Set<String> properties = new HashSet<>(propertiesN.length + 1);
    properties.add(property1);
    Collections.addAll(properties, propertiesN);
    return getIndexesByProperties(properties);
  }

  @Override
  public List<TypeIndex> getIndexesByProperties(final Collection<String> properties) {
    final List<TypeIndex> result = new ArrayList<>();

    for (final Map.Entry<List<String>, TypeIndex> entry : indexesByProperties.entrySet()) {
      for (final String prop : entry.getKey()) {
        if (properties.contains(prop)) {
          result.add(entry.getValue());
          break;
        }
      }
    }
    return result;
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(final String... properties) {
    return getPolymorphicIndexByProperties(Arrays.asList(properties));
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(final List<String> properties) {
    TypeIndex idx = indexesByProperties.get(properties);

    if (idx == null)
      for (final DocumentType t : superTypes) {
        idx = t.getPolymorphicIndexByProperties(properties);
        if (idx != null)
          break;
      }

    return idx;
  }

  @Override
  public TypeIndex getIndexByProperties(final String... properties) {
    return getIndexByProperties(Arrays.asList(properties));
  }

  @Override
  public TypeIndex getIndexByProperties(final List<String> properties) {
    return indexesByProperties.get(properties);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean isTheSameAs(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final LocalDocumentType that = (LocalDocumentType) o;
    if (!Objects.equals(name, that.name))
      return false;

    if (superTypes.size() != that.superTypes.size())
      return false;

    final Set<String> set = new HashSet<>();
    for (final LocalDocumentType t : superTypes)
      set.add(t.name);

    for (final LocalDocumentType t : that.superTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (subTypes.size() != that.subTypes.size())
      return false;

    for (final LocalDocumentType t : subTypes)
      set.add(t.name);

    for (final LocalDocumentType t : that.subTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (buckets.size() != that.buckets.size())
      return false;

    for (final Bucket t : buckets)
      set.add(t.getName());

    for (final Bucket t : that.buckets)
      set.remove(t.getName());

    if (!set.isEmpty())
      return false;

    if (properties.size() != that.properties.size())
      return false;

    for (final Property p : properties.values())
      set.add(p.getName());

    for (final Property p : that.properties.values())
      set.remove(p.getName());

    if (!set.isEmpty())
      return false;

    for (final Property p1 : properties.values()) {
      final Property p2 = that.properties.get(p1.getName());
      if (!p1.equals(p2))
        return false;
    }

    if (bucketIndexesByBucket.size() != that.bucketIndexesByBucket.size())
      return false;

    for (final Map.Entry<Integer, List<IndexInternal>> entry1 : bucketIndexesByBucket.entrySet()) {
      final List<IndexInternal> value2 = that.bucketIndexesByBucket.get(entry1.getKey());
      if (value2 == null)
        return false;
      if (entry1.getValue().size() != value2.size())
        return false;

      for (int i = 0; i < value2.size(); ++i) {
        final Index m1 = entry1.getValue().get(i);
        final Index m2 = value2.get(i);

        if (m1.getAssociatedBucketId() != m2.getAssociatedBucketId())
          return false;
        // Index names contain timestamps that differ across HA nodes, so compare structurally
        if (m1.getPropertyNames().size() != m2.getPropertyNames().size())
          return false;

        for (int p = 0; p < m1.getPropertyNames().size(); ++p) {
          if (!m1.getPropertyNames().get(p).equals(m2.getPropertyNames().get(p)))
            return false;
        }
      }
    }

    if (indexesByProperties.size() != that.indexesByProperties.size())
      return false;

    for (final Map.Entry<List<String>, TypeIndex> entry1 : indexesByProperties.entrySet()) {
      final TypeIndex index2 = that.indexesByProperties.get(entry1.getKey());
      if (index2 == null)
        return false;

      final TypeIndex index1 = entry1.getValue();

      if (!index1.equals(index2))
        return false;
    }

    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final LocalDocumentType that = (LocalDocumentType) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  protected void addIndexInternal(final IndexInternal index, final int bucketId, final String[] propertyNames,
      TypeIndex propIndex) {
    index.getMetadata().typeName = name;
    index.getMetadata().propertyNames = List.of(propertyNames);
    index.getMetadata().associatedBucketId = bucketId;

    final List<IndexInternal> list = bucketIndexesByBucket.computeIfAbsent(bucketId, k -> new ArrayList<>());
    list.add(index);

    if (propIndex == null) {
      final List<String> propertyList = Arrays.asList(propertyNames);
      propIndex = indexesByProperties.get(propertyList);
      // A wrapper found here can be "invalidated" if a prior {@link TypeIndex#drop()} (e.g.
      // existingTypeIndex.drop() in {@link TypeIndexBuilder#create()}) ran on it but the schema's
      // own auto-removal hook (LocalSchema#dropIndex, #4179) failed to dislodge it - this is the
      // observable symptom of the "is not valid" failures during REBUILD INDEX / index recreation.
      // Treat a stale entry as if it weren't there: mint a fresh wrapper so the new bucket index
      // can attach.
      if (propIndex != null && !propIndex.isValid()) {
        indexesByProperties.remove(propertyList);
        schema.removeIndexDuringLoad(propIndex.getName());
        propIndex = null;
      }
      if (propIndex == null) {
        // CREATE THE TYPE-INDEX FOR THE 1ST TIME. Honour any user-supplied name carried on the
        // bucket-level index metadata (issue #4139): {@code CREATE INDEX <manual_name> ON ...}
        // sets metadata.typeIndexName via TypeIndexBuilder, the schema reload re-attaches it via
        // LSMTreeIndex.setMetadata, and we use it here so the TypeIndex (and therefore the
        // schema-level indexMap entry / Studio's "Indexes" view) is keyed by the manual name.
        // Falling back to the auto-derived form keeps the public default name unchanged for all
        // callers that did not supply a manual name.
        final String customName = index.getMetadata().typeIndexName;
        final String typeIndexName = customName != null && !customName.isEmpty() ?
            customName :
            name + Arrays.toString(propertyNames).replace(" ", "");
        propIndex = new TypeIndex(typeIndexName, this);
        // Staged while a schema load is in flight (issue #7213): this wrapper is the name a query resolves, and
        // publishing it here would hand a reader a TypeIndex over a bucket-level index that has not run its
        // onAfterSchemaLoad() yet - for a vector index, one with no vectors loaded.
        schema.publishIndexDuringLoad(propIndex.getName(), propIndex);
        indexesByProperties.put(propertyList, propIndex);
      }
    }

    // ADD AS SUB-INDEX
    propIndex.addIndexOnBucket(index);
    this.bucketSelectionStrategy.setType(this);
  }

  public void removeTypeIndexInternal(final TypeIndex index) {
    for (final Iterator<TypeIndex> it = indexesByProperties.values().iterator(); it.hasNext(); ) {
      final TypeIndex idx = it.next();
      if (idx == index) {
        it.remove();
        break;
      }
    }

    for (final IndexInternal idx : index.getIndexesOnBuckets())
      removeBucketIndexInternal(idx);

    for (final LocalDocumentType superType : superTypes)
      superType.removeTypeIndexInternal(index);
  }

  public void removeBucketIndexInternal(final Index index) {
    final List<IndexInternal> list = bucketIndexesByBucket.get(index.getAssociatedBucketId());
    if (list != null) {
      list.remove(index);
      if (list.isEmpty())
        bucketIndexesByBucket.remove(index.getAssociatedBucketId());
    }
  }

  protected void addBucketInternal(final Bucket bucket) {
    for (final DocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new SchemaException("Cannot add the bucket '" + bucket.getName() + "' to the type '" + name
            + "', because the bucket is already associated to the type '" + cl.getName() + "'");
    }

    // If we already have a partitioned strategy in place, growing the bucket count changes the
    // {@code hash(value) % bucketCount} modulus and invalidates the partition mapping for any
    // existing record. Mark the type as needing a repartition so the planner-side pruning rule
    // suppresses itself until {@code REBUILD TYPE} clears the flag. Skip when the strategy is
    // not partitioned (round-robin / thread don't care about bucket count for correctness),
    // skip on the very first bucket of a type (CREATE TYPE adds buckets one by one before any
    // record can exist), and skip when the type is established but empty (no records means
    // there's nothing to repartition - flagging would prompt a no-op rebuild).
    final boolean partitionedAndPopulated = bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy
        && !buckets.isEmpty()
        && hasAnyRecord();

    buckets = CollectionUtils.addToUnmodifiableList(buckets, bucket);
    cachedPolymorphicBuckets = CollectionUtils.addToUnmodifiableList(cachedPolymorphicBuckets, bucket);

    bucketIds = CollectionUtils.addToUnmodifiableList(bucketIds, bucket.getFileId());
    cachedPolymorphicBucketIds = CollectionUtils.addToUnmodifiableList(cachedPolymorphicBucketIds, bucket.getFileId());

    // PROPAGATE THE NEW BUCKET UP THE INHERITANCE TREE. THE SUPER TYPES ARE OFTEN LINKED BEFORE THE BUCKET EXISTS
    // (`ALTER TYPE x BUCKET +y`, GROWING AN EXISTING TYPE THROUGH THE TYPE BUILDER), AND THEIR POLYMORPHIC CACHES
    // ARE OTHERWISE ONLY REBUILT ON SCHEMA RELOAD, LEAVING EVERY POLYMORPHIC READ ON THE SUPER TYPE BLIND TO THIS
    // BUCKET UNTIL THE NEXT RESTART (ISSUE #5297).
    if (!superTypes.isEmpty()) {
      final List<Bucket>  addedBuckets   = List.of(bucket);
      final List<Integer> addedBucketIds = List.of(bucket.getFileId());
      for (final LocalDocumentType s : superTypes)
        s.updatePolymorphicBucketsCache(true, addedBuckets, addedBucketIds);
    }

    bucketSelectionStrategy.setType(this);

    if (partitionedAndPopulated)
      // Use the typed setter so the schema-save-on-transition contract fires consistently with
      // every other site that mutates this flag (see setNeedsRepartition Javadoc).
      setNeedsRepartition(true);

    // AUTOMATICALLY CREATES THE INDEX ON THE NEW BUCKET (INCLUDING INHERITED INDEXES FROM PARENT TYPES)
    final Collection<TypeIndex> existentIndexes = getAllIndexes(true);

    if (!existentIndexes.isEmpty()) {
      // ONE TRANSACTION OF ITS OWN, and unlike the sibling propagation in addSuperType() this one does NOT have to
      // join the caller's - do not "fix" it by symmetry (issue #6359, item 1). The bucket being indexed here has just
      // been created, so no transaction can be holding uncommitted writes into it and the scan has nothing to miss;
      // joining would only cost the build its chunked commit.
      schema.getDatabase().transaction(() -> {
        for (TypeIndex idx : existentIndexes) {
          // getPageSizeForNewFile(), not getPageSize(): this creates a NEW index file, so the page size carried over has
          // to be one the creation path accepts. A HASH index predating #5713 can hold an unaddressable one, and
          // adding a bucket to its type must not fail because of it (#5713).
          //
          // getMetadataForNewFile(), not getMetadata(), for the same reason applied to the configuration: on a WRAPPER
          // index type the latter answers with the underlying LSM-Tree's plain metadata, so the sub-index minted here
          // for the new bucket came up with the DEFAULT analyzer, the DEFAULT geohash resolution or - for a sparse
          // vector index, which refuses a plain IndexMetadata outright - not at all (issue #5742). The instance is
          // handed over as-is rather than copied: this is one more bucket of the SAME logical index, which is exactly
          // what index creation does when it passes one metadata instance to every bucket sub-index, and what keeps
          // the full-text corpus counters type-wide instead of restarting them at zero for the new bucket.
          schema.createBucketIndex(this, idx.getKeyTypes(), bucket, name, idx.getType(), idx.isUnique(),
              idx.getPageSizeForNewFile(),
              idx.getNullStrategy(), null, idx.getPropertyNames().toArray(new String[idx.getPropertyNames().size()]), idx,
              IndexBuilder.BUILD_BATCH_SIZE,
              idx.getMetadataForNewFile());
        }
      });
    }

    // IF THE TYPE ALREADY HAS EXTERNAL PROPERTIES, ENSURE A PAIRED EXTERNAL BUCKET FOR THIS NEW PRIMARY BUCKET
    if (hasExternalProperties())
      ensureExternalBucketFor((LocalBucket) bucket);
  }

  /** Polymorphic: counts inherited EXTERNAL properties too. O(1) on own count + O(depth) on supertype walk. */
  public boolean hasExternalProperties() {
    if (ownExternalPropertyCount.get() > 0)
      return true;
    for (final LocalDocumentType st : superTypes)
      if (st.hasExternalProperties())
        return true;
    return false;
  }

  public Integer getExternalBucketIdFor(final int primaryBucketId) {
    return externalBucketIdByPrimaryBucketId.get(primaryBucketId);
  }

  /** True when this type still owns at least one paired external-property bucket. */
  public boolean hasExternalBuckets() {
    return !externalBucketIdByPrimaryBucketId.isEmpty();
  }

  public void ensureExternalBuckets() {
    for (final Bucket b : buckets)
      ensureExternalBucketFor((LocalBucket) b);
  }

  /** Recurses into subtypes: records of a subtype live in subtype primary buckets, so each needs its own paired ext. */
  public void ensureExternalBucketsRecursive() {
    ensureExternalBuckets();
    for (final LocalDocumentType sub : subTypes)
      sub.ensureExternalBucketsRecursive();
  }

  private void ensureExternalBucketFor(final LocalBucket primary) {
    // Atomic check-and-create: two threads racing through ensureExternalBuckets()/addBucketInternal() must not
    // both attempt to allocate the paired _ext bucket and trip schema.createBucket's "already exists" guard.
    externalBucketIdByPrimaryBucketId.computeIfAbsent(primary.getFileId(), pid -> {
      final String extName = InternalBucketNaming.externalPropertyBucketName(primary.getName());
      final LocalBucket external;
      final LocalBucket registered = schema.lookupBucket(extName);
      if (registered != null) {
        external = registered;
        // Refuse to adopt a bucket that is already registered as the primary bucket of some user type.
        // {@code bucketId2TypeMap} is the authoritative source of "this bucket is a user type's primary
        // bucket": it is rebuilt from each type's {@code getBuckets(false)} list (primary buckets only;
        // adopted external buckets are NOT in this map, they live in per-type externalBucketIdByPrimaryBucketId).
        // We therefore do NOT consult {@link LocalBucket#getPurpose}: purpose is transient and reset to PRIMARY
        // on load, and depending on it would create a fragile ordering requirement against
        // restoreExternalBuckets() running first. {@code computeIfAbsent} above already short-circuits when
        // this type's own ext bucket is already adopted, so any hit here means the candidate name belongs to
        // a different type's primary bucket, full stop.
        if (schema.getTypeByBucketId(external.getFileId()) != null)
          throw new SchemaException(
              "Cannot adopt bucket '" + extName + "' as the external-property bucket for type '" + name
                  + "': it is already a primary bucket of another user type. The paired external bucket is named"
                  + " <primaryBucketName>_ext, so a primary bucket called '" + extName + "' (or one whose name"
                  + " ends in '_ext' that matches another type's primary bucket) collides with this convention."
                  + " Resolve by renaming the conflicting primary bucket via 'ALTER BUCKET " + extName
                  + " NAME ...' so its name no longer ends in '_ext' or no longer collides, then retry the EXTERNAL"
                  + " property change.");
      } else {
        // External buckets get larger pages (256KB vs 64KB primary), a smaller slot table (256 vs 2048: file-format
        // version EXTERNAL_BUCKET_VERSION), and optional placement on cheaper-storage tier via
        // resolveExternalBucketPath() which returns <override>/<dbName>.
        final int pageSize = schema.getDatabase().getConfiguration()
            .getValueAsInteger(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE);
        // Unwrap to the embedded LocalDatabase: schema.getDatabase() can be the HA wrapper
        // (e.g. RaftReplicatedDatabase) when this method runs after HA is enabled. Issue #4144.
        final LocalDatabase localDb = (LocalDatabase) ((DatabaseInternal) schema.getDatabase()).getEmbedded();
        final String overridePath = localDb.resolveExternalBucketPath();
        external = schema.createBucket(extName, pageSize, overridePath, LocalBucket.EXTERNAL_BUCKET_VERSION);
      }
      external.setPurpose(LocalBucket.Purpose.EXTERNAL_PROPERTY);
      return external.getFileId();
    });
  }

  /**
   * Drops paired external-property buckets that no longer back any record (typical case: a REBUILD TYPE that
   * moved every value back inline because the EXTERNAL flag was just toggled off). Skips buckets that still
   * hold records so we never lose data; those point at persistent corruption and need investigation.
   * <p>
   * <b>Preconditions (caller-enforced).</b>
   * <ol>
   *   <li>{@code !hasExternalProperties()} - the type no longer has any EXTERNAL property to write into the
   *       bucket. Without this, a concurrent insert could legitimately write into the bucket between the
   *       {@code count() == 0} check and {@code dropBucket}.</li>
   *   <li>No transaction is active (or any active transaction has been committed before this call). The
   *       {@code count()} read consults pageManager state that has not yet flushed dirty pages from a still-open
   *       transaction, so an open tx can hide records from this method and lead to a data-losing drop.</li>
   * </ol>
   * The current production caller ({@code RebuildTypeStatement}) only invokes this on the
   * {@code implicitTx == true} path, after committing its own transaction, so both preconditions are met.
   * Any new caller MUST honour both, or restructure to take a schema-level write lock that blocks inserts
   * for the duration of the count-then-drop sequence.
   */
  public void reclaimEmptyExternalBuckets() {
    // Fail-fast on precondition #1 from the javadoc. A no-op return would silently mask the bug; throwing
    // surfaces it during development AND production. The cost is one O(1) atomic read on a hot path that
    // only runs after schema mutations and REBUILD TYPE, so it is negligible.
    if (hasExternalProperties())
      throw new IllegalStateException(
          "reclaimEmptyExternalBuckets() requires !hasExternalProperties() but type '" + name + "' still has at"
              + " least one EXTERNAL property. Calling this with EXTERNAL properties present would race with"
              + " concurrent inserts that legitimately write into the bucket between the count() check and"
              + " dropBucket(). Drop the EXTERNAL flag on every property of the type before reclaiming.");
    // The TOCTOU window between count() == 0 and dropBucket() is closed when the caller honours the second
    // precondition (no active transaction). With the EXTERNAL flag off, no new code path will write into
    // these buckets; with no in-flight tx, no queued update can have a record waiting to flush either.
    // Use an explicit throw instead of `assert`: production JVMs run without -ea, and a violation here can
    // lose data (we'd drop a bucket that has a queued tx record about to flush into it). Same pattern as
    // the hasExternalProperties() guard two lines above.
    // isTransactionActive() is on the Database interface; calling it directly avoids the
    // unsafe (LocalDatabase) cast that fails when the database is HA-wrapped. Issue #4144.
    if (schema.getDatabase().isTransactionActive())
      throw new IllegalStateException(
          "reclaimEmptyExternalBuckets() requires no active transaction but one is open on database '"
              + schema.getDatabase().getName() + "'. Queued record updates have not flushed yet, so the"
              + " count() check could miss records about to land in the bucket and we'd drop it under them."
              + " Commit (or rollback) the active transaction before calling reclaimEmptyExternalBuckets().");
    final List<Integer> toDrop = new ArrayList<>();
    for (final Map.Entry<Integer, Integer> entry : externalBucketIdByPrimaryBucketId.entrySet()) {
      final LocalBucket extBucket = schema.getBucketById(entry.getValue(), false);
      if (extBucket == null || extBucket.count() == 0L)
        toDrop.add(entry.getKey());
    }
    for (final Integer primaryBucketId : toDrop) {
      final Integer extBucketId = externalBucketIdByPrimaryBucketId.remove(primaryBucketId);
      if (extBucketId == null)
        continue;
      final LocalBucket extBucket = schema.getBucketById(extBucketId, false);
      if (extBucket != null)
        schema.dropBucket(extBucket.getName());
    }
    schema.saveConfiguration();
  }

  /**
   * Re-applies EXTERNAL_PROPERTY purpose (transient on {@link LocalBucket}) and rebuilds the map from JSON at
   * load time. After processing the JSON-driven entries, runs a name-based heuristic sweep over this type's
   * primary buckets: for any primary bucket whose paired '<primary>_ext' sibling exists in the schema's
   * bucketMap but was missing from the JSON (corruption, partial migration, JSON edited by hand), adopt the
   * sibling as the external bucket and tag its purpose. Without this fallback the {@code purpose} field would
   * default to {@code PRIMARY}, the DML write guard ({@code LocalDatabase.createRecordNoLock}) would let users
   * target the bucket directly, and an INSERT could corrupt internal payload bytes. Adoption is refused for
   * any '_ext' bucket that bucketId2TypeMap already claims as another type's primary bucket.
   */
  void restoreExternalBuckets(final Map<String, String> primaryNameToExternalName) {
    externalBucketIdByPrimaryBucketId.clear();
    for (final Map.Entry<String, String> entry : primaryNameToExternalName.entrySet()) {
      final LocalBucket primary = schema.lookupBucket(entry.getKey());
      final LocalBucket external = schema.lookupBucket(entry.getValue());
      if (primary == null) {
        LogManager.instance()
            .log(this, Level.WARNING, "Cannot restore external bucket mapping for type '%s': primary bucket '%s' not found",
                null, name, entry.getKey());
        continue;
      }
      if (external == null) {
        // Tiered bucket file not found: usually means arcadedb.externalPropertyBucketPath is unset on this restart
        // but was set when the bucket was created. Reads of EXTERNAL properties for records in this primary
        // bucket would silently fail - so we surface the configuration mismatch loudly and explicitly.
        LogManager.instance().log(this, Level.SEVERE,
            """
            Cannot find external bucket '%s' for type '%s' primary bucket '%s'. If the bucket was tiered to a \
            secondary path, set 'arcadedb.externalPropertyBucketPath' to the same value used at creation \
            time before reopening the database. EXTERNAL property reads on this type will fail until fixed.""",
            null, entry.getValue(), name, entry.getKey());
        continue;
      }
      external.setPurpose(LocalBucket.Purpose.EXTERNAL_PROPERTY);
      externalBucketIdByPrimaryBucketId.put(primary.getFileId(), external.getFileId());
    }

    // Heuristic recovery: for every primary bucket of this type that does NOT yet have a paired entry in
    // externalBucketIdByPrimaryBucketId, look for '<primaryName>_ext' in bucketMap and adopt it. Defends
    // against schema.json missing the externalBuckets key (corruption, migration from an older snapshot, or
    // a hand-edit). Refuses to adopt if the candidate is already registered as another type's primary
    // bucket (bucketId2TypeMap is authoritative; the field-level Purpose is transient and unreliable here).
    for (final Bucket primaryBucket : buckets) {
      if (externalBucketIdByPrimaryBucketId.containsKey(primaryBucket.getFileId()))
        continue;
      final String candidateName = InternalBucketNaming.externalPropertyBucketName(primaryBucket.getName());
      final LocalBucket candidate = schema.lookupBucket(candidateName);
      if (candidate == null)
        continue;
      if (schema.getTypeByBucketId(candidate.getFileId()) != null) {
        // candidate is some other type's primary bucket; refuse to repurpose, just log so the operator sees it.
        LogManager.instance().log(this, Level.WARNING,
            """
            Heuristic recovery for type '%s': bucket '%s' looks like a paired external bucket by name but is\
             already a primary bucket of another user type. Skipping adoption.""",
            null, name, candidateName);
        continue;
      }
      candidate.setPurpose(LocalBucket.Purpose.EXTERNAL_PROPERTY);
      externalBucketIdByPrimaryBucketId.put(primaryBucket.getFileId(), candidate.getFileId());
      LogManager.instance().log(this, Level.WARNING,
          """
          Heuristic recovery for type '%s': adopted bucket '%s' as the external-property bucket for primary\
           '%s'. The schema.json was missing the matching externalBuckets entry; it will be re-saved on\
           the next schema mutation.""",
          null, name, candidateName, primaryBucket.getName());
    }
  }

  protected void removeBucketInternal(final Bucket bucket) {
    if (!buckets.contains(bucket))
      throw new SchemaException("Cannot remove the bucket '" + bucket.getName() + "' to the type '" + name
          + "', because the bucket is not associated to the type '" + getName() + "'");

    // Symmetric to addBucketInternal: shrinking the bucket count under a partitioned strategy
    // also invalidates the modulus. Skip on an empty type so a no-op rebuild isn't requested
    // and a needless schema save isn't triggered. Use the typed setter so the
    // schema-save-on-transition contract fires consistently (see setNeedsRepartition Javadoc).
    if (bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy && hasAnyRecord())
      setNeedsRepartition(true);

    buckets = CollectionUtils.removeFromUnmodifiableList(buckets, bucket);
    cachedPolymorphicBuckets = CollectionUtils.removeFromUnmodifiableList(cachedPolymorphicBuckets, bucket);

    bucketIds = CollectionUtils.removeFromUnmodifiableList(bucketIds, bucket.getFileId());
    cachedPolymorphicBucketIds = CollectionUtils.removeFromUnmodifiableList(cachedPolymorphicBucketIds, bucket.getFileId());

    // SYMMETRIC TO addBucketInternal: A BUCKET BELONGS TO EXACTLY ONE TYPE, SO ONCE IT IS DETACHED FROM THIS TYPE NO
    // SUPER TYPE CAN REACH IT ANY LONGER AND EVERY POLYMORPHIC CACHE UP THE TREE MUST DROP IT (ISSUE #5297).
    if (!superTypes.isEmpty()) {
      final List<Bucket>  removedBuckets   = List.of(bucket);
      final List<Integer> removedBucketIds = List.of(bucket.getFileId());
      for (final LocalDocumentType s : superTypes)
        s.updatePolymorphicBucketsCache(false, removedBuckets, removedBucketIds);
    }

    // SYMMETRIC TO addBucketInternal: REBIND THE STRATEGY SO ITS CACHED BUCKET COUNT (E.G. RoundRobinBucketSelectionStrategy.total)
    // TRACKS THE SHRUNK LIST. WITHOUT THIS THE STRATEGY CAN STILL HAND OUT AN INDEX ONE PAST THE NEW LAST BUCKET (ISSUE #6380).
    bucketSelectionStrategy.setType(this);

    // AUTOMATICALLY DROP THE INDEX ON THE REMOVED BUCKET (INCLUDING INHERITED INDEXES FROM PARENT TYPES)
    final Collection<TypeIndex> existentIndexes = getAllIndexes(true);

    if (!existentIndexes.isEmpty()) {
      schema.getDatabase().transaction(() -> {
        for (TypeIndex idx : existentIndexes) {
          for (IndexInternal subIndex : idx.getIndexesOnBuckets())
            if (subIndex.getAssociatedBucketId() == bucket.getFileId())
              schema.dropIndex(subIndex.getName());
        }
      });
    }
  }

  @Override
  public boolean hasBucket(final String bucketName) {
    for (final Bucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  @Override
  public int getFirstBucketId() {
    return buckets.getFirst().getFileId();
  }

  @Override
  public boolean isSubTypeOf(final String type) {
    if (type == null)
      return false;

    if (type.equalsIgnoreCase(getName()))
      return true;
    for (final DocumentType superType : superTypes) {
      if (superType.isSubTypeOf(type))
        return true;
    }
    return false;
  }

  @Override
  public boolean isSuperTypeOf(final String type) {
    if (type == null)
      return false;

    if (type.equalsIgnoreCase(getName()))
      return true;
    for (final DocumentType subType : subTypes) {
      if (subType.isSuperTypeOf(type))
        return true;
    }
    return false;
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  @Override
  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  @Override
  public Object setCustomValue(final String key, final Object value) {
    checkForSchemaMutation();
    return recordFileChanges(() -> {
      if (value == null)
        return custom.remove(key);
      return custom.put(key, value);
    });
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject type = new JSONObject();

    final String kind;
    if (this instanceof LocalTimeSeriesType)
      kind = "t";
    else if (this instanceof LocalVertexType)
      kind = "v";
    else if (this instanceof LocalEdgeType edgeType) {
      kind = "e";
      if (!edgeType.isBidirectional())
        type.put("bidirectional", false);
      // Both default to false, so only write them when set: an older engine reading this schema simply ignores the
      // keys, and a schema written before the flags existed reads back with both off.
      if (edgeType.isLightweight())
        type.put("lightweight", true);
      if (edgeType.isUnique())
        type.put("unique", true);
    } else
      kind = "d";
    type.put("type", kind);

    final String[] parents = new String[getSuperTypes().size()];
    for (int i = 0; i < parents.length; ++i)
      parents[i] = getSuperTypes().get(i).getName();
    type.put("parents", parents);

    final List<Bucket> originalBuckets = getBuckets(false);
    final String[] buckets = new String[originalBuckets.size()];
    for (int i = 0; i < buckets.length; ++i)
      buckets[i] = originalBuckets.get(i).getName();

    type.put("buckets", buckets);

    if (!externalBucketIdByPrimaryBucketId.isEmpty()) {
      // PRIMARY BUCKET NAME -> EXTERNAL BUCKET NAME. NAMES (NOT IDS) ARE PERSISTED FOR HUMAN READABILITY AND
      // BECAUSE FILE IDS CAN BE REMAPPED ON FILE MIGRATION (LocalSchema.migratedFileIds).
      // NAME DEPENDENCY: this serialised mapping is keyed by string. ArcadeDB does not currently expose a
      // user-level RENAME BUCKET command, so the names are stable in practice. If a future feature lets a
      // user rename a bucket, the rename code MUST update both sides of this mapping (or it will go stale on
      // restart, and restoreExternalBuckets() will log SEVERE for the missing entry). Same constraint applies
      // to the external bucket itself: its file name carries the primary's name + "_ext" suffix.
      // TODO(rename-bucket): when a user-level RENAME BUCKET is introduced (see LocalSchema), it MUST also
      // (a) re-key this map's primary entry, (b) rename the paired '<oldName>_ext' bucket file to
      // '<newName>_ext' to keep the naming convention consistent, and (c) re-save the schema so the JSON
      // mirrors the new state. A grep for "TODO(rename-bucket)" surfaces every site that needs updating.
      final JSONObject extBuckets = new JSONObject();
      for (final Map.Entry<Integer, Integer> e : externalBucketIdByPrimaryBucketId.entrySet()) {
        final LocalBucket primary = schema.getBucketById(e.getKey(), false);
        final LocalBucket external = schema.getBucketById(e.getValue(), false);
        if (primary != null && external != null)
          extBuckets.put(primary.getName(), external.getName());
      }
      type.put("externalBuckets", extBuckets);
    }

    type.put("aliases", aliases);

    final JSONObject properties = new JSONObject();
    type.put("properties", properties);

    for (final String propName : getPropertyNames())
      properties.put(propName, getProperty(propName).toJSON());

    final JSONObject indexes = new JSONObject();
    type.put("indexes", indexes);

    final BucketSelectionStrategy strategy = getBucketSelectionStrategy();
    if (!RoundRobinBucketSelectionStrategy.NAME.equals(strategy.getName()))
      // WRITE ONLY IF NOT DEFAULT
      type.put("bucketSelectionStrategy", strategy.toJSON());

    // Persist the repartition flag only when set, so the default case adds nothing to schema.json.
    // Loaded back in LocalSchema after the strategy itself is restored.
    if (needsRepartition.get())
      type.put("needsRepartition", true);

    for (final TypeIndex i : getAllIndexes(false)) {
      // Persist a user-supplied TypeIndex name once per bucket entry (issue #4139). Stored on the
      // bucket-level JSON because that is what {@link LocalSchema#load} iterates and feeds back to
      // {@link #addIndexInternal} via setMetadata. Detected by comparing the TypeIndex name to the
      // auto-derived form so any non-LSM implementation (HashIndex, FullText, Geo, Vector, ...)
      // gets the manual name persisted without needing each one's toJSON to know about it.
      final String autoName = name + Arrays.toString(i.getPropertyNames().toArray()).replace(" ", "");
      final String custom = i.getName().equals(autoName) ? null : i.getName();
      for (final IndexInternal entry : i.getIndexesOnBuckets()) {
        final JSONObject indexJSON = entry.toJSON();
        if (custom != null)
          indexJSON.put("typeIndexName", custom);
        indexes.put(entry.getMostRecentFileName(), indexJSON);
      }
    }

    type.put("custom", new JSONObject(custom));
    return type;
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    return schema.recordFileChanges(callback);
  }

  private void updatePolymorphicBucketsCache(final boolean add, final List<Bucket> buckets, final List<Integer> bucketIds) {
    if (add) {
      cachedPolymorphicBuckets = CollectionUtils.addAllToUnmodifiableList(cachedPolymorphicBuckets, buckets);
      cachedPolymorphicBucketIds = CollectionUtils.addAllToUnmodifiableList(cachedPolymorphicBucketIds, bucketIds);
      // ADD ONLY THE INCOMING BUCKETS, SYMMETRIC WITH THE REMOVE BRANCH: FORWARDING THE WHOLE CACHE WOULD HAND EVERY
      // ANCESTOR THE BUCKETS IT ALREADY OWNS ON EVERY SINGLE UPDATE, WHICH IS O(TREE) WORK PER BUCKET AND ONLY STAYS
      // CORRECT BECAUSE addAllToUnmodifiableList HAPPENS TO DEDUPLICATE
      for (LocalDocumentType s : superTypes)
        s.updatePolymorphicBucketsCache(add, buckets, bucketIds);
    } else {
      cachedPolymorphicBuckets = CollectionUtils.removeAllFromUnmodifiableList(cachedPolymorphicBuckets, buckets);
      cachedPolymorphicBucketIds = CollectionUtils.removeAllFromUnmodifiableList(cachedPolymorphicBucketIds, bucketIds);
      // REMOVE ONLY THE BUCKETS OF THE REMOVED TYPE
      for (LocalDocumentType s : superTypes)
        s.updatePolymorphicBucketsCache(add, buckets, bucketIds);
    }
  }

  /**
   * The mirror image of {@link #linkSuperType}: severs the link in both directions, then has the former super type
   * rebuild its polymorphic bucket caches from what is still linked to it. Used by {@link #removeSuperType(DocumentType)}
   * and by {@link #addSuperTypeInternal} to hand a failed linkage back, so the two cannot drift apart.
   * <p>
   * The caches are REBUILT rather than subtracted from. Linking contributed this type's whole polymorphic subtree, so a
   * subtraction has to withdraw the same subtree, and withdrawing only the type's own buckets left every grandchild
   * visible in {@code SELECT FROM <ancestor>} after the link was gone (issue #6935). A subtraction of the subtree is not
   * right either: a bucket the former super type still reaches through another path (a diamond, where the subtree is
   * also linked to it directly or through a sibling) would be withdrawn although it still belongs there. Recomputing
   * from the surviving links is the only answer that is correct in both shapes, and a schema change is rare enough that
   * its O(tree) cost is not worth a cheaper rule that is wrong in one of them.
   */
  private void unlinkSuperType(final LocalDocumentType superType) {
    superTypes.remove(superType);
    superType.subTypes.remove(this);
    superType.rebuildPolymorphicBucketsCache();
  }

  /**
   * Recomputes {@link #cachedPolymorphicBuckets} and {@link #cachedPolymorphicBucketIds} from this type's own buckets and
   * the polymorphic caches of its sub types, in that order, then does the same up every super type, whose caches are
   * derived from this one. Each list is published as a fresh unmodifiable copy, the same copy-on-write contract the
   * lock-free readers of the two volatile fields rely on (issue #6678).
   * <p>
   * The order is load-bearing: a type's cache is read by its super types and never by its sub types, so this type is
   * rebuilt BEFORE the walk goes up, and the sub types' caches it reads are already final because the change that
   * triggered the rebuild happened above them. Rebuilding a super type first would read this type's stale cache.
   */
  private void rebuildPolymorphicBucketsCache() {
    final List<Bucket> polymorphicBuckets = new ArrayList<>(buckets);
    final List<Integer> polymorphicBucketIds = new ArrayList<>(bucketIds);
    // A DIAMOND HANDS THE SAME BUCKET OVER THROUGH MORE THAN ONE SUB TYPE: IT IS LISTED ONCE
    final Set<Bucket> seen = new HashSet<>(buckets);
    for (final LocalDocumentType subType : subTypes)
      for (final Bucket bucket : subType.cachedPolymorphicBuckets)
        if (seen.add(bucket)) {
          polymorphicBuckets.add(bucket);
          polymorphicBucketIds.add(bucket.getFileId());
        }

    cachedPolymorphicBuckets = Collections.unmodifiableList(polymorphicBuckets);
    cachedPolymorphicBucketIds = Collections.unmodifiableList(polymorphicBucketIds);

    for (final LocalDocumentType superType : superTypes)
      superType.rebuildPolymorphicBucketsCache();
  }

  DocumentType addSuperType(final DocumentType superType, final boolean createIndexes) {
    checkForSchemaMutation();
    if (this.equals(superType))
      return this;

    if (superTypes.contains(superType))
      // ALREADY PARENT
      return this;

    // QUIESCED for the same reason TypeIndexBuilder and BucketIndexBuilder are (issue #6303, item 2), and taken HERE
    // rather than around the propagation itself: the barrier answers about the past, and a build needs the other half
    // too - that nothing WRITES during the scan - but recordFileChanges runs under the database WRITE LOCK, which is
    // the lock an async worker needs to commit its batch. Requested from inside it, the quiescence waits for workers
    // that cannot proceed until it returns, and gives up 60 seconds later.
    //
    // Only when there are indexes to propagate: the schema-load path passes createIndexes=false and scans nothing.
    // quiesceAsync() reads the executor field rather than calling async(), so a database that never touched the async
    // API does not grow a thread pool here, and it is reentrant, so a builder reached from below rides on this one.
    try (final AsyncQuiesce asyncPaused = createIndexes ?
        ((DatabaseInternal) schema.getDatabase()).quiesceAsync() : () -> {
    }) {
      addSuperTypeInternal(superType, createIndexes);
    }

    return this;
  }

  /**
   * Applies the linkage, then the three steps that make being a subtype mean something - and hands the linkage back
   * if any of them refuses.
   * <p>
   * The ordering constraints below are load-bearing and are the checklist any change here has to keep:
   * <ul>
   *   <li>the async quiescence is taken by the CALLER, outside {@code recordFileChanges}: that runs under the
   *       database write lock, which is the lock an async worker needs to commit;</li>
   *   <li>{@link IndexBuilder#callerTransactionHasChanges} is asked BEFORE any transaction of ours opens, while the
   *       answer is still about the transaction that was already there;</li>
   *   <li>{@code created} is declared HERE, outside the propagation, because a step that refuses AFTER the indexes
   *       are committed still has to take them away;</li>
   *   <li>both lists are emptied on entry to the component transaction's lambda, which retries internally.</li>
   * </ul>
   */
  private void addSuperTypeInternal(final DocumentType superType, final boolean createIndexes) {
    recordFileChanges(() -> {
      final LocalDocumentType embeddedSuperType = (LocalDocumentType) superType;

      // Inside the callback, immediately before the mutation it guards: checkTimeSeriesHierarchy's descendant walk
      // reads the (mutable) subTypes list of this type and every one below it, and linkSuperType is what mutates
      // those same lists. Checked here, both are serialised by the database write lock recordFileChanges takes; read
      // any earlier and a concurrent addSuperType/removeSuperType elsewhere in the hierarchy could structurally
      // modify a list this walk is iterating, straight into a ConcurrentModificationException.
      checkTimeSeriesHierarchy(superType);

      // CHECK FOR CONFLICT WITH PROPERTIES NAMES. Here for the same reason checkTimeSeriesHierarchy is, and the
      // reason it used to be outside the callback is that it only logs: getPolymorphicPropertyNames() recurses over
      // the super type's (mutable) superTypes lists, which linkSuperType/unlinkSuperType structurally modify, so a
      // walk outside the write lock can raise a ConcurrentModificationException out of an unrelated ALTER TYPE -
      // a spurious failure of a call that was going to succeed (issue #7918, the same exposure createProperty had).
      final Set<String> allProperties = getPropertyNames();
      for (final String p : superType.getPolymorphicPropertyNames())
        if (allProperties.contains(p))
          LogManager.instance()
              .log(this, Level.WARNING, "Property '" + p + "' is already defined in type '" + name + "' or one of the super types");

      linkSuperType(embeddedSuperType);

      // EVERYTHING THAT FOLLOWS THE LINKAGE IS GUARDED BY IT. Every step below can still refuse - a paired external
      // bucket that cannot be created, an index propagation that hits a duplicate, an inherited partition the
      // suitability check rejects for this subtype (#5637) - and a refusal that left the link standing would hand
      // back a type that IS a subtype, with none of what being one implies.
      //
      // Every sub-index the propagation attaches is recorded HERE, outside the propagation's own block, because the
      // steps that can refuse do not all come before it: a strategy the subtype cannot take (#5637) is raised AFTER
      // the indexes are committed and attached to the super type's wrapper, so a cleanup scoped to the propagation
      // would unlink the type and leave the super type's index pointing at buckets that are no longer its subtype's.
      final List<Index> created = new ArrayList<>();
      try {
        applyPostLinkageSteps(embeddedSuperType, createIndexes, created);
      } catch (final RuntimeException e) {
        // EVERY sub-index the propagation made goes, not only the ones that still had a build outstanding, and not
        // only when the propagation is what refused. It cannot hang off a transaction's error callback: inside a
        // JOINED transaction, LocalDatabase.transaction rethrows a NeedRetryException or a DuplicatedKeyException
        // immediately rather than calling it (#661), and a duplicate is exactly what this build can hit now that it
        // sees the caller's pending writes.
        for (final Index subIndex : created)
          IndexBuilder.dropPartiallyBuiltIndex(schema, subIndex);

        // THE LINK GOES BACK, which is what makes the refusal reach the caller at all: LocalDatabase.transaction
        // retries a DuplicatedKeyException once (#4959), and a retry that found the super type already linked would
        // return early, propagate nothing, and COMMIT - handing back a subtype whose index holds no entry for any of
        // its records.
        //
        // The paired external buckets ensureExternalBucketsRecursive may have created are deliberately left: they are
        // additive, idempotent, and reused by the next attempt.
        unlinkSuperType(embeddedSuperType);
        throw e;
      }

      return null;
    });
  }

  /**
   * The in-memory linkage, and nothing else: the three lines {@link #unlinkSuperType} undoes, kept next to it so the
   * two stay symmetric. The super type receives this type's whole polymorphic subtree, which is what a polymorphic read
   * on it has to reach from now on.
   */
  private void linkSuperType(final LocalDocumentType superType) {
    superTypes.add(superType);
    superType.subTypes.add(this);
    superType.updatePolymorphicBucketsCache(true, cachedPolymorphicBuckets, cachedPolymorphicBucketIds);
  }

  /**
   * The three steps that follow the linkage, in the order they have to run. Each of them can refuse, and the caller's
   * guard is what turns a refusal into "as unlinked as it found it".
   *
   * @param created accumulates every sub-index the propagation commits, so the caller can take them away whichever
   *                step refuses - including a step that comes after the propagation succeeded
   */
  private void applyPostLinkageSteps(final LocalDocumentType superType, final boolean createIndexes,
      final List<Index> created) {
    // 1. IF THE NEWLY-LINKED SUPERTYPE HAS ANY EXTERNAL PROPERTY (OWN OR INHERITED), THIS SUBTYPE MUST OWN PAIRED
    // EXTERNAL BUCKETS FOR ITS OWN PRIMARY BUCKETS, BECAUSE RECORDS OF THIS SUBTYPE LIVE IN THIS SUBTYPE'S BUCKETS.
    if (superType.hasExternalProperties())
      ensureExternalBucketsRecursive();

    // 2. CREATE INDEXES AUTOMATICALLY ON PROPERTIES DEFINED IN SUPER TYPES. The schema-load path passes
    // createIndexes=false: the sub-indexes are already on disk and are read back with the schema.
    if (createIndexes) {
      final Collection<TypeIndex> indexes = new ArrayList<>(getAllIndexes(true));
      indexes.removeAll(indexesByProperties.values());
      propagateSuperTypeIndexes(superType, indexes, created);
    }

    // 3. INHERIT THE BUCKET SELECTION STRATEGY FROM THE SUPER TYPE. The enclosing recordFileChanges saves the schema
    // when it completes, and that write carries this strategy, so the setter does not persist on its own.
    //
    // An inherited partition that is unsuitable for this subtype still refuses here, as it always has: what changed
    // with issue #5637 is that the refusal now arrives as the SchemaException the suitability check raises rather
    // than the IllegalArgumentException the binding used to throw. Checked against every caller of addSuperType -
    // CreateTypeAbstractStatement, AlterTypeStatement's SUPERTYPE branch, TypeBuilder, the Cypher Labels helper, and
    // LocalSchema's dropType re-attach - and none of them catches either type, so nothing distinguishes the two.
    // Neither is a CommandParsingException, so the HTTP status is unchanged too. (AlterTypeStatement's one catch
    // names both, but it guards the BucketSelectionStrategy branch, not this.)
    if (!superType.getBucketSelectionStrategy().getName().equalsIgnoreCase(getBucketSelectionStrategy().getName()))
      setBucketSelectionStrategy(superType.getBucketSelectionStrategy().copy(), false);
  }

  /**
   * Propagates the super type's indexes over THIS type's buckets, which already hold records - the whole difference
   * between this call site and the sibling in {@link #createBucket}, where the bucket has just been created and a scan
   * has nothing to miss. A caller that inserts and then links a super type in ONE transaction must get an index
   * covering those records; without the split below it gets one that is readable, reported healthy by
   * {@code CHECK DATABASE}, and answers the lookup it exists for with nothing (issues #6324 and #6359, item 1).
   *
   * @param created every component this commits, appended for the caller's cleanup: it owns them from the moment they
   *                exist, because the steps that can still refuse do not all come before this one
   */
  private void propagateSuperTypeIndexes(final LocalDocumentType superType, final Collection<TypeIndex> indexes,
      final List<Index> created) {
    // Decided BEFORE the transactions below, while the answer is still about the transaction that was already there -
    // see IndexBuilder#buildSharesCallerTransaction, which owns the predicate for every call site.
    //
    // Per index rather than once: a family that cannot share a caller's transaction (the vector ones, whose search
    // path reads through the page cache rather than through the transaction) keeps building in one go whatever the
    // caller holds. That is also the LIMIT of this - a propagated vector index does not see the caller's uncommitted
    // writes, and never has.
    final DatabaseInternal database = (DatabaseInternal) schema.getDatabase();
    final boolean callerTransactionHasChanges = IndexBuilder.callerTransactionHasChanges(database);

    // Two lists, because "has to be built later" and "has to be taken away if anything goes wrong" are not the same
    // set. An index family that cannot share the caller's transaction is built INLINE during component creation and
    // never reaches toBuild, so a cleanup keyed on toBuild alone would leave it behind, committed and attached to a
    // type relationship unlinkSuperType has just undone. Reachable whenever a super type carries both a vector index
    // and an ordinary one.
    final List<Index> toBuild = new ArrayList<>();

    try {
      // The COMPONENTS are created in a transaction of their OWN. The enclosing recordFileChanges writes the schema
      // entry that names each of them whatever the caller's transaction goes on to do, so the index FILES have to be
      // committed on the same terms: leaving a first page inside a caller's transaction that later rolls back would
      // leave the schema pointing at a file with no pages, which fails on the next write with "the file is invalid".
      database.transaction(() -> {
        // Emptied on entry, not on declaration: this transaction retries on its own (a NeedRetryException from a
        // concurrent schema mutation), and a retry re-runs this lambda from scratch. Left to accumulate, the
        // rolled-back attempt's indexes would be handed to the build alongside the surviving ones, which reports a
        // failure that has nothing to do with the conflict that caused the retry. TypeIndexBuilder's equivalent loop
        // is immune because it assigns into a pre-sized array by position.
        created.clear();
        toBuild.clear();

        for (final TypeIndex index : indexes) {
          if (index.getType() == null)
            LogManager.instance().log(this, Level.WARNING,
                "Error on creating implicit indexes from super type '" + superType.getName() + "': key types is null");
          else
            createSubIndexesFor(index, callerTransactionHasChanges && index.getType().buildCanShareCallerTransaction(),
                created, toBuild);
        }
      }, false);

      if (!toBuild.isEmpty())
        // The BUILD then joins the caller's transaction, because a scan reads the transaction it runs in: the pages
        // that transaction has modified first, the committed ones underneath. That is the only way it sees records
        // the caller has written and not yet committed, and it makes the entries commit, or roll back, with the
        // records they describe.
        database.transaction(() -> {
          for (final Index subIndex : toBuild)
            IndexBuilder.buildCreatedIndex(subIndex, IndexBuilder.BUILD_BATCH_SIZE, true, null);
        }, true);
    } catch (final RuntimeException e) {
      // The indexes and the LINK are both handed back by the caller's guard around the whole post-linkage region, not
      // from here: this is not the only step that can refuse after they exist. What is left here is naming which super
      // type's propagation the failure came out of - for every failure shape, not only IndexException. The build joins
      // the caller's transaction now, so it can fail with a DuplicatedKeyException raised on the caller's own pending
      // writes or with a NeedRetryException - neither of which is an IndexException, and both of which used to reach
      // the caller with no line saying which super type's propagation they came out of.
      LogManager.instance()
          .log(this, Level.WARNING, "Error on creating implicit indexes from super type '" + superType.getName() + "'", e);
      throw e;
    }
  }

  /**
   * One super-type index, over every bucket of THIS type that does not already carry it.
   *
   * @param sharesCallerTransaction when true the component is created without an inline build and is appended to
   *                                {@code toBuild}, so the build can join the caller's transaction later and see its
   *                                pending writes; when false the build runs inline, here and now
   */
  private void createSubIndexesFor(final TypeIndex index, final boolean sharesCallerTransaction, final List<Index> created,
      final List<Index> toBuild) {
    for (int i = 0; i < buckets.size(); i++) {
      final Bucket bucket = buckets.get(i);

      boolean alreadyCreated = false;
      for (final IndexInternal idx : getPolymorphicBucketIndexByBucketId(bucket.getFileId(), index.getPropertyNames())) {
        final TypeIndex typeIndex = idx.getTypeIndex();
        if (typeIndex != null && typeIndex.equals(index)) {
          alreadyCreated = true;
          break;
        }
      }

      if (alreadyCreated)
        continue;

      // Inherit the page size of the index being propagated, like the createBucket() path above does. Hardcoding the
      // LSM default here gave a HASH index a page size it cannot address (#5713), and getPageSizeForNewFile() is the
      // accessor that guarantees the value is legal to create with. getMetadataForNewFile() is its counterpart for
      // everything that is not the page size, and is handed over as-is, for the reasons spelled out in
      // addBucketInternal(): the sub-index created here belongs to the SAME logical index being propagated (#5742).
      final Index subIndex = schema.createBucketIndex(this, index.getKeyTypes(), bucket, name, index.getType(),
          index.isUnique(), index.getPageSizeForNewFile(), index.getNullStrategy(), null,
          index.getPropertyNames().toArray(new String[index.getPropertyNames().size()]), index,
          IndexBuilder.BUILD_BATCH_SIZE, index.getMetadataForNewFile(), !sharesCallerTransaction);

      created.add(subIndex);
      if (sharesCallerTransaction)
        toBuild.add(subIndex);
    }
  }
}
