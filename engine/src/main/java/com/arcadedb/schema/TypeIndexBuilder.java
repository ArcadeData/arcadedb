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
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Builder class for type indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeIndexBuilder extends IndexBuilder<TypeIndex> {
  private static final ThreadLocal<Consumer<SortedIndexBuildMetrics>> SORTED_BUILD_METRICS_TEST_HOOK = new ThreadLocal<>();

  private static final int DEFAULT_SORTED_BUILD_MERGE_FAN_IN = 8;
  private static final long MIN_SORTED_BUILD_MEMORY_BUDGET    = 1L << 20;

  // When set, lets {@link #create()} accept properties that aren't declared on the type yet:
  // the index is materialised with this Type as the key serialisation, while the property
  // stays "free-form" on the document type so writes don't get coerced. Used by the OpenCypher
  // engine, where {@code CREATE INDEX} can run on an empty/typeless property (issue #4222).
  private Type[] defaultKeyTypesForUndeclaredProperties;
  private IndexBuildMode buildMode              = IndexBuildMode.DEFAULT;
  private long           buildMemoryBudgetBytes;
  private Path           buildSpillDirectory;
  private int            buildMergeFanIn        = DEFAULT_SORTED_BUILD_MERGE_FAN_IN;
  private int            buildParallelism       = 1;

  protected TypeIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, TypeIndex.class);
    this.metadata = new IndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Tells the builder to fall back to {@code defaultKeyType} (one entry per property) when a
   * property is not yet declared on the document type, instead of throwing
   * {@link SchemaException}. The property remains undeclared on the schema: this avoids the
   * silent value coercion that breaks Cypher's strict-typed equality when integers are stored
   * against a {@code STRING}-typed property.
   */
  public TypeIndexBuilder withDefaultKeyTypesForUndeclaredProperties(final Type[] defaultKeyTypes) {
    this.defaultKeyTypesForUndeclaredProperties = defaultKeyTypes;
    return this;
  }

  public TypeIndexBuilder withBuildMode(final IndexBuildMode buildMode) {
    this.buildMode = Objects.requireNonNull(buildMode, "buildMode");
    return this;
  }

  /** Sets the approximate heap budget for one sorted run. Zero keeps automatic heap-based sizing. */
  public TypeIndexBuilder withBuildMemoryBudget(final long bytes) {
    if (bytes < 0L)
      throw new IllegalArgumentException("build memory budget cannot be negative");
    if (bytes > 0L && bytes < MIN_SORTED_BUILD_MEMORY_BUDGET)
      throw new IllegalArgumentException("build memory budget must be at least " + MIN_SORTED_BUILD_MEMORY_BUDGET + " bytes");
    this.buildMemoryBudgetBytes = bytes;
    return this;
  }

  public TypeIndexBuilder withBuildSpillDirectory(final Path directory) {
    this.buildSpillDirectory = Objects.requireNonNull(directory, "directory");
    return this;
  }

  public TypeIndexBuilder withBuildMergeFanIn(final int mergeFanIn) {
    if (mergeFanIn < 2)
      throw new IllegalArgumentException("build merge fan-in must be at least 2");
    this.buildMergeFanIn = mergeFanIn;
    return this;
  }

  /**
   * Sets requested parallelism for independent sorted-build merge groups and bucket writers. Each stage is admitted
   * independently against its work count, processor headroom, memory budget, and available file descriptors. The final
   * global stream remains serial. The default is one.
   */
  public TypeIndexBuilder withBuildParallelism(final int parallelism) {
    if (parallelism < 1)
      throw new IllegalArgumentException("build parallelism must be at least 1");
    this.buildParallelism = parallelism;
    return this;
  }

  /**
   * Sets the index type, returning the builder subclass that carries the settings of that index type: an
   * LSMVectorIndexBuilder for LSM_VECTOR, a TypeFullTextIndexBuilder for FULL_TEXT, a TypeGeoIndexBuilder for
   * GEOSPATIAL, and so on. An index type missing from this list ends up with the generic {@link IndexMetadata} and
   * therefore has nowhere to keep its own settings.
   *
   * @param indexType the index type
   *
   * @return appropriate builder for the index type
   */
  @Override
  public TypeIndexBuilder withType(final Schema.INDEX_TYPE indexType) {
    if (buildMode == IndexBuildMode.SORTED && indexType != Schema.INDEX_TYPE.LSM_TREE)
      throw new IllegalArgumentException("Sorted build currently supports only LSM_TREE indexes");

    // Record the type on THIS builder before handing back a specialised one: a caller that ignores the returned
    // instance (`builder.withType(X); builder.create();`) still gets a builder that knows what to create, instead of
    // "indexType was not specified" from an object the type never landed on.
    super.withType(indexType);

    if (indexType == Schema.INDEX_TYPE.LSM_VECTOR && !(this instanceof TypeLSMVectorIndexBuilder))
      return new TypeLSMVectorIndexBuilder(this);
    if (indexType == Schema.INDEX_TYPE.LSM_SPARSE_VECTOR && !(this instanceof TypeLSMSparseVectorIndexBuilder))
      return new TypeLSMSparseVectorIndexBuilder(this);
    if (indexType == Schema.INDEX_TYPE.FULL_TEXT && !(this instanceof TypeFullTextIndexBuilder))
      return new TypeFullTextIndexBuilder(this);
    if (indexType == Schema.INDEX_TYPE.GEOSPATIAL && !(this instanceof TypeGeoIndexBuilder))
      return new TypeGeoIndexBuilder(this);
    return this;
  }

  /**
   * Returns this builder as a TypeFullTextIndexBuilder for full-text specific configuration.
   * Only valid after withType(FULL_TEXT) has been called.
   *
   * @return a TypeFullTextIndexBuilder for full-text configuration
   * @throws IllegalStateException if withType(FULL_TEXT) has not been called
   */
  public TypeFullTextIndexBuilder withFullTextType() {
    if (this instanceof TypeFullTextIndexBuilder)
      return (TypeFullTextIndexBuilder) this;
    if (indexType != Schema.INDEX_TYPE.FULL_TEXT)
      throw new IllegalStateException("withFullTextType() can only be called after withType(FULL_TEXT)");
    return new TypeFullTextIndexBuilder(this);
  }

  /**
   * Returns this builder as a TypeGeoIndexBuilder for geospatial specific configuration.
   * Only valid after withType(GEOSPATIAL) has been called.
   *
   * @return a TypeGeoIndexBuilder for geospatial configuration
   * @throws IllegalStateException if withType(GEOSPATIAL) has not been called
   */
  public TypeGeoIndexBuilder withGeoType() {
    if (this instanceof TypeGeoIndexBuilder)
      return (TypeGeoIndexBuilder) this;
    if (indexType != Schema.INDEX_TYPE.GEOSPATIAL)
      throw new IllegalStateException("withGeoType() can only be called after withType(GEOSPATIAL)");
    return new TypeGeoIndexBuilder(this);
  }

  @Override
  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // Checked before the existing-index lookup below, which needs the requested index type to decide whether an index
    // already on those properties covers the request.
    if (indexType == null)
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + metadata.typeName + "' because indexType was not specified");

    // Wait for any running async tasks (e.g., compaction) to complete before creating new index
    // This prevents NeedRetryException when creating multiple indexes sequentially on large datasets
    while (database.isAsyncProcessing())
      database.async().waitCompletion();

    // Carry the user-supplied TypeIndex name (set via {@link #withIndexName}) onto the
    // IndexMetadata so it propagates to each per-bucket index and ultimately to
    // {@link LocalDocumentType#addIndexInternal} which reads it when minting the TypeIndex.
    // Without this hop the manual name is dropped and the auto-derived
    // typeName + "[propertyNames]" form is used (issue #4139).
    if (indexName != null && !indexName.isEmpty())
      metadata.typeIndexName = indexName;

    final LocalSchema schema = database.getSchema().getEmbedded();

    final LocalDocumentType type = schema.getType(metadata.typeName);
    // First try the type's own index (no hierarchy walk), then fall back to the polymorphic walk so an index the type
    // only inherits is found too. Either way the outcome below is the same: it is used to answer the request, never
    // to make room for it.
    TypeIndex existingTypeIndex = type.getIndexByProperties(metadata.propertyNames);
    if (existingTypeIndex == null)
      existingTypeIndex = type.getPolymorphicIndexByProperties(metadata.propertyNames);

    // Non-null only on the replacement path below: the definition to put back if the new index cannot be built.
    ReplacedIndexDefinition replaced = null;

    if (existingTypeIndex != null) {
      // "Ignore if exists" means the caller can live with what is already there - but only when what is already there
      // provides what was asked for. A mismatch used to DROP the existing index and rebuild it with the requested
      // definition: on an inherited index that wiped the parent type's index and left an orphan entry in the schema
      // JSON (issue #4083), and on the type's own index it threw away a working index for a rebuild the stored data
      // may not even allow, leaving the type with none at all (issue #5675). Only an explicit
      // {@link #withReplaceIfIncompatible} takes an index away now, and it puts it back on failure.
      //
      // Two halves to "provides what was asked for": the structural definition (kind, uniqueness) and, when the
      // statement carried a METADATA clause, the settings that clause NAMED. The second half is skipped entirely for
      // a request that named nothing, so a guarded statement without METADATA behaves exactly as before. The
      // structural check runs first because it is the cheap one and because comparing the settings of two different
      // index KINDS would be comparing key spaces that have nothing to do with each other.
      boolean satisfied = satisfiesRequest(existingTypeIndex, indexType, unique);

      //
      // Not wrapped the way the SQL shortcut wraps its own call: reading the clause here CANNOT fail on any path that
      // sets {@link #withUserMetadata} the way it is meant to be set. The clause is read once by
      // {@code withMetadata(JSONObject)} first, which is where an unreadable value is reported - the SQL branches do
      // both inside one try, so the statement never reaches create() with a clause that would raise. A caller that
      // sets the user clause WITHOUT the parsed one has skipped that validation, and gets the reader's own message
      // here instead of a friendlier one; there is no such caller, and inventing a second error path for it would
      // only re-type an exception the reader already words correctly.
      List<String> settingMismatches = List.of();
      if (satisfied) {
        settingMismatches = findUnsatisfiedSettings(existingTypeIndex, userMetadata, indexType);
        satisfied = settingMismatches.isEmpty();
      }

      if (satisfied && ignoreIfExists)
        return existingTypeIndex;

      if (!satisfied && replaceIfIncompatible && existingTypeIndex.getTypeName().equals(metadata.typeName))
        // Own index, and the caller explicitly asked to replace what does not fit. Recorded here and dropped further
        // down, once everything that could still refuse the request has had its say.
        replaced = ReplacedIndexDefinition.of(existingTypeIndex);
      else if (!satisfied && (ignoreIfExists || replaceIfIncompatible))
        throw conflictWithExistingIndex(existingTypeIndex, indexType, unique, metadata.typeName, metadata.propertyNames,
            settingMismatches);
      else
        // Reached when the request was NOT guarded, whether or not the existing index satisfies it. The suffix names
        // the guard that makes the statement idempotent: the satisfied case here is a caller asking twice for
        // something already there - a Cypher CREATE CONSTRAINT without IF NOT EXISTS over its own unique index, say -
        // and the bare sentence left them to work out that the guard was the missing piece. The sentence itself is
        // unchanged: it is long-standing wording that callers may already match on.
        throw new IllegalArgumentException(
            "Found the existent index '" + existingTypeIndex.getName() + "' defined on the properties '" + Arrays.asList(
                metadata.propertyNames) + "' for type '" + metadata.typeName + "'" + (satisfied ?
                ". It already provides what was requested: use IF NOT EXISTS to make this statement idempotent" :
                ". Drop it first if the definition has to change"));
    }

    if (metadata.propertyNames.isEmpty())
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + metadata.typeName + "' because there are no property defined");

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[metadata.propertyNames.size()];
    int i = 0;

    for (final String propertyName : metadata.propertyNames) {
      if (type instanceof LocalEdgeType && ("@out".equals(propertyName) || "@in".equals(propertyName))) {
        keyTypes[i++] = Type.LINK;
      } else {
        // Check for the "by item" (LIST) / "by key" / "by value" (MAP) modifiers
        String actualPropertyName = propertyName;
        boolean isByItem = false;
        boolean isByKey = false;
        boolean isByValue = false;

        if (propertyName.endsWith(" by item")) {
          isByItem = true;
          actualPropertyName = propertyName.substring(0, propertyName.length() - 8);
        } else if (propertyName.endsWith(" by key")) {
          isByKey = true;
          actualPropertyName = propertyName.substring(0, propertyName.length() - 7);
        } else if (propertyName.endsWith(" by value")) {
          isByValue = true;
          actualPropertyName = propertyName.substring(0, propertyName.length() - 9);
        }

        // First, try to find the property with the exact name (handles properties with dots in their names)
        Property property = type.getPolymorphicPropertyIfExists(actualPropertyName);

        if (property == null && actualPropertyName.contains(".")) {
          // Property with exact name doesn't exist, check if this could be a nested path
          final String[] pathParts = actualPropertyName.split("\\.", 2); // Split into at most 2 parts
          final String rootPropertyName = pathParts[0];

          // Try to find the root property
          property = type.getPolymorphicPropertyIfExists(rootPropertyName);

          if (property != null) {
            // Found root property - this is a nested path
            // For nested paths with BY ITEM, the root must be a LIST
            if (isByItem && property.getType() != Type.LIST) {
              throw new SchemaException(
                  "Cannot create index with BY ITEM on nested property path '" + metadata.typeName + "." + actualPropertyName
                      + "' because the root property '" + rootPropertyName + "' is not a LIST type (found: " + property.getType()
                      + ")");
            }

            // For nested properties, we'll use STRING as the key type since we can't validate the nested structure at schema definition time
            // The actual type will be determined at runtime during indexing
            keyTypes[i++] = Type.STRING;
            continue;
          }
        }

        // If we still don't have a property, it doesn't exist
        if (property == null) {
          if (defaultKeyTypesForUndeclaredProperties != null && i < defaultKeyTypesForUndeclaredProperties.length
              && defaultKeyTypesForUndeclaredProperties[i] != null) {
            // Caller (e.g. OpenCypher CREATE INDEX) opted into keeping the property undeclared so
            // writes preserve their original Java type. The index keys are serialised using this
            // fall-back Type via the usual Type.convert path.
            keyTypes[i] = defaultKeyTypesForUndeclaredProperties[i];
            i++;
            continue;
          }
          throw new SchemaException(
              "Cannot create the index on type '" + metadata.typeName + "." + actualPropertyName
                  + "' because the property does not exist");
        }

        // Validate BY ITEM is only used with LIST type
        if (isByItem && property.getType() != Type.LIST) {
          throw new SchemaException("Cannot create index with BY ITEM on property '" + metadata.typeName + "." + actualPropertyName
              + "' because it is not a LIST type (found: " + property.getType() + ")");
        }

        // Validate BY KEY / BY VALUE are only used with MAP type
        if ((isByKey || isByValue) && property.getType() != Type.MAP) {
          throw new SchemaException(
              "Cannot create index with BY " + (isByKey ? "KEY" : "VALUE") + " on property '" + metadata.typeName + "."
                  + actualPropertyName + "' because it is not a MAP type (found: " + property.getType() + ")");
        }

        if (isByItem) {
          // For BY ITEM on LIST, the key type should be STRING (since list items are indexed individually).
          // Lists can contain heterogeneous types, so we use STRING as a generic type for list items
          keyTypes[i++] = Type.STRING;
        } else if (isByKey) {
          // MAP keys are strings
          keyTypes[i++] = Type.STRING;
        } else if (isByValue) {
          // MAP values use the declared "OF" sub-type when available, otherwise fall back to STRING
          final String ofType = property.getOfType();
          final Type valueType = ofType != null ? Type.getTypeByName(ofType) : null;
          keyTypes[i++] = valueType != null ? valueType : Type.STRING;
        } else {
          keyTypes[i++] = property.getType();
        }
      }
    }

    final List<Bucket> buckets = type.getBuckets(true);
    final Index[] indexes = new Index[buckets.size()];
    final boolean sortedBuild = buildMode == IndexBuildMode.SORTED;
    final SortedIndexBuildRecoveryMarker[] recoveryMarker = new SortedIndexBuildRecoveryMarker[1];
    final SortedIndexBuildMetrics[] sortedBuildMetrics = new SortedIndexBuildMetrics[1];
    final long sortedBuildStarted = sortedBuild ? System.nanoTime() : 0L;

    if (sortedBuild)
      validateSortedBuildPreconditions();

    // Last thing before the build: everything above can still refuse the request (an undeclared property, a BY ITEM on
    // a non-LIST, a sorted build on a replicated database), and none of those refusals may cost the caller the index it
    // already had.
    //
    // NOT ATOMIC, and deliberately so. The drop is its own committed schema change - LocalSchema.dropIndex wraps itself
    // in recordFileChanges, and so does the build below - so the two cannot be rolled back as one. The window is
    // covered for a FAILING build (restoreReplacedIndex in the catch arms below) but not for a process that dies inside
    // it: the type then reopens with no index on those properties and has to be given one again. Making it atomic means
    // holding a schema transaction across a full index build, which is the wrong trade for a DDL statement that already
    // rebuilds every entry. Narrowing it further would mean building the new index alongside the old one, which
    // LocalDocumentType.indexesByProperties cannot represent - one index per property set is the invariant.
    //
    // Only an explicit withReplaceIfIncompatible reaches here, so nothing is exposed to this window without asking.
    if (replaced != null)
      existingTypeIndex.drop();

    final TypeIndex created;
    try {
      final long recordFileChangesStarted = System.nanoTime();
      schema.recordFileChanges(() -> {
        if (sortedBuild) {
          recoveryMarker[0] = SortedIndexBuildRecoveryMarker.create(database, metadata.typeName, metadata.propertyNames,
              buildSpillDirectory);
          sortedBuildMetrics[0] = createWithSortedBuild(schema, type, keyTypes, buckets, indexes, recoveryMarker[0]);
        } else
          for (int idx = 0; idx < buckets.size(); ++idx) {
            final int finalIdx = idx;
            database.transaction(() -> {

              final LocalBucket bucket = (LocalBucket) buckets.get(finalIdx);

              indexes[finalIdx] = createBucketIndex(schema, type, keyTypes, bucket);

            }, false, maxAttempts, null, null);
          }

        return null;
      }, sortedBuild);
      final long recordFileChangesNanos = System.nanoTime() - recordFileChangesStarted;

      long cleanupNanos = 0L;
      if (recoveryMarker[0] != null) {
        final long cleanupStarted = System.nanoTime();
        recoveryMarker[0].clear();
        cleanupNanos = System.nanoTime() - cleanupStarted;
      }

      if (sortedBuildMetrics[0] != null) {
        final SortedIndexBuildMetrics completed = sortedBuildMetrics[0].completed(
            Math.max(0L, recordFileChangesNanos - sortedBuildMetrics[0].pipelineNanos()), cleanupNanos,
            System.nanoTime() - sortedBuildStarted);
        emitSortedBuildMetrics(completed);
      }

      created = type.getIndexByProperties(metadata.propertyNames);
    } catch (final NeedRetryException e) {
      if (cleanupIndexes(schema, indexes, e)
          && (recoveryMarker[0] == null || recoveryMarker[0].baselineRestored(database)))
        clearRecoveryMarker(recoveryMarker[0], e);
      restoreReplacedIndex(replaced, e);
      throw e;
    } catch (final Throwable e) {
      if (cleanupIndexes(schema, indexes, e)
          && (recoveryMarker[0] == null || recoveryMarker[0].baselineRestored(database)))
        clearRecoveryMarker(recoveryMarker[0], e);
      restoreReplacedIndex(replaced, e);
      // Carry the root cause into the message: this exception is what the SQL/HTTP layers report back, and
      // without the reason a configuration mistake (a missing vector 'dimensions', an incompatible property
      // type, ...) reaches the user as a bare "Error on creating index" with nowhere to go (issue #5607).
      throw new IndexException("Error on creating index on type '" + metadata.typeName + "', properties " + metadata.propertyNames
          + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
    }

    // An index is half of what decides whether a partition is any use, so one created after the strategy was assigned
    // can undo the check that accepted it: recollating the partition index COLLATE CI makes it unprunable, and an
    // index on other properties adds a lookup that fans out (issue #5637). Reported from here rather than from
    // LocalDocumentType.addIndexInternal, which runs once per BUCKET and also during schema reload, so the same lines
    // would be multiplied by the bucket count and repeated on every open.
    //
    // Never refuses, unlike the identical diagnosis at assignment time: there the strategy was what the user asked
    // for and a blocked one is pure cost, whereas here the INDEX is what was asked for and it is useful, so the
    // partitioning is what gives way. Outside the try/catch above on purpose - a diagnostic must not be able to undo
    // an index that was built successfully by falling into the cleanup arm.
    try {
      type.reportPartitionSuitabilityAfterSchemaChange();
    } catch (final RuntimeException e) {
      // Same reasoning one step further. By this point the index is built, committed and registered on the type, so
      // letting anything unexpected out of the diagnosis would fail the command over an index that exists - and the
      // obvious retry then fails again with "already exists". SCHEMA_CHANGE mode cannot raise the refusal, so this
      // only catches a genuine fault in the check or the logging, which is worth a line and nothing more.
      LogManager.instance().log(this, Level.WARNING,
          "Cannot report the partition suitability of type '%s' after creating the index on %s. The index itself was "
              + "created successfully", e, metadata.typeName, metadata.propertyNames);
    }

    return created;
  }

  /**
   * The definition of an index dropped to make room for a replacement, kept so it can be rebuilt if the replacement
   * fails. Enough to reproduce it: the kind, the uniqueness, the null strategy, the page size, the properties, and the
   * metadata that carries the collations plus whatever settings the index type keeps of its own (analyzers,
   * geospatial precision, vector dimensions, ...).
   */
  private record ReplacedIndexDefinition(Schema.INDEX_TYPE indexType, boolean unique,
                                         LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, int pageSize, String typeName,
                                         String[] propertyNames, IndexMetadata metadata) {

    static ReplacedIndexDefinition of(final TypeIndex index) {
      final IndexInternal[] onBuckets = index.getIndexesOnBuckets();

      // Templated off a BUCKET-level metadata rather than built fresh, deliberately: only its concrete subclass knows
      // the settings of that index type (analyzers, geospatial precision, vector dimensions), and there is no generic
      // way to copy them onto a new instance. It is read as a TEMPLATE, exactly like the one the normal path builds:
      // LocalSchema.createBucketIndex takes the collations and the TypeIndex name off it and leaves the rest to the
      // per-bucket index it creates, so the bucket id it happens to carry never reaches the restored index. Nothing
      // else reads it either - the index it belongs to is dropped before the restore can run.
      final IndexMetadata metadata = onBuckets.length > 0 ? onBuckets[0].getMetadata() : null;

      // getPageSizeForNewFile(), not getPageSize(): the value goes back through the creation path, which validates it,
      // and an index whose current page size is not one creation would accept answers with a legal one instead. Asking
      // for the raw current size could make the restore fail on exactly the index the restore exists to save (#5713).
      return new ReplacedIndexDefinition(index.getType(), index.isUnique(), index.getNullStrategy(),
          index.getPageSizeForNewFile(), index.getTypeName(), index.getPropertyNames().toArray(new String[0]), metadata);
    }
  }

  /**
   * Puts back the index that {@link #withReplaceIfIncompatible(boolean)} took away, so a replacement the stored data
   * could not satisfy - the reason the implicit rebuild was removed in the first place (issue #5675) - costs the caller
   * an error instead of an index.
   * <p>
   * A failure to restore is attached to the original error rather than raised: what the caller has to act on is why the
   * new index could not be built, and hiding that behind the second failure would leave them with neither answer.
   */
  private void restoreReplacedIndex(final ReplacedIndexDefinition replaced, final Throwable failure) {
    if (replaced == null)
      return;

    try {
      final TypeIndexBuilder restore = database.getSchema().getEmbedded()
          .buildTypeIndex(replaced.typeName(), replaced.propertyNames()).withType(replaced.indexType());
      restore.withUnique(replaced.unique());
      restore.withNullStrategy(replaced.nullStrategy());
      restore.withPageSize(replaced.pageSize());
      if (replaced.metadata() != null)
        restore.withMetadata(replaced.metadata());
      restore.create();
    } catch (final Throwable restoreError) {
      failure.addSuppressed(restoreError);
      LogManager.instance().log(this, Level.SEVERE,
          "Cannot restore the index on type '%s' properties %s that was replaced by a failed CREATE INDEX. The type is "
              + "left without an index on those properties", restoreError, replaced.typeName(),
          Arrays.asList(replaced.propertyNames()));
    }
  }

  protected Index createBucketIndex(final LocalSchema schema, final LocalDocumentType type, final Type[] keyTypes,
      final LocalBucket bucket) {
    return createBucketIndex(schema, type, keyTypes, bucket, true);
  }

  protected Index createBucketIndex(final LocalSchema schema, final LocalDocumentType type, final Type[] keyTypes,
      final LocalBucket bucket, final boolean build) {
    return schema.createBucketIndex(type, keyTypes, bucket, metadata.typeName, indexType, unique, pageSize, nullStrategy, callback,
        metadata.propertyNames.toArray(new String[0]), null, batchSize,
        metadata, build);
  }

  private void validateSortedBuildPreconditions() {
    if (indexType != Schema.INDEX_TYPE.LSM_TREE)
      throw new IndexException("Sorted build currently supports only LSM_TREE indexes");
    if (database.isReplicated() || database.getWrappedDatabaseInstance().isReplicated())
      throw new IndexException("Sorted build is not supported on replicated databases");
    if (database.isTransactionActive())
      throw new IndexException("Sorted build requires no active transaction");
  }

  private SortedIndexBuildMetrics createWithSortedBuild(final LocalSchema schema, final LocalDocumentType type,
      final Type[] keyTypes,
      final List<Bucket> buckets, final Index[] indexes, final SortedIndexBuildRecoveryMarker recoveryMarker) {
    final long pipelineStarted = System.nanoTime();
    final long bucketIndexCreationStarted = System.nanoTime();
    for (int idx = 0; idx < buckets.size(); ++idx) {
      final int finalIdx = idx;
      database.transaction(() -> indexes[finalIdx] = createBucketIndex(schema, type, keyTypes,
          (LocalBucket) buckets.get(finalIdx), false), false, maxAttempts, null, null);
    }
    final long bucketIndexCreationNanos = System.nanoTime() - bucketIndexCreationStarted;

    final Map<Integer, LSMTreeIndex> indexesByBucket = new HashMap<>(indexes.length);
    for (final Index index : indexes) {
      if (!(index instanceof LSMTreeIndex lsmIndex))
        throw new IndexException("Sorted build requires LSM bucket indexes, found " + index.getClass().getName());
      indexesByBucket.put(index.getAssociatedBucketId(), lsmIndex);
    }

    final String logicalIndexName = metadata.typeName + metadata.propertyNames;
    final AtomicLong scannedRecords = new AtomicLong();
    final LSMTreeIndexBulkLoader.BuildOutcome outcome;
    final LSMTreeIndexBulkLoader.StageMetrics stageMetrics;
    final long logicalEntries;
    final long sourceScanNanos;
    try (LSMTreeIndexBulkLoader bulkLoader = new LSMTreeIndexBulkLoader(database, logicalIndexName,
        buildMemoryBudgetBytes, buildSpillDirectory, buildMergeFanIn, recoveryMarker.getSpillWorkspace(),
        buildParallelism)) {
      final long sourceScanStarted = System.nanoTime();
      database.transaction(() -> database.scanType(metadata.typeName, true, record -> {
          final LSMTreeIndex bucketIndex = indexesByBucket.get(record.getIdentity().getBucketId());
          if (bucketIndex == null)
            throw new IndexException("No empty bucket index found for record " + record.getIdentity() + " while building '"
                + logicalIndexName + "'");

          bulkLoader.add(bucketIndex, record);
          final long total = scannedRecords.incrementAndGet();
          if (callback != null)
            callback.onDocumentIndexed(record, total);
          return true;
        }, (rid, exception) -> {
          if (exception instanceof RuntimeException runtimeException)
            throw runtimeException;
          throw new IndexException("Error collecting sorted index entry at record " + rid, exception);
        }), false, maxAttempts, null, null);
      sourceScanNanos = System.nanoTime() - sourceScanStarted;

      outcome = bulkLoader.writeCompacted();
      logicalEntries = bulkLoader.size();
      stageMetrics = bulkLoader.getStageMetrics();
      publishIndexes(indexes);
    }

    LogManager.instance().log(this, Level.INFO,
        "Published sorted index '%s': records=%,d bucketIndexes=%d", logicalIndexName, scannedRecords.get(), indexes.length);
    return new SortedIndexBuildMetrics(logicalIndexName, unique, scannedRecords.get(), logicalEntries,
        outcome.entries(), indexes.length, outcome.memoryBudgetBytes(), stageMetrics.requestedMergeFanIn(),
        stageMetrics.admittedMergeFanIn(), stageMetrics.requestedBuildParallelism(),
        stageMetrics.admittedMergeParallelism(), stageMetrics.maxConcurrentMerges(),
        stageMetrics.admittedWriterParallelism(), stageMetrics.maxConcurrentWriters(),
        stageMetrics.initialRuns(), stageMetrics.finalRuns(),
        stageMetrics.materializedMergeGenerations(), stageMetrics.initialRunEntries(), stageMetrics.initialRunBytes(),
        stageMetrics.materializedMergeEntries(), stageMetrics.materializedMergeBytes(), outcome.spillBytes(),
        bucketIndexCreationNanos, sourceScanNanos, stageMetrics.initialRunNanos(), stageMetrics.inMemorySortNanos(),
        stageMetrics.materializedMergeNanos(), stageMetrics.finalStreamAndWriteNanos(), stageMetrics.attachmentNanos(),
        System.nanoTime() - pipelineStarted, 0L, 0L, 0L);
  }

  private void emitSortedBuildMetrics(final SortedIndexBuildMetrics metrics) {
    LogManager.instance().log(this, Level.INFO, "%s%s", SortedIndexBuildMetrics.LOG_MARKER, metrics.toJSON());
    final Consumer<SortedIndexBuildMetrics> hook = SORTED_BUILD_METRICS_TEST_HOOK.get();
    if (hook != null)
      hook.accept(metrics);
  }

  static void setSortedBuildMetricsTestHook(final Consumer<SortedIndexBuildMetrics> hook) {
    if (hook == null)
      SORTED_BUILD_METRICS_TEST_HOOK.remove();
    else
      SORTED_BUILD_METRICS_TEST_HOOK.set(hook);
  }

  private void publishIndexes(final Index[] indexes) {
    for (final Index index : indexes)
      if (!((IndexInternal) index).setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.UNAVAILABLE },
          IndexInternal.INDEX_STATUS.AVAILABLE))
        throw new IndexException("Cannot publish sorted index bucket '" + index.getName() + "'");
  }

  private boolean cleanupIndexes(final LocalSchema schema, final Index[] indexes, final Throwable originalError) {
    boolean cleaned = true;
    for (final Index index : indexes)
      if (index != null)
        try {
          if (schema.existsIndex(index.getName()))
            schema.dropIndex(index.getName());
        } catch (final Throwable cleanupError) {
          cleaned = false;
          originalError.addSuppressed(cleanupError);
        }
    return cleaned;
  }

  private void clearRecoveryMarker(final SortedIndexBuildRecoveryMarker recoveryMarker, final Throwable originalError) {
    if (recoveryMarker == null)
      return;
    try {
      recoveryMarker.clear();
    } catch (final Throwable cleanupError) {
      originalError.addSuppressed(cleanupError);
    }
  }

  public TypeIndexBuilder withCollations(final List<String> collations) {
    metadata.collations = collations;
    return this;
  }

  public String getTypeName() {
    return metadata.typeName;
  }

  public String[] getPropertyNames() {
    return metadata.propertyNames.toArray(new String[0]);
  }
}
