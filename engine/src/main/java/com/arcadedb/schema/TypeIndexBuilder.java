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
import com.arcadedb.database.CommittedReadScope;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexBulkLoader;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
  private static final long SORTED_BUILD_PROGRESS_INTERVAL    = 1_000_000L;

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
  // Whether the last create() built the index, as opposed to answering with one already on the properties
  private boolean        built;
  // Whether that build ran inside the caller's transaction, so its entries are still queued on it (issue #10438)
  private boolean        builtInCallerTransaction;

  protected TypeIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, TypeIndex.class);
    this.metadata = new IndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * The key type of a {@code BY ITEM} index over a LIST: the declared {@code OF} item type when it is a plain scalar, so
   * a lookup by a number of another Java type ({@code CONTAINS 7} against a {@code LIST OF DOUBLE} holding {@code 7.0})
   * is converted to the stored type the way a scan compares it (issue #8890). STRING is kept for:
   * <ul>
   *   <li>a list with no declared item type, since lists can hold heterogeneous values;</li>
   *   <li>non-scalar items (links, embedded documents, nested collections);</li>
   *   <li>BOOLEAN, since a lookup by the text 'true' cannot be read as a boolean key;</li>
   *   <li>a FULL_TEXT index, which tokenizes text.</li>
   * </ul>
   */
  static Type listItemKeyType(final Property property, final Schema.INDEX_TYPE indexType) {
    if (indexType == Schema.INDEX_TYPE.FULL_TEXT)
      return Type.STRING;

    final String ofType = property.getOfType();
    final Type itemType = ofType != null ? Type.getTypeByName(ofType) : null;
    if (itemType == null)
      return Type.STRING;

    return switch (itemType) {
      case LIST, MAP, EMBEDDED, LINK, BINARY, BOOLEAN, ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS,
           ARRAY_OF_DOUBLES -> Type.STRING;
      default -> itemType;
    };
  }

  /**
   * The one copy constructor of the whole builder hierarchy: the specialised builders {@link #withType} swaps in -
   * {@link TypeFullTextIndexBuilder}, {@link TypeLSMVectorIndexBuilder}, {@link TypeLSMSparseVectorIndexBuilder} and
   * {@link TypeGeoIndexBuilder} - all delegate here, each supplying only the two things that make it different: the
   * index type it stands for and the {@link IndexMetadata} subclass its own settings live in.
   * <p>
   * Each of the four used to copy the common state itself, field by field, so a field added to {@link IndexBuilder} or
   * to this class kept working for {@code LSM_TREE} and {@code HASH} - which never swap builders - and was dropped on
   * the floor for all four specialised types, with no compile error and nothing to notice it (issue #5606). Now a new
   * field has exactly one place to be added: here if it is declared by this class, {@link #copyBaseFieldsFrom} if it
   * is declared by the base one.
   *
   * @param copyFrom  the builder being replaced, whose configuration must survive the swap
   * @param indexType the index type this specialised builder stands for
   * @param metadata  a freshly built metadata of the subclass the specialised builder writes its settings into; the
   *                  common definition ({@code collations}, {@code typeIndexName}) is carried onto it from
   *                  {@code copyFrom}
   */
  protected TypeIndexBuilder(final TypeIndexBuilder copyFrom, final Schema.INDEX_TYPE indexType,
      final IndexMetadata metadata) {
    super(copyFrom.database, TypeIndex.class);

    // The type/properties/bucket identity comes from the metadata the subclass just built out of copyFrom's; what is
    // left is the rest of the DEFINITION, which used to survive the swap only for GEOSPATIAL.
    this.metadata = copyFrom.metadata.copyCommonTo(metadata);
    this.indexType = indexType;

    copyBaseFieldsFrom(copyFrom);

    this.defaultKeyTypesForUndeclaredProperties = copyFrom.defaultKeyTypesForUndeclaredProperties;
    // Sorted build cannot actually reach a specialised builder today - withType() refuses it for anything but
    // LSM_TREE - so these five ride along for correctness rather than because something depends on them. That guard is
    // circumstantial, not a design: the day a second index family learns to build sorted, the settings must already be
    // here rather than being remembered as a sixth thing to add.
    this.buildMode = copyFrom.buildMode;
    this.buildMemoryBudgetBytes = copyFrom.buildMemoryBudgetBytes;
    this.buildSpillDirectory = copyFrom.buildSpillDirectory;
    this.buildMergeFanIn = copyFrom.buildMergeFanIn;
    this.buildParallelism = copyFrom.buildParallelism;
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

  /**
   * Whether the last {@link #create()} BUILT the index it returned, as opposed to answering a guarded request
   * ({@link #withIgnoreIfExists}) with the index already on those properties. Told apart here rather than by comparing
   * the returned name with the requested one: concurrent creators of an unnamed index all derive the same name, and every
   * one of them used to report the single index only one of them built as its own (issue #9741).
   * <p>
   * State of THIS builder instance, reset by every {@code create()}: read it on the same thread, right after the call.
   * A builder is not meant to be shared between threads.
   */
  public boolean isBuilt() {
    return built;
  }

  /**
   * Whether the last {@link #create()} built the index INSIDE the transaction the caller already had open with pending
   * writes, so its entries are staged on that transaction and reach the index only when the caller commits (issue
   * #6324 item 1, extended to the vector families by issue #10438). Anything that would read the index's committed
   * state to derive more state from it - building a vector graph eagerly is the case - has to wait for that commit.
   * <p>
   * Same contract as {@link #isBuilt()}: state of this builder instance, reset by every {@code create()}.
   */
  public boolean isBuiltInCallerTransaction() {
    return builtInCallerTransaction;
  }

  @Override
  public TypeIndex create() {
    built = false;
    builtInCallerTransaction = false;
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // Checked before the existing-index lookup below, which needs the requested index type to decide whether an index
    // already on those properties covers the request.
    if (indexType == null)
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + metadata.typeName + "' because indexType was not specified");

    // The index is built by SCANNING the buckets, so everything the async executor has been asked to write has to be
    // committed first, or the build simply does not see it and the records end up with no entry at all - a silent
    // wrong answer on every lookup, not a slow one (issue #6281).
    //
    // This used to be `while (database.isAsyncProcessing()) waitCompletion()`, which is not the same barrier: that
    // predicate reports queued and executing TASKS, while a worker holds ONE transaction open across up to
    // ASYNC_TX_BATCH_SIZE (10240 by default) of them. An executor whose queues happen to be drained at the instant of
    // the check therefore answers "idle" while still sitting on thousands of uncommitted records, and the guard was
    // then skipped entirely. It also prevents the NeedRetryException a running compaction used to raise when several
    // indexes are created in a row on a large dataset, which is what the guard was originally added for.
    database.waitForAsyncCompletion();

    // Carry the user-supplied TypeIndex name (set via {@link #withIndexName}) onto the
    // IndexMetadata so it propagates to each per-bucket index and ultimately to
    // {@link LocalDocumentType#addIndexInternal} which reads it when minting the TypeIndex.
    // Without this hop the manual name is dropped and the auto-derived
    // typeName + "[propertyNames]" form is used (issue #4139).
    if (indexName != null && !indexName.isEmpty())
      metadata.typeIndexName = indexName;

    final LocalSchema schema = database.getSchema().getEmbedded();

    final LocalDocumentType type = schema.getType(metadata.typeName);
    // The index already on these properties, if any, and whether this request replaces it (dropped under the lock below)
    TypeIndex found = findIndexOnProperties(type);
    boolean replace = false;
    if (found != null) {
      try {
        replace = decideOverExistingIndex(found, true);
      } catch (final IndexException e) {
        // Read without the lock, so a concurrent replacer can be dropping the index while it is decided over here (issue
        // #9741). It stays registered - dropping, then invalid - until that drop completes under the schema write lock, so
        // the request is decided there instead, over whatever replaced it or over nothing. Any other refusal is the answer.
        if (found.isValid() && !found.isDropping())
          throw e;
        found = null;
      }
      if (found != null && !replace)
        return found;
    }
    final TypeIndex existingTypeIndex = found;
    final boolean replacing = replace;

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
          keyTypes[i++] = listItemKeyType(property, indexType);
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
    final boolean sortedBuild = buildMode == IndexBuildMode.SORTED || isSortedBuildChosenFor(buckets);
    // A large UNIQUE index the sorted build cannot take (a replicated database, a transaction with pending writes to its buckets) checks its keys
    // once, over the finished index, instead of at every commit (issue #10349)
    final boolean deferredUniqueCheck = !sortedBuild && isUniqueCheckDeferredFor(buckets);
    final SortedIndexBuildRecoveryMarker[] recoveryMarker = new SortedIndexBuildRecoveryMarker[1];
    final SortedIndexBuildMetrics[] sortedBuildMetrics = new SortedIndexBuildMetrics[1];
    final long sortedBuildStarted = sortedBuild ? System.nanoTime() : 0L;

    if (sortedBuild)
      validateSortedBuildPreconditions(buckets);

    // The replaced index is dropped as the last thing before the build, under the lock below: everything above can still
    // refuse the request (an undeclared property, a BY ITEM on a non-LIST, a sorted build on a replicated database), and
    // none of those refusals may cost the caller the index it already had.
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
    // Set once the replaced index has been dropped: the definition to put back if the new index cannot be built.
    final ReplacedIndexDefinition[] replaced = new ReplacedIndexDefinition[1];

    // Asked here because below this line there is ALWAYS a transaction, and the answer is about the one that was
    // already there (issue #6324, item 1), and about the buckets this build scans (issue #10450). See
    // IndexBuilder#buildSharesCallerTransaction.
    final boolean sharesCallerTransaction = buildSharesCallerTransaction(buckets);

    final TypeIndex created;
    // Set when another creator built an index on these properties after the lookup above (issue #9741, below)
    final TypeIndex[] builtConcurrently = new TypeIndex[1];
    // The refusal decided over that index, carried out of the build's try so it is not wrapped as a failed build
    final RuntimeException[] refusedOverConcurrent = new RuntimeException[1];
    // The barrier of #6281, paid at the top of this method, covers what the async side wrote BEFORE the build. This
    // covers what it would write DURING it (issue #6303, item 2), and only the first half was ever here:
    // LocalDocumentType.addIndexInternal registers the index before the scan, so a record saved AFTER that point
    // stages its own entry and is safe - but one saved in the window between the barrier and that registration, and
    // still uncommitted when the scan reads, is in neither the scan nor the index. Nothing stopped an async worker
    // from opening exactly that window, because unlike BucketIndexBuilder this path never parked the workers at all.
    //
    // Held across the whole build rather than only across the scan: the registration is what the window is measured
    // from, and it happens inside recordFileChanges. Reentrant, so the nested quiescence of any builder reached from
    // here simply rides on this one.
    // Registered from here to the end of the build: the index is visible to the planners bucket by bucket while it is
    // populated, and until it is complete it must not answer a query (issue #9331)
    type.beginIndexConstruction(metadata.propertyNames);
    // Whether this build still holds the deferral: set under the write lock below, given back under it once the final pass has run
    final boolean[] deferralHeld = new boolean[1];
    try (final AsyncQuiesce asyncPaused = database.quiesceAsync()) {
      final long recordFileChangesStarted = System.nanoTime();
      schema.recordFileChanges(() -> {
        // ASKED AGAIN UNDER THE LOCK. The lookup at the top runs before it, so concurrent creators of the same index all
        // found nothing there and then built one each, one after the other: the type ended up with one sub-index per
        // creator on every bucket, all fed by every write (issue #9741). Every build runs inside this write lock, so an
        // index found here is complete, and the request is decided over it exactly as if it had been there all along.
        //
        // The replacement is decided here too. Only the type's OWN index counts on that path - a parent's index on the
        // same properties must not stand in for the replacement that was explicitly asked for - and it is dropped only
        // if it is still the very index the decision above was made over. Concurrent replacers all found that one
        // before the first of them dropped it, and dropping an index twice is a no-op, so dropping it outside the lock
        // let every one of them go on to build its own; the later ones now find the first one's replacement instead.
        final TypeIndex current = replacing ? type.getIndexByProperties(metadata.propertyNames) : findIndexOnProperties(type);
        if (current != null) {
          if (current != existingTypeIndex) {
            // Decided here, under the lock, so no other replacer can drop the index while it is decided over. A
            // replacement is never decided over an index another creator has just built: dropping that one is not what
            // either request asked for.
            try {
              decideOverExistingIndex(current, false);
              builtConcurrently[0] = current;
            } catch (final RuntimeException refusal) {
              refusedOverConcurrent[0] = refusal;
            }
            return null;
          }
          // Recorded only once dropped: a refused drop takes nothing away, so there is nothing to put back
          final ReplacedIndexDefinition definition = ReplacedIndexDefinition.of(current);
          current.drop();
          replaced[0] = definition;
        }

        if (deferredUniqueCheck) {
          // UNDER THE WRITE LOCK, AFTER THE DECISION OVER AN EXISTING INDEX AND THE DROP OF A REPLACED ONE: NO OTHER INDEX ON THESE
          // PROPERTIES IS LIVE, AND NOBODY ELSE CAN COMMIT TO THE ONE BUILT HERE UNTIL verifyUniqueKeys HAS RUN. Set any earlier, a
          // commit to an index another creator had just published would skip its check while this build waited for the lock
          type.deferUniqueCheck(metadata.propertyNames);
          deferralHeld[0] = true;
        }

        if (sortedBuild) {
          // The sorted build writes its pages outside any transaction, so the caller's - free of writes to these buckets,
          // see validateSortedBuildPreconditions - is set aside for its duration and given back untouched. Whatever the
          // caller wrote ELSEWHERE stays pending in it (issue #10466): the build writes only into the index files it
          // creates below, which no transaction of the caller can hold a page of, and reads only these buckets. An HTTP command
          // always arrives inside the auto-commit transaction of its handler, which is how CREATE INDEX from Studio runs
          // (issue #10334). Reading outside a transaction misses nothing: no other transaction can commit while the build
          // holds the database write lock taken by recordFileChanges, and the async workers are quiesced above.
          try (final CommittedReadScope outsideCallerTransaction = CommittedReadScope.open(database)) {
            recoveryMarker[0] = SortedIndexBuildRecoveryMarker.create(database, metadata.typeName, metadata.propertyNames,
                buildSpillDirectory);
            sortedBuildMetrics[0] = createWithSortedBuild(schema, type, keyTypes, buckets, indexes, recoveryMarker[0]);
          }
        } else
          for (int idx = 0; idx < buckets.size(); ++idx) {
            final int finalIdx = idx;

            if (!sharesCallerTransaction) {
              // ONE TRANSACTION, exactly as before issue #6324: with nothing of anybody else's to see, creating and
              // building in one go is both simpler and one commit per bucket cheaper.
              database.transaction(() -> {

                final LocalBucket bucket = (LocalBucket) buckets.get(finalIdx);

                indexes[finalIdx] = createBucketIndex(schema, type, keyTypes, bucket, true);

              }, false, maxAttempts, null, null);
              continue;
            }

            // TWO TRANSACTIONS, and the split is the whole of issue #6324, item 1.
            //
            // The COMPONENT is created in a transaction of its OWN. The schema entry that names this index is written
            // whatever the caller's transaction goes on to do - postponed to its end, commit and rollback alike
            // (#8635), since the index exists either way - so the index FILE has to be committed on the same terms:
            // leaving its first page inside a caller's transaction that later rolls back would leave the schema
            // pointing at a file with no pages, which fails on the next write with "the file is invalid". Committing
            // it also keeps the index usable from a NESTED transaction, which cannot see an outer transaction's
            // uncommitted pages.
            database.transaction(() -> {

              final LocalBucket bucket = (LocalBucket) buckets.get(finalIdx);

              indexes[finalIdx] = createBucketIndex(schema, type, keyTypes, bucket, false);

            }, false, maxAttempts, null, null);

            // The BUILD then indexes the COMMITTED records in a transaction of its own and queues on the caller's
            // transaction only what the caller itself created, updated or deleted in the bucket (issue #10490, see
            // IndexBuilder#buildCreatedIndex). A scan that read the caller's transaction saw the records the caller has
            // written and not yet committed - `INSERT INTO V SET id = 7; CREATE INDEX ON V (id)` in one transaction used
            // to end with the record present and NO entry for it, in neither the scan nor the index - but it queued the
            // committed records' entries there too, and a rollback of the caller took them all away. Now the entries of
            // the pending records commit, or roll back, with the records they describe and the committed ones are not
            // the caller's to roll back.
            database.transaction(() -> buildCreatedIndex(indexes[finalIdx], true), true, maxAttempts, null, null);
          }

        if (deferredUniqueCheck) {
          // EVERY BUCKET IS BUILT: A KEY TWO RECORDS SHARE ACROSS BUCKETS IS ONLY VISIBLE FROM HERE. A REFUSAL ENDS THE BUILD LIKE
          // THE DUPLICATE OF A BATCH ALWAYS DID, THROUGH THE CLEAN-UP BELOW
          verifyUniqueKeys(type.getIndexByProperties(metadata.propertyNames));
          // STILL UNDER THE WRITE LOCK: EVERY COMMIT AFTER THE LOCK IS RELEASED CHECKS THE INDEX AGAIN, WHATEVER THE SCHEMA SESSION
          // DOES NEXT (A REPLICATED ONE SHIPS ITS ENTRY AFTER THE LOCK IS GONE)
          type.resumeUniqueCheck(metadata.propertyNames);
          deferralHeld[0] = false;
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

      if (builtConcurrently[0] == null && refusedOverConcurrent[0] == null) {
        created = type.getIndexByProperties(metadata.propertyNames);
        built = true;
        // A sorted build writes its pages outside any transaction (see the sortedBuild branch above), so nothing of it
        // is left queued on the caller's even when the caller had pending writes
        builtInCallerTransaction = sharesCallerTransaction && !sortedBuild;
      } else
        created = null;
    } catch (final NeedRetryException e) {
      if (cleanupIndexes(schema, indexes, e)
          && (recoveryMarker[0] == null || recoveryMarker[0].baselineRestored(database)))
        clearRecoveryMarker(recoveryMarker[0], e);
      restoreReplacedIndex(replaced[0], e);
      throw e;
    } catch (final Throwable e) {
      if (cleanupIndexes(schema, indexes, e)
          && (recoveryMarker[0] == null || recoveryMarker[0].baselineRestored(database)))
        clearRecoveryMarker(recoveryMarker[0], e);
      restoreReplacedIndex(replaced[0], e);
      // Carry the root cause into the message: this exception is what the SQL/HTTP layers report back, and
      // without the reason a configuration mistake (a missing vector 'dimensions', an incompatible property
      // type, ...) reaches the user as a bare "Error on creating index" with nowhere to go (issue #5607). The whole
      // cause chain is read, not one level, so no wrapper in between can hide the reason again (issue #10382).
      throw new IndexException("Error on creating index on type '" + metadata.typeName + "', properties " + metadata.propertyNames
          + describeFailure(e), e);
    } finally {
      // A build that failed holds the deferral still (a finished one gave it back under the lock): the index it was filling is
      // being cleaned up, and giving the check back only costs the commits that reach it meanwhile a lookup
      if (deferralHeld[0])
        type.resumeUniqueCheck(metadata.propertyNames);
      type.endIndexConstruction(metadata.propertyNames);
    }

    // Thrown outside the build's try, so a refusal reaches the caller exactly as it does when the index was there before
    // the request, rather than wrapped as a failed build
    if (refusedOverConcurrent[0] != null)
      throw refusedOverConcurrent[0];
    if (builtConcurrently[0] != null)
      return builtConcurrently[0];

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
   * The index already on the requested properties: the type's own first (no hierarchy walk), then an inherited one.
   * Either way it is used to answer the request, never to make room for it.
   */
  private TypeIndex findIndexOnProperties(final LocalDocumentType type) {
    final TypeIndex own = type.getIndexByProperties(metadata.propertyNames);
    return own != null ? own : type.getPolymorphicIndexByProperties(metadata.propertyNames);
  }

  /**
   * Decides a request over {@code existingTypeIndex}, an index already on the requested properties: answers false when
   * that index answers the request, true when an explicit {@link #withReplaceIfIncompatible} replaces it and
   * {@code replacementAllowed}, and throws otherwise - with the same message whichever path asks.
   */
  private boolean decideOverExistingIndex(final TypeIndex existingTypeIndex,
      final boolean replacementAllowed) {
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

    // Third half, once the definition lines up: the NAME the caller wrote. A name is not a label on the request -
    // an index is reachable only through it (SEARCH_INDEX('<name>', ...), DROP INDEX, REBUILD INDEX) - so an index
    // already on these properties under a different name does not provide what was asked for either, and a type
    // holds one index per property set so the requested name cannot be added next to it. Answering the guard with
    // the other index left the requested name uncreated while the statement reported on the request, and every
    // later lookup through that name failed (issue #6921).
    //
    // Last of the three on purpose: a definition mismatch is the harder problem and stays the reported one, so a
    // named request over an index of the wrong kind or configuration still names the kind or the settings rather
    // than the name. And only a name the caller WROTE reaches this - {@link CreateIndexStatement} forwards the
    // manual name alone and leaves the auto-derived typeName[properties] form out - which keeps a guarded request
    // that named nothing the plain no-op it has always been.
    //
    // Applied on the unguarded path too: a name conflict is not something IF NOT EXISTS makes idempotent, so the
    // "use IF NOT EXISTS" hint the unguarded message below ends with would be the wrong advice here.
    //
    // The other production caller that sets a name on a TYPE index is {@code RebuildIndexStatement}, which carries
    // the logical index's own name across the rebuild (issue #5791). It never meets this check: it drops the index
    // first, inside the same {@code executeLockingFiles} closure, so the lookup in create() finds nothing on those
    // properties by the time create() runs. The one shape that still reaches here - a type whose OWN index was just
    // dropped while a parent type indexes the same properties - was already refused before this check existed, by
    // the unguarded branch below; only the message it is refused with changes.
    if (satisfied && indexName != null && !indexName.isEmpty() && !indexName.equals(existingTypeIndex.getName()))
      throw conflictWithExistingIndexName(existingTypeIndex, indexName, metadata.typeName, metadata.propertyNames);

    if (satisfied && ignoreIfExists)
      return false;

    if (!satisfied && replaceIfIncompatible && replacementAllowed && existingTypeIndex.getTypeName().equals(metadata.typeName))
      // Own index, and the caller explicitly asked to replace what does not fit. Dropped by create() under the write
      // lock, once everything that could still refuse the request has had its say.
      return true;
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
   * Renders why a build failed as the suffix of the reported message: {@code ": <message>"} for the failure and each cause
   * under it that says something the text so far does not, so a wrapper that only names the step ("Cannot build sorted
   * index") cannot hide the reason sitting below it. A cause whose message is already contained in the text, as for a
   * {@code RuntimeException} built around another exception, adds nothing and is skipped. Identity-based, so a cyclic
   * chain ends.
   *
   * @return the suffix, empty when no message in the chain says anything
   */
  private static String describeFailure(final Throwable failure) {
    final StringBuilder description = new StringBuilder();
    final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable current = failure; current != null && visited.add(current); current = current.getCause()) {
      final String message = current.getMessage();
      if (message != null && description.indexOf(message) < 0)
        description.append(": ").append(message);
    }
    return description.toString();
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
      final LocalBucket bucket, final boolean build) {
    return schema.createBucketIndex(type, keyTypes, bucket, metadata.typeName, indexType, unique, pageSize, nullStrategy, callback,
        metadata.propertyNames.toArray(new String[0]), null, batchSize,
        metadata, build);
  }

  private void validateSortedBuildPreconditions(final List<Bucket> buckets) {
    final String refusal = sortedBuildRefusal(buckets);
    if (refusal != null)
      throw new IndexException(refusal);
  }

  /**
   * Why the sorted build cannot run here, or null when it can. The one list of guards, asked both by an explicit
   * {@link IndexBuildMode#SORTED} request, which is refused with the reason, and by the automatic choice of
   * {@link #isSortedBuildChosenFor}, which falls back to the record-at-a-time build instead: a guard known to one and not the
   * other would turn a DEFAULT build into a refused one.
   */
  private String sortedBuildRefusal(final List<Bucket> buckets) {
    if (indexType != Schema.INDEX_TYPE.LSM_TREE)
      return "Sorted build currently supports only LSM_TREE indexes";
    if (database.isReplicated() || database.getWrappedDatabaseInstance().isReplicated())
      return "Sorted build is not supported on replicated databases";
    if (anyCallerTransactionHasChangesIn(database, buckets))
      // The build has to see those changes, and only the record-at-a-time build reads through the caller's transaction. A
      // write elsewhere does not count (issue #10466): the scan would not find it in these buckets either way
      return "Sorted build requires no pending changes to the indexed buckets in any active transaction";
    return null;
  }

  /**
   * Whether a build asked for in the DEFAULT mode takes the sorted build all the same: a UNIQUE LSM_TREE index over a type holding at
   * least {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS} records (issue #10334), or a NOTUNIQUE one over a type
   * holding at least {@link GlobalConfiguration#INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS} (issue #10267).
   * <p>
   * For a UNIQUE index the record-at-a-time build commits every batch through the uniqueness check, which looks each key up in the
   * index being built, and that lookup reads every page written so far - nothing compacts an index while it is built - so the build
   * time grows with the square of the record count. The sorted build refuses a duplicate key in its sorted stream instead, within a
   * bounded memory budget. For a NOTUNIQUE index nothing is quadratic: the record-at-a-time build pays the transaction bookkeeping of
   * every key, which a bulk build over existing records does not need, and the sorted build skips it, so its threshold is far
   * higher and only where the saving is felt.
   * <p>
   * Never chosen where the sorted build cannot run ({@link #sortedBuildRefusal(List)}). The record count is the buckets' cached
   * counters, polymorphic like the build itself, so asking costs nothing; a counter that is off after an unclean shutdown only
   * picks the other build, which produces the same index.
   */
  private boolean isSortedBuildChosenFor(final List<Bucket> buckets) {
    if (buildMode != IndexBuildMode.DEFAULT || indexType != Schema.INDEX_TYPE.LSM_TREE)
      return false;

    final GlobalConfiguration threshold = unique ? GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS :
        GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS;
    final long minRecords = database.getConfiguration().getValueAsLong(threshold);
    if (minRecords <= 0)
      return false;

    long records = 0;
    for (final Bucket bucket : buckets)
      records += bucket.count();
    if (records < minRecords)
      return false;

    final String refusal = sortedBuildRefusal(buckets);
    if (refusal != null) {
      if (unique)
        // Said out loud: on a type this large the record-at-a-time build is the slow one, and this is the only place that knows why
        LogManager.instance().log(this, Level.INFO,
            "Building unique index on %s%s over %,d records one record at a time, checking the keys once over the finished index: %s",
            metadata.typeName, metadata.propertyNames, records, refusal);
      else
        LogManager.instance().log(this, Level.FINE, "Building index on %s%s over %,d records one record at a time: %s",
            metadata.typeName, metadata.propertyNames, records, refusal);
      return false;
    }

    LogManager.instance().log(this, Level.INFO, "Building %s index on %s%s over %,d records with the sorted build (%s=%,d)",
        unique ? "unique" : "non-unique", metadata.typeName, metadata.propertyNames, records, threshold.getKey(), minRecords);
    return true;
  }

  /**
   * Whether the uniqueness check of a record-at-a-time build is left to {@link #verifyUniqueKeys} (issue #10349): a UNIQUE LSM_TREE
   * index, in the default build mode, over a type holding at least {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS}
   * records. Those are the sizes the sorted build takes when it can run (issue #10334), so this is what the builds it cannot take
   * are left with: a replicated database, whose sorted build would write pages no replica receives, and a transaction with pending
   * writes to the buckets it indexes, which the sorted build cannot read through (issue #10466).
   * <p>
   * The record-at-a-time build checks every key it adds against the index it is filling, and that lookup reads every page written so
   * far. Nothing compacts the index while it is built - a replicated build cannot even ask, it runs inside the schema session that
   * ships its pages - so the build time grew with the square of the record count. The build owns the database write lock, so nobody
   * else commits to the index until it ends, and the keys of the build are the only ones the index holds: one ordered pass over
   * them at the end is the same check, at the cost of reading the pages once.
   * <p>
   * A smaller type keeps the check of every batch: it fails at the first duplicate instead of after the last record, and no lookup
   * is long enough to matter.
   */
  private boolean isUniqueCheckDeferredFor(final List<Bucket> buckets) {
    if (!unique || buildMode != IndexBuildMode.DEFAULT || indexType != Schema.INDEX_TYPE.LSM_TREE)
      return false;

    final long minRecords = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS);
    if (minRecords <= 0)
      return false;

    long records = 0;
    for (final Bucket bucket : buckets)
      records += bucket.count();
    return records >= minRecords;
  }

  /**
   * The uniqueness check a build left to the end (issue #10349): one pass over the finished index, in the order of the index itself
   * and across every bucket, refuses the first key two records share with the refusal a commit gives - the index, the key and the
   * record already holding it.
   * <p>
   * Two keys are the same key when the comparator of the index says so, the one that merges the buckets here and that a lookup uses,
   * so the key types and the collation decide what a duplicate is exactly as they do for the check of a commit. A key whose values
   * are all null is skipped, as the check of a commit skips it: any number of records may hold it.
   * <p>
   * Read outside the caller's transaction: it holds none of the build's entries, and what the caller wrote queues entries of its own
   * that are checked when it commits.
   */
  private void verifyUniqueKeys(final TypeIndex index) {
    final String indexName = index.getName();
    try (final CommittedReadScope committedOnly = CommittedReadScope.open(database)) {
      database.transaction(() -> {
        try (final IndexCursor cursor = index.iterator(true)) {
          Object[] previousKeys = null;
          RID previousRid = null;
          while (cursor.hasNext()) {
            final RID rid = cursor.next().getIdentity();
            // EVERY ENTRY DESERIALIZES ITS OWN KEY ARRAY (PageIterator.getKeys()), AND THE MERGE ONLY PASSES IT ON: HOLDING THE PREVIOUS ONE
            // ACROSS next() IS SAFE
            final Object[] keys = cursor.getKeys();
            if (previousKeys != null && !rid.equals(previousRid) && !LSMTreeIndexAbstract.isKeyNull(keys)
                && LSMTreeIndexAbstract.compareKeys(cursor.getComparator(), cursor.getBinaryKeyTypes(), previousKeys, keys) == 0)
              throw new DuplicatedKeyException(indexName, Arrays.toString(keys), previousRid);
            previousKeys = keys;
            previousRid = rid;
          }
        }
      }, false, maxAttempts, null, null);
    }
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

    // The canonical name of the TypeIndex, as the record-at-a-time build and the SQL layer spell it (no space after the
    // comma), so a refusal names the same index whichever build raised it (issue #10382)
    final String logicalIndexName = metadata.typeName + metadata.propertyNames.toString().replace(" ", "");
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
          if (total % SORTED_BUILD_PROGRESS_INTERVAL == 0)
            LogManager.instance().log(this, Level.INFO, "Building sorted index '%s': scanned %,d records (%,.0f records/sec)...",
                logicalIndexName, total, total / Math.max(0.001, (System.nanoTime() - sourceScanStarted) / 1e9));
          if (callback != null)
            callback.onDocumentIndexed(record, total);
          return true;
        }, (rid, exception) -> {
          if (exception instanceof RuntimeException runtimeException)
            throw runtimeException;
          throw new IndexException("Error collecting sorted index entry at record " + rid, exception);
        }), false, maxAttempts, null, null);
      sourceScanNanos = System.nanoTime() - sourceScanStarted;
      LogManager.instance().log(this, Level.INFO, "Building sorted index '%s': scanned %,d records in %,dms, writing the index...",
          logicalIndexName, scannedRecords.get(), sourceScanNanos / 1_000_000);

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

  /**
   * Every collation reaching an index is normalised and validated here - see
   * {@link IndexMetadata#normalizeCollation} for what used to get through and why it could not be repaired
   * afterwards (issue #7900). Both the SQL {@code COLLATE} clause and a programmatic caller land on this method,
   * so this is the one place the rule has to hold.
   */
  public TypeIndexBuilder withCollations(final List<String> collations) {
    if (collations == null) {
      metadata.collations = null;
      return this;
    }

    final List<String> normalized = new ArrayList<>(collations.size());
    for (final String collation : collations)
      normalized.add(IndexMetadata.normalizeCollation(collation));

    metadata.collations = normalized;
    return this;
  }

  public String getTypeName() {
    return metadata.typeName;
  }

  public String[] getPropertyNames() {
    return metadata.propertyNames.toArray(new String[0]);
  }
}
