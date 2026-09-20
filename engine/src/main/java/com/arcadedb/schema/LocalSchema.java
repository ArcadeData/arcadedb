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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesMaintenanceScheduler;
import com.arcadedb.engine.timeseries.TimeSeriesTagDictionary;
import com.arcadedb.event.*;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.function.FunctionLibraryFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactory;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.hash.HashIndex;
import com.arcadedb.index.hash.HashIndexBucket;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.SparseSegmentComponent;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.trigger.*;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Local implementation of the database schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalSchema implements Schema {
  public static final String                                 DEFAULT_ENCODING              = "UTF-8";
  public static final String                                 SCHEMA_FILE_NAME              = "schema.json";
  public static final String                                 SCHEMA_PREV_FILE_NAME         = "schema.prev.json";
  public static final String                                 CACHED_COUNT_FILE_NAME_LEGACY = "cached-count.json"; // DEPRECATED FROM v25.2.1
  public static final String                                 STATISTICS_FILE_NAME          = "statistics.json";
  public static final int                                    BUILD_TX_BATCH_SIZE           = 100_000;

  // The rest of the NTFS/Windows-reserved character set beyond '/', '\' and '*', which checkValidBucketName()
  // checks separately: '<', '>', ':', '"', '|' and '?'.
  private static final String                                WINDOWS_ILLEGAL_CHARS         = "<>:\"|?";

  // Windows reserves these as device names for the segment of a file name up to (and not including) the first
  // dot, regardless of what an extension or further dotted segment says: "CON.txt" is refused exactly like "CON".
  private static final Set<String>                           WINDOWS_RESERVED_NAMES        = Set.of(//
      "CON", "PRN", "AUX", "NUL",//
      "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",//
      "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

  /**
   * Components whose load has a side effect on ANOTHER component, so they cannot be ADDED to an already-loaded
   * schema in isolation (issue #6988):
   * <ul>
   *   <li>the dictionary is what every component resolves property names through;</li>
   *   <li>a compacted index is claimed by the mutable index that names it in its page 0 - and the claim is what
   *       {@link #sweepOrphanCompactedIndexFiles} distinguishes a live compacted file from an orphan by;</li>
   *   <li>a bloom filter is in turn claimed by its compacted index.</li>
   * </ul>
   * An entry carrying one of these falls back to the full rebuild, where every component is re-instantiated and
   * every claim is re-established in one pass.
   * <p>
   * This set governs the FIRST pass of {@link #loadIncremental} only - files with no component yet. A file that
   * already has one and was merely written into is a different question, and
   * {@link #NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS} answers it.
   */
  private static final Set<String> NON_INCREMENTAL_COMPONENT_EXTENSIONS = Set.of(//
      Dictionary.DICT_EXT, //
      LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, //
      LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, //
      LSMTreeIndexBloomFilter.FILE_EXT);

  /**
   * Components whose already-registered instance cannot be refreshed in isolation when an entry writes pages INTO
   * it, so the second pass of {@link #loadIncremental} hands the caller back to the full rebuild (issue #7266).
   * <ul>
   *   <li>a compacted index is claimed by the mutable index holding it as its sub-index, so handing the file id a
   *       new instance would leave that claim pointing at the old one;</li>
   *   <li>a bloom filter reads its directory into RAM once, in {@code loadDirectory()} by way of
   *       {@link #attachBloomFilters}, and no load hook re-reads it - so pages appended to the file leave the
   *       in-RAM directory describing the file as it was before the entry.</li>
   * </ul>
   * <b>Deliberately NOT the same set as {@link #NON_INCREMENTAL_COMPONENT_EXTENSIONS}: the dictionary is absent.</b>
   * A dictionary that ARRIVES has to be adopted by the full load, but a dictionary merely written into needs
   * nothing here - {@code TransactionManager.applyChanges} reloads it itself - and every DDL entry that adds a type
   * or a property name writes dictionary pages. Refusing on those would send the common case straight back to the
   * O(total files) rebuild issue #6988 removed, which is the cost this whole method exists to avoid.
   */
  private static final Set<String> NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS = Set.of(//
      LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, //
      LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, //
      LSMTreeIndexBloomFilter.FILE_EXT);

  final               IndexFactory                           indexFactory                  = new IndexFactory();
  /**
   * The logical schema graph. A REFERENCE and no longer a fixed map, because a load publishes a whole new graph at
   * one instant rather than tearing this one down and refilling it in place (issue #7961).
   * <p>
   * Read through {@link #typeMap()} by everything that can run while a load is in flight - which includes the load
   * itself, and {@link LocalDocumentType}/{@link TypeBuilder}, whose writes reach the graph the schema rebuild is
   * assembling rather than the one still being served.
   */
  /**
   * The published schema state: the logical type graph and the two bucket-id maps derived from it, in ONE
   * immutable holder behind ONE volatile field (PR #8001 review).
   * <p>
   * The three used to be separate fields assigned one after another, which left two problems a reader could hit.
   * A reader could see the new graph through {@code getType()} while {@code getTypeByBucketId()} still answered
   * from the previous maps - two generations in one query - and the maps were plain fields, so the contents of the
   * {@code HashMap}s a load built were not safely published at all. One volatile write of one holder settles both:
   * a reader sees all three of the previous generation or all three of the new one, and everything reachable from
   * the holder is published with it.
   */
  private volatile    SchemaState                            published                     = SchemaState.empty();

  /**
   * One generation of the schema's published state. A record, so nothing in it can be swapped out from under a
   * reader that has taken the reference: the maps are replaced wholesale by a load, never edited in place by one.
   * <p>
   * The type map itself stays mutable - ordinary DDL adds and removes types through {@link #typeMap()} without
   * going near a load - so what this makes immutable is WHICH maps a generation consists of, not their contents.
   */
  private record SchemaState(Map<String, LocalDocumentType> types, Map<Integer, LocalDocumentType> bucketId2TypeMap,
                             Map<Integer, LocalDocumentType> bucketId2InvolvedTypeMap) {
    private static SchemaState empty() {
      return new SchemaState(new ConcurrentHashMap<>(), new HashMap<>(), new HashMap<>());
    }
  }

  /**
   * The type graph a load is assembling, reachable only by {@link #stagingThread} until it is published (issue
   * #7961).
   * <p>
   * #7213 staged the by-NAME component maps; this is the other half of the same window. {@code readConfiguration()}
   * used to open with {@code types.clear()} and rebuild the {@link LocalDocumentType} objects in place, and
   * {@code LocalDocumentType.addIndexInternal()} binds an index into its type WHILE that runs - which is by
   * construction before the {@code onAfterSchemaLoad()} pass. So a reader resolving an index through its TYPE
   * rather than by name - {@code getType(t).getAllIndexes()}, {@code getPolymorphicIndexByProperties()}, which is
   * what SQL query planning uses to pick an index for a {@code WHERE} clause - could still obtain an
   * {@code LSMVectorIndex} whose vectors had not been loaded: a search that silently finds nothing. The same reader
   * could also see a type whose properties or buckets were only half restored, on every {@code load()} and every
   * {@code loadIncremental()}.
   * <p>
   * Held as a separate map and swapped in whole, rather than merged the way the component maps are: a load REBUILDS
   * the graph from {@code schema.json}, so a type the new one does not carry is a type that must be gone, which a
   * merge cannot express.
   */
  private final       Map<String, LocalDocumentType>         stagedTypes                   = new ConcurrentHashMap<>();

  /**
   * The graph {@link #stagedTypes} is replacing, kept from the moment the staging window opens so the
   * {@code TimeSeries} types it holds can be closed once the new graph is published - never before, which is what
   * {@code readConfiguration()} used to do to types a concurrent reader could still be holding.
   */
  private             Map<String, LocalDocumentType>         supersededTypes;

  /**
   * The bucket-id maps a load has derived from {@link #stagedTypes}, held back with it (PR #8001 review).
   * <p>
   * {@code readConfiguration()} rebuilds these from the graph it has just assembled, and it does so BEFORE the
   * barrier. Assigning them to the live fields there would let {@code getTypeByBucketId()} hand out a
   * new-generation type - carrying indexes whose {@code onAfterSchemaLoad()} has not run - while {@code getType()}
   * still answers with the previous graph, and would publish the involved-bucket map security reads through a
   * generation early. They are published in the same step as the graph they describe instead.
   */
  private             Map<Integer, LocalDocumentType>        stagedBucketId2TypeMap;
  private             Map<Integer, LocalDocumentType>        stagedBucketId2InvolvedTypeMap;

  /**
   * The graph {@link #commitStagedPublication()} published, so {@link #endStagedPublication()} can tell an abort
   * from a commit and close only what an abort leaves behind.
   */
  private             Map<String, LocalDocumentType>         publishedFromStaging;
  private             String                                 encoding                      = DEFAULT_ENCODING;
  private final       DatabaseInternal                       database;
  private final       SecurityManager                        security;
  private final       List<Component>                        files                         = Collections.synchronizedList(new ArrayList<>());
  // Concurrent for the same reason indexMap below is, and the reason is not symmetry: the bucket lookup maps are
  // written by the schema load and by DDL from arbitrary user threads while queries resolve bucket names on the
  // correctness path (LocalDocumentType.restoreExternalBuckets and ensureExternalBucketFor read it directly). A
  // plain HashMap made that publication rest on whichever file lock the two sides happened to share, and left a
  // concurrent getBuckets() free to throw ConcurrentModificationException at a reader that did nothing wrong
  // (issue #7213). Null keys and values never reach it: every put passes a component's own name, and the accessors
  // below (existsBucket, getBucketByName, getBucketByNameIfExists) null-guard the one name a caller supplies.
  final               Map<String, LocalBucket>               bucketMap                     = new ConcurrentHashMap<>();
  // Concurrent, not because the map is written often, but because it is written from threads that are not the ones
  // reading it and the reads are on the correctness path. DDL (CREATE/DROP INDEX) has always mutated it from
  // arbitrary user threads while queries resolve index names; since #6105 a compaction re-keys it too
  // ({@link #indexRenamed}), and an AUTOMATIC compaction runs on the async executor - a thread the writer whose
  // commit then has to see the new key never synchronizes with. Under a plain HashMap that publication rested on
  // whichever file lock the two sides happened to share, which is not a guarantee anyone should have to reconstruct.
  // Null keys and values never reach it, which is what makes the map type safe to change: the accessors below
  // (existsIndex, getIndexByName, dropIndexInternal, checkIndexIsNotBackingAConstraint) null-guard the one name a
  // caller supplies, and the three places that touch this field directly rather than through them pass a name that
  // cannot be null - LocalDocumentType uses the TypeIndex's own name, and ManualIndexBuilder.create() rejects a null
  // one up front, it being the only route by which a caller-supplied name reaches this map unmediated.
  protected final     Map<String, IndexInternal>             indexMap                      = new ConcurrentHashMap<>();

  /**
   * The index and bucket components an in-flight schema load has instantiated but whose {@code onAfterSchemaLoad()}
   * has not run yet (issue #7213).
   * <p>
   * Both {@link #load(ComponentFile.MODE, boolean)} and {@link #loadIncremental} have to make a component resolvable
   * BY NAME before {@link #readConfiguration()} runs, because that is how the logical schema binds an index to its
   * type and a type to its buckets; and {@code readConfiguration()} in turn has to run before the schema hooks,
   * because a hook reads what it set - {@code LSMVectorIndexMutable.onAfterSchemaLoad()} loads the index' vectors
   * only once {@code readConfiguration()} has set its dimensions. Publishing straight into {@link #indexMap} and
   * {@link #bucketMap} therefore left a window in which {@link #getIndexByName} answered with an index whose hook
   * had not run, which for a vector index is an index with no vectors loaded: a search that silently finds nothing.
   * <p>
   * These two maps hold the new components for the length of that window. Only the thread named by
   * {@link #stagingThread} ever reads them, so no other thread can reach a component through them, and
   * {@link #commitStagedPublication()} moves them into the live maps once every hook has run. Publication is atomic
   * PER NAME rather than for the map as a whole: a concurrent lookup resolves a name to the fully initialized new
   * component or to whatever the live map held before, never to one in between. What the live map held before
   * differs by path - {@link #loadIncremental} leaves the previous component in place, while
   * {@link #load(ComponentFile.MODE, boolean)} empties the maps up front and so answers "not found" for the
   * duration, which is what it answered before this barrier existed too (issue #7963).
   * <p>
   * The loading thread itself sees straight through the barrier: {@link #lookupIndex} and {@link #lookupBucket}
   * resolve its staged components first, so the schema rebuild resolves the components it has just built exactly as
   * it did when they went into the live maps directly. Every by-name accessor on this class goes through those two.
   */
  // Plain maps, and safely so although SUCCESSIVE loads run on different threads (a database open, then the Ratis
  // apply thread on a follower): the only writes to them happen between a successful compareAndSet on
  // stagingThread and the set(null) that releases it, so the release/acquire pair on that AtomicReference orders
  // one load's last write before the next load's first read. Nothing outside that window touches them - every
  // accessor reaches them only after isStagingPublication() has answered true, which only the owning thread gets.
  private final       Map<String, IndexInternal>             stagedIndexMap                = new HashMap<>();
  private final       Map<String, LocalBucket>               stagedBucketMap               = new HashMap<>();

  /**
   * The thread whose load owns {@link #stagedIndexMap}/{@link #stagedBucketMap}, or {@code null} when nothing is
   * staging. Read by threads other than the loading one - every staged-aware lookup tests it, and the
   * external-bucket restore in {@link LocalDocumentType} reaches one from DDL threads too - and claimed with a
   * {@code compareAndSet} rather than a plain write, so two loads arriving at the same instant cannot both decide
   * the window is free.
   */
  private final       AtomicReference<Thread>                stagingThread                 = new AtomicReference<>();
  protected final     Map<String, Trigger>                   triggers                      = new HashMap<>();
  protected final     Map<String, MaterializedViewImpl>     materializedViews             = new LinkedHashMap<>();
  protected final     Map<String, ContinuousAggregateImpl> continuousAggregates          = new LinkedHashMap<>();
  protected final     Map<String, JSONObject>               extensions                    = new LinkedHashMap<>();
  private final       Map<String, TriggerListenerAdapter> triggerAdapters = new HashMap<>();
  private final       String                                 databasePath;
  private final       File                                   configurationFile;
  private final       ComponentFactory                       componentFactory;
  private             Dictionary                             dictionary;
  private             String                                 dateFormat                    = GlobalConfiguration.DATE_FORMAT.getValueAsString();
  private             String                                 dateTimeFormat                = GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString();
  private             TimeZone                               timeZone                      = TimeZone.getDefault();
  private             ZoneId                                 zoneId                        = ZoneId.systemDefault();
  private             boolean                                readingFromFile               = false;
  private final       AtomicLong                             dirtyGeneration               = new AtomicLong(0);
  private volatile    long                                   savedGeneration               = 0;
  private             boolean                                loadInRamCompleted            = false;
  private             boolean                                multipleUpdate                = false;
  /**
   * Non-null while {@link #dropType} is dropping its own indexes as part of removing the type entirely (issue
   * #5646 review follow-up on PR #5946). Checked by {@link #dropIndexInternal} to skip the partition-suitability
   * report for a type that is going away in the same operation - at every nesting depth, including the bucket
   * sub-index drops {@link com.arcadedb.index.TypeIndex#drop()} makes back into the public {@link #dropIndex}, which
   * a parameter on {@code dropIndexInternal} alone cannot reach. A field rather than a parameter because the
   * suppression has to cross that public-API boundary; save/restore around the cascade, matching {@link #multipleUpdate}.
   */
  private             String                                 typeBeingDropped              = null;
  /** Nesting depth of {@link #recordFileChanges} frames. Read and written under the database write lock only. */
  private             int                                    recordingDepth                = 0;
  private final       AtomicLong                             versionSerial                 = new AtomicLong();
  private final       Map<String, FunctionLibraryDefinition> functionLibraries             = new ConcurrentHashMap<>();
  private final       Map<Integer, Integer>                  migratedFileIds               = new ConcurrentHashMap<>();
  /**
   * Logical index names whose {@link IndexInternal#getUpgradeWarning()} has already been logged. The schema is
   * re-read on every DDL and on every HA SCHEMA_ENTRY apply, and one logical index is N bucket sub-indexes, so
   * without this the advice would be repeated N times per reload forever.
   */
  private final       Set<String>                            reportedUpgradeWarnings       = ConcurrentHashMap.newKeySet();
  private              MaterializedViewScheduler              materializedViewScheduler;
  private              TimeSeriesMaintenanceScheduler         timeSeriesMaintenanceScheduler;

  public LocalSchema(final DatabaseInternal database, final String databasePath, final SecurityManager security) {
    this.database = database;
    this.databasePath = databasePath;
    this.security = security;

    componentFactory = new ComponentFactory(database);
    componentFactory.registerComponent(Dictionary.DICT_EXT, new Dictionary.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    componentFactory.registerComponent(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    componentFactory.registerComponent(LSMTreeIndexBloomFilter.FILE_EXT,
        new LSMTreeIndexBloomFilter.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(LSMVectorIndex.FILE_EXT, new LSMVectorIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(SparseSegmentComponent.FILE_EXT,
        new SparseSegmentComponent.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(TimeSeriesBucket.BUCKET_EXT, new TimeSeriesBucket.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(TimeSeriesTagDictionary.DICT_EXT,
        new TimeSeriesTagDictionary.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(HashIndexBucket.UNIQUE_INDEX_EXT,
        new HashIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(HashIndexBucket.NOTUNIQUE_INDEX_EXT,
        new HashIndex.PaginatedComponentFactoryHandlerNotUnique());
    // Note: LSMVectorIndexGraphFile is NOT registered here - it's a sub-component discovered by its parent LSMVectorIndex

    indexFactory.register(INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndex.LSMTreeIndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.FULL_TEXT.name(), new LSMTreeFullTextIndex.LSMTreeFullTextIndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.LSM_VECTOR.name(), new LSMVectorIndex.LSMVectorIndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.LSM_SPARSE_VECTOR.name(), new LSMSparseVectorIndex.LSMSparseVectorIndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.GEOSPATIAL.name(), new LSMTreeGeoIndex.GeoIndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.HASH.name(), new HashIndex.HashIndexFactoryHandler());
    configurationFile = new File(databasePath + File.separator + SCHEMA_FILE_NAME);
  }

  @Override
  public LocalSchema getEmbedded() {
    return this;
  }

  public void create(final ComponentFile.MODE mode) {
    loadInRamCompleted = true;
    database.begin();
    try {
      dictionary = new Dictionary(database, "dictionary", databasePath + File.separator + "dictionary", mode, Dictionary.DEF_PAGE_SIZE);
      files.add(dictionary);

      database.commit();

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
      database.rollback();
      throw new DatabaseMetadataException("Error on loading dictionary (error=" + e + ")", e);
    }
  }

  public void load(final ComponentFile.MODE mode, final boolean initialize) throws IOException {
    // Claim the staging window FIRST, before a single field is cleared. beginStagedPublication() refuses a load
    // that overlaps another, and a refusal has to leave the schema exactly as it found it: clearing first would
    // mean a refused load empties the live schema for everyone, including the load legitimately in flight on the
    // other thread - a worse outcome than the one the refusal exists to prevent.
    beginStagedPublication();
    try {
      files.clear();
      // types is NOT cleared: the graph a load replaces stays served, whole, until the new one is published at the
      // barrier below (issue #7961). The rebuild assembles its own in stagedTypes, which beginStagedPublication()
      // has just emptied.
      bucketMap.clear();
      indexMap.clear();
      dictionary = null;

      // Nothing this rebuild instantiates reaches the by-name lookup maps until every schema hook below has run
      // (issue #7213). The clears stay: a full rebuild drops every component instance, so the previous generation
      // cannot be kept alive as a stand-in the way loadIncremental keeps its untouched ones.
      SortedIndexBuildRecoveryMarker.recoverInterruptedBuilds(database, mode);

      final Collection<ComponentFile> filesToOpen = database.getFileManager().getFiles();

      // REGISTER THE DICTIONARY FIRST
      for (final ComponentFile file : filesToOpen) {
        if (file != null)
          if (Dictionary.DICT_EXT.equals(file.getFileExtension())) {
            dictionary = (Dictionary) componentFactory.createComponent(file, mode);
            registerFile(dictionary);
            // Only now can the dictionary write a missing header page: doing so commits a transaction
            // that has to resolve the dictionary's file id, which registerFile above has just made
            // resolvable. Relevant when the database was killed before the page reached disk.
            if (mode == ComponentFile.MODE.READ_WRITE)
              dictionary.createHeaderPageIfMissing();
            break;
          }
      }

      if (dictionary == null)
        throw new ConfigurationException("Dictionary file not found in database directory");

      for (final ComponentFile file : filesToOpen) {
        if (file != null && !Dictionary.DICT_EXT.equals(file.getFileExtension())) {
          final Component pf = componentFactory.createComponent(file, mode);

          if (pf != null)
            registerLoadedComponent(pf);
        }
      }

      if (initialize)
        initComponents();

      readConfiguration();

      final List<Component> snapshot;
      synchronized (files) {
        snapshot = new ArrayList<>(files);
      }
      for (final Component f : snapshot)
        if (f != null)
          f.onAfterSchemaLoad();

      // Every component is registered by now, which is what resolving a filter to its compacted index by name needs.
      attachBloomFilters(snapshot);

      if (mode == ComponentFile.MODE.READ_WRITE)
        sweepOrphanCompactedIndexFiles(snapshot);

      // Only here, with every hook run and every bloom filter attached, do the new components become reachable by
      // name. A lookup that arrives before this point gets "not found" - which is what it already got, since the
      // clears above emptied the maps - rather than an index that has not finished loading itself.
      commitStagedPublication();

      updateSecurity();
    } finally {
      endStagedPublication();
    }
  }

  /**
   * Registers a component instantiated by {@link ComponentFactory} into the by-name lookup map its main component
   * belongs to, and into the file-id array. Shared by the full {@link #load(ComponentFile.MODE, boolean)} and by
   * {@link #loadIncremental} so the two paths cannot drift apart on what "registered" means.
   */
  private void registerLoadedComponent(final Component component) {
    registerInLookupMaps(component);
    registerFile(component);
  }

  /**
   * Same as {@link #registerLoadedComponent} for a file id that ALREADY has a component: the new instance takes the
   * old one's slot in a single set, so a concurrent {@link #getFileById} never observes the slot empty.
   * <p>
   * The by-name registration goes through {@link #registerInLookupMaps}, which stages it while a load is in flight
   * (issue #7213), so {@link #getIndexByName} keeps answering with the component the previous load published until
   * every schema hook of this one has run. The file-id slot is still taken over immediately, because
   * {@link #readConfiguration()} and the load hooks resolve sibling components through it while the load runs; that
   * half of the window is tracked by issue #7962.
   */
  private void replaceLoadedComponent(final Component component) {
    registerInLookupMaps(component);

    synchronized (files) {
      files.set(component.getFileId(), component);
    }
  }

  private void registerInLookupMaps(final Component component) {
    final Object mainComponent = component.getMainComponent();
    final boolean staged = isStagingPublication();

    if (mainComponent instanceof LocalBucket bucket)
      (staged ? stagedBucketMap : bucketMap).put(component.getName(), bucket);
    else if (mainComponent instanceof IndexInternal internal)
      (staged ? stagedIndexMap : indexMap).put(component.getName(), internal);
  }

  /**
   * Whether THIS thread is the one running a load that is staging its components (issue #7213). Every staged-aware
   * accessor below tests this rather than merely "is a load running", so a component in {@link #stagedIndexMap} is
   * reachable only by the load that built it.
   */
  private boolean isStagingPublication() {
    return stagingThread.get() == Thread.currentThread();
  }

  /**
   * The factory that turns a {@link ComponentFile} into its {@link Component}. Package-visible so the regression
   * test for issue #7213 can register a handler whose {@code onAfterSchemaLoad()} blocks: holding a load inside the
   * window between publication and the schema hooks is the only way to observe that window from another thread.
   */
  ComponentFactory getComponentFactory() {
    return componentFactory;
  }

  /**
   * Opens the window in which a load's new components are held back from {@link #indexMap}/{@link #bucketMap}.
   * Paired with {@link #commitStagedPublication()} on the way out, and with {@link #endStagedPublication()} in a
   * {@code finally} so a load that throws leaves nothing staged behind.
   */
  private void beginStagedPublication() {
    // ONE load at a time per schema, and the refusal is loud on purpose. Two loads sharing these maps would have the
    // second clear the first one's staged components and take `stagingThread` from under it, so the first would
    // commit nothing and the schema would come up missing whatever it had staged - silently, on a database that
    // opened. Concurrent loads already corrupt each other through the `files.clear()` at the head of load(), so this
    // is not a new restriction; it is the first place that says so out loud rather than leaving the next caller to
    // find out from a schema that lost half its indexes.
    final Thread current = Thread.currentThread();
    if (!stagingThread.compareAndSet(null, current)) {
      // compareAndSet and not "read, test, write": the whole point is to refuse a load that arrives at the same
      // instant as another, and a check-then-act on a volatile field lets both of them pass the check.
      final Thread other = stagingThread.get();
      throw new IllegalStateException(
          "A schema load is already in flight on thread '" + (other != null ? other.getName() : "?") + "'"
              + (other == current ? " (this one)" : "") + ": loads of the same schema cannot overlap");
    }

    stagedIndexMap.clear();
    stagedBucketMap.clear();
    // The graph the load is about to assemble, and the one it is replacing (issue #7961). The superseded one is
    // remembered rather than dropped: its TimeSeries types own engines that have to be closed, and closing them
    // before the replacement is published would close them under readers still holding the old graph.
    stagedTypes.clear();
    supersededTypes = published.types();
  }

  /**
   * Publishes everything staged by this thread's load, which is what makes the new components reachable by name.
   * {@code putAll} on a {@link ConcurrentHashMap} is a sequence of single puts, so this is atomic per NAME and not
   * for the map as a whole - and per name is exactly the guarantee issue #7213 asks for: no lookup can resolve a
   * name to a component whose {@code onAfterSchemaLoad()} has not run.
   */
  private void commitStagedPublication() {
    if (!isStagingPublication())
      return;

    // Buckets first, and the order is not arbitrary: a published index names the bucket it is associated with, so
    // publishing indexes first would let a reader resolve an index by name a few instructions before the bucket it
    // points at is resolvable. Nothing points the other way.
    bucketMap.putAll(stagedBucketMap);
    indexMap.putAll(stagedIndexMap);

    // The type graph goes last and goes whole (issue #7961). A reader that resolves an index through its type
    // reaches it only from here on, by which point every component's onAfterSchemaLoad() has run - and it sees
    // either the previous graph entire or the new one entire, never a type mid-rebuild, because what changes is
    // one reference and not the contents of a map somebody may be walking.
    //
    // Last, and therefore AFTER the two maps above, which leaves a window where getIndexByName() answers with a
    // new-generation index while getType() still answers with the old-generation type. That window is harmless in
    // the one direction it could matter (PR #8001 review): a LocalDocumentType holds its own TypeIndex references
    // in indexesByProperties and never resolves an index through indexMap, so no reader that goes THROUGH a type
    // can observe the mismatch. The reverse order would not be harmless - it would publish a type graph pointing
    // at indexes whose names do not resolve yet - which is why this order and not the other one.
    final Map<String, LocalDocumentType> publishedTypes = new ConcurrentHashMap<>(stagedTypes);
    final Map<String, LocalDocumentType> superseded = supersededTypes;

    // ONE volatile write, carrying the graph and both maps derived from it. getType(), getTypeByBucketId() and
    // getInvolvedTypeByBucketId() therefore cannot be caught answering from two different generations, and the
    // maps the load built are safely published rather than handed over through a plain field (PR #8001 review).
    final SchemaState previous = published;
    published = new SchemaState(publishedTypes,
        stagedBucketId2TypeMap != null ? stagedBucketId2TypeMap : previous.bucketId2TypeMap(),
        stagedBucketId2InvolvedTypeMap != null ? stagedBucketId2InvolvedTypeMap : previous.bucketId2InvolvedTypeMap());
    publishedFromStaging = publishedTypes;

    // Only now, with nothing able to reach them through the schema any more. A TimeSeries type owns an engine with
    // open files; the rebuild has already opened a fresh one per type, so leaving these behind would leak them.
    // Never the types the new graph carries: a rebuild that reused an instance would otherwise close the live one.
    if (superseded != null && !superseded.isEmpty()) {
      // The survivors as an identity SET, built once. containsValue() is itself a scan, so asking it per superseded
      // type made this O(superseded x published) - and a follower rebuilds its schema once per applied entry, on
      // schemas that reach four figures of types (issue #6982 reported 1209). By identity because what must not be
      // closed is the very INSTANCE the new graph is serving, whatever it calls itself (PR #8001 review).
      final Set<LocalDocumentType> survivors = Collections.newSetFromMap(new IdentityHashMap<>(publishedTypes.size()));
      survivors.addAll(publishedTypes.values());

      for (final LocalDocumentType type : superseded.values())
        if (type instanceof final LocalTimeSeriesType tsType && !survivors.contains(tsType)) {
          try {
            tsType.close();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Error closing TimeSeries type '%s' superseded by a schema reload: %s", null, tsType.getName(),
                e.getMessage());
          }
        }
    }

    endStagedPublication();
  }

  /**
   * Closes the window without publishing. Idempotent, and a no-op on any thread that is not the staging one, so it
   * is safe in the {@code finally} that follows {@link #commitStagedPublication()}.
   */
  private void endStagedPublication() {
    if (!isStagingPublication())
      return;

    stagedIndexMap.clear();
    stagedBucketMap.clear();
    // Only when the graph was NOT published: after a successful commit these very instances are the live ones
    // (PR #8001 review). A load that dies after readConfiguration() has initialised a TimeSeries engine per type
    // would otherwise leak one engine, and its file handles, per failed reload.
    if (published.types() != publishedFromStaging)
      closeTimeSeriesTypesOf(stagedTypes);
    publishedFromStaging = null;
    stagedTypes.clear();
    stagedBucketId2TypeMap = null;
    stagedBucketId2InvolvedTypeMap = null;
    supersededTypes = null;
    stagingThread.set(null);
  }


  /**
   * The index registered under {@code name} AS THIS THREAD SEES IT: a load in flight sees what it has staged, every
   * other thread sees only what is published. Every by-name index accessor resolves through this, which is what
   * makes the barrier invisible to the load itself - the schema rebuild resolves the components it has just built,
   * exactly as it did when they went straight into {@link #indexMap} - while a concurrent reader cannot reach one
   * whose {@code onAfterSchemaLoad()} has not run (issue #7213).
   */
  IndexInternal lookupIndex(final String name) {
    if (name == null)
      return null;

    if (isStagingPublication()) {
      final IndexInternal staged = stagedIndexMap.get(name);
      if (staged != null)
        return staged;
    }
    return indexMap.get(name);
  }

  /**
   * The type graph AS THIS THREAD SEES IT: the one a load in flight is assembling for that load's own thread, the
   * published one for everybody else (issue #7961).
   * <p>
   * Every read AND every write of the graph goes through this, which is what makes the barrier invisible to the
   * load - the rebuild resolves and mutates the types it has just built, exactly as it did when they went straight
   * into the live map - while a concurrent reader keeps seeing the previous graph, whole, until the new one is
   * published in one reference swap.
   * <p>
   * Package-visible because {@link LocalDocumentType} and {@link TypeBuilder} reach the graph directly, and two of
   * those reaches happen DURING a load: {@code setAliases} registers a type's aliases as it is restored, and
   * {@code addSuperType} resolves the parents the rebuild wires up.
   */
  Map<String, LocalDocumentType> typeMap() {
    return isStagingPublication() ? stagedTypes : published.types();
  }

  /** Closes the TimeSeries engines of a graph that is about to be discarded. */
  private void closeTimeSeriesTypesOf(final Map<String, LocalDocumentType> graph) {
    for (final DocumentType type : graph.values())
      if (type instanceof final LocalTimeSeriesType tsType)
        try {
          tsType.close();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Error closing TimeSeries type '%s' during schema reload: %s", null, tsType.getName(), e.getMessage());
        }
  }

  /**
   * Bucket counterpart of {@link #lookupIndex}. Package-visible because {@link LocalDocumentType} resolves bucket
   * names directly while {@link #readConfiguration()} rebuilds it - {@code restoreExternalBuckets} and
   * {@code ensureExternalBucketFor} - and a load that staged its buckets would otherwise be told they do not exist
   * and create a second one under the same name.
   */
  LocalBucket lookupBucket(final String name) {
    if (name == null)
      return null;

    if (isStagingPublication()) {
      final LocalBucket staged = stagedBucketMap.get(name);
      if (staged != null)
        return staged;
    }
    return bucketMap.get(name);
  }

  /**
   * Registers a bucket by name, staged while a load is in flight. The one caller that is not the component
   * registration itself is {@link #createBucket}, which the schema rebuild reaches through
   * {@code LocalDocumentType.ensureExternalBucketFor} when a type's paired external-property bucket is missing.
   */
  private void publishBucketDuringLoad(final String name, final LocalBucket bucket) {
    if (isStagingPublication())
      stagedBucketMap.put(name, bucket);
    else
      bucketMap.put(name, bucket);
  }

  /**
   * Every bucket this thread can see, published plus this thread's staged ones. Same rule as
   * {@link #indexesDuringLoad()}.
   */
  private Collection<LocalBucket> bucketsDuringLoad() {
    if (!isStagingPublication() || stagedBucketMap.isEmpty())
      return bucketMap.values();

    final Map<String, LocalBucket> merged = new LinkedHashMap<>(bucketMap);
    merged.putAll(stagedBucketMap);
    return merged.values();
  }

  /**
   * Registers an index by name on behalf of the schema-rebuilding code, staged while a load is in flight. Three
   * kinds of registration come through here, and all three are part of rebuilding the logical schema rather than of
   * serving a reader:
   * <ul>
   *   <li>the decorating index types {@link #readConfiguration()} mints - full-text, geospatial, sparse vector -
   *       taking over the name of the plain LSM index the load registered;</li>
   *   <li>the {@link com.arcadedb.index.TypeIndex} wrapper {@link LocalDocumentType#addIndexInternal} mints, which
   *       is the name a user's {@code SELECT} resolves ({@code MyType[myProperty]}) and therefore the one that made
   *       issue #7213 observable at all: it wraps the bucket-level index whose schema hook has not run;</li>
   *   <li>the bucket-level index {@code createBucketIndex} builds, which {@link #readConfiguration()} can reach
   *       through {@code LocalDocumentType.addBucketInternal} - that propagates the type's existing indexes onto a
   *       bucket it is binding, and the load calls it for every bucket of every type it restores.</li>
   * </ul>
   */
  void publishIndexDuringLoad(final String name, final IndexInternal index) {
    if (isStagingPublication())
      stagedIndexMap.put(name, index);
    else
      indexMap.put(name, index);
  }

  /**
   * Withdraws a name the schema-rebuilding code registered. It has to reach the live map even mid-load: the one
   * caller is {@link LocalDocumentType#addIndexInternal} dislodging a {@code TypeIndex} wrapper that a previous
   * drop left behind invalid, and leaving that entry published would hand a reader an index that answers
   * {@code isValid() == false}.
   */
  void removeIndexDuringLoad(final String name) {
    if (isStagingPublication())
      stagedIndexMap.remove(name);

    indexMap.remove(name);
  }

  /**
   * Bucket counterpart of {@link #removeIndexDuringLoad}, and the same reason: a name withdrawn while a load is
   * staging has to leave the staged map too, or the commit would publish it after the drop.
   */
  private void removeBucketDuringLoad(final String name) {
    if (isStagingPublication())
      stagedBucketMap.remove(name);

    bucketMap.remove(name);
  }

  /**
   * Every index the schema-rebuilding code can see: the published ones plus this thread's staged ones, staged
   * winning on a name they share. {@link #readConfiguration()}'s orphan-relinking pass walks this, and on a full
   * load every index in the database is staged, so walking {@link #indexMap} alone would walk nothing.
   */
  Collection<IndexInternal> indexesDuringLoad() {
    if (!isStagingPublication() || stagedIndexMap.isEmpty())
      return indexMap.values();

    final Map<String, IndexInternal> merged = new LinkedHashMap<>(indexMap);
    merged.putAll(stagedIndexMap);
    return merged.values();
  }

  /**
   * Incremental counterpart of {@link #load(ComponentFile.MODE, boolean)} for the HA follower apply path (issue
   * #6988). {@code load()} is a from-scratch rebuild: it drops every {@code Component} instance and re-instantiates
   * one per file in the database, reading page 0 of each. Running it once per committed DDL entry - which is what
   * {@code ArcadeStateMachine.applySchemaEntry} used to do - makes a schema build cost O(entries x total files),
   * i.e. quadratic in the number of types, all of it on the single Ratis apply thread (a 1209-type schema took
   * about 2h53m to replicate in issue #6982).
   * <p>
   * This method instead instantiates a component only for the files that do not have one yet, plus a replacement for
   * the already-registered INDEX components this entry wrote pages into ({@code touchedFileIds} - that is what
   * re-reads an LSM mutable index' page 0 for its key types, sub-index pointer and mutable page count), and then
   * refreshes the logical schema from {@code schema.json} through the very same {@link #readConfiguration()} the full
   * load runs. Every other component instance is left untouched, so the cost is O(changed files) rather than
   * O(total files) - the file walk itself stays O(total files) but touches no page and allocates no component.
   * <p>
   * WHY THE SET OF NEW COMPONENTS IS DERIVED FROM THE FILE MANAGER and not from the entry's {@code filesToAdd}. A
   * schema change too large for one Raft entry is split (see {@code RaftTransactionBroker.splitSchemaEntry}): the
   * leading chunks carry {@code filesToAdd} and NO schema JSON, and their apply deliberately skips the refresh
   * entirely (issue #5443), so their files sit in the {@link com.arcadedb.engine.FileManager} with no component. Only
   * the last chunk publishes the schema, and its own {@code filesToAdd} does not name them. Registering just that
   * chunk's files would leave the index unregistered exactly when {@link #readConfiguration()} looks for it, and that
   * is not a transient miss: the unresolvable index reference is dropped from the in-memory schema and SAVED, so the
   * follower loses the index permanently (the #4083 self-heal path). Asking the file manager what has no component
   * yet converges on precisely the set {@code load()} would have registered, whatever produced the files.
   * <p>
   * The logical refresh is deliberately NOT made incremental here: the leader ships the whole schema JSON in every
   * entry, so parsing it is O(schema size) no matter what this method does. Removing that term is the separate
   * "delta schema shipping" change.
   *
   * @param mode           open mode for the newly instantiated components
   * @param removedFileIds file ids retired by this entry; any removal forces the full rebuild (see below)
   * @param touchedFileIds file ids this entry wrote pages into; an already-registered index component among them is
   *                       rebuilt from its file rather than refreshed in place (see the second pass), and one
   *                       carrying a {@link #NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS} extension forces the
   *                       full rebuild instead. May be {@code null}
   *
   * @return {@code true} when the schema was refreshed incrementally, {@code false} when the caller must fall back
   * to {@link #load(ComponentFile.MODE, boolean)}. When {@code false} is returned nothing has been modified.
   */
  public boolean loadIncremental(final ComponentFile.MODE mode, final Collection<Integer> removedFileIds,
      final Collection<Integer> touchedFileIds) throws IOException {

    // Nothing was ever loaded in this lifecycle, so there is no baseline to add to.
    if (dictionary == null || !loadInRamCompleted)
      return false;

    // A retired file leaves a stale entry behind in bucketMap/indexMap (removeFile() only clears the file-id array),
    // and the mutable index that superseded it has to re-read its page 0 anyway. Both are what the full rebuild is
    // for, and both are the ordering the comments on issues #4743 and #5443 in applySchemaEntry pin down.
    if (removedFileIds != null && !removedFileIds.isEmpty())
      return false;

    // FIRST PASS decides, without modifying anything, so a refusal leaves the caller's fallback a consistent state.

    // Files the file manager holds that no component is registered for yet.
    final List<ComponentFile> toInstantiate = new ArrayList<>();
    for (final ComponentFile file : database.getFileManager().getFiles()) {
      if (file == null || getFileByIdIfExists(file.getFileId()) != null)
        continue;

      if (NON_INCREMENTAL_COMPONENT_EXTENSIONS.contains(file.getFileExtension()))
        return false;

      toInstantiate.add(file);
    }

    // Files this entry wrote pages into whose component is already registered AND caches on-disk state its load
    // hooks re-derive. Only index components do, and only three classes override a hook at all:
    // LSMTreeIndexMutable.onAfterLoad re-reads page 0 (key types, sub-index file id, mutable page count),
    // HashIndexBucket re-reads its metadata in BOTH hooks, and LSMVectorIndexMutable overrides
    // onAfterSchemaLoad ONLY, to load its vectors once readConfiguration has set its dimensions. Every other
    // component inherits the no-ops on Component, so writing pages into a bucket - or into the dictionary, which
    // TransactionManager.applyChanges reloads on its own - needs nothing here.
    final List<ComponentFile> toReplace = new ArrayList<>();
    if (touchedFileIds != null)
      for (final Integer fileId : touchedFileIds) {
        final Component current = getFileByIdIfExists(fileId);
        if (current == null)
          continue;

        if (!database.getFileManager().existsFile(fileId))
          return false;

        final ComponentFile file = database.getFileManager().getFile(fileId);

        // Issue #7266: the extension check comes BEFORE the instanceof narrowing below, and not after it as it
        // used to. Neither component this set names answers an IndexInternal from getMainComponent() by its own
        // construction - a bloom filter answers ITSELF, and a compacted index answers the mutable index only once
        // that mutable's onAfterLoad() wired the field, which a factory-built instance starts with null - so the
        // narrowing dropped a touched bloom filter out of the loop before the guard could refuse the entry, and
        // left the compacted index refusing by accident rather than by rule.
        if (NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS.contains(file.getFileExtension()))
          return false;

        if (!(current.getMainComponent() instanceof IndexInternal))
          continue;

        toReplace.add(file);
      }

    // SECOND PASS instantiates. A touched component is REPLACED by a freshly built instance rather than having its
    // load hooks re-run on it: those hooks write plain, non-volatile fields (an LSM mutable index' keyTypes,
    // binaryKeyTypes, storageKeyTypes, subIndex) that readers reach WITHOUT the index lock through
    // LSMTreeIndex.getKeyTypes()/getBinaryKeyTypes()/convertKeys(). Their safety rests on publish-once,
    // mutate-never-after - which is exactly why LSMTreeIndexAbstract#splitIndex() builds a new instance and swaps
    // the volatile reference instead of updating the old one in place. Re-running the hooks on an instance a
    // follower's query threads are already reading could let one observe a torn combination of those fields. This
    // way the component's construction is finished before anything can reach it, and the swap is the single
    // synchronized set in replaceLoadedComponent - which is also what the full load() would have produced for
    // that file.
    final List<Component> loaded = new ArrayList<>(toInstantiate.size() + toReplace.size());

    // What to undo in the file-id array if this load dies before its commit. replaceLoadedComponent() and
    // registerLoadedComponent() take those slots immediately - the load hooks and readConfiguration() resolve
    // sibling components through them while the load runs - so unlike the staged name maps they are not rolled back
    // by simply dropping them. Leaving them would hand a file-id lookup a half-built component while
    // getIndexByName() still answered with the previous, fully built one.
    final Map<Integer, Component> replacedSlots = new HashMap<>();
    final List<Integer> addedSlots = new ArrayList<>();
    boolean committed = false;

    // Nothing instantiated below reaches the by-name lookup maps until every schema hook has run (issue #7213).
    // On this path that is a stronger guarantee than on the full load: every index this entry did not touch keeps
    // its published instance throughout, and a REPLACED index keeps answering with the instance the previous load
    // published until its replacement has finished loading itself.
    beginStagedPublication();
    try {
      for (final ComponentFile file : toInstantiate) {
        final Component component = componentFactory.createComponent(file, mode);
        if (component == null)
          continue;

        registerLoadedComponent(component);
        addedSlots.add(component.getFileId());
        loaded.add(component);
      }

      for (final ComponentFile file : toReplace) {
        final Component component = componentFactory.createComponent(file, mode);
        if (component == null)
          continue;

        final Component previous = getFileByIdIfExists(component.getFileId());
        replaceLoadedComponent(component);
        replacedSlots.put(component.getFileId(), previous);
        loaded.add(component);
      }

      // Same ordering as load(): every load hook runs BEFORE readConfiguration(), because the logical schema binds
      // to what the hooks published (an index' key types, a hash index' metadata)...
      for (final Component component : loaded)
        component.onAfterLoad();

      readConfiguration();

      // ...and every schema hook runs AFTER it, because those read what readConfiguration() just set on the index
      // metadata (a vector index loads its vectors only once its dimensions are known).
      for (final Component component : loaded)
        component.onAfterSchemaLoad();

      commitStagedPublication();
      committed = true;

      // attachBloomFilters() is not called: it acts only on LSMTreeIndexCompacted, and a compacted index can never
      // be in `loaded` - the entry is refused instead, by NON_INCREMENTAL_COMPONENT_EXTENSIONS in the first pass and
      // by NON_INCREMENTAL_TOUCHED_COMPONENT_EXTENSIONS in the second. Relaxing either set means restoring the call.
      //
      // sweepOrphanCompactedIndexFiles() is deliberately NOT run here either. It proves a compacted file is an
      // orphan by observing that no mutable index claimed it during the load - a proof that only holds when EVERY
      // mutable index was re-instantiated in the same pass. On this path most of them were not, so the sweep would
      // drop live files. An orphan left behind is reclaimed by the next full load (a restart, or any entry that
      // falls back).

      updateSecurity();

      return true;
    } finally {
      if (!committed)
        rollbackFileSlots(replacedSlots, addedSlots);

      endStagedPublication();
    }
  }

  /**
   * Puts the file-id array back the way an aborted {@link #loadIncremental} found it. The array is written directly
   * rather than through {@link #removeFile}: this runs while an exception is on its way out, and {@code removeFile}
   * would also rewrite the migrated-file map and touch the transaction, neither of which this load changed.
   */
  private void rollbackFileSlots(final Map<Integer, Component> replacedSlots, final List<Integer> addedSlots) {
    synchronized (files) {
      for (final Map.Entry<Integer, Component> slot : replacedSlots.entrySet())
        if (slot.getKey() < files.size())
          files.set(slot.getKey(), slot.getValue());

      for (final Integer fileId : addedSlots)
        if (fileId < files.size())
          files.set(fileId, null);
    }
  }

  /**
   * Links every {@code .bfidx} bloom filter component (#5517) to the compacted index it was written for, matching it
   * by name. Only a compacted index the load claimed gets one: an orphan is about to be dropped anyway, and an
   * unattached filter is inert, since nothing but its compacted index ever probes it.
   */
  private void attachBloomFilters(final List<Component> snapshot) {
    for (final Component component : snapshot)
      if (component instanceof LSMTreeIndexCompacted compacted && compacted.getMainIndex() != null)
        compacted.attachBloomFilter();
  }

  /**
   * Drops compacted index files that no mutable index claimed during the load, together with the bloom filter files
   * that describe them. A crash between a
   * compaction's publication (schema saved, the mutable header already pointing at its CURRENT compacted
   * file) and the physical drop of a replaced or aborted compacted file leaves the stale file on disk; the
   * directory scan re-registers it at the next open, but nothing references it anymore, leaking its space
   * forever. Every legitimate compacted component gets its mainIndex set by the owning mutable index while
   * reading its header (SUB-INDEX FILE ID), so a compacted component still unclaimed after the whole schema
   * load is provably an orphan.
   */
  private void sweepOrphanCompactedIndexFiles(final List<Component> snapshot) {
    for (final Component component : snapshot) {
      if (component instanceof LSMTreeIndexBloomFilter filter) {
        // A filter is owned by exactly one compacted index; without it nothing can ever read the file again.
        final Component owner = filter.getOwnerName() != null ? getFileByName(filter.getOwnerName()) : null;
        if (!(owner instanceof LSMTreeIndexCompacted compactedOwner) || compactedOwner.getMainIndex() == null) {
          LogManager.instance().log(this, Level.INFO,
              "Dropping orphan index bloom filter file '%s' (fileId=%d)", null, filter.getName(), filter.getFileId());
          filter.dropQuietly();
        }
        continue;
      }

      if (!(component instanceof LSMTreeIndexCompacted compacted) || compacted.getMainIndex() != null)
        continue;

      LogManager.instance().log(this, Level.INFO,
          "Dropping orphan compacted index file '%s' (fileId=%d) left behind by an interrupted compaction", null,
          compacted.getName(), compacted.getFileId());
      try {
        database.getPageManager().deleteFile(database, compacted.getFileId());
        database.getFileManager().dropFile(compacted.getFileId());
        removeFile(compacted.getFileId());
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.WARNING, "Error on dropping orphan compacted index file '%s'", e, compacted.getName());
      }
    }
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public void setTimeZone(final TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  @Override
  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(final ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  @Override
  public String getDateFormat() {
    return dateFormat;
  }

  @Override
  public void setDateFormat(final String dateFormat) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS);
    this.dateFormat = dateFormat;
  }

  @Override
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  @Override
  public void setDateTimeFormat(final String dateTimeFormat) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_DATABASE_SETTINGS);
    this.dateTimeFormat = dateTimeFormat;
  }

  @Override
  public JSONObject getExtension(final String name) {
    final JSONObject ext = extensions.get(name);
    return ext != null ? ext.copy() : null;
  }

  @Override
  public void setExtension(final String name, final JSONObject value) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    if (value == null)
      extensions.remove(name);
    else
      extensions.put(name, value);
    saveConfiguration();
  }

  @Override
  public Component getFileById(final int id) {
    synchronized (files) {
      if (id >= files.size())
        throw new SchemaException("File with id '" + id + "' was not found");

      final Component p = files.get(id);
      if (p == null)
        throw new SchemaException("File with id '" + id + "' was not found");
      return p;
    }
  }

  @Override
  public Component getFileByIdIfExists(final int id) {
    synchronized (files) {
      if (id >= files.size())
        return null;

      return files.get(id);
    }
  }

  public Component getFileByName(final String name) {
    synchronized (files) {
      for (final Component f : files)
        if (f != null && name.equals(f.getName()))
          return f;
      return null;
    }
  }

  public void removeFile(final int fileId) {
    synchronized (files) {
      if (fileId >= files.size())
        return;

      files.set(fileId, null);
    }

    final Integer replacementFileId = migratedFileIds.get(fileId);
    for (final Map.Entry<Integer, Integer> migration : migratedFileIds.entrySet())
      if (migration.getValue() == fileId) {
        if (replacementFileId != null)
          migratedFileIds.replace(migration.getKey(), fileId, replacementFileId);
        else
          migratedFileIds.remove(migration.getKey(), fileId);
      }

    database.getTransaction().removeFile(fileId);
  }

  @Override
  public Collection<? extends Bucket> getBuckets() {
    return Collections.unmodifiableCollection(bucketsDuringLoad());
  }

  public boolean existsBucket(final String bucketName) {
    // Null-guarded because bucketMap is a ConcurrentHashMap, which rejects a null key with an NPE where the previous
    // HashMap simply answered "absent". Callers pass a name straight from SQL, so keep the old answer.
    return lookupBucket(bucketName) != null;
  }

  /**
   * {@inheritDoc}
   *
   * @throws SchemaException if no bucket is registered under that name. Callers that want to handle the missing case
   *                         must use {@link #getBucketByNameIfExists(String)}; a {@code null} check after this call is
   *                         unreachable.
   */
  @Override
  public Bucket getBucketByName(final String name) {
    // Same null guard as existsBucket, for the same reason.
    final Bucket p = lookupBucket(name);
    if (p == null)
      throw new SchemaException("Bucket with name '" + name + "' was not found");
    return p;
  }

  @Override
  public Bucket getBucketByNameIfExists(final String name) {
    // Same null guard as existsBucket, for the same reason.
    return lookupBucket(name);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SchemaException if the id is out of range or the component it maps to is not a bucket. Callers that want to
   *                         handle the missing case must use {@link #getBucketByIdIfExists(int)}; a {@code null} check
   *                         after this call is unreachable.
   */
  @Override
  public LocalBucket getBucketById(final int id) {
    return getBucketById(id, true);
  }

  @Override
  public LocalBucket getBucketByIdIfExists(final int id) {
    return getBucketById(id, false);
  }

  public LocalBucket getBucketById(final int id, final boolean throwExceptionIfNotFound) {
    synchronized (files) {
      if (id < 0 || id >= files.size())
        if (throwExceptionIfNotFound)
          throw new SchemaException("Bucket with id '" + id + "' was not found");
        else
          return null;

      final Component p = files.get(id);
      if (!(p instanceof LocalBucket)) {
        if (throwExceptionIfNotFound)
          throw new SchemaException("Bucket with id '" + id + "' was not found");
        else
          return null;
      }
      return (LocalBucket) p;
    }
  }

  @Override
  public LocalBucket createBucket(final String bucketName) {
    return createBucket(bucketName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  public LocalBucket createBucket(final String bucketName, final int pageSize) {
    return createBucket(bucketName, pageSize, databasePath, LocalBucket.CURRENT_VERSION);
  }

  /** Creates the bucket file under {@code parentDirectory} instead of the database directory; null/empty falls back. */
  public LocalBucket createBucket(final String bucketName, final int pageSize, final String parentDirectory) {
    return createBucket(bucketName, pageSize, parentDirectory, LocalBucket.CURRENT_VERSION);
  }

  /**
   * Full overload: creates a bucket with an explicit file-format version. Paired external-property buckets pass
   * {@link LocalBucket#EXTERNAL_BUCKET_VERSION} so they get the smaller (256-slot) page-slot table appropriate for
   * heavy payloads; everything else uses {@link LocalBucket#CURRENT_VERSION}.
   */
  public LocalBucket createBucket(final String bucketName, final int pageSize, final String parentDirectory, final int version) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    checkValidBucketName(bucketName);

    if (lookupBucket(bucketName) != null)
      throw new SchemaException("Cannot create bucket '" + bucketName + "' because already exists");

    // Discoverability warning for the EXTERNAL property naming convention. The engine creates paired buckets
    // as '<primary>_ext' with file-format version EXTERNAL_BUCKET_VERSION, so version == CURRENT_VERSION (0)
    // here means a user-driven CREATE BUCKET. A user bucket named '*_ext' will collide if a primary bucket
    // with the matching prefix later gains an EXTERNAL property; ensureExternalBucketFor() rejects with a
    // SchemaException at that point. Surfacing the constraint at create time is much cheaper than debugging
    // the later failure.
    if (version == LocalBucket.CURRENT_VERSION && InternalBucketNaming.looksLikeAnExternalPropertyBucketName(bucketName))
      LogManager.instance().log(this, Level.WARNING,
          """
          Bucket name '%s' ends with '_ext'. The engine reserves the '<primaryName>_ext' suffix for paired\
           EXTERNAL-property buckets. If a primary bucket whose name + '_ext' equals this name later\
           gains an EXTERNAL property, that property change will fail with a SchemaException. Consider\
           renaming this bucket to avoid the collision.""",
          null, bucketName);

    final String dir = parentDirectory == null || parentDirectory.isEmpty() ? databasePath : parentDirectory;

    return recordFileChanges(() -> {
      try {
        final File parent = new File(dir);
        if (!parent.exists() && !parent.mkdirs())
          throw new SchemaException("Cannot create directory '" + dir + "' for bucket '" + bucketName + "'");
        final LocalBucket bucket = new LocalBucket(database, bucketName, dir + File.separator + bucketName,
            ComponentFile.MODE.READ_WRITE, pageSize, version);
        registerFile((Component) bucket);
        publishBucketDuringLoad(bucketName, bucket);

        return bucket;

      } catch (final IOException e) {
        throw new SchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
      }
    });
  }

  /**
   * A bucket name becomes the last path segment of its component file, so it must address a file inside the
   * database directory and nothing else. A type name reaches the same place already percent-encoded by
   * {@link FileUtils#encode} (which escapes both separators), so a bucket created directly is the only unencoded
   * route in.
   * <p>
   * Dots inside the name are deliberately allowed: the component-file name is parsed right-to-left, peeling the
   * fixed {@code .fileId.pageSize.vVersion.ext} tail, so a dot in the name survives the round trip. Only a name
   * that is exactly "." or ".." references a directory.
   * <p>
   * The remaining checks reject the rest of the NTFS/Windows-illegal character set and the reserved device stems
   * ({@code CON}, {@code PRN}, {@code AUX}, {@code NUL}, {@code COM1}-{@code COM9}, {@code LPT1}-{@code LPT9}).
   * Nothing here is a directory-escape concern, since a bucket name is never used as anything but the last path
   * segment: it is error quality on Windows, turning a raw {@code IOException} out of {@code Files.move}/file
   * creation into a {@code SchemaException} that names the offending character or stem at validation time.
   */
  public static void checkValidBucketName(final String bucketName) {
    if (bucketName == null || bucketName.isEmpty())
      throw new SchemaException("Invalid bucket name '" + bucketName + "'");

    if (bucketName.indexOf('/') > -1 || bucketName.indexOf('\\') > -1 || ".".equals(bucketName) || "..".equals(bucketName))
      throw new SchemaException("Invalid bucket name '" + bucketName + "': it cannot reference a path");

    // '*' is left untouched by URLEncoder and is not a legal file name character on Windows.
    if (bucketName.indexOf('*') > -1)
      throw new SchemaException("Invalid bucket name '" + bucketName + "': it cannot contain '*'");

    for (int i = 0; i < bucketName.length(); ++i) {
      final char c = bucketName.charAt(i);
      if (c < 0x20 || WINDOWS_ILLEGAL_CHARS.indexOf(c) > -1) {
        // A control character is not printable, so render it as \\uXXXX to keep the exception message and any
        // log it lands in readable.
        final String printableChar = c < 0x20 ? String.format("\\u%04x", (int) c) : String.valueOf(c);
        throw new SchemaException(
            "Invalid bucket name '" + bucketName + "': it cannot contain the character '" + printableChar + "' (illegal on Windows)");
      }
    }

    final int firstDot = bucketName.indexOf('.');
    final String stem = firstDot > -1 ? bucketName.substring(0, firstDot) : bucketName;
    if (WINDOWS_RESERVED_NAMES.contains(stem.toUpperCase(Locale.ROOT)))
      throw new SchemaException("Invalid bucket name '" + bucketName + "': '" + stem + "' is a reserved device name on Windows");
  }

  /**
   * Re-bases a component name built as {@code <encodedTypeName><suffix>} onto a new type name, keeping the suffix
   * verbatim. The suffix carries the bucket index, the {@code _out_edges}/{@code _in_edges} marker and the index
   * timestamp, so it must not be re-derived by searching for a delimiter: both '_' and '.' are legal inside a type
   * name and any such search picks the wrong one.
   * <p>
   * Returns {@code null} when the component name is not derived from the old type name, which is the case for a
   * bucket created on its own and attached with {@link DocumentType#addBucket} (and for anything named after such a
   * bucket). A name that was never built from the type name must not follow it when the type is renamed, so the
   * caller leaves that component untouched.
   * <p>
   * The result needs no further validation: it is {@link FileUtils#encode}d, and the suffix it carries came from a
   * name that was validated by {@link #checkValidBucketName} when its bucket was created.
   */
  public static String rebaseComponentName(final String componentName, final String oldTypeName, final String newTypeName,
      final String encoding) {
    final String oldPrefix = FileUtils.encode(oldTypeName, encoding);

    // Every derived name is '<encodedTypeName>_<something>': '_<index>' for a bucket, plus '_out_edges'/'_in_edges'
    // or '_<timestamp>' for what hangs off it. Requiring the '_' is what separates a derived name from an attached
    // bucket that merely happens to start with the type name, e.g. "OrderArchive" on type "Order": a bare prefix
    // match would rebase that one too, which is the same infer-the-boundary-from-content mistake this method exists
    // to remove.
    if (componentName.length() <= oldPrefix.length() || !componentName.startsWith(oldPrefix)
        || componentName.charAt(oldPrefix.length()) != '_')
      return null;

    return FileUtils.encode(newTypeName, encoding) + componentName.substring(oldPrefix.length());
  }

  public String getEncoding() {
    return encoding;
  }

  @Override
  public void setEncoding(final String encoding) {
    this.encoding = encoding;
  }

  /**
   * Creates {@code newTypeName} as a copy of {@code typeName}: its properties, its records, and the definitions of the
   * indexes it declares itself.
   * <p>
   * <b>Not atomic, by construction.</b> The records commit first - in batches of {@code transactionBatchSize}, so a
   * large type does not hold one transaction open - and only then are the indexes built, each in its own transaction.
   * The index build has to run outside the record-copy transaction to see the records at all (see the comment at that
   * call site), which is what puts a commit boundary in the middle of the operation. The safety net for a failure on
   * either side of it is the {@code catch} below: it drops {@code newTypeName}, and with it the buckets and records
   * already committed, so a failed copy leaves the schema as it found it rather than a half-built type. The SOURCE type
   * is only ever read, so it is unaffected either way.
   * <p>
   * What the safety net cannot cover is a hard CRASH inside that same window - after the records commit, before or
   * during the index build - because nothing runs to drop the copy. What survives is a populated type whose indexes are
   * empty or partial, which reads as a working type that silently answers nothing to an indexed query, and
   * {@code CHECK DATABASE} does not report it: index checking here is STRUCTURAL ({@code IndexInternal.checkIntegrity}
   * walks the key order of the index's own pages), so an index that is merely missing entries looks healthy. The repair
   * is {@code REBUILD INDEX <name>} on the copy's indexes, or simply dropping the copy and running {@code copyType()}
   * again; both are cheap next to making the operation atomic, which would mean holding every copied record in one
   * transaction (issue #5742).
   *
   * @param typeName             type to copy from, left untouched
   * @param newTypeName          type to create, which must not exist yet
   * @param newTypeClass         {@link LocalDocumentType} or {@link LocalVertexType}; edge types are not supported
   * @param buckets              number of buckets of the new type
   * @param pageSize             page size of the new type's buckets, not of its indexes - those keep the page size of
   *                             the index they are copied from
   * @param transactionBatchSize records to copy per transaction, or 0 to copy them all in one
   */
  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newTypeClass,
      final int buckets, final int pageSize, final int transactionBatchSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (existsType(newTypeName))
      throw new IllegalArgumentException("Type '" + newTypeName + "' already exists");

    final DocumentType oldType = getType(typeName);

    DocumentType newType = null;
    try {
      // CREATE THE NEW TYPE
      if (newTypeClass == LocalVertexType.class)
        newType = buildVertexType().withName(newTypeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
      else if (newTypeClass == LocalEdgeType.class)
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");
      else if (newTypeClass == LocalDocumentType.class)
        newType = buildDocumentType().withName(newTypeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
      else
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");

      // COPY PROPERTIES
      for (final String propName : oldType.getPropertyNames()) {
        final Property prop = oldType.getProperty(propName);
        newType.createProperty(propName, prop.getType(), prop.getOfType());
      }

      // COPY ALL THE RECORDS
      long copied = 0;
      database.begin();
      try {
        for (final Iterator<Record> iter = database.iterateType(typeName, false); iter.hasNext(); ) {

          final Document record = (Document) iter.next();

          final MutableDocument newRecord;
          if (newType instanceof LocalVertexType)
            newRecord = database.newVertex(newTypeName);
          else
            newRecord = database.newDocument(newTypeName);

          newRecord.fromMap(record.propertiesAsMap());
          newRecord.save();

          ++copied;

          if (transactionBatchSize > 0 && copied % transactionBatchSize == 0) {
            database.commit();
            database.begin();
          }
        }

        database.commit();

      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      // COPY INDEXES. Deliberately outside the record-copy transaction: each index build opens its own transaction
      // (TypeIndexBuilder.create wraps every bucket's build in a database.transaction(..., joinCurrent=false, ...)) and
      // has to both see the copied records and commit its own entries. Run from inside an enclosing transaction it did
      // neither, so every index on the copy came out EMPTY - a copy whose indexed queries silently answer nothing.
      for (final Index index : oldType.getAllIndexes(false))
        copyIndexDefinition((IndexInternal) index, newTypeName);

    } catch (final Exception e) {
      // "copying", not "renaming": nothing here renames anything, and the source type is still there afterwards. The
      // old wording is why issue #5723 was filed against a renameType() that does not exist - a rename goes through
      // LocalDocumentType.rename(), which renames buckets in place and never recreates an index.
      LogManager.instance().log(this, Level.SEVERE, "Error on copying type '%s' into '%s'", e, typeName, newTypeName);

      if (newType != null)
        try {
          dropType(newTypeName);
        } catch (final Exception e2) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on dropping temporary type '%s' created during copyType() operation from type '%s'",
                  e2, newTypeName, typeName);
        }

      throw e;
    }

    return newType;
  }

  /**
   * Recreates one index of the source type on the copy, carrying the WHOLE definition over rather than only the index
   * type, the uniqueness flag and the property list.
   * <p>
   * Everything else used to be silently replaced by a default (issue #5723): the page size deliberately tuned at
   * creation, the null strategy, the collations that make an index case-insensitive, and the type-specific
   * configuration - a full-text index's analyzers and BM25 parameters, a geospatial index's resolution, a vector
   * index's dimensions and similarity, without which the copy is not merely differently tuned but unusable.
   * <p>
   * The one attribute deliberately NOT carried over is a user-supplied index name: it is unique across the schema, so
   * reusing it would collide with the index still held by the source type. The copy takes the auto-derived
   * {@code newTypeName[properties]} form instead.
   */
  private void copyIndexDefinition(final IndexInternal index, final String newTypeName) {
    final List<String> propertyNames = index.getPropertyNames();
    final String[] properties = propertyNames.toArray(new String[propertyNames.size()]);

    // withType() may swap the builder for a type-specific subclass (TypeFullTextIndexBuilder, TypeLSMVectorIndexBuilder,
    // ...), so call it first and keep the returned reference instead of chaining off the original.
    final TypeIndexBuilder builder = buildTypeIndex(newTypeName, properties).withType(index.getType());
    builder.withUnique(index.isUnique());
    // getPageSizeForNewFile(), not getPageSize(): the definition goes back through the validating creation path, and a
    // HASH index predating #5713 can hold a page size that path refuses - which must not make copyType() fail.
    builder.withPageSize(index.getPageSizeForNewFile());
    builder.withNullStrategy(index.getNullStrategy());

    final IndexMetadata sourceMetadata = index.getMetadataForNewFile();
    if (sourceMetadata != null) {
      // bucketId -1: the per-bucket builder binds each sub-index during create().
      final IndexMetadata metadata = sourceMetadata.copy(newTypeName, properties, -1);
      metadata.typeIndexName = null;
      builder.withMetadata(metadata);
    }

    builder.create();
  }

  @Override
  public boolean existsIndex(final String indexName) {
    // Null-guarded because indexMap is a ConcurrentHashMap, which rejects a null key with an NPE where the previous
    // HashMap simply answered "absent". Callers pass a name straight from SQL, so keep the old answer.
    return lookupIndex(indexName) != null;
  }

  @Override
  public Index[] getIndexes() {
    final Collection<IndexInternal> visible = indexesDuringLoad();
    final Index[] indexes = new Index[visible.size()];
    int i = 0;
    for (final Index index : visible)
      indexes[i++] = index;
    return indexes;
  }

  @Override
  public void dropIndex(final String indexName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    checkIndexIsNotBackingAConstraint(indexName);

    dropIndexInternal(indexName);
  }

  /**
   * Refuses to drop the index that materialises an edge type's {@code UNIQUE} declaration.
   * <p>
   * The type flag is the source of truth for the constraint, so dropping its index directly would leave the schema
   * advertising a guarantee that nothing enforces. Withdraw the declaration instead - {@code ALTER TYPE <name> WITH
   * unique = false} - which drops the flag and this index together.
   */
  private void checkIndexIsNotBackingAConstraint(final String indexName) {
    final IndexInternal index = lookupIndex(indexName);
    if (index == null || index.getTypeName() == null || !existsType(index.getTypeName()))
      return;

    if (getType(index.getTypeName()) instanceof LocalEdgeType edgeType && edgeType.isUnique()
        && indexName.equals(LocalEdgeType.uniqueIndexName(edgeType.getName())))
      throw new SchemaException("Cannot drop index '" + indexName + "' because it enforces the UNIQUE declaration of "
          + "edge type '" + edgeType.getName() + "'. Use ALTER TYPE " + edgeType.getName()
          + " WITH unique = false to withdraw the constraint and drop this index");
  }

  private void dropIndexInternal(final String indexName) {
    recordFileChanges(() -> {
      boolean setMultipleUpdate = !multipleUpdate;
      if (!multipleUpdate)
        multipleUpdate = true;

      try {
        final IndexInternal index = lookupIndex(indexName);
        if (index == null)
          return null;

        final LocalDocumentType affectedType =
            index.getTypeName() != null && existsType(index.getTypeName()) ? getType(index.getTypeName()) : null;

        if (affectedType != null) {
          final BucketSelectionStrategy strategy = affectedType.getBucketSelectionStrategy();
          if (strategy instanceof PartitionedBucketSelectionStrategy selectionStrategy) {
            if (List.of(selectionStrategy.getProperties()).equals(index.getPropertyNames()))
              // CURRENT INDEX WAS USED FOR PARTITION, SETTING DEFAULT STRATEGY
              affectedType.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());
          }
        }

        try {
          database.executeLockingFiles(index.getFileIds(), () -> {
            final TypeIndex parentTypeIndex = index.getTypeIndex();
            if (parentTypeIndex != null)
              parentTypeIndex.removeIndexOnBucket(index);

            index.drop();
            // Staging-aware: createBucketIndex()'s failure rollback reaches this method for an index it registered
            // through publishIndexDuringLoad(), and readConfiguration() can reach createBucketIndex() by way of
            // LocalDocumentType.addBucketInternal(). A plain indexMap.remove() would be a no-op against a staged
            // entry and commitStagedPublication() would then publish the dropped index as live.
            removeIndexDuringLoad(indexName);

            if (index.getTypeName() != null) {
              final LocalDocumentType type = getType(index.getTypeName());
              if (index instanceof TypeIndex typeIndex)
                type.removeTypeIndexInternal(typeIndex);
              else {
                type.removeBucketIndexInternal(index);
                // A TypeIndex with no remaining bucket children must not stay in indexesByProperties:
                // schema serialization (toJSON) calls TypeIndex.getPropertyNames(), which fails on an
                // empty wrapper.
                if (parentTypeIndex != null && parentTypeIndex.countIndexesOnBuckets() == 0) {
                  type.removeTypeIndexInternal(parentTypeIndex);
                  removeIndexDuringLoad(parentTypeIndex.getName());
                }
              }
            }
            return null;
          });

        } catch (final NeedRetryException e) {
          throw e;
        } catch (final Exception e) {
          throw new SchemaException("Cannot drop the index '" + indexName + "' (error=" + e + ")", e);
        }

        // Symmetric with the CREATE INDEX side (TypeIndexBuilder, issue #5637): the index just dropped is half of
        // what decided whether the partition is any use, so losing the automatic unique index on the partition
        // properties can turn a suitable partition into one with none left to prune with. That used to go
        // unreported until the next open (issue #5646); reportPartitionSuitabilityAfterSchemaChange() defers to the
        // enclosing transaction's commit when one is active, so a DROP immediately followed by a re-CREATE
        // (recollating, or any other index-surface edit on the same type in one transaction) is diagnosed once
        // against the settled state rather than reporting the transient gap.
        if (affectedType != null && !affectedType.getName().equals(typeBeingDropped)) {
          try {
            affectedType.reportPartitionSuitabilityAfterSchemaChange();
          } catch (final RuntimeException e) {
            // By this point the index is already dropped and the schema updated, so letting a diagnostic fault
            // escape would fail a DROP INDEX that otherwise succeeded over nothing more than a reporting bug.
            LogManager.instance().log(this, Level.WARNING,
                "Cannot report the partition suitability of type '%s' after dropping index '%s'. The index itself "
                    + "was dropped successfully", e, affectedType.getName(), indexName);
          }
        }

        return null;

      } finally {
        if (setMultipleUpdate)
          multipleUpdate = false;
      }
    });
  }

  // TRIGGER MANAGEMENT

  @Override
  public boolean existsTrigger(final String triggerName) {
    return triggers.containsKey(triggerName);
  }

  @Override
  public Trigger getTrigger(final String triggerName) {
    return triggers.get(triggerName);
  }

  @Override
  public Trigger[] getTriggers() {
    return triggers.values().toArray(new Trigger[0]);
  }

  @Override
  public Trigger[] getTriggersForType(final String typeName) {
    return triggers.values().stream()
        .filter(t -> t.getTypeName().equals(typeName))
        .toArray(Trigger[]::new);
  }

  @Override
  public void createTrigger(final Trigger trigger) {
    // A JAVASCRIPT or JAVA trigger is arbitrary host code that fires with the engine's own privileges: the JS
    // executor binds the real database object into a GraalVM context (HostAccess.ALL minus reflection), so the
    // script can reach database.getSecurity().createUser(...) and mint a server-wide admin, and the JAVA executor
    // loads and runs an arbitrary class. Creating one therefore requires security-admin (UPDATE_SECURITY), not
    // merely UPDATE_SCHEMA - mirroring the DEFINE FUNCTION ... LANGUAGE js gate (GHSA-vwjc-v7x7-cm6g) and closing
    // the UPDATE_SCHEMA -> server-admin escalation (GHSA-38pf-6hp2-pxww). A declarative SQL trigger is not host
    // code and keeps the standard schema-level protection.
    if (trigger.getActionType() == Trigger.ActionType.JAVASCRIPT || trigger.getActionType() == Trigger.ActionType.JAVA)
      database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);
    else
      database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      // Validate trigger does not already exist
      if (triggers.containsKey(trigger.getName())) {
        throw new SchemaException("Trigger '" + trigger.getName() + "' already exists");
      }

      // Validate type exists
      if (!existsType(trigger.getTypeName())) {
        throw new SchemaException("Type '" + trigger.getTypeName() + "' does not exist");
      }

      // Store trigger
      triggers.put(trigger.getName(), trigger);

      // Register event listener
      registerTriggerListener(trigger);

      return null;
    });
  }

  @Override
  public void dropTrigger(final String triggerName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      final Trigger trigger = triggers.get(triggerName);
      if (trigger == null) {
        throw new SchemaException("Trigger '" + triggerName + "' does not exist");
      }

      // Unregister event listener
      unregisterTriggerListener(triggerName);

      // Remove trigger
      triggers.remove(triggerName);

      return null;
    });
  }

  // -- Materialized View management --

  @Override
  public synchronized boolean existsMaterializedView(final String viewName) {
    return materializedViews.containsKey(viewName);
  }

  @Override
  public synchronized MaterializedView getMaterializedView(final String viewName) {
    final MaterializedViewImpl view = materializedViews.get(viewName);
    if (view == null)
      throw new SchemaException("Materialized view '" + viewName + "' not found");
    return view;
  }

  @Override
  public synchronized MaterializedView[] getMaterializedViews() {
    return materializedViews.values().toArray(new MaterializedView[0]);
  }

  @Override
  public void dropMaterializedView(final String viewName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // #7457: THE MONITOR IS NEVER HELD ACROSS THE recordFileChanges CALL. That call waits for the database write lock,
    // and every schema save runs under that lock and takes this monitor (saveConfiguration is synchronized): a thread
    // holding the monitor while waiting for the write lock is the reverse order, and it deadlocks against any
    // concurrent DDL that is saving. The same shape is kept by alterMaterializedView and dropContinuousAggregate.
    synchronized (this) {
      if (!materializedViews.containsKey(viewName))
        throw new SchemaException("Materialized view '" + viewName + "' not found");
    }

    // Wrap in recordFileChanges so that the MV metadata removal and backing type
    // drop are replicated atomically to HA replicas. THE WHOLE LIFECYCLE TRANSITION - REMOVAL, SCHEDULER, LISTENERS,
    // BACKING TYPE - RUNS UNDER THE WRITE LOCK, SO IT CANNOT INTERLEAVE WITH A CREATE OR AN ALTER OF THE SAME VIEW
    // THAT IS STILL INSTALLING ITS REFRESH RESOURCES: WHAT IS TORN DOWN HERE IS WHAT THE VIEW REMOVED HERE OWNED
    recordFileChanges(() -> {
      final MaterializedViewImpl view;
      final MaterializedViewScheduler scheduler;
      synchronized (this) {
        // Two drops of the same view can both pass the check above: the second loses here
        view = materializedViews.remove(viewName);
        if (view == null)
          throw new SchemaException("Materialized view '" + viewName + "' not found");
        scheduler = materializedViewScheduler;
      }

      // Cancel periodic scheduler if active
      if (scheduler != null)
        scheduler.cancel(viewName);

      // Unregister incremental listeners from source types
      if (view.getRefreshMode() == MaterializedViewRefreshMode.INCREMENTAL)
        MaterializedViewBuilder.unregisterListeners(this, view);

      // Drop the backing type (which drops buckets and indexes)
      if (existsType(view.getBackingTypeName()))
        dropType(view.getBackingTypeName());

      saveConfiguration();
      return null;
    });
  }

  @Override
  public void alterMaterializedView(final String viewName, final MaterializedViewRefreshMode newMode,
      final long newIntervalMs) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // See dropMaterializedView for why the monitor is not held across recordFileChanges, and why the teardown of the
    // old refresh resources and the setup of the new ones both run inside it (#7457)
    synchronized (this) {
      if (!materializedViews.containsKey(viewName))
        throw new SchemaException("Materialized view '" + viewName + "' not found");
    }

    recordFileChanges(() -> {
      final MaterializedViewImpl oldView;
      final MaterializedViewImpl newView;
      final MaterializedViewScheduler scheduler;
      synchronized (this) {
        oldView = materializedViews.get(viewName);
        if (oldView == null)
          throw new SchemaException("Materialized view '" + viewName + "' not found");
        // Create new view instance with updated refresh mode
        newView = oldView.copyWithRefreshMode(newMode, newIntervalMs);
        materializedViews.put(viewName, newView);
        scheduler = materializedViewScheduler;
      }

      // Tear down old refresh infrastructure
      if (oldView.getRefreshMode() == MaterializedViewRefreshMode.INCREMENTAL)
        MaterializedViewBuilder.unregisterListeners(this, oldView);
      if (scheduler != null)
        scheduler.cancel(viewName);

      saveConfiguration();

      // Set up new refresh infrastructure
      if (newMode == MaterializedViewRefreshMode.INCREMENTAL)
        MaterializedViewBuilder.registerListeners(this, newView, newView.getSourceTypeNames());
      if (newMode == MaterializedViewRefreshMode.PERIODIC && newIntervalMs > 0)
        getMaterializedViewScheduler().schedule((DatabaseInternal) database, newView);

      return null;
    });
  }

  @Override
  public MaterializedViewBuilder buildMaterializedView() {
    return new MaterializedViewBuilder((DatabaseInternal) database);
  }

  // -- Continuous Aggregate management --

  @Override
  public synchronized boolean existsContinuousAggregate(final String name) {
    return continuousAggregates.containsKey(name);
  }

  @Override
  public synchronized ContinuousAggregate getContinuousAggregate(final String name) {
    final ContinuousAggregateImpl ca = continuousAggregates.get(name);
    if (ca == null)
      throw new SchemaException("Continuous aggregate '" + name + "' not found");
    return ca;
  }

  @Override
  public synchronized ContinuousAggregate[] getContinuousAggregates() {
    return continuousAggregates.values().toArray(new ContinuousAggregate[0]);
  }

  @Override
  public void dropContinuousAggregate(final String name) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // See dropMaterializedView for why the monitor is not held across recordFileChanges (#7457)
    final ContinuousAggregateImpl ca;
    synchronized (this) {
      ca = continuousAggregates.get(name);
      if (ca == null)
        throw new SchemaException("Continuous aggregate '" + name + "' not found");
    }

    recordFileChanges(() -> {
      synchronized (this) {
        if (continuousAggregates.remove(name) == null)
          throw new SchemaException("Continuous aggregate '" + name + "' not found");
      }

      if (existsType(ca.getBackingTypeName()))
        dropType(ca.getBackingTypeName());

      saveConfiguration();
      return null;
    });
  }

  @Override
  public ContinuousAggregateBuilder buildContinuousAggregate() {
    return new ContinuousAggregateBuilder((DatabaseInternal) database);
  }

  /**
   * Register a trigger as an event listener on the appropriate type.
   */
  private void registerTriggerListener(final Trigger trigger) {
    // typeMap(): triggers are restored from inside readConfiguration(), so during a load the type they bind to is
    // one of the graph being assembled, not one of the graph still being served (issue #7961).
    final LocalDocumentType type = typeMap().get(trigger.getTypeName());
    if (type == null) {
      throw new SchemaException("Type '" + trigger.getTypeName() + "' not found");
    }

    // Create executor
    final TriggerExecutor executor;
    if (trigger.getActionType() == Trigger.ActionType.SQL) {
      executor = new SQLTriggerExecutor(trigger.getName(), trigger.getActionCode());
    } else if (trigger.getActionType() == Trigger.ActionType.JAVASCRIPT) {
      executor = new ScriptTriggerExecutor(trigger.getName(), trigger.getActionCode());
    } else if (trigger.getActionType() == Trigger.ActionType.JAVA) {
      executor = new JavaClassTriggerExecutor(trigger.getName(), trigger.getActionCode());
    } else {
      throw new SchemaException("Unknown trigger action type: " + trigger.getActionType());
    }

    // Create adapter
    final TriggerListenerAdapter adapter =
        new TriggerListenerAdapter(database, trigger, executor);

    // Register listener based on timing and event
    final RecordEventsRegistry events = (RecordEventsRegistry) type.getEvents();
    switch (trigger.getTiming()) {
      case BEFORE -> {
        switch (trigger.getEvent()) {
          case CREATE -> events.registerListener((BeforeRecordCreateListener) adapter);
          case READ -> events.registerListener((BeforeRecordReadListener) adapter);
          case UPDATE -> events.registerListener((BeforeRecordUpdateListener) adapter);
          case DELETE -> events.registerListener((BeforeRecordDeleteListener) adapter);
        }
      }
      case AFTER -> {
        switch (trigger.getEvent()) {
          case CREATE -> events.registerListener((AfterRecordCreateListener) adapter);
          case READ -> events.registerListener((AfterRecordReadListener) adapter);
          case UPDATE -> events.registerListener((AfterRecordUpdateListener) adapter);
          case DELETE -> events.registerListener((AfterRecordDeleteListener) adapter);
        }
      }
    }

    // Store adapter for cleanup
    triggerAdapters.put(trigger.getName(), adapter);
  }

  /**
   * Unregister a trigger's event listener.
   */
  private void unregisterTriggerListener(final String triggerName) {
    final TriggerListenerAdapter adapter = triggerAdapters.get(triggerName);
    if (adapter == null) {
      return; // Already unregistered
    }

    final Trigger trigger = adapter.getTrigger();
    final LocalDocumentType type = typeMap().get(trigger.getTypeName());
    if (type != null) {
      final RecordEventsRegistry events = (RecordEventsRegistry) type.getEvents();

      // Unregister listener based on timing and event
      switch (trigger.getTiming()) {
        case BEFORE -> {
          switch (trigger.getEvent()) {
            case CREATE -> events.unregisterListener((BeforeRecordCreateListener) adapter);
            case READ -> events.unregisterListener((BeforeRecordReadListener) adapter);
            case UPDATE -> events.unregisterListener((BeforeRecordUpdateListener) adapter);
            case DELETE -> events.unregisterListener((BeforeRecordDeleteListener) adapter);
          }
        }
        case AFTER -> {
          switch (trigger.getEvent()) {
            case CREATE -> events.unregisterListener((AfterRecordCreateListener) adapter);
            case READ -> events.unregisterListener((AfterRecordReadListener) adapter);
            case UPDATE -> events.unregisterListener((AfterRecordUpdateListener) adapter);
            case DELETE -> events.unregisterListener((AfterRecordDeleteListener) adapter);
          }
        }
      }
    }

    // Cleanup executor resources
    adapter.cleanup();

    // Remove adapter
    triggerAdapters.remove(triggerName);
  }

  @Override
  public Index getIndexByName(final String indexName) {
    // Same null guard as existsIndex: a null name must still surface as "not found", not as the NPE a
    // ConcurrentHashMap raises on a null key.
    final Index p = lookupIndex(indexName);
    if (p == null)
      throw new SchemaException("Index with name '" + indexName + "' was not found");
    return p;
  }

  @Override
  public TypeIndexBuilder buildTypeIndex(final String typeName, final String[] propertyNames) {
    return new TypeIndexBuilder(database, typeName, propertyNames);
  }

  @Override
  public BucketIndexBuilder buildBucketIndex(final String typeName, final String bucketName, final String[] propertyNames) {
    return new BucketIndexBuilder(database, typeName, bucketName, propertyNames);
  }

  @Override
  public ManualIndexBuilder buildManualIndex(final String indexName, final Type[] keyTypes) {
    return new ManualIndexBuilder(database, indexName, keyTypes);
  }


  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withNullStrategy(nullStrategy).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public Index createBucketIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String bucketName,
      final String[] propertyNames, final int pageSize, final NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildBucketIndex(typeName, bucketName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).create();
  }

  @Override
  @Deprecated
  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final Type[] keyTypes,
      final int pageSize, final NULL_STRATEGY nullStrategy) {
    // withType is NOT optional here: the index factory resolves the handler by index type, so dropping the argument
    // this overload takes made every call through it fail with a NullPointerException (issue #5765).
    return buildManualIndex(indexName, keyTypes).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).create();
  }

  public void close() {
    // Save dirty configuration before clearing everything
    if (dirtyGeneration.get() > savedGeneration) {
      try {
        // Force save even if transaction is active - this is the last chance to save
        LogManager.instance().log(this, Level.INFO, "Saving dirty schema configuration before close");
        final long capturedGeneration = dirtyGeneration.get();
        versionSerial.incrementAndGet();
        update(toJSON());
        savedGeneration = capturedGeneration;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error saving schema configuration during close: %s", e,
            e.getMessage());
      }
    }

    if (materializedViewScheduler != null) {
      materializedViewScheduler.shutdown();
      materializedViewScheduler = null;
    }

    if (timeSeriesMaintenanceScheduler != null) {
      timeSeriesMaintenanceScheduler.shutdown();
      timeSeriesMaintenanceScheduler = null;
    }

    writeStatisticsFile();
    materializedViews.clear();
    continuousAggregates.clear();
    extensions.clear();
    files.clear();
    for (final DocumentType type : published.types().values()) {
      if (type instanceof LocalTimeSeriesType tsType)
        tsType.close();
    }
    published = SchemaState.empty();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;
  }

  public synchronized MaterializedViewScheduler getMaterializedViewScheduler() {
    if (materializedViewScheduler == null)
      materializedViewScheduler = new MaterializedViewScheduler(database.getName());
    return materializedViewScheduler;
  }

  public synchronized TimeSeriesMaintenanceScheduler getTimeSeriesMaintenanceScheduler() {
    if (timeSeriesMaintenanceScheduler == null)
      timeSeriesMaintenanceScheduler = new TimeSeriesMaintenanceScheduler();
    return timeSeriesMaintenanceScheduler;
  }

  private void readStatisticsFile() {
    try {
      boolean legacyFile = false;
      File file = new File(databasePath + File.separator + STATISTICS_FILE_NAME);
      if (!file.exists() || file.length() == 0) {
        // TRY LEGACY FILE (<v25.2.1)
        file = new File(databasePath + File.separator + CACHED_COUNT_FILE_NAME_LEGACY);
        if (!file.exists() || file.length() == 0)
          return;

        legacyFile = true;
      }

      final JSONObject json;
      try (final FileInputStream fis = new FileInputStream(file)) {
        final String fileContent = FileUtils.readStreamAsString(fis, encoding);
        json = new JSONObject(fileContent);
      }

      for (String key : json.keySet()) {
        final LocalBucket bucket = lookupBucket(key);
        if (bucket != null) {
          if (legacyFile) {
            bucket.setCachedRecordCount(json.getLong(key));
          } else {
            final JSONObject obj = json.getJSONObject(key);
            if (!obj.isNull("count"))
              bucket.setCachedRecordCount(obj.getLong("count"));
            if (!obj.isNull("pages"))
              bucket.setPageStatistics(obj.getJSONArray("pages"));
          }
        }
      }

    } catch (Throwable e) {
      LogManager.instance().log(this, Level.WARNING, "Error on reading cached count file", e);
    }
  }

  private void writeStatisticsFile() {
    final File directory = new File(databasePath);
    if (!directory.exists())
      // DATABASE DIRECTORY WAS DELETED
      return;

    try {
      final JSONObject json = new JSONObject();
      for (Map.Entry<String, LocalBucket> b : bucketMap.entrySet())
        json.put(b.getKey(), b.getValue().getStatistics());

      try (final FileWriter file = new FileWriter(new File(directory, STATISTICS_FILE_NAME))) {
        file.write(json.toString());
      }
    } catch (Throwable e) {
      LogManager.instance().log(this, Level.WARNING, "Error on saving statistics file", e);
    }
  }

  public Dictionary getDictionary() {
    return dictionary;
  }

  public Database getDatabase() {
    return database;
  }

  public Collection<DocumentType> getTypes() {
    // Use a LinkedHashSet to deduplicate: aliases map to the same DocumentType object in the types map,
    // so values() can contain the same instance multiple times
    return new ArrayList<>(new LinkedHashSet<>(typeMap().values()));
  }

  public LocalDocumentType getType(final String typeName) {
    final LocalDocumentType t = typeMap().get(typeName);
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public LocalDocumentType getTypeOrNull(final String typeName) {
    return typeMap().get(typeName);
  }

  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    final DocumentType type = getTypeByBucketId(bucketId);
    return type != null ? type.getName() : null;
  }

  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    return published.bucketId2TypeMap().get(bucketId);
  }

  @Override
  public DocumentType getInvolvedTypeByBucketId(final int bucketId) {
    return published.bucketId2InvolvedTypeMap().get(bucketId);
  }

  @Override
  public DocumentType getTypeByBucketName(final String bucketName) {
    return published.bucketId2TypeMap().get(getBucketByName(bucketName).getFileId());
  }

  public boolean existsType(final String typeName) {
    return typeMap().containsKey(typeName);
  }

  public void dropType(final String typeName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // Prevent dropping a type that is a backing type or source type for a materialized view or continuous aggregate
    synchronized (this) {
      for (final MaterializedViewImpl view : materializedViews.values()) {
        if (view.getBackingTypeName().equals(typeName))
          throw new SchemaException(
              "Cannot drop type '" + typeName + "' because it is the backing type for materialized view '" + view.getName() + "'. " +
                  "Drop the materialized view first with: DROP MATERIALIZED VIEW " + view.getName());
        if (view.getSourceTypeNames().contains(typeName))
          throw new SchemaException(
              "Cannot drop type '" + typeName + "' because it is a source type for materialized view '" + view.getName() + "'. " +
                  "Drop the materialized view first with: DROP MATERIALIZED VIEW " + view.getName());
      }
      for (final ContinuousAggregateImpl ca : continuousAggregates.values()) {
        if (ca.getBackingTypeName().equals(typeName))
          throw new SchemaException(
              "Cannot drop type '" + typeName + "' because it is the backing type for continuous aggregate '" + ca.getName() + "'. " +
                  "Drop the continuous aggregate first with: DROP CONTINUOUS AGGREGATE " + ca.getName());
        if (ca.getSourceTypeName().equals(typeName))
          throw new SchemaException(
              "Cannot drop type '" + typeName + "' because it is the source type for continuous aggregate '" + ca.getName() + "'. " +
                  "Drop the continuous aggregate first with: DROP CONTINUOUS AGGREGATE " + ca.getName());
      }
    }

    recordFileChanges(() -> {
      boolean setMultipleUpdate = !multipleUpdate;
      if (!multipleUpdate)
        multipleUpdate = true;

      final String previousTypeBeingDropped = typeBeingDropped;
      // Covers the whole cascade below, not just the index-drop loop: dropBucket() a few lines down can also
      // reach dropIndexInternal() for a leftover bucket-associated index, and the suppression must hold there too.
      typeBeingDropped = typeName;

      try {
        final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);

        // CHECK INHERITANCE TREE AND ATTACH SUB-TYPES DIRECTLY TO THE PARENT TYPE
        final List<LocalDocumentType> superTypes = new ArrayList<>(type.superTypes);
        for (final LocalDocumentType parent : superTypes)
          type.removeSuperType(parent);

        for (final LocalDocumentType sub : type.subTypes) {
          sub.superTypes.remove(type);
          for (final LocalDocumentType parent : superTypes)
            sub.addSuperType(parent, false);
        }

        // DELETE ALL ASSOCIATED INDEXES. typeBeingDropped (set above) makes dropIndexInternal() skip the
        // partition-suitability report it would otherwise trigger on this type - directly, and through the nested
        // calls TypeIndex.drop() makes back into dropIndex() for each bucket sub-index. Reporting here would
        // describe a type that no longer exists by the time anyone reads it (issue #5646 review follow-up on
        // PR #5946).
        for (final Index m : new ArrayList<>(type.getAllIndexes(true)))
          dropIndexInternal(m.getName());

        if (type instanceof LocalVertexType vertexType)
          // DELETE IN/OUT EDGE FILES
          database.getGraphEngine().dropVertexType(vertexType);

        // DELETE ALL ASSOCIATED BUCKETS
        final List<Bucket> buckets = new ArrayList<>(type.getBuckets(false));
        for (final Bucket b : buckets) {
          type.removeBucket(b);
          dropBucket(b.getName());
        }

        if (type instanceof LocalTimeSeriesType tsType)
          tsType.drop();

        if (typeMap().remove(typeName) == null)
          throw new SchemaException("Type '" + typeName + "' not found");
      } finally {
        typeBeingDropped = previousTypeBeingDropped;
        if (setMultipleUpdate)
          multipleUpdate = false;
        saveConfiguration();
        updateSecurity();
      }
      return null;
    });
  }

  @Override
  public void dropBucket(final String bucketName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    final Bucket bucket = getBucketByName(bucketName);

    recordFileChanges(() -> {
      boolean setMultipleUpdate = !multipleUpdate;
      if (!multipleUpdate)
        multipleUpdate = true;

      try {
        for (final LocalDocumentType type : typeMap().values()) {
          if (type.buckets.contains(bucket))
            throw new SchemaException(
                "Error on dropping bucket '" + bucketName + "' because it is assigned to type '" + type.getName()
                    + "'. Remove the association first");
        }

        // Drop the dependent sub-indexes BEFORE deleting the bucket file. This ordering matters for crash
        // consistency: these steps are not atomic, so if the process dies mid-drop the surviving on-disk state
        // must be recoverable. Deleting the bucket file first would leave an index file whose bucket is gone -
        // on reload that index cannot be relinked to any bucket, stays an orphan in indexMap with
        // associatedBucketId=-1, and breaks REBUILD INDEX * (getBucketById(-1)) as well as its own toJSON().
        // Dropping the indexes first leaves at worst a bucket with no index, which is fully recoverable with a
        // plain REBUILD/CREATE INDEX. Both directions still need schema.json saved (finally) to be complete.
        for (final Index idx : new ArrayList<>(indexMap.values())) {
          if (idx.getAssociatedBucketId() == bucket.getFileId())
            dropIndexInternal(idx.getName());
        }

        database.getPageManager().deleteFile(database, bucket.getFileId());
        try {
          database.getFileManager().dropFile(bucket.getFileId());
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on deleting bucket '%s'", e, bucketName);
        }
        removeFile(bucket.getFileId());

        removeBucketDuringLoad(bucketName);

        return null;

      } finally {
        if (setMultipleUpdate)
          multipleUpdate = false;
        saveConfiguration();
      }
    });
  }

  @Override
  public DocumentType createDocumentType(final String typeName) {
    return buildDocumentType().withName(typeName).create();
  }

  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets) {
    return buildDocumentType().withName(typeName).withBuckets(buckets).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    return buildDocumentType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true)
        .create();
  }

  @Override
  public TypeBuilder<LocalDocumentType> buildDocumentType() {
    return new TypeBuilder<>(database, LocalDocumentType.class);
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    return buildVertexType().withName(typeName).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances) {
    return buildVertexType().withName(typeName).withBuckets(bucketInstances).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets, final int pageSize) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances, final int pageSize) {
    return buildVertexType().withName(typeName).withBuckets(bucketInstances).withPageSize(pageSize).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    return buildVertexType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets, final int pageSize) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeBuilder<VertexType> buildVertexType() {
    return new TypeBuilder<>(database, VertexType.class);
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    return buildEdgeType().withName(typeName).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets) {
    return buildEdgeType().withName(typeName).withBuckets(buckets).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName) {
    return buildEdgeType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeBuilder<EdgeType> buildEdgeType() {
    return new TypeBuilder<>(database, EdgeType.class);
  }

  @Override
  public TimeSeriesTypeBuilder buildTimeSeriesType() {
    return new TimeSeriesTypeBuilder(database);
  }

  protected synchronized void readConfiguration() {
    // The graph this rebuild produces goes into the map typeMap() resolves to, which for a load in flight is the
    // staged one - so the published graph is neither emptied nor mutated here, and the TimeSeries types it holds
    // are closed by commitStagedPublication() once the replacement is live rather than before it exists (issue
    // #7961). A readConfiguration() outside a staging window still writes straight into the live map and is
    // responsible for its own tear-down, which is what the arm below does.
    //
    // That arm is unreachable today - both callers, load() and loadIncremental(), run inside a
    // beginStagedPublication()/endStagedPublication() bracket - and is kept for the same reason the setKeys
    // fallback in TransactionIndexContext.getIndexKeyLanes is: a third caller must not silently inherit the
    // staging assumption. It is NOT covered by a test, so anything relying on it needs to bring one.
    final Map<String, LocalDocumentType> graph = typeMap();
    if (graph == published.types())
      closeTimeSeriesTypesOf(graph);
    graph.clear();

    loadInRamCompleted = false;
    readingFromFile = true;

    boolean saveConfiguration = false;
    try {
      File file = new File(databasePath + File.separator + SCHEMA_FILE_NAME);
      final File prevFile = new File(databasePath + File.separator + SCHEMA_PREV_FILE_NAME);
      if (!file.exists() || file.length() == 0) {
        file = prevFile;
        if (!file.exists())
          return;

        LogManager.instance().log(this, Level.WARNING, "Could not find schema file, loading the previous version saved");
      }

      JSONObject root;
      try (final FileInputStream fis = new FileInputStream(file)) {
        final String fileContent = FileUtils.readStreamAsString(fis, encoding);
        root = new JSONObject(fileContent);
      } catch (final Exception e) {
        // The primary schema.json is non-empty but unparseable: this is the classic "server killed in the middle of a
        // schema save" corruption (issue #1249). Fall back to the previous good copy saved in schema.prev.json instead
        // of letting the schema reset to empty (which would make every type/index disappear even though the records are
        // still on disk). Self-heal by flagging a rewrite so the next save restores a valid schema.json.
        if (file != prevFile && prevFile.exists() && prevFile.length() > 0) {
          LogManager.instance().log(this, Level.WARNING,
              "Schema file '%s' is corrupt (%s), loading the previous version saved in '%s'", null, file.getName(),
              e.getMessage(), prevFile.getName());
          try (final FileInputStream fis = new FileInputStream(prevFile)) {
            root = new JSONObject(FileUtils.readStreamAsString(fis, encoding));
          }
          saveConfiguration = true;
        } else
          throw e;
      }

      if (root.names() == null || root.names().isEmpty())
        // EMPTY SCHEMA
        return;

      versionSerial.set(root.has("schemaVersion") ? root.getLong("schemaVersion") : 0L);

      final JSONObject settings = root.getJSONObject("settings");

      if (settings.has("timeZone")) {
        timeZone = TimeZone.getTimeZone(settings.getString("timeZone"));
        zoneId = timeZone.toZoneId();
      } else if (settings.has("zoneId")) {
        zoneId = ZoneId.of(settings.getString("zoneId"));
        timeZone = TimeZone.getTimeZone(zoneId);
      }

      dateFormat = settings.getString("dateFormat");
      dateTimeFormat = settings.getString("dateTimeFormat");

      final JSONObject types = root.getJSONObject("types");

      final Map<String, String[]> parentTypes = new HashMap<>();

      final Map<String, JSONObject> orphanIndexes = new HashMap<>();

      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);

        final LocalDocumentType type;

        final String kind = (String) schemaType.get("type");
        type = switch (kind) {
          case "v" -> new LocalVertexType(this, typeName);
          case "e" -> new LocalEdgeType(this, typeName,
              !schemaType.has("bidirectional") || schemaType.getBoolean("bidirectional"),
              schemaType.getBoolean("lightweight", false), schemaType.getBoolean("unique", false));
          case "d" -> new LocalDocumentType(this, typeName);
          case "t" -> {
            final LocalTimeSeriesType tsType = new LocalTimeSeriesType(this, typeName);
            tsType.fromJSON(schemaType);
            try {
              tsType.initEngine();
            } catch (final IOException e) {
              // Register the type anyway rather than letting it vanish from the schema (issue #6356): the
              // exception this catches means one derived file (a .ts.sealed most commonly, rebuildable under HA
              // by recompacting the replicated mutable pages) failed to open, not that the type or its mutable
              // data is gone. Registering it keeps the type VISIBLE - CHECK DATABASE already has a branch for
              // exactly this (DatabaseChecker#checkTimeSeries: "the storage engine is not initialised") that a
              // type missing from the schema map could never reach - and every read/write against it now fails
              // loudly through LocalTimeSeriesType#requireEngine() instead of the type silently reappearing empty
              // on the next write. Not registering it here is what issue #6356 reported: the database opened
              // cleanly with the type simply gone and nothing said why.
              tsType.markEngineUnavailable(e.getMessage());
              LogManager.instance().log(this, Level.SEVERE,
                  "Error initializing TimeSeries engine for type '%s', the type is registered but its storage is "
                      + "unavailable until this is resolved: %s", e, typeName, e.getMessage());
            }
            // Schedule automatic retention/downsampling if policies are defined. Kept OUTSIDE the try above and
            // behind its own catch: this can only run once the engine is actually available, and a scheduling
            // failure (the executor rejecting the task, e.g. mid-shutdown) is unrelated to whether the engine
            // itself works - it must not be mistaken for one and must not escape to the outer catch in this
            // method, which would abort every type the load has not reached yet for a reason that has nothing to
            // do with any of them.
            if (tsType.isEngineAvailable()) {
              try {
                getTimeSeriesMaintenanceScheduler().schedule(database, tsType);
              } catch (final RejectedExecutionException e) {
                LogManager.instance().log(this, Level.WARNING,
                    "Could not schedule automatic TimeSeries maintenance for type '%s': %s", e, typeName, e.getMessage());
              }
            }
            yield tsType;
          }
          case null, default -> throw new ConfigurationException("Type '" + kind + "' is not supported");
        };

        graph.put(typeName, type);

        final Set<String> aliases = !schemaType.isNull("aliases") ?
            new HashSet<>(schemaType.getJSONArray("aliases").toListOfStrings()) :
            Collections.emptySet();
        type.setAliases(aliases);

        final JSONArray schemaParent = schemaType.getJSONArray("parents");
        if (schemaParent != null) {
          // SAVE THE PARENT HIERARCHY FOR LATER
          final String[] parents = new String[schemaParent.length()];
          parentTypes.put(typeName, parents);
          for (int i = 0; i < schemaParent.length(); ++i)
            parents[i] = schemaParent.getString(i);
        }

        final JSONArray schemaBucket = schemaType.getJSONArray("buckets");
        if (schemaBucket != null) {
          for (int i = 0; i < schemaBucket.length(); ++i) {
            final Bucket bucket = lookupBucket(schemaBucket.getString(i));
            if (bucket == null) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot find bucket '%s' for type '%s', removing it from type configuration", null,
                      schemaBucket.getString(i), type);

              // GO BACK
              schemaBucket.remove(i);
              --i;

              saveConfiguration = true;
            } else
              type.addBucketInternal(bucket);
          }
        }

        // RESTORE THE primaryBucket -> externalBucket MAP BEFORE PROPERTIES ARE LOADED, SO THAT setExternal(true) ON A
        // PROPERTY DOES NOT TRY TO LAZY-CREATE BUCKETS THAT ALREADY EXIST. Always call restoreExternalBuckets
        // - even when the JSON has no externalBuckets key - so the name-based heuristic inside it can adopt
        // any orphan '<primary>_ext' files that exist on disk but were lost from the JSON (partial corruption,
        // migration from an older snapshot, etc.). Without that pass the affected buckets would default to
        // purpose=PRIMARY and our DML write guard would let users target them.
        final Map<String, String> primaryToExternal = new HashMap<>();
        if (schemaType.has("externalBuckets")) {
          final JSONObject extBuckets = schemaType.getJSONObject("externalBuckets");
          for (final String primaryName : extBuckets.keySet())
            primaryToExternal.put(primaryName, extBuckets.getString(primaryName));
        }
        type.restoreExternalBuckets(primaryToExternal);

        type.custom.clear();
        if (schemaType.has("custom"))
          type.custom.putAll(schemaType.getJSONObject("custom").toMap());
      }

      // CREATE THE PROPERTIES AFTER ALL THE TYPES HAVE BEEN CREATED TO FIND ALL THE REFERENCES LINKED WITH `TO`
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        final LocalDocumentType type = getType(typeName);
        if (schemaType.has("properties")) {
          final JSONObject schemaProperties = schemaType.getJSONObject("properties");
          if (schemaProperties != null) {
            for (final String propName : schemaProperties.keySet()) {
              final JSONObject prop = schemaProperties.getJSONObject(propName);
              type.createProperty(propName, prop);
            }
          }
        }
      }

      // RESTORE THE INHERITANCE
      for (final Map.Entry<String, String[]> entry : parentTypes.entrySet()) {
        final LocalDocumentType type = getType(entry.getKey());
        for (final String p : entry.getValue())
          type.addSuperType(getType(p), false);
      }

      // PARSE INDEXES. Warnings for indexes that are not yet present in {@code indexMap} are
      // deferred: the orphan-relinking pass below can match them by bucket prefix when index
      // files have been renamed (e.g. by LSM compaction). Logging upfront produces noisy
      // "Cannot find index" warnings for cases that are then silently relinked, which masks
      // the genuine cases where the file is truly missing (issue #4063).
      final Map<String, List<String>> deferredMissingIndexWarnings = new LinkedHashMap<>();
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        final JSONObject typeIndexesJSON = schemaType.getJSONObject("indexes");
        if (typeIndexesJSON != null) {
          final LocalDocumentType type = getType(typeName);

          final List<String> orderedIndexes = new ArrayList<>(typeIndexesJSON.keySet());
          orderedIndexes.sort(Comparator.naturalOrder());

          for (final String indexName : orderedIndexes) {
            final JSONObject indexJSON = typeIndexesJSON.getJSONObject(indexName);

            final JSONArray schemaIndexProperties = indexJSON.getJSONArray("properties");
            final String[] properties = new String[schemaIndexProperties.length()];
            for (int i = 0; i < properties.length; ++i)
              properties[i] = schemaIndexProperties.getString(i);

            IndexInternal index = lookupIndex(indexName);
            if (index != null) {
              index.setMetadata(indexJSON);
              // Apply the user-supplied TypeIndex name (issue #4139) here so it works for every
              // index implementation (LSM, Hash, FullText, Geo, Sparse/Dense Vector). Each
              // {@code setMetadata(JSONObject)} differs across classes and we do not want to
              // duplicate this read in all of them; addIndexInternal below consults the metadata.
              if (indexJSON.has("typeIndexName"))
                index.getMetadata().typeIndexName = indexJSON.getString("typeIndexName");

              if (indexJSON.has("type")) {
                final String configuredIndexType = indexJSON.getString("type");

                if (!index.getType().toString().equals(configuredIndexType)) {
                  if (configuredIndexType.equalsIgnoreCase(Schema.INDEX_TYPE.FULL_TEXT.toString())) {
                    // bucketId = -1 ("not set"): the bucket association is already established on the underlying index and read via
                    // its getAssociatedBucketId(); this metadata only carries the full-text/BM25 configuration, not the binding.
                    final FullTextIndexMetadata ftMeta = new FullTextIndexMetadata(typeName, properties, -1);
                    ftMeta.fromJSON(indexJSON);
                    // The bucket-level index JSON carries no "typeName" key, so fromJSON() above skipped the base-field
                    // read: take the collations and the manual TypeIndex name from the underlying definition, which
                    // setMetadata(indexJSON) has just populated. Without this the full-text metadata comes back from a
                    // restart missing both, and every site that carries the definition into a new index file through
                    // getMetadataForNewFile() loses them (issue #5742).
                    ftMeta.inheritCommonSettingsFrom(index.getMetadata());
                    // Same reserved-name guard as the creation path, in case a hand-edited/restored schema reintroduced a property
                    // colliding with the query parser's default-field sentinel.
                    LSMTreeFullTextIndex.checkReservedPropertyNames(ftMeta.propertyNames);
                    index = new LSMTreeFullTextIndex((LSMTreeIndex) index, ftMeta);
                    publishIndexDuringLoad(indexName, index);
                  } else if (configuredIndexType.equalsIgnoreCase(Schema.INDEX_TYPE.GEOSPATIAL.toString())) {
                    final int precision = indexJSON.getInt("precision", GeoIndexMetadata.DEFAULT_PRECISION);
                    // A definition with no tokenization field predates the FRONTIER layout (#5478), so its entries are
                    // the full ancestor chain: reading it as anything else would make put/remove miss them.
                    index = new LSMTreeGeoIndex((LSMTreeIndex) index, precision, GeoIndexMetadata.readTokenization(indexJSON));
                    publishIndexDuringLoad(indexName, index);
                  } else if (configuredIndexType.equalsIgnoreCase(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR.toString())) {
                    final LSMSparseVectorIndexMetadata sparseMeta = new LSMSparseVectorIndexMetadata(typeName, properties, -1);
                    sparseMeta.fromJSON(indexJSON);
                    // Same reason as the full-text branch above (issue #5742).
                    sparseMeta.inheritCommonSettingsFrom(index.getMetadata());
                    index = new LSMSparseVectorIndex((LSMTreeIndex) index, sparseMeta);
                    publishIndexDuringLoad(indexName, index);
                  } else {
                    orphanIndexes.put(indexName, indexJSON);
                    indexJSON.put("type", typeName);
                    LogManager.instance()
                        .log(this, Level.WARNING, "Index '%s' of type %s is different from definition %s. Ignoring it",//
                            index.getName(), index.getType(), configuredIndexType);
                    continue;
                  }
                }
              }

              final String bucketName = indexJSON.getString("bucket");
              final Bucket bucket = lookupBucket(bucketName);
              if (bucket == null) {
                orphanIndexes.put(indexName, indexJSON);
                indexJSON.put("type", typeName);
                LogManager.instance()
                    .log(this, Level.WARNING, "Cannot find bucket '%s' defined in index '%s'. Ignoring it", null, bucketName,
                        index.getName());
              } else {
                type.addIndexInternal(index, bucket.getFileId(), properties, null);
                reportUpgradeWarning(index, typeName, properties);
              }

            } else {
              orphanIndexes.put(indexName, indexJSON);
              indexJSON.put("type", typeName);
              deferredMissingIndexWarnings.computeIfAbsent(typeName, k -> new ArrayList<>()).add(indexName);
            }
          }
        }
      }

      // ASSOCIATE ORPHAN INDEXES. Bucket-prefix matching reattaches orphans to indexes already
      // present in {@code indexMap} under a different (renamed) name. When this succeeds, the
      // earlier "missing" entry must be dropped from {@code deferredMissingIndexWarnings} so we
      // do not emit a misleading warning at the end of this method.
      final Set<String> relinkedOrphanNames = new HashSet<>();
      boolean completed = false;
      while (!completed) {
        completed = true;
        for (final IndexInternal index : indexesDuringLoad()) {
          if (index.getTypeName() == null) {
            final String indexName = index.getName();

            // A type-less index is USUALLY a bucket sub-index whose schema entry could not be matched by name, and
            // those are always named "<bucketName>_<timestamp>". A MANUAL index is type-less too and is named by the
            // caller, so it can carry no underscore at all - and lastIndexOf then returned -1, making this substring
            // raise StringIndexOutOfBoundsException. That exception escaped into this method's own catch, reported as
            // "Error on loading schema. The schema will be reset". The types are already parsed by this point and
            // survive, which is what hid it: what a database merely CONTAINING such an index silently lost on every
            // open was everything the loader had not reached yet - the bucket selection strategies, the triggers, the
            // materialized views and continuous aggregates, the function libraries, the extensions, and the
            // compaction file-migration map WAL recovery redirects through (issue #5780). Nothing to relink here.
            final int pos = indexName.lastIndexOf("_");
            if (pos < 1)
              continue;

            final String bucketName = indexName.substring(0, pos);
            final Bucket bucket = lookupBucket(bucketName);
            if (bucket != null) {
              for (final Map.Entry<String, JSONObject> entry : orphanIndexes.entrySet()) {
                // Same guard as above, for the same reason: these keys are persisted schema entries, so a hand-edited
                // or restored schema.json is enough to put a name with no underscore here.
                final int pos2 = entry.getKey().lastIndexOf("_");
                if (pos2 < 1)
                  continue;

                final String bucketNameIndex = entry.getKey().substring(0, pos2);

                if (bucketName.equals(bucketNameIndex)) {
                  final LocalDocumentType type = graph.get(entry.getValue().getString("type"));
                  if (type != null) {
                    final JSONArray schemaIndexProperties = entry.getValue().getJSONArray("properties");

                    final String[] properties = new String[schemaIndexProperties.length()];
                    for (int i = 0; i < properties.length; ++i)
                      properties[i] = schemaIndexProperties.getString(i);

                    final NULL_STRATEGY nullStrategy = entry.getValue().has("nullStrategy") ?
                        NULL_STRATEGY.valueOf(entry.getValue().getString("nullStrategy")) :
                        NULL_STRATEGY.ERROR;

                    index.setNullStrategy(nullStrategy);
                    // Apply the persisted definition, exactly as the by-name path above does. An LSM-tree index
                    // carries its key types in its own file header and survives without this, but an index whose
                    // whole definition lives in schema.json - a vector index with its dimensions, similarity and
                    // quantization - comes up empty when its file is relinked under a new name after a compaction:
                    // with dimensions still 0 the component skips loading its vectors at the end of this load.
                    index.setMetadata(entry.getValue());
                    // Carry the manual TypeIndex name (issue #4139) onto the metadata for the
                    // orphan-relinking path too. addIndexInternal reads this when minting the
                    // TypeIndex; without this hop, an index file renamed by compaction loses its
                    // user-supplied name on the next reload.
                    if (entry.getValue().has("typeIndexName"))
                      index.getMetadata().typeIndexName = entry.getValue().getString("typeIndexName");
                    type.addIndexInternal(index, bucket.getFileId(), properties, null);
                    LogManager.instance()
                        .log(this, Level.FINE, "Relinked orphan index '%s' to type '%s'", null, indexName, type.getName());
                    relinkedOrphanNames.add(entry.getKey());
                    saveConfiguration = true;
                    completed = false;
                    break;
                  }
                }
              }

              if (!completed)
                break;
            }
          }
        }
      }

      // Emit warnings only for indexes that the orphan-relinking pass could not reattach. The
      // orphan reference is dropped from the in-memory schema (no addIndexInternal call), so the
      // next saveConfiguration() will rewrite schema.json without it - mark the schema dirty so
      // we self-heal: subsequent loads do not repeat the same warning forever (#4083 follow-up
      // reported by mdre on 2026-05-07; an HA follower whose SCHEMA_ENTRY apply produced an
      // unrelinkable index reference would log "Cannot find indexes [...]" on every later
      // SCHEMA_ENTRY apply because applySchemaEntry calls load() each time and the persisted
      // schema.json kept the dangling reference).
      for (final Map.Entry<String, List<String>> entry : deferredMissingIndexWarnings.entrySet()) {
        final List<String> stillMissing = new ArrayList<>(entry.getValue().size());
        for (final String n : entry.getValue())
          if (!relinkedOrphanNames.contains(n))
            stillMissing.add(n);
        if (!stillMissing.isEmpty()) {
          LogManager.instance()
              .log(this, Level.WARNING, "Cannot find indexes %s defined in type '%s'. Ignoring them", null, stillMissing,
                  entry.getKey());
          saveConfiguration = true;
        }
      }

      // SET THE BUCKET STRATEGY AFTER THE INDEXES BECAUSE SOME OF THEM REQUIRE INDEXES (LIKE THE PARTITIONED)
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        if (schemaType.has("bucketSelectionStrategy")) {
          final JSONObject bucketSelectionStrategy = schemaType.getJSONObject("bucketSelectionStrategy");

          final Object[] properties = bucketSelectionStrategy.has("properties") ?
              bucketSelectionStrategy.getJSONArray("properties").toList().toArray() :
              new Object[0];

          final DocumentType type = getType(typeName);
          try {
            type.setBucketSelectionStrategy(bucketSelectionStrategy.getString("name"), properties);
          } catch (final Exception e) {
            // One type's strategy must not take the rest of the load down with it (issue #5637). This block sits
            // near the end of readConfiguration, so an exception escaping here aborts every remaining type's
            // strategy AND everything the loader has not reached yet - triggers, function libraries, extensions,
            // and the compaction file-migration map WAL recovery redirects through - while the outer catch reports
            // the whole schema as "reset". The type stays on its default round-robin strategy, which loses the
            // partition pruning but leaves a database that opens and says why.
            //
            // The catch has to be broad to give that guarantee, so the LEVEL carries what the type cannot: a
            // SchemaException or IllegalArgumentException is the strategy declining to be restored - an
            // unresolvable implementation class, a configuration the suitability check refuses - which is a
            // property of this database and worth a WARNING. Anything else reaching here is a fault in the bind
            // path itself, and would otherwise be indistinguishable from an expected refusal in the log of a
            // database that opens successfully.
            // IllegalArgumentException is listed alongside SchemaException for the strategies this engine does not
            // ship: a custom BucketSelectionStrategy named by class in schema.json runs its own setType() here, and
            // rejecting the type it is handed is what that exception is for. The engine's own strategies no longer
            // raise it from the bind path - that is the change this issue made - so on a stock database only the
            // SchemaException arm fires.
            final boolean expected = e instanceof SchemaException || e instanceof IllegalArgumentException;
            LogManager.instance().log(this, expected ? Level.WARNING : Level.SEVERE,
                "Cannot restore the '%s' bucket selection strategy on type '%s': %s. The type falls back to `%s`",
                // Falling back to toString() because the failures this catch is broad enough to reach include the
                // ones that carry no message - an NPE most of all - and "...: null" names neither what went wrong
                // nor where, on the one line an operator is likely to read.
                e, bucketSelectionStrategy.getString("name"), typeName,
                e.getMessage() != null ? e.getMessage() : e.toString(), RoundRobinBucketSelectionStrategy.NAME);
          }
        }
        // Restore the persisted needsRepartition flag AFTER the strategy is set. We always force
        // the flag to the persisted value (true OR false), because {@link
        // LocalDocumentType#setBucketSelectionStrategy} can itself flip the flag to true when
        // it sees a strategy shape change with records present - which is exactly the picture
        // during load (default round-robin -> persisted partitioned, with records on disk). The
        // persisted value wins: if the previous run cleared the flag via REBUILD TYPE WITH
        // repartition, that cleared state must survive the restart.
        final DocumentType type = getType(typeName);
        if (type instanceof LocalDocumentType ldt) {
          final boolean persisted = schemaType.has("needsRepartition") && schemaType.getBoolean("needsRepartition");
          ldt.setNeedsRepartition(persisted);
        }
      }

      if (saveConfiguration)
        saveConfiguration();

      // The five members that live beside "types" in the schema object: triggers, materialized views, continuous
      // aggregates, function libraries and extensions. Restored through the same method any other reader of a
      // schema object uses, so a second reader cannot come back with a subset of them (issue #7886).
      restoreSchemaMembersFromJSON(root, SchemaMemberSource.SCHEMA_FILE);

      // Restore compaction file-migration map so WAL recovery can redirect or safely skip
      // pages that reference old (pre-compaction) file IDs.
      migratedFileIds.clear();
      if (root.has("migratedFileIds") && !root.isNull("migratedFileIds")) {
        final JSONObject migratedJSON = root.getJSONObject("migratedFileIds");
        for (final String key : migratedJSON.keySet())
          migratedFileIds.put(Integer.parseInt(key), migratedJSON.getInt(key));
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading schema. The schema will be reset", e);
    } finally {
      readingFromFile = false;
      loadInRamCompleted = true;

      if (dirtyGeneration.get() > savedGeneration)
        saveConfiguration();

      rebuildBucketTypeMap();
      readStatisticsFile();
    }
  }

  /**
   * Where a schema object handed to {@link #restoreSchemaMembersFromJSON} came from. One argument rather than two
   * booleans, because the two decisions it settles - replace or merge, and trusted or not - are never independent:
   * every combination other than these two is incoherent.
   */
  public enum SchemaMemberSource {
    /**
     * The database's own {@code schema.json}. Trusted: it records what this database already had installed, so
     * nothing in it is an escalation, and refusing a member here would make the database unopenable. Replaces.
     */
    SCHEMA_FILE,

    /**
     * A file handed to the engine from outside - a JSONL export being restored. Merges, and a member that is
     * arbitrary host code has to earn the same permission creating it by hand would need: a {@code JAVASCRIPT} or
     * {@code JAVA} trigger fires with the engine's privileges, so {@code createTrigger} gates it on
     * {@code UPDATE_SECURITY} rather than {@code UPDATE_SCHEMA} (GHSA-38pf-6hp2-pxww), and a {@code js} function
     * library is host code a later {@code SELECT} can invoke, which {@code DefineFunctionStatement} gates the same
     * way (GHSA-vwjc-v7x7-cm6g). Restoring either from a file without that gate would hand the escalation back
     * through the file (found in review of PR #7943).
     * <p>
     * Defence in depth rather than the only gate: {@code IMPORT DATABASE} itself already requires
     * {@code UPDATE_SECURITY}. The check belongs here too, at the layer that actually installs the code, because
     * that is the layer every route into a restore passes through.
     */
    IMPORTED_FILE
  }

  /**
   * Restores the schema-level members a schema object carries beside {@code "types"}: triggers, materialized views,
   * continuous aggregates, user-defined function libraries and module extensions.
   * <p>
   * Extracted so that every reader of a {@link #toJSON()} object restores the same set. It was inline in the
   * schema-file loader and nowhere else, so the JSONL importer - which reads the very object the JSONL exporter
   * writes - read only {@code settings} and {@code types} out of it: a database restored from a JSONL export came
   * back with no triggers, no materialized views, no continuous aggregates, no {@code DEFINE FUNCTION} libraries and
   * no extension configuration, with no warning and an import that reported success (issue #7886).
   * <p>
   * Every member is restored under its own {@code try}: one that cannot be recreated is logged and counted, and the
   * rest still land. Aborting is the wrong trade in both callers - on open it would reset a schema over one bad
   * trigger, and on import it would discard a restore that has already rebuilt every type.
   * <p>
   * The caller MUST have registered the types first: a trigger binds to a type by name, and a materialized view to
   * its backing type and its sources.
   *
   * @param root   the schema object, as written by {@link #toJSON()}. Members it does not carry are left alone (or
   *               cleared, see {@link SchemaMemberSource}); none of the five is mandatory.
   * @param source where that object came from, which settles both whether to replace or merge and whether the
   *               members in it are privileged to install themselves. See {@link SchemaMemberSource}.
   *
   * @return how many members could not be restored, for a caller that reports warnings
   */
  public synchronized int restoreSchemaMembersFromJSON(final JSONObject root, final SchemaMemberSource source) {
    // The schema file IS the database's own state: what it names is already installed, so re-reading it replaces
    // rather than merges. An imported file is a second database's state arriving into a live one, which keeps
    // whatever the export did not name.
    final boolean replaceExisting = source == SchemaMemberSource.SCHEMA_FILE;

    int failures = 0;

    // LOAD TRIGGERS
    // Dropped and repopulated on a schema-file read, the way the four members below already were. A bare
    // triggers.clear() would NOT have been the equivalent and is why this was left out when the blocks sat inline:
    // a trigger owns a listener adapter registered on its type's event registry, so forgetting the map entry
    // without unregistering leaves the trigger FIRING while invisible to the schema. dropTrigger() pairs the two,
    // and so does this. Without it a trigger deleted from schema.json by hand survived a reload, while the same
    // edit to a materialized view or an extension took effect.
    if (replaceExisting) {
      for (final String triggerName : new ArrayList<>(triggers.keySet()))
        unregisterTriggerListener(triggerName);
      triggers.clear();
    }
    if (root.has("triggers")) {
      final JSONObject triggersJSON = root.getJSONObject("triggers");
      for (final String triggerName : triggersJSON.keySet()) {
        final JSONObject triggerJSON = triggersJSON.getJSONObject(triggerName);
        try {
          final Trigger trigger = TriggerImpl.fromJSON(triggerJSON);

          // ARBITRARY HOST CODE ARRIVING IN A FILE EARNS THE PERMISSION IT WOULD HAVE EARNED AT THE KEYBOARD.
          // createTrigger() gates a JAVASCRIPT or JAVA trigger on UPDATE_SECURITY and not UPDATE_SCHEMA for the
          // reason written there - the executor binds the real database into the script, so the trigger can mint a
          // server admin (GHSA-38pf-6hp2-pxww). Running an import needs only UPDATE_SCHEMA, so restoring one of
          // these without the gate would hand that escalation straight back through a JSONL file. Refused per
          // trigger and counted, not thrown: the rest of the restore is legitimate and has already landed.
          if (source == SchemaMemberSource.IMPORTED_FILE
              && (trigger.getActionType() == Trigger.ActionType.JAVASCRIPT
              || trigger.getActionType() == Trigger.ActionType.JAVA)) {
            try {
              database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);
            } catch (final SecurityException e) {
              ++failures;
              LogManager.instance().log(this, Level.SEVERE,
                  "Refused trigger '%s' from the imported schema: a %s trigger runs with the engine's own "
                      + "privileges, so installing one requires security-admin (UPDATE_SECURITY) and not merely "
                      + "UPDATE_SCHEMA. Everything else in the import is unaffected", null, triggerName,
                  trigger.getActionType());
              continue;
            }
          }

          // CHECKED BEFORE THE MAP IS TOUCHED. Putting first and warning after left an entry no listener backed,
          // which saveConfiguration() then wrote to schema.json: the name stayed occupied, so createTrigger()
          // refused a later valid definition of it, and on the merge path a live, correctly registered trigger of
          // that name was replaced by one that could never fire.
          if (!existsType(trigger.getTypeName())) {
            ++failures;
            LogManager.instance().log(this, Level.WARNING,
                "Cannot register trigger '%s' because type '%s' does not exist",
                null, triggerName, trigger.getTypeName());

            // Recorded only when the name is free, which on the replace path it always is - the sweep above just
            // emptied the map - so a trigger whose type is merely absent right now keeps its definition across the
            // reload instead of being silently dropped from the schema on the next save.
            if (!triggers.containsKey(trigger.getName()))
              triggers.put(trigger.getName(), trigger);
            continue;
          }

          // A trigger of this name already installed is being REPLACED by this one, not joined by it - the map
          // holds one entry per name either way. The sweep above covers that on the replace path; on the MERGE
          // path (an import into a database with a trigger of its own by that name) nothing did, and the put
          // below would have left the previous adapter registered on ITS type's event registry with nothing
          // pointing at it any more: firing on every matching record, unreachable even to dropTrigger(), which
          // would only ever find the newer one. Redundant after the sweep and harmless there - the adapter is
          // already gone, so this returns immediately.
          unregisterTriggerListener(trigger.getName());

          triggers.put(trigger.getName(), trigger);
          registerTriggerListener(trigger);

        } catch (final Exception e) {
          ++failures;
          LogManager.instance().log(this, Level.SEVERE,
              "Error loading trigger '%s': %s", e, triggerName, e.getMessage());
        }
      }
    }

    // Load materialized views
    // On a schema-file read, always clear and re-populate to keep in sync - taking the refresh resources of every
    // view down first, for the reason the trigger sweep above does: an INCREMENTAL view holds listeners on its
    // source types and a PERIODIC one holds a scheduled task, and neither goes away with the map entry.
    if (replaceExisting) {
      for (final String viewName : new ArrayList<>(materializedViews.keySet()))
        unregisterMaterializedViewRefresh(viewName);
      materializedViews.clear();
    }
    if (root.has("materializedViews")) {
      final JSONObject mvJSON = root.getJSONObject("materializedViews");
      for (final String viewName : mvJSON.keySet()) {
        // What was installed under this name before the restore touched it, so a replacement that fails halfway
        // can be undone rather than left as the registered view. Null on the replace path, where the sweep above
        // has already emptied the map.
        final MaterializedViewImpl replaced = materializedViews.get(viewName);

        try {
          final JSONObject viewDef = mvJSON.getJSONObject(viewName);
          final MaterializedViewImpl view = MaterializedViewImpl.fromJSON(database, viewDef);

          // Same replacement rule as the trigger above, and the same merge-path hole: a same-named view already
          // installed has its own listeners and schedule, and the put below is the only thing that used to happen
          // to it - leaving the old instance maintaining itself off records the new one is also maintaining.
          unregisterMaterializedViewRefresh(viewName);

          materializedViews.put(viewName, view);

          installMaterializedViewRefresh(view);

          // Crash recovery: if status is BUILDING, it was interrupted
          if (MaterializedViewStatus.BUILDING.name().equals(view.getStatus()))
            view.setStatus(MaterializedViewStatus.STALE);
        } catch (final Exception e) {
          ++failures;
          LogManager.instance().log(this, Level.SEVERE, "Error loading materialized view '%s': %s", e, viewName,
              e.getMessage() != null ? e.getMessage() : e.toString());

          // UNDONE, not left half-installed. The failure can come from registering the listeners themselves -
          // MaterializedViewBuilder.registerListeners walks the source types and raises on the first one the
          // target does not have, after the earlier ones are already registered - and by then the view this one
          // replaced has had its own resources taken down. Logging and moving on would leave the name mapped to a
          // view that is refreshed by nothing, which reads as a working view and is not one.
          unregisterMaterializedViewRefresh(viewName);

          if (replaced != null) {
            materializedViews.put(viewName, replaced);
            try {
              installMaterializedViewRefresh(replaced);
            } catch (final Exception restoreFailure) {
              LogManager.instance().log(this, Level.SEVERE,
                  "Could not reinstate the materialized view '%s' the failed restore replaced: it stays registered "
                      + "but is no longer refreshed, and a REFRESH MATERIALIZED VIEW reinstalls it", restoreFailure,
                  viewName);
            }
          } else
            materializedViews.remove(viewName);
        }
      }
    }

    // Load continuous aggregates
    if (replaceExisting)
      continuousAggregates.clear();
    if (root.has("continuousAggregates")) {
      final JSONObject caJSON = root.getJSONObject("continuousAggregates");
      for (final String caName : caJSON.keySet()) {
        try {
          final JSONObject caDef = caJSON.getJSONObject(caName);
          final ContinuousAggregateImpl ca = ContinuousAggregateImpl.fromJSON(database, caDef);
          continuousAggregates.put(caName, ca);

          // Crash recovery: if status is BUILDING, it was interrupted
          if (MaterializedViewStatus.BUILDING.name().equals(ca.getStatus()))
            ca.setStatus(MaterializedViewStatus.STALE);
        } catch (final Exception e) {
          ++failures;
          LogManager.instance().log(this, Level.SEVERE, "Error loading continuous aggregate '%s': %s", e, caName,
              e.getMessage() != null ? e.getMessage() : e.toString());
        }
      }
    }

    // Load user-defined function libraries (DEFINE FUNCTION, issue #5121). Only persistable libraries (js/sql/cypher)
    // are stored, so drop any previously loaded persistable library and rebuild from the schema file, while keeping
    // libraries registered programmatically from native Java code (getLanguage() == null).
    if (replaceExisting)
      functionLibraries.values().removeIf(l -> l.getLanguage() != null);
    if (root.has("functions")) {
      final JSONObject functionsJSON = root.getJSONObject("functions");
      for (final String libraryName : functionsJSON.keySet()) {
        try {
          final JSONObject libraryJSON = functionsJSON.getJSONObject(libraryName);
          final String language = libraryJSON.getString("language");

          // THE SIBLING OF THE TRIGGER GATE ABOVE, AND THE SAME RULE. DefineFunctionStatement requires
          // UPDATE_SECURITY on top of UPDATE_SCHEMA for LANGUAGE js, because a polyglot function is arbitrary host
          // code a later SELECT can invoke (GHSA-vwjc-v7x7-cm6g, over the scripting gate GHSA-48qw introduced).
          // A library arriving in a file is the same code by another route, so it earns the same permission.
          // SQL and Cypher libraries are declarative and keep the schema-level protection, exactly as that
          // statement treats them.
          if (source == SchemaMemberSource.IMPORTED_FILE && "js".equalsIgnoreCase(language)) {
            try {
              database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SECURITY);
            } catch (final SecurityException e) {
              ++failures;
              LogManager.instance().log(this, Level.SEVERE,
                  "Refused function library '%s' from the imported schema: a '%s' function is host code a query can "
                      + "invoke, so installing one requires security-admin (UPDATE_SECURITY) and not merely "
                      + "UPDATE_SCHEMA. Everything else in the import is unaffected", null, libraryName, language);
              continue;
            }
          }

          final FunctionLibraryDefinition library = FunctionLibraryFactory.createLibrary(database, libraryName, language);

          final JSONObject funcsJSON = libraryJSON.getJSONObject("functions");
          for (final String funcName : funcsJSON.keySet()) {
            final JSONObject funcJSON = funcsJSON.getJSONObject(funcName);
            final String[] params = funcJSON.getJSONArray("parameters").toListOfStrings().toArray(new String[0]);
            library.registerFunction(FunctionLibraryFactory.createFunction(database, language, funcName,
                funcJSON.getString("code"), params));
          }

          functionLibraries.put(libraryName, library);
        } catch (final Exception e) {
          ++failures;
          LogManager.instance().log(this, Level.SEVERE, "Error loading function library '%s': %s", e, libraryName,
              e.getMessage());
        }
      }
    }

    // Load extensions (module-specific configuration). Under its own try like the four members above, and not
    // because a malformed entry is expected - ArcadeDB's own exporter is the only writer - but because the
    // alternative on the import path is an uncaught throw AFTER every type and every record has already landed,
    // which is the "abort everything over one bad member" outcome this whole method is shaped to avoid.
    if (replaceExisting)
      extensions.clear();
    if (root.has("extensions")) {
      final JSONObject extJSON = root.getJSONObject("extensions");
      for (final String extName : extJSON.keySet()) {
        try {
          extensions.put(extName, extJSON.getJSONObject(extName));
        } catch (final Exception e) {
          ++failures;
          LogManager.instance().log(this, Level.SEVERE, "Error loading extension '%s': %s", e, extName,
              e.getMessage() != null ? e.getMessage() : e.toString());
        }
      }
    }

    return failures;
  }

  /**
   * Installs the refresh resources a registered materialized view needs: an INCREMENTAL view's listeners on its
   * source types, a PERIODIC view's scheduled task. The counterpart of {@link #unregisterMaterializedViewRefresh},
   * and the view must already be in {@code materializedViews} so that one can find it again to take them down.
   */
  private void installMaterializedViewRefresh(final MaterializedViewImpl view) {
    if (view.getRefreshMode() == MaterializedViewRefreshMode.INCREMENTAL)
      MaterializedViewBuilder.registerListeners(this, view, view.getSourceTypeNames());

    if (view.getRefreshMode() == MaterializedViewRefreshMode.PERIODIC)
      getMaterializedViewScheduler().schedule(database, view);
  }

  /**
   * Takes down the refresh resources a registered materialized view owns - an INCREMENTAL view's listeners on its
   * source types, a PERIODIC view's scheduled task - leaving the view itself in the map for the caller to replace or
   * remove.
   * <p>
   * What {@link #dropMaterializedView} tears down, minus the backing type: that type holds the view's rows, and a
   * definition replacing this one names the same type, so dropping it here would delete the data the restore is
   * about to adopt. A view of that name that is not registered is a no-op.
   */
  private void unregisterMaterializedViewRefresh(final String viewName) {
    final MaterializedViewImpl previous = materializedViews.get(viewName);
    if (previous == null)
      return;

    if (materializedViewScheduler != null)
      materializedViewScheduler.cancel(viewName);

    if (previous.getRefreshMode() == MaterializedViewRefreshMode.INCREMENTAL)
      MaterializedViewBuilder.unregisterListeners(this, previous);
  }

  public synchronized void saveConfiguration() {
    rebuildBucketTypeMap();

    if (readingFromFile || !loadInRamCompleted || multipleUpdate || database.isTransactionActive()) {
      // POSTPONE THE SAVING - ensure at least one generation is marked dirty
      dirtyGeneration.updateAndGet(cur -> Math.max(cur, savedGeneration + 1));
      return;
    }

    // Capture the generation BEFORE serializing. Any concurrent modification that increments
    // dirtyGeneration after this point will remain unsaved, so isDirty() stays true.
    final long capturedGeneration = dirtyGeneration.get();

    try {
      LogManager.instance().log(this, Level.FINE, "Saving schema configuration to file - versionSerial = %s ", versionSerial);
      versionSerial.incrementAndGet();

      update(toJSON());

      savedGeneration = capturedGeneration;

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving schema configuration to file: %s", e,
          databasePath + File.separator + SCHEMA_FILE_NAME);
    }

    // #5269: the schema reached a stable state (buckets/indexes just created are now registered). Refresh the per-user
    // security file-access map so runtime-created files are covered immediately, instead of chronically falling through
    // the "allow by default" path in ServerSecurityDatabaseUser.requestAccessOnFile() (which also floods the logs).
    updateSecurity();
  }

  public synchronized JSONObject toJSON() {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", versionSerial.get());
    root.put("dbmsVersion", Constants.getRawVersion());
    root.put("dbmsBuild", Constants.getBuildNumber());

    final JSONObject settings = new JSONObject();
    root.put("settings", settings);

    settings.put("zoneId", zoneId.getId());
    settings.put("dateFormat", dateFormat);
    settings.put("dateTimeFormat", dateTimeFormat);

    final JSONObject types = new JSONObject();
    root.put("types", types);

    for (final DocumentType t : typeMap().values())
      types.put(t.getName(), t.toJSON());

    final JSONObject triggersJson = new JSONObject();
    root.put("triggers", triggersJson);

    for (final Trigger trigger : this.triggers.values())
      triggersJson.put(trigger.getName(), trigger.toJSON());

    // Serialize materialized views
    final JSONObject mvJSON = new JSONObject();
    for (final Map.Entry<String, MaterializedViewImpl> entry : materializedViews.entrySet())
      mvJSON.put(entry.getKey(), entry.getValue().toJSON());
    root.put("materializedViews", mvJSON);

    // Serialize continuous aggregates
    final JSONObject caJSON = new JSONObject();
    for (final Map.Entry<String, ContinuousAggregateImpl> entry : continuousAggregates.entrySet())
      caJSON.put(entry.getKey(), entry.getValue().toJSON());
    root.put("continuousAggregates", caJSON);

    // Serialize user-defined function libraries (DEFINE FUNCTION) so they survive a restart (issue #5121). Libraries
    // backed by native Java code are not persistable and return null from toJSON(): they are skipped here.
    final JSONObject functionsJSON = new JSONObject();
    for (final FunctionLibraryDefinition library : functionLibraries.values()) {
      final JSONObject libraryJSON = library.toJSON();
      if (libraryJSON != null)
        functionsJSON.put(library.getName(), libraryJSON);
    }
    root.put("functions", functionsJSON);

    // Serialize extensions (module-specific configuration)
    if (!extensions.isEmpty()) {
      final JSONObject extJSON = new JSONObject();
      for (final Map.Entry<String, JSONObject> entry : extensions.entrySet())
        extJSON.put(entry.getKey(), entry.getValue());
      root.put("extensions", extJSON);
    }

    // Serialize compaction file-migration map so WAL recovery after a restart can distinguish
    // safe compaction skips from genuinely unexpected missing files.
    if (!migratedFileIds.isEmpty()) {
      final JSONObject migratedJSON = new JSONObject();
      for (final Map.Entry<Integer, Integer> entry : migratedFileIds.entrySet())
        migratedJSON.put(String.valueOf(entry.getKey()), entry.getValue());
      root.put("migratedFileIds", migratedJSON);
    }

    return root;
  }

  void registerType(final LocalDocumentType type) {
    typeMap().put(type.getName(), type);
  }

  public void registerFile(final Component file) {
    final int fileId = file.getFileId();

    synchronized (files) {
      while (files.size() < fileId + 1)
        files.add(null);

      if (files.get(fileId) != null)
        throw new SchemaException(
            "File with id '" + fileId + "' already exists (previous=" + files.get(fileId) + " new=" + file + ")");

      files.set(fileId, file);
    }
  }

  public void initComponents() {
    final List<Component> snapshot;
    synchronized (files) {
      snapshot = new ArrayList<>(files);
    }
    for (final Component f : snapshot)
      if (f != null)
        f.onAfterLoad();
  }

  @Override
  public Schema registerFunctionLibrary(final FunctionLibraryDefinition library) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    if (functionLibraries.putIfAbsent(library.getName(), library) != null)
      throw new IllegalArgumentException("Function library '" + library.getName() + "' already registered");
    return this;
  }

  @Override
  public Schema unregisterFunctionLibrary(final String name) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    functionLibraries.remove(name);
    return this;
  }

  @Override
  public Iterable<FunctionLibraryDefinition> getFunctionLibraries() {
    return functionLibraries.values();
  }

  @Override
  public boolean hasFunctionLibrary(final String name) {
    return functionLibraries.containsKey(name);
  }

  public FunctionLibraryDefinition getFunctionLibrary(final String name) {
    final FunctionLibraryDefinition flib = functionLibraries.get(name);
    if (flib == null)
      throw new IllegalArgumentException("Function library '" + name + "' not defined");
    return flib;
  }

  @Override
  public FunctionDefinition getFunction(final String libraryName, final String functionName) throws IllegalArgumentException {
    return getFunctionLibrary(libraryName).getFunction(functionName);
  }

  public void setMigratedFileId(final int oldFileId, final int newFileId) {
    setMigratedFileId(oldFileId, newFileId, true);
  }

  public void setMigratedFileId(final int oldFileId, final int newFileId, final boolean saveConfiguration) {
    LogManager.instance().log(this, Level.FINE, "Migrating file id %d to %d", null, oldFileId, newFileId);
    migratedFileIds.put(oldFileId, newFileId);
    if (saveConfiguration)
      saveConfiguration();
  }

  public Integer getMigratedFileId(final int oldFileId) {
    return migratedFileIds.get(oldFileId);
  }

  /**
   * Re-keys {@code indexMap} after an index changed the name it answers to, so the schema keeps resolving it under
   * {@link IndexInternal#getName()} at every moment of its life.
   * <p>
   * Only {@link com.arcadedb.index.vector.LSMVectorIndex} renames itself: it is named after the component file it
   * holds and a compaction swaps that file in, a rename every node has to follow or a leader's schema stops matching
   * the followers that rebuilt the index from the file it shipped them. Every other index type keeps its creation
   * name for life, which is why this had no counterpart before.
   * <p>
   * Leaving the map keyed by the retired name is not a cosmetic inconsistency: index maintenance is queued on
   * {@link com.arcadedb.database.TransactionIndexContext} under {@code index.getName()}, and its {@code commit()}
   * opens by discarding the lanes of indexes the schema no longer knows - the TYPE DROP case. A freshly compacted
   * vector index matched that filter exactly, so the first writes after a {@code COMPACT INDEX} were dropped without
   * a word while the records themselves were written (issue #6105).
   * <p>
   * The new name is published BEFORE the old one is retired, so a concurrent lookup sees the index under one name or
   * transiently under both, never under neither. The removal is conditional on the value so a name already taken over
   * by another index - two components can only share a name after a hand-edited or restored schema, but the map is
   * the only thing standing between that and a lost registration - is left alone.
   *
   * @param oldName the name the index was registered under
   * @param index   the index, already answering to its new name
   */
  public void indexRenamed(final String oldName, final IndexInternal index) {
    final String newName = index.getName();
    if (newName == null || newName.equals(oldName))
      return;

    LogManager.instance().log(this, Level.FINE, "Index '%s' renamed to '%s'", null, oldName, newName);

    indexMap.put(newName, index);
    if (oldName != null)
      indexMap.remove(oldName, index);
  }

  public boolean isDirty() {
    return dirtyGeneration.get() > savedGeneration;
  }

  public File getConfigurationFile() {
    return configurationFile;
  }

  public long getVersion() {
    return versionSerial.get();
  }

  public synchronized void update(final JSONObject newSchema) throws IOException {
    // Validate before touching either file: getLong() throws on a non-numeric or explicitly null value, and a
    // rejected schema must leave both generations exactly as they were. An ABSENT version keeps the current one,
    // which is why the default-value getter cannot be used here - it treats an explicit null as absent too.
    final long newVersion = newSchema.has("schemaVersion") ? newSchema.getLong("schemaVersion") : versionSerial.get();
    final String latestSchema = newSchema.toString();

    if (configurationFile.exists()) {
      // #6114: A COPY, NOT A RENAME. The rename this replaces moved schema.json out of the way and only then wrote
      // the new one, so between the two statements schema.json DID NOT EXIST and while the writer ran it was
      // truncated. A crash in that window left a database whose schema file was missing - recoverable only from
      // schema.prev.json - and any concurrent reader (the backup's lock-free t0 configuration capture, the HA
      // snapshot ship) could observe nothing, or half a JSON document. The copy is published as a hard link where
      // the file store allows it, so it costs an inode operation rather than a re-read of the whole schema, and it
      // is atomic on the target either way: schema.prev.json is what readConfiguration() falls back TO, so it can
      // never be half-written. It is also byte-identical by construction - literally the same bytes, so no charset
      // from setEncoding() is applied to it on the way out.
      final File copy = new File(databasePath + File.separator + SCHEMA_PREV_FILE_NAME);
      FileUtils.atomicCopyFile(configurationFile, copy);
    }

    // The primary is replaced by an atomic rename, so a reader sees either this generation or the previous one.
    //
    // UTF-8 UNCONDITIONALLY, NOT `encoding`. readConfiguration() reads this file back with `encoding`, but that field
    // is a transient per-instance setting that is never persisted and starts every open at DEFAULT_ENCODING: a file
    // written in anything else would be unreadable on the next open unless the caller happened to re-apply
    // setEncoding() first. So `encoding` is a READ-side compatibility knob for a legacy file, and every write
    // normalises the primary back onto UTF-8 - which is also what makes the recovery in readConfiguration() self-heal
    // a legacy database instead of perpetuating its charset. This replaces a FileWriter that used the JVM's DEFAULT
    // charset, which was asymmetric with the reader on any platform whose default is not UTF-8 (issue #6114).
    FileUtils.atomicWriteFile(configurationFile, latestSchema);

    // Only after the bytes are on disk: a failed publication must not leave the in-memory version claiming a
    // generation that no file holds.
    versionSerial.set(newVersion);

    database.getExecutionPlanCache().invalidate();
    // The OpenCypher plan cache embeds schema-derived physical operators (index-seek vs scan, bucket
    // sets, cost estimates), so a schema change - including one received from the HA leader via this
    // update() path - must flush it too. The Cypher statement cache holds only the syntactic AST and
    // is schema-independent, so it is intentionally left untouched (same as SQL's statement cache).
    database.getCypherPlanCache().invalidate();
  }

  /**
   * Opens ONE schema recording session around {@code callback}, so every DDL statement executed inside it nests into
   * that single frame instead of opening a frame of its own (issue #6990). See {@link Schema#bulkChange(Runnable)}
   * for what that buys and what it costs.
   * <p>
   * WHY THE FAILURE IS SWALLOWED AND RETHROWN. Letting the exception escape the callback would abort the session
   * before it published anything, and the session is the Raft entry boundary: the followers would end up with none of
   * the batch while this node keeps whatever the callback managed to apply to its in-memory schema, which no schema
   * rollback exists to undo. That is a divergence the per-statement path never produces. Catching it here lets the
   * session close normally - the prefix is saved locally and replicated as one entry, so both sides hold the same
   * thing - and the failure is then rethrown unchanged, so the caller still fails. "All of it, or the prefix that
   * succeeded, on every node" is the strongest atomicity available without transactional DDL.
   * <p>
   * ONLY {@link RuntimeException}, NEVER {@link Error}. The argument above rests on the failing statement having
   * thrown from its own validation, before it mutated anything, so the prefix is a state somebody meant to reach.
   * An {@code Error} carries no such promise: it can strike mid-mutation and leave a schema object half-written, and
   * what this method does next - serialize the whole schema and take a synchronous quorum round trip - is exactly the
   * allocation-heavy work an {@code OutOfMemoryError} is telling us not to do. So an {@code Error} propagates and
   * aborts the session unpublished, which is what the per-statement path has always done with it.
   */
  @Override
  public void bulkChange(final Runnable callback) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    final RuntimeException[] failure = new RuntimeException[1];

    recordFileChanges(() -> {
      try {
        callback.run();
      } catch (final RuntimeException e) {
        failure[0] = e;
      }
      return null;
    }, true);

    if (failure[0] != null)
      throw failure[0];
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    return recordFileChanges(callback, false);
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback,
      final boolean deferIntermediateSchemaSaves) {
    if (readingFromFile || !loadInRamCompleted) {
      try {
        return (RET) callback.call();
      } catch (final Exception e) {
        throw new DatabaseOperationException("Error on updating the schema", e);
      }
    }

    final long prevGeneration = dirtyGeneration.get();
    dirtyGeneration.updateAndGet(cur -> Math.max(cur, savedGeneration + 1));

    final boolean suspendIntermediateSaves = deferIntermediateSchemaSaves && !multipleUpdate;
    if (suspendIntermediateSaves)
      multipleUpdate = true;

    final boolean[] executed = new boolean[1];
    try {
      final RET result = database.getWrappedDatabaseInstance().recordFileChanges(() -> {
        // UNDER THE WRITE LOCK, SO THE DEPTH IS CONSISTENT: A NESTED FRAME (A TYPE CREATION AND THE BUCKET CREATIONS
        // INSIDE IT) SEES THE FRAME ENCLOSING IT
        final boolean outermost = recordingDepth++ == 0;
        try {
          final Object callbackResult = callback.call();
          executed[0] = true;

          // #7457: SAVE schema.json BEFORE THE WRITE LOCK IS RELEASED, NOT AFTER. The callback registered or dropped
          // files in the FileManager, and until the schema file names exactly those files an observer that lists
          // the files and then reads the schema - a backup - archives a schema naming buckets it did not copy. The
          // database write lock is what excludes such an observer, so the save has to happen under it. Taking this
          // schema's monitor under the write lock is the order every DDL that saves from inside its own callback
          // (dropType, dropBucket, the materialized view ones) had already established; the reverse order - the
          // monitor held while waiting for the write lock - is the one no method may take, see dropMaterializedView.
          if (suspendIntermediateSaves)
            multipleUpdate = false;
          // UNCONDITIONAL, AS IT WAS OUTSIDE THE LOCK: NOT EVERY IN-MEMORY MUTATION MARKS A GENERATION DIRTY (A TYPE
          // INDEX REGISTERING ITS BUCKET SUB-INDEXES AFTER THEIR OWN SAVES DOES NOT), SO "NOTHING TO SAVE" CANNOT BE
          // READ OFF isDirty() HERE. AND AT EVERY NESTING LEVEL, ALSO AS BEFORE: A NESTED FRAME SAVES UNLESS
          // multipleUpdate POSTPONES IT (bulkChange, dropType), SO A DDL OVER N BUCKETS STILL WRITES THE FILE N TIMES
          saveConfiguration();

          // THE LAST STEP UNDER THE WRITE LOCK OF THE OUTERMOST FRAME: THE CHANGE IS APPLIED, ITS FILES REGISTERED OR
          // DROPPED AND schema.json SAVED, AND NOTHING ELSE HAPPENS BEFORE THE LOCK IS RELEASED. A TEST ASSERTING
          // HERE THAT THE SCHEMA FILE AGREES WITH THE FILE SET PROVES THE SAVE RUNS UNDER THE LOCK (#7457) - MOVED
          // AFTER THE RELEASE, IT WOULD ALSO BE AFTER THIS HOOK. A NESTED FRAME HAD ITS SAVE POSTPONED TO THE FRAME
          // ENCLOSING IT, SO IT DOES NOT FIRE
          if (outermost)
            database.executeCallbacks(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES);
          return callbackResult;
        } finally {
          --recordingDepth;
        }
      });

      // INVALIDATE EXECUTION PLAN IN CASE TYPE OR INDEX CONCUR IN THE GENERATED PLANS
      database.getExecutionPlanCache().invalidate();
      // Same reasoning for the OpenCypher plan cache: a cached PhysicalPlan can reference an index or
      // type that this schema mutation just added or dropped (e.g. a NodeIndexSeek over an index that
      // no longer exists), so flush it on every local schema change. The Cypher statement (AST) cache
      // is syntactic and schema-independent, so it is deliberately not flushed here.
      database.getCypherPlanCache().invalidate();

      return result;

    } finally {
      if (suspendIntermediateSaves)
        multipleUpdate = false;
      if (!executed[0] && prevGeneration <= savedGeneration)
        // ROLLBACK THE DIRTY STATUS - restore only if we were the ones who made it dirty
        savedGeneration = dirtyGeneration.get();
    }
  }

  protected Index createBucketIndex(final LocalDocumentType type,
      final Type[] keyTypes,
      final Bucket bucket,
      final String typeName,
      final INDEX_TYPE indexType,
      final boolean unique,
      final int pageSize,
      final NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback,
      final String[] propertyNames,
      final TypeIndex propIndex,
      final int batchSize,
      final IndexMetadata metadata) {
    return createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback,
        propertyNames, propIndex, batchSize, metadata, true);
  }

  protected Index createBucketIndex(final LocalDocumentType type,
      final Type[] keyTypes,
      final Bucket bucket,
      final String typeName,
      final INDEX_TYPE indexType,
      final boolean unique,
      final int pageSize,
      final NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback,
      final String[] propertyNames,
      final TypeIndex propIndex,
      final int batchSize,
      final IndexMetadata metadata,
      final boolean build) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (bucket == null)
      throw new IllegalArgumentException("bucket is null");

    final String indexName = bucket.getName() + "_" + System.nanoTime();

    if (lookupIndex(indexName) != null)
      throw new DatabaseMetadataException(
          "Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

    final IndexBuilder<Index> builder = buildBucketIndex(typeName, bucket.getName(), propertyNames)
        .withUnique(unique)
        .withType(indexType)
        .withFilePath(databasePath + File.separator + indexName)
        .withKeyTypes(keyTypes)
        .withPageSize(pageSize)
        .withNullStrategy(nullStrategy)
        .withCallback(callback)
        .withIndexName(indexName)
        .withMetadata(metadata);

    final IndexInternal index = indexFactory.createIndex(builder);

    // Copy collation settings from builder metadata to the index
    if (metadata != null && metadata.collations != null)
      index.getMetadata().collations = metadata.collations;
    // Copy the user-supplied TypeIndex name through to the bucket-level metadata so the
    // upcoming addIndexInternal mints the TypeIndex under the manual name (issue #4139).
    if (metadata != null && metadata.typeIndexName != null)
      index.getMetadata().typeIndexName = metadata.typeIndexName;

    try {
      registerFile(index.getComponent());

      publishIndexDuringLoad(indexName, index);

      // An index created but not populated here is parked UNAVAILABLE, so nothing can read it while it is empty. Two
      // callers arrive with build=false: the sorted build, which populates every bucket index in one streamed pass
      // and publishes them together, and the two-transaction split of issue #6324 item 1, which commits the component
      // on its own before building it inside whatever transaction the caller holds.
      if (!build && !index.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.AVAILABLE },
          IndexInternal.INDEX_STATUS.UNAVAILABLE))
        throw new IndexException("Cannot prepare empty index '" + indexName + "' for population");

      type.addIndexInternal(index, bucket.getFileId(), propertyNames, propIndex);

      // Re-set metadata after addIndexInternal populated propertyNames, to propagate
      // caseInsensitiveKeys to the underlying mutable/compacted indexes.
      index.setMetadata(index.getMetadata());

      if (build)
        index.build(batchSize, callback);

      return index;

    } catch (final NeedRetryException e) {
      dropIndexInternal(indexName);
      throw e;
    } catch (final Exception e) {
      dropIndexInternal(indexName);
      throw new IndexException("Error on creating index '" + indexName + "'", e);
    }
  }

  protected boolean isSchemaLoaded() {
    return loadInRamCompleted;
  }

  /**
   * True while the schema is hydrating types from {@code schema.json}. Same-package callers
   * (notably {@link LocalDocumentType#setBucketSelectionStrategy}) consult this to skip work
   * the load path will redo immediately - e.g. the partition-shape-change flag-flip, where the
   * persisted {@code needsRepartition} value is reapplied right after the strategy assignment.
   */
  boolean isReadingFromFile() {
    return readingFromFile;
  }

  protected void updateSecurity() {
    if (security != null)
      security.updateSchema(database);
  }

  /**
   * Logs, once per opened database and per LOGICAL index, the reason an index should be rebuilt. How much that
   * matters is carried by the message itself: an index whose on-disk layout merely predates a change the engine
   * cannot apply in place keeps working exactly as it did, while one whose physical key order predates #5321 answers
   * lookups with fewer records than a scan (#5802). Either way this is the only moment an operator would otherwise
   * have no way of learning that a `REBUILD INDEX` is worth running - and the reason it names the LOGICAL index, not
   * the bucket sub-index that raised it. The same text reaches Studio through {@code schema:indexes}.
   *
   * @see IndexInternal#getUpgradeWarning()
   */
  private void reportUpgradeWarning(final IndexInternal index, final String typeName, final String[] properties) {
    final String warning;
    try {
      warning = index.getUpgradeWarning();
    } catch (final Exception e) {
      // Never let advisory reporting break a schema load
      return;
    }
    if (warning == null)
      return;

    // Report the LOGICAL index once, not each of its bucket sub-indexes: the name below is the one REBUILD INDEX takes.
    final TypeIndex typeIndex = index.getTypeIndex();
    final String logicalName = typeIndex != null ? typeIndex.getName() : typeName + Arrays.toString(properties);
    if (!reportedUpgradeWarnings.add(logicalName))
      return;

    LogManager.instance().log(this, Level.WARNING,
        "Index '%s' of database '%s' should be rebuilt: %s. Run: REBUILD INDEX `%s`", null, logicalName,
        database.getName(), warning, logicalName);
  }

  /**
   * Replaces the map to allow concurrent usage while rebuilding the map.
   */
  private void rebuildBucketTypeMap() {
    final Map<Integer, LocalDocumentType> newBucketId2TypeMap = new HashMap<>();
    for (final LocalDocumentType t : typeMap().values()) {
      for (final Bucket b : t.getBuckets(false))
        newBucketId2TypeMap.put(b.getFileId(), t);
    }

    // COMPUTE INVOLVED BUCKETS FOR SECURITY
    final Map<Integer, LocalDocumentType> newBucketId2InvolvedTypeMap = new HashMap<>();
    for (final LocalDocumentType t : typeMap().values()) {
      for (final Bucket b : t.getInvolvedBuckets())
        newBucketId2InvolvedTypeMap.put(b.getFileId(), t);
    }

    if (isStagingPublication()) {
      // Derived from the staged graph, so they belong to it and are published with it (PR #8001 review).
      stagedBucketId2TypeMap = newBucketId2TypeMap;
      stagedBucketId2InvolvedTypeMap = newBucketId2InvolvedTypeMap;
      return;
    }

    published = new SchemaState(published.types(), newBucketId2TypeMap, newBucketId2InvolvedTypeMap);
  }
}
