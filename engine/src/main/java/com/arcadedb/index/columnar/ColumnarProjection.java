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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntHashSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * The columnar projection of one bucket: the published {@link ProjectionState} (an {@link AtomicReference} readers take without a
 * lock), the in-memory overlay, the writer dictionaries, the dirty page trackers and content owner maps (primary and, when the bucket
 * has a paired external-property bucket, external, R7) and the lifetime of the files of {@code <db>/columnar/b<bucketFileId>/}.
 * <p>
 * Every state the projection builds lists its segments oldest first and its overlay views with the live overlay as the LAST one
 * ({@link #publishOverlay()} replaces that last view). Publishing is serialized by {@link #publishLock()}; readers never take it.
 * A publish that changes the segment set also holds {@link #applyLock()}, so the apply pass sees one segment set for its whole pass.
 * Lock order: {@link #applyLock()}, then {@link #publishLock()}, then an owner map lock, then a tracker's {@code movementLock}.
 * <p>
 * In-flight files: a writer (flush, merge, rebuild, compaction) names its new files through {@link #reserveSegmentId()},
 * {@link #reserveDictionaryGeneration(int)} and {@link #holdFiles(String...)} before writing them, and {@link #releaseFiles} them once
 * the manifest naming them is published (or once it deleted them). Held names are never deleted by an unreferenced-file scan.
 * <p>
 * Flush (Task 10, {@link ColumnarFlusher}): {@link #freezeOverlay()} hands the live overlay to the flush and publishes its view before the
 * now empty live one; until {@link #flushPublished} swaps it for the new segment, {@link #liveValueLookup} answers it with
 * {@link #LOOKUP_FROZEN} and the apply pass records the positions it supersedes there ({@link #supersedeFrozen}), which the published
 * frozen view hides (V-B2) and the new segment starts with as its dead set. At most one flush, merge or compaction runs per projection
 * ({@link #tryBeginMaintenance()}, V-B6).
 * <p>
 * File lifetime (R1, R2): a state's last release ({@link ProjectionState}'s {@code onLastRelease}, often on a query thread) closes the
 * segment readers no retained state holds any more and, when the state named a file neither the current manifest nor a retained state
 * names, deletes the unreferenced files under {@code publishLock} and calls {@link SegmentFile#deleted()} for each deleted segment: the
 * only path that invalidates {@code BlockCache} entries. READ_ONLY databases never write or delete a file (R16).
 */
public final class ColumnarProjection {
  /** {@link #liveValueLookup}: no live row holds the position. */
  static final long   LOOKUP_ABSENT   = -1;
  /** {@link #liveValueLookup}: the live overlay holds the position's value. */
  static final long   LOOKUP_OVERLAY  = -2;
  /** {@link #liveValueLookup}: the frozen overlay of a running flush holds the position's value. */
  static final long   LOOKUP_FROZEN   = -3;
  static final String MISSING         = "missing";
  static final String CORRUPT         = "corrupt";
  /** Reached by {@link #acquire()} between the read of the pending positions and the read of the state. */
  static final String ACQUIRE_BETWEEN = "acquire.pendingRead";
  /** Reached by {@link #open} before it reads the files of the bucket directory. */
  static final String LOAD_FILES      = "projection.loadFiles";

  private static final SegmentReader[] NO_SEGMENTS       = new SegmentReader[0];
  private static final String          SEGMENT_PREFIX    = "seg-";
  private static final String          DICTIONARY_PREFIX = "dict-";
  private static final String          DICTIONARY_SUFFIX = ".dic";

  private final int              bucketId;
  /** The bucket's and its type's names, replaced by a {@link #rename} (the directory is keyed by the file id, R9). */
  private volatile String        bucketName;
  private volatile String        typeName;
  private final Path             bucketDir;
  private final boolean          readOnly;
  private final int              maxRecordsInPage;
  private final ColumnarStats    stats;
  private final DirtyPageTracker tracker;
  private final DirtyPageTracker externalTracker;
  private final int              externalFileId;
  private final int              segmentBlockRows;

  private final ReentrantReadWriteLock ownersLock         = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock externalOwnersLock;
  private volatile ContentOwnerMap     owners             = new ContentOwnerMap();
  private volatile ContentOwnerMap     externalOwners;

  private final AtomicReference<ProjectionState> state       = new AtomicReference<>();
  private final ReentrantLock                    publishLock = new ReentrantLock();
  private final ReentrantLock                    applyLock   = new ReentrantLock();

  private volatile ColumnDefinition[]            columns;
  private volatile ColumnarOverlay               overlay;
  private volatile Map<String, GlobalDictionary> dictionaries = Map.of();
  private volatile ColumnarManifest              manifest;
  private volatile String                        unusableReason;
  private volatile Consumer<String>              rebuildRequester = reason -> {
  };
  /** {@link ColumnarRebuilder}'s coalescing state of this projection (R17): null while no rebuild is queued or running. */
  final AtomicReference<ColumnarRebuilder.Request> rebuildRequest = new AtomicReference<>();

  /**
   * No-WAL marker protocol (R8, V-B3), state for Task 13: {@code markPagesWithoutWAL} marks the pages, {@link #bumpNoWalEpoch()}s,
   * and under {@link #noWalMarkerLock}, when {@link #noWalMarkerEpoch} is -1, writes {@code <bucketDir>/NOWAL} and records the bumped
   * epoch there. A flush never deletes the marker: the bucket pages of a no-WAL commit reach the disk asynchronously (PageManager), so
   * after a flush the projection could be newer than the bucket. Only a clean close, once the database flushed every page, deletes it
   * and resets {@link #noWalMarkerEpoch} to -1 (Tasks 12/13).
   */
  private final AtomicLong noWalEpoch            = new AtomicLong();
  final         Object     noWalMarkerLock       = new Object();
  /** Epoch at which NOWAL was last written, -1 when no marker is current; guarded by {@link #noWalMarkerLock}. */
  long                     noWalMarkerEpoch      = -1;

  // flush state, written under applyLock
  private volatile ColumnarOverlay     frozenOverlay;
  private          ColumnarSegmentView frozenView;          // the frozen overlay's published view, before masking
  private          PositionBitmap      supersededInFrozen;  // positions the apply pass superseded in the frozen overlay (V-B2)
  private          boolean             supersededChanged;
  private          long                generationFloor;     // generations at or below it may name files of an uncertain publish
  private          ContentOwnerMap     publishedOwners;     // the owner maps and their modification counts at the last publish
  private          long                publishedOwnersModifications;
  private          ContentOwnerMap     publishedExternalOwners;
  private          long                publishedExternalOwnersModifications;
  private final    AtomicBoolean       maintenance         = new AtomicBoolean();
  private volatile long                lastFlushMs         = System.currentTimeMillis();

  // apply pass state, under applyLock (the retry clock of unreadablePages is also read lock-free by the tick)
  private final ColumnarApplier.UnreadablePages unreadablePages = new ColumnarApplier.UnreadablePages();
  private       ColumnarApplier.Pass            applyScratch;

  // guarded by publishLock
  /** Pages the apply pass completed since the last freeze (R14, V-B2): a flush keeps them unflushed. */
  private final IntHashSet               appliedSinceFreeze  = new IntHashSet();
  /** {@link #appliedSinceFreeze} of the external tracker's pages (R7), null when the bucket has no external bucket. */
  private final IntHashSet               externalAppliedSinceFreeze;
  /** Pages the apply pass completed while a rebuild scans, marked dirty again by its publish; null while no rebuild scans. */
  private IntHashSet                     appliedDuringRebuild;
  private IntHashSet                     externalAppliedDuringRebuild;
  private final Set<ProjectionState>     retained            = new HashSet<>();
  private final Set<SegmentReader>       openReaders         = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<String, SegmentFile> segmentFiles        = new HashMap<>();
  private final Set<GlobalDictionary>    openDictionaries    = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<String>              inFlightFiles       = new HashSet<>();
  private final Map<Integer, Integer>    nextGeneration      = new HashMap<>();
  private int                            nextSegmentId;
  private int                            reportedSegments;
  private int                            reportedOpenFiles;
  private boolean                        ownReferenceDropped;
  private boolean                        directoryDeleted;
  private volatile boolean               closed;
  private volatile boolean               dropped;
  private volatile boolean               stopped;

  private ColumnarProjection(final Path databasePath, final LocalBucket bucket, final String typeName, final ColumnDefinition[] columns,
      final ColumnarStats stats, final boolean readOnly, final LocalBucket external, final int segmentBlockRows) {
    this.bucketId = bucket.getFileId();
    this.bucketName = bucket.getName();
    this.typeName = typeName;
    this.bucketDir = ColumnarStore.bucketDir(databasePath, bucketId);
    this.readOnly = readOnly;
    this.maxRecordsInPage = bucket.getMaxRecordsInPage();
    this.segmentBlockRows = segmentBlockRows;
    this.stats = stats;
    this.columns = columns.clone();
    this.overlay = new ColumnarOverlay(maxRecordsInPage, this.columns, System::currentTimeMillis);
    this.tracker = new DirtyPageTracker(maxRecordsInPage, System::currentTimeMillis);
    this.supersededInFrozen = new PositionBitmap(maxRecordsInPage);
    if (external == null) {
      this.externalTracker = null;
      this.externalFileId = -1;
      this.externalOwnersLock = null;
      this.externalAppliedSinceFreeze = null;
    } else {
      this.externalTracker = new DirtyPageTracker(external.getMaxRecordsInPage(), System::currentTimeMillis);
      this.externalFileId = external.getFileId();
      this.externalOwnersLock = new ReentrantReadWriteLock();
      this.externalOwners = new ContentOwnerMap();
      this.externalAppliedSinceFreeze = new IntHashSet();
    }
  }

  /**
   * Opens the projection of the bucket from {@code ColumnarStore.bucketDir(databasePath, bucket.getFileId())} (R9): manifest, segments
   * with their dead sets, dictionaries and owner maps. Never rebuilds and never calls the rebuild requester (V-M10): a manifest naming a
   * file that does not exist records {@code missing}, a file failing its CRC or format check records {@code corrupt}, and the
   * projection serves an unusable empty state. Without a manifest, a bucket with records records {@code missing}; an empty bucket is
   * usable at once (V-B1), published as an empty generation-1 manifest with one empty dictionary per ordinal column on a writable
   * database, kept in memory only on a READ_ONLY one. A writable open deletes the files the loaded manifest does not name (crash
   * orphans, V-M11).
   */
  static ColumnarProjection open(final DatabaseInternal db, final LocalBucket bucket, final ColumnDefinition[] columns,
      final ColumnarStats stats) {
    final DocumentType type = db.getSchema().getTypeByBucketId(bucket.getFileId());
    final Integer externalId = type instanceof LocalDocumentType local ? local.getExternalBucketIdFor(bucket.getFileId()) : null;
    final LocalBucket external = externalId == null ? null : (LocalBucket) db.getSchema().getBucketById(externalId);
    final ColumnarProjection projection = new ColumnarProjection(Path.of(db.getDatabasePath()), bucket,
        type == null ? bucket.getName() : type.getName(), columns, stats, db.getMode() == ComponentFile.MODE.READ_ONLY, external,
        db.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_SEGMENT_BLOCK_ROWS));
    projection.load(db, bucket);
    return projection;
  }

  private void load(final DatabaseInternal db, final LocalBucket bucket) {
    publishLock.lock();
    try {
      final List<SegmentReader> readers = new ArrayList<>();
      final Map<String, GlobalDictionary> loaded = new HashMap<>();
      ColumnarManifest found = null;
      String reason = null;
      try {
        ColumnarTestHooks.reach(LOAD_FILES);
        // first, on its own: a manifest that fails to load must still leave the reservations above every existing file
        seedFromDirectory();
        found = ColumnarManifest.load(bucketDir);
        if (found != null) {
          seedFromManifest(found);
          loadFiles(found, readers, loaded);
        }
      } catch (final NoSuchFileException e) {
        reason = MISSING;
      } catch (final UncheckedIOException e) {
        reason = e.getCause() instanceof NoSuchFileException ? MISSING : CORRUPT;
        if (reason == CORRUPT)
          LogManager.instance().log(this, Level.WARNING, "Columnar projection of bucket '%s' cannot be opened", e, bucketName);
      } catch (final ColumnarTestHooks.SimulatedCrash e) {
        throw e;
      } catch (final IOException | RuntimeException e) {
        // a RuntimeException here is bytes the format checks did not catch (a BufferUnderflowException, a negative size, a wrong
        // class): corrupt files, rebuilt, never a reason to fail or retry the open
        LogManager.instance().log(this, Level.WARNING, "Columnar projection of bucket '%s' cannot be opened", e, bucketName);
        reason = CORRUPT;
      }

      if (reason != null) {
        for (final SegmentReader reader : readers)
          reader.close();
        for (final GlobalDictionary dictionary : loaded.values())
          dictionary.close();
        readers.clear();
        loaded.clear();
        found = null;
        owners = new ContentOwnerMap();
        if (externalOwnersLock != null)
          externalOwners = new ContentOwnerMap();
      } else if (found != null) {
        if (!readOnly)
          ColumnarStore.deleteUnreferenced(bucketDir, found.files(), inFlightFiles);
      } else if (!isEmpty(bucket))
        reason = MISSING;
      else if (!readOnly)
        found = publishEmpty(Path.of(db.getDatabasePath()), db.getTransactionManager().getLastTransactionId(), loaded);

      publish(found, readers.toArray(NO_SEGMENTS), new ColumnarSegmentView[] { overlay.publish() }, Map.copyOf(loaded), reason == null);
      unusableReason = reason;
      ownersPublished();
    } finally {
      publishLock.unlock();
    }
  }

  /** Raises the reservations above every segment id and dictionary generation a file of the directory uses, orphans included. */
  private void seedFromDirectory() throws IOException {
    if (!Files.isDirectory(bucketDir))
      return;
    try (final DirectoryStream<Path> entries = Files.newDirectoryStream(bucketDir)) {
      for (final Path entry : entries) {
        final String name = entry.getFileName().toString();
        try {
          if (name.startsWith(SEGMENT_PREFIX) && name.length() >= SEGMENT_PREFIX.length() + 16)
            raiseSegmentId(Long.parseUnsignedLong(name.substring(SEGMENT_PREFIX.length(), SEGMENT_PREFIX.length() + 16), 16));
          else if (name.startsWith(DICTIONARY_PREFIX) && name.endsWith(DICTIONARY_SUFFIX)) {
            final String[] parts = name.substring(DICTIONARY_PREFIX.length(), name.length() - DICTIONARY_SUFFIX.length()).split("\\.");
            if (parts.length == 2)
              raiseGeneration(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
          }
        } catch (final NumberFormatException e) {
          // not a name this projection writes
        }
      }
    }
  }

  /** Raises the reservations above every segment id and dictionary generation the manifest names. */
  private void seedFromManifest(final ColumnarManifest found) {
    for (final ColumnarManifest.SegmentEntry entry : found.segments())
      raiseSegmentId(entry.segmentId());
    for (final ColumnarManifest.DictionaryEntry entry : found.dictionaries())
      raiseGeneration(entry.indexFileId(), entry.generation());
  }

  private void raiseSegmentId(final long used) {
    if (used < 0 || used >= Integer.MAX_VALUE)
      nextSegmentId = Integer.MAX_VALUE;
    else
      nextSegmentId = Math.max(nextSegmentId, (int) used + 1);
  }

  private void raiseGeneration(final int indexFileId, final int used) {
    nextGeneration.merge(indexFileId, used == Integer.MAX_VALUE ? Integer.MAX_VALUE : used + 1, Math::max);
  }

  private void loadFiles(final ColumnarManifest found, final List<SegmentReader> readers, final Map<String, GlobalDictionary> loaded)
      throws IOException {
    if (found.bucketFileId() != bucketId)
      throw new BlockCorruptionException("Columnar manifest of bucket " + bucketId + " was published for bucket " + found.bucketFileId());
    if (!found.segments().isEmpty() && found.maxRecordsInPage() != maxRecordsInPage)
      throw new BlockCorruptionException("Columnar manifest of bucket " + bucketId + " has " + found.maxRecordsInPage()
          + " slots per page, the bucket " + maxRecordsInPage);

    for (final ColumnarManifest.DictionaryEntry entry : found.dictionaries()) {
      final String property = propertyOf(entry.indexFileId());
      if (property == null)
        continue; // a column the schema no longer has: the reconciler drops it
      final Path file = bucketDir.resolve(entry.file());
      loaded.put(property, readOnly ?
          GlobalDictionary.openReadOnly(file, entry.bytes(), entry.entries()) :
          GlobalDictionary.open(file, entry.bytes(), entry.entries()));
    }

    for (final ColumnarManifest.SegmentEntry entry : found.segments()) {
      final PositionBitmap dead = entry.deadVersion() == 0 ? null : DeadSetFile.read(bucketDir.resolve(entry.deadFile()), maxRecordsInPage);
      final SegmentReader reader;
      try {
        reader = SegmentReader.open(bucketDir.resolve(entry.file()), dead);
      } catch (final IllegalArgumentException e) {
        // the segment's page size differs from its dead set's, which was checked against the bucket's
        throw new BlockCorruptionException("Segment " + entry.file() + " does not match its dead set: " + e.getMessage());
      }
      readers.add(reader);
      if (reader.segmentId() != entry.segmentId() || reader.rowCount() != entry.rows())
        throw new BlockCorruptionException("Segment " + entry.file() + " holds segment " + reader.segmentId() + " with " + reader.rowCount()
            + " rows, the manifest says " + entry.segmentId() + " with " + entry.rows());
      // a RENAME the files do not record yet: the segment answers under the schema's names
      final SegmentReader named = reader.withColumnNames(columns);
      if (named != reader) {
        readers.set(readers.size() - 1, named);
        reader.close();
      }
    }

    if (found.ownerMapFile() != null)
      owners = ContentOwnerMap.readFrom(bucketDir.resolve(found.ownerMapFile()));
    if (externalOwnersLock != null && found.externalOwnerMapFile() != null)
      externalOwners = ContentOwnerMap.readFrom(bucketDir.resolve(found.externalOwnerMapFile()));
  }

  private static boolean isEmpty(final LocalBucket bucket) {
    return bucket.getCommittedPageCount() == 0 || bucket.count() == 0;
  }

  /**
   * V-B1: writes and publishes the generation-1 manifest of an empty bucket, with an empty durable dictionary per ordinal column.
   *
   * @throws UncheckedIOException when a file cannot be written: a write failure is no reason to rebuild, the open fails
   */
  private ColumnarManifest publishEmpty(final Path databasePath, final long lastTransactionId, final Map<String, GlobalDictionary> created) {
    final List<String> names = new ArrayList<>();
    try {
      ColumnarStore.createBucketDir(databasePath, bucketId);
      // crash leftovers of an earlier empty publish; the reserved generations are above them anyway
      ColumnarStore.deleteUnreferenced(bucketDir, Set.of(), inFlightFiles);
      final List<ColumnarManifest.DictionaryEntry> entries = new ArrayList<>();
      for (final ColumnDefinition column : columns) {
        if (column.kind() != ColumnKind.ORDINAL && column.kind() != ColumnKind.ORDINAL_LIST)
          continue;
        final int generation = reserveDictionaryGeneration(column.indexFileId());
        final String name = SegmentFormat.dictionaryFileName(column.indexFileId(), generation);
        names.add(name);
        final GlobalDictionary dictionary = GlobalDictionary.create(bucketDir.resolve(name));
        created.put(column.property(), dictionary);
        final GlobalDictionary.DurableState durable = dictionary.fsync();
        entries.add(new ColumnarManifest.DictionaryEntry(column.indexFileId(), generation, name, durable.entries(), durable.bytes()));
      }
      final ColumnarManifest empty = new ColumnarManifest(1, bucketId, bucketName, maxRecordsInPage, columns, List.of(), entries, null,
          null, lastTransactionId);
      empty.publish(bucketDir);
      return empty;
    } catch (final IOException | RuntimeException e) {
      for (final GlobalDictionary dictionary : created.values())
        dictionary.close();
      created.clear();
      if (e instanceof IOException io)
        throw new UncheckedIOException("Cannot publish the empty columnar projection of bucket '" + bucketName + "'", io);
      throw (RuntimeException) e;
    } finally {
      releaseFiles(names.toArray(new String[0]));
    }
  }

  /**
   * Reserves the id of a new segment, never used before by this directory, and holds its file name until {@link #releaseFiles}. The
   * dead set files of the new segment are held by the writer with {@link #holdFiles(String...)}.
   */
  int reserveSegmentId() {
    publishLock.lock();
    try {
      if (nextSegmentId == Integer.MAX_VALUE)
        throw new IllegalStateException("Columnar projection of bucket '" + bucketName + "' ran out of segment ids");
      final int id = nextSegmentId++;
      inFlightFiles.add(SegmentFormat.segmentFileName(id));
      return id;
    } finally {
      publishLock.unlock();
    }
  }

  /** Reserves a new dictionary generation of the column and holds its file name until {@link #releaseFiles}. */
  int reserveDictionaryGeneration(final int indexFileId) {
    publishLock.lock();
    try {
      final int generation = nextGeneration.getOrDefault(indexFileId, 0);
      if (generation == Integer.MAX_VALUE)
        throw new IllegalStateException("Columnar projection of bucket '" + bucketName + "' ran out of dictionary generations of index "
            + indexFileId);
      nextGeneration.put(indexFileId, generation + 1);
      inFlightFiles.add(SegmentFormat.dictionaryFileName(indexFileId, generation));
      return generation;
    } finally {
      publishLock.unlock();
    }
  }

  /** Protects the names (dead sets, owner maps, any file a writer creates) from every unreferenced-file deletion. */
  void holdFiles(final String... names) {
    publishLock.lock();
    try {
      Collections.addAll(inFlightFiles, names);
    } finally {
      publishLock.unlock();
    }
  }

  /** Ends the protection of {@link #holdFiles}, {@link #reserveSegmentId} and {@link #reserveDictionaryGeneration}. */
  void releaseFiles(final String... names) {
    publishLock.lock();
    try {
      for (final String name : names)
        inFlightFiles.remove(name);
    } finally {
      publishLock.unlock();
    }
  }

  /** The property of the column of the index file id, null when the projection has no such column. */
  String propertyOf(final int indexFileId) {
    for (final ColumnDefinition column : columns)
      if (column.indexFileId() == indexFileId)
        return column.property();
    return null;
  }

  /**
   * A consistent view for a query. The pending positions (both trackers, R7) are read BEFORE the state: a page leaves the pending set
   * only after the apply pass published its rows, so a page that is not pending any more is in the state read afterwards. Takes no
   * publish lock; only the owner map read locks.
   *
   * @throws IllegalStateException when the projection is closed or dropped
   */
  public ColumnarSnapshot acquire() {
    if (closed || dropped)
      throw new IllegalStateException("Columnar projection of bucket '" + bucketName + "' is " + (dropped ? "dropped" : "closed"));
    final PositionBitmap pending;
    ownersLock.readLock().lock();
    try {
      pending = tracker.pendingPositions(owners);
    } finally {
      ownersLock.readLock().unlock();
    }
    long oldest = tracker.oldestPendingEpochStartMs();
    if (externalTracker != null) {
      externalOwnersLock.readLock().lock();
      try {
        externalTracker.addPendingOwners(externalOwners, pending);
      } finally {
        externalOwnersLock.readLock().unlock();
      }
      final long external = externalTracker.oldestPendingEpochStartMs();
      oldest = oldest == 0 ? external : external == 0 ? oldest : Math.min(oldest, external);
    }
    ColumnarTestHooks.reach(ACQUIRE_BETWEEN);
    return new Snapshot(bucketId, retainState(), pending, oldest);
  }

  /**
   * A reference on the current state, released by the caller: its segment readers stay open until then (a merge reads them).
   *
   * @throws IllegalStateException when the projection is closed or dropped
   */
  ProjectionState retainState() {
    while (true) {
      final ProjectionState current = state.get();
      try {
        return current.retain();
      } catch (final IllegalStateException e) {
        // released by a publish that already swapped in a newer state: retry on it; released and still current: closed or dropped meanwhile
        if (state.get() == current)
          throw new IllegalStateException("Columnar projection of bucket '" + bucketName + "' is closed", e);
      }
    }
  }

  /** The current state, not retained: the caller holds {@link #applyLock()}, so its segment set cannot change meanwhile. */
  ProjectionState currentState() {
    return state.get();
  }

  /**
   * The current writer dictionary of the property, null when the column has none (a LONG or DOUBLE column, or a column added by
   * {@link #addColumn} that no rebuild published yet: the apply pass stages it as missing). Readers decode with
   * {@link ColumnarSnapshot#dictionary(String)} instead.
   */
  public GlobalDictionary dictionary(final String property) {
    return dictionaries.get(property);
  }

  /** The writer dictionaries by property, the map the current state was published with; immutable. */
  Map<String, GlobalDictionary> dictionaries() {
    return dictionaries;
  }

  int bucketId() {
    return bucketId;
  }

  String bucketName() {
    return bucketName;
  }

  /** The type name the stats of this projection are kept under. */
  String typeName() {
    return typeName;
  }

  Path bucketDir() {
    return bucketDir;
  }

  ColumnarStats stats() {
    return stats;
  }

  /** Rows per row block of the segments this projection writes ({@code arcadedb.columnar.segmentBlockRows} of the database). */
  int segmentBlockRows() {
    return segmentBlockRows;
  }

  /** False on a READ_ONLY database and once closed, dropped or stopped: no flush or merge writes then. */
  boolean canWrite() {
    return !readOnly && !closed && !dropped && !stopped;
  }

  /**
   * The database closes or drops: from now on no publisher writes a manifest (a running flush or merge aborts at its next step, deleting
   * only what it wrote before a rename was attempted) and no release deletes a file. Readers and dictionaries stay open until
   * {@link #close()}.
   */
  void stopPublishing() {
    publishLock.lock();
    try {
      stopped = true;
    } finally {
      publishLock.unlock();
    }
  }

  int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  /** A copy of the column definitions; the overlay's blocks are built with exactly these. */
  ColumnDefinition[] columns() {
    return columns.clone();
  }

  DirtyPageTracker tracker() {
    return tracker;
  }

  /** The primary owner map: the apply pass writes it under {@link #ownersLock()}'s write lock, other readers hold its read lock. */
  ContentOwnerMap owners() {
    return owners;
  }

  ReentrantReadWriteLock ownersLock() {
    return ownersLock;
  }

  /** The live overlay, owned by the apply thread. */
  ColumnarOverlay overlay() {
    return overlay;
  }

  /** R7: the tracker of the paired external bucket, null when the bucket has none. */
  DirtyPageTracker externalTracker() {
    return externalTracker;
  }

  /** R7: owners (primary positions) of the external bucket's pages, null when the bucket has no external bucket. */
  ContentOwnerMap externalOwners() {
    return externalOwners;
  }

  /** The lock of {@link #externalOwners()}, null when the bucket has no external bucket. */
  ReentrantReadWriteLock externalOwnersLock() {
    return externalOwnersLock;
  }

  /** R7: file id of the paired external bucket, -1 when none. */
  int externalFileId() {
    return externalFileId;
  }

  /** The manifest the current state was published with, null when none was loaded or published (missing or corrupt). */
  ColumnarManifest manifest() {
    return manifest;
  }

  /** The last reason passed to {@link #markUnusable} or recorded by {@link #open}, null when usable. */
  String unusableReason() {
    return unusableReason;
  }

  /** Serializes every publish (apply pass, flush, merge, rebuild, compaction) and the file deletion of a last release (V-M11). */
  ReentrantLock publishLock() {
    return publishLock;
  }

  /** Held for a whole apply pass (R13); {@link #addColumn} takes it so the overlay is re-keyed between two passes. */
  ReentrantLock applyLock() {
    return applyLock;
  }

  /**
   * True when the current state is valid and its manifest has the column of the index file id with the definition the projection has
   * for it ({@link ColumnarReconciler#sameData}, the property name aside): a column added, or retyped, after the last publish is not
   * published until a rebuild publishes it.
   */
  boolean isPublishedWith(final int indexFileId) {
    final ColumnarManifest current = manifest;
    if (current == null || !isUsable())
      return false;
    final ColumnDefinition own = column(indexFileId);
    if (own == null)
      return false;
    for (final ColumnDefinition column : current.columns())
      if (column.indexFileId() == indexFileId)
        return ColumnarReconciler.sameData(column, own);
    return false;
  }

  /** The columns of the projection whose definition the current manifest does not publish, see {@link #isPublishedWith}. */
  boolean hasUnpublishedColumn() {
    for (final ColumnDefinition column : columns)
      if (!isPublishedWith(column.indexFileId()))
        return true;
    return false;
  }

  /**
   * I2: a column the current manifest publishes with another definition than the projection's (a RETYPE whose {@code schema} rebuild
   * did not publish before a close); false without a valid manifest.
   */
  boolean hasRetypedColumn() {
    final ColumnarManifest current = manifest;
    if (current == null || !isUsable())
      return false;
    for (final ColumnDefinition published : current.columns()) {
      final ColumnDefinition own = column(published.indexFileId());
      if (own != null && !ColumnarReconciler.sameData(published, own))
        return true;
    }
    return false;
  }

  private ColumnDefinition column(final int indexFileId) {
    for (final ColumnDefinition column : columns)
      if (column.indexFileId() == indexFileId)
        return column;
    return null;
  }

  /** True when the current state is valid: a manifest was loaded or published and no {@link #markUnusable} came after it. */
  boolean isUsable() {
    return state.get().isUsable();
  }

  /** The overlay views of the current state, the live overlay last. */
  ColumnarSegmentView[] currentOverlaysForTesting() {
    return state.get().overlays();
  }

  /**
   * Makes a new state current and releases the projection's reference on the previous one. The caller holds {@link #publishLock()}
   * and has made every file the manifest names durable and published the manifest (Task 5 order); the projection takes ownership of
   * the readers it did not know yet and closes them once no retained state holds them. {@code overlays} must end with the live overlay
   * view. A usable state clears {@link #unusableReason()}. A publish whose segment array differs from the current one (a flush, merge,
   * rebuild, dead set change) or that passes another dictionaries map (a rebuild, a compaction) must also hold {@link #applyLock()}:
   * the apply pass encodes with the writer dictionaries under that lock, and a replaced generation may be closed at the next release.
   *
   * @param manifest     the published manifest the state reads, null for none (missing or corrupt); its {@code files()} are the state's
   *                     file names (V-B7)
   * @param dictionaries the dictionary generation of each property the segments and overlays are encoded with, immutable; becomes the
   *                     writer dictionaries
   *
   * @return false, publishing nothing, when the projection is closed or dropped: the caller still owns its new readers, closes them,
   * deletes the files it wrote for this publish and {@link #releaseFiles releases} their names
   *
   * @throws IllegalStateException when the publish lock is not held, or the segment set changes without the apply lock
   */
  boolean publish(final ColumnarManifest manifest, final SegmentReader[] segments, final ColumnarSegmentView[] overlays,
      final Map<String, GlobalDictionary> dictionaries, final boolean usable) {
    if (!publishLock.isHeldByCurrentThread())
      throw new IllegalStateException("Publishing the columnar projection of bucket '" + bucketName + "' needs its publish lock");
    final ProjectionState current = state.get();
    if (current != null && (!sameReaders(current.segments(), segments) || dictionaries != this.dictionaries)
        && !applyLock.isHeldByCurrentThread())
      throw new IllegalStateException(
          "Changing the segments or dictionaries of the columnar projection of bucket '" + bucketName + "' needs its apply lock");
    if (closed || dropped)
      return false;
    final ProjectionState next = new ProjectionState(manifest == null ? 0 : manifest.generation(), segments, overlays, dictionaries,
        manifest == null ? Set.of() : manifest.files(), usable, this::stateReleased);
    for (final SegmentReader reader : segments)
      if (openReaders.add(reader))
        segmentFiles.putIfAbsent(fileName(reader), reader.file());
    openDictionaries.addAll(dictionaries.values());
    retained.add(next);
    this.manifest = manifest;
    this.dictionaries = dictionaries;
    if (usable)
      unusableReason = null;
    stats.addSegments(typeName, segments.length - reportedSegments);
    reportedSegments = segments.length;
    reportOpenFiles();
    final ProjectionState previous = state.getAndSet(next);
    if (previous != null)
      previous.release();
    return true;
  }

  /** Under the publish lock: moves the type's open segment file count by what changed since the last report. */
  private void reportOpenFiles() {
    final int open = openReaders.size();
    stats.addOpenFiles(typeName, open - reportedOpenFiles);
    reportedOpenFiles = open;
  }

  private static boolean sameReaders(final SegmentReader[] a, final SegmentReader[] b) {
    if (a == b)
      return true;
    if (a.length != b.length)
      return false;
    for (int i = 0; i < a.length; i++)
      if (a[i] != b[i])
        return false;
    return true;
  }

  /**
   * Apply thread, end of a pass (the caller holds {@link #applyLock()}): publishes the overlay's current content as the state's live
   * overlay view and, when the pass superseded positions of the frozen overlay, the frozen view hiding them (V-B2), in the same state.
   * Nothing after drop.
   */
  void publishOverlay() {
    publishLock.lock();
    try {
      final ProjectionState current = state.get();
      final ColumnarSegmentView[] overlays = current.overlays().clone();
      overlays[overlays.length - 1] = overlay.publish();
      if (supersededChanged && frozenView != null && overlays.length > 1) {
        overlays[overlays.length - 2] = new MaskedView(frozenView, supersededInFrozen.snapshot());
        supersededChanged = false;
      }
      publish(manifest, current.segments(), overlays, dictionaries, current.isUsable());
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Marks dead every position of {@code deadNow} that a segment of the current state holds, by publishing a state whose readers of the
   * affected segments carry a NEW dead set (copy-on-write: a snapshot acquired before keeps counting them live). Positions no segment
   * holds are ignored. Persisting the dead sets is the next flush's job; the block cache is untouched. Takes {@link #applyLock()}
   * (reentrant: the apply pass already holds it); does nothing once the projection is closed or dropped.
   *
   * @throws IllegalArgumentException when {@code deadNow} has another page size than the bucket
   */
  void applyDeadPositions(final PositionBitmap deadNow) {
    if (deadNow.maxRecordsInPage() != maxRecordsInPage)
      throw new IllegalArgumentException("Dead positions of " + deadNow.maxRecordsInPage() + " slots per page for a bucket of " + maxRecordsInPage);
    if (deadNow.isEmpty() || closed || dropped)
      return;
    applyLock.lock();
    publishLock.lock();
    try {
      final ProjectionState current = state.get();
      final SegmentReader[] segments = current.segments();
      SegmentReader[] next = null;
      for (int i = 0; i < segments.length; i++) {
        final PositionBitmap dead = withNewDead(segments[i], deadNow);
        if (dead == null)
          continue;
        if (next == null)
          next = segments.clone();
        next[i] = segments[i].withDead(dead);
      }
      if (next != null && !publish(manifest, next, current.overlays(), dictionaries, current.isUsable()))
        for (int i = 0; i < next.length; i++)
          if (next[i] != segments[i])
            next[i].close();
    } finally {
      publishLock.unlock();
      applyLock.unlock();
    }
  }

  /** The segment's dead set plus the positions of {@code deadNow} it holds and still counts live, null when that adds nothing. */
  private PositionBitmap withNewDead(final SegmentReader segment, final PositionBitmap deadNow) {
    final int minPage = segment.minPage();
    if (minPage < 0)
      return null;
    final int maxPage = segment.maxPage();
    final PositionBitmap old = segment.deadSet();
    PositionBitmap result = null;
    for (final int page : deadNow.pages()) {
      if (page < minPage || page > maxPage)
        continue;
      final long[] stripe = deadNow.stripe(page);
      final long base = (long) page * maxRecordsInPage;
      for (int w = 0; w < stripe.length; w++)
        for (long word = stripe[w]; word != 0; word &= word - 1) {
          final long position = base + ((long) w << 6) + Long.numberOfTrailingZeros(word);
          if (segment.row(position) < 0 || old != null && old.get(position))
            continue;
          if (result == null)
            result = old == null ? new PositionBitmap(maxRecordsInPage) : old.copy();
          result.set(position);
        }
    }
    return result;
  }

  /**
   * Apply thread: reads the live value of column {@code column} (an index of {@link #columns()}) at the position into the same slot of
   * {@code out}, newest first: the live overlay, the frozen overlay of a running flush (unless the position was
   * {@link #supersedeFrozen superseded} there), then the segments of the current state from the newest, skipping dead rows. A live row
   * without a value for the column (a column the segment does not have, or a missing value) answers with {@code present} false.
   * Segment columns are matched by index file id, so a RENAME keeps its values. The caller holds {@link #applyLock()}, so the segment
   * set it reads cannot change during the pass (every publish changing it holds the same lock).
   *
   * @return {@link #LOOKUP_OVERLAY}, {@link #LOOKUP_FROZEN}, the id of the segment holding the live row, or {@link #LOOKUP_ABSENT}
   * (then {@code present} is false)
   */
  long liveValueLookup(final long position, final int column, final RowValues out) {
    if (position >= 0 && position / maxRecordsInPage <= Integer.MAX_VALUE) {
      if (readOverlayValue(overlay, position, column, out))
        return LOOKUP_OVERLAY;
      final ColumnarOverlay frozen = frozenOverlay;
      if (frozen != null && !supersededInFrozen.get(position) && readOverlayValue(frozen, position, column, out))
        return LOOKUP_FROZEN;
      final SegmentReader[] segments = state.get().segments();
      for (int i = segments.length - 1; i >= 0; i--) {
        final SegmentReader segment = segments[i];
        if (segment.row(position) < 0)
          continue;
        final PositionBitmap dead = segment.deadSet();
        if (dead != null && dead.get(position))
          continue;
        readSegmentValue(segment, position, columns[column], column, out);
        return segment.segmentId();
      }
    }
    out.present[column] = false;
    return LOOKUP_ABSENT;
  }

  /**
   * Apply thread, under {@link #applyLock()}: fills {@code out} ({@code maxRecordsInPage / 64} words) with the slots of the page that
   * {@link #liveValueLookup} answers live: the rows of the live overlay, those of the frozen overlay it did not
   * {@link #supersedeFrozen supersede}, and the rows of every segment its dead set does not hold. Allocates only for the segments
   * covering the page.
   */
  void liveSlots(final int page, final long[] out) {
    Arrays.fill(out, 0L);
    final OverlayPageBlock live = overlay.page(page);
    if (live != null)
      or(out, live.liveStripe());
    final ColumnarOverlay frozen = frozenOverlay;
    final OverlayPageBlock frozenBlock = frozen == null ? null : frozen.page(page);
    if (frozenBlock != null) {
      final long[] stored = frozenBlock.liveStripe();
      final long[] hidden = supersededInFrozen.stripe(page);
      for (int w = 0; w < out.length; w++)
        out[w] |= hidden == null ? stored[w] : stored[w] & ~hidden[w];
    }
    for (final SegmentReader segment : state.get().segments())
      if (segment.minPage() >= 0 && page >= segment.minPage() && page <= segment.maxPage()) {
        final long[] rows = segment.live(page, page + 1).stripe(page);
        if (rows != null)
          or(out, rows);
      }
  }

  private static void or(final long[] target, final long[] words) {
    for (int w = 0; w < target.length; w++)
      target[w] |= words[w];
  }

  /** The pages whose last apply found an unreadable record, retried with a backoff (apply thread, under {@link #applyLock()}). */
  ColumnarApplier.UnreadablePages unreadablePages() {
    return unreadablePages;
  }

  /** The apply pass's reusable buffers, null before the first pass; under {@link #applyLock()}. */
  ColumnarApplier.Pass applyScratch() {
    return applyScratch;
  }

  void setApplyScratch(final ColumnarApplier.Pass scratch) {
    this.applyScratch = scratch;
  }

  /** Reads the value of column k at the position from the overlay into {@code out}; false when the overlay holds no row there. */
  private boolean readOverlayValue(final ColumnarOverlay from, final long position, final int k, final RowValues out) {
    final OverlayPageBlock block = from.page((int) (position / maxRecordsInPage));
    if (block == null)
      return false;
    final int row = block.row((int) (position % maxRecordsInPage));
    if (row < 0)
      return false;
    readOverlayValue(block, row, k, out);
    return true;
  }

  private static void readOverlayValue(final OverlayPageBlock block, final int row, final int k, final RowValues out) {
    final boolean present = block.present(k, row);
    out.present[k] = present;
    if (!present)
      return;
    switch (block.columnsView()[k].kind()) {
    case ORDINAL -> out.ordinal[k] = (int) block.scalar(k, row);
    case LONG -> out.longs[k] = block.scalar(k, row);
    case DOUBLE -> out.doubles[k] = Double.longBitsToDouble(block.scalar(k, row));
    case ORDINAL_LIST -> {
      int count = block.ordinals(k, row, listOf(out, k, 0));
      if (count > out.list[k].length)
        count = block.ordinals(k, row, listOf(out, k, count));
      out.listCount[k] = count;
    }
    }
  }

  /**
   * Reads the stored value of the column {@code definition} (matched by index file id and kind) at the position of the segment into
   * slot {@code k} of {@code out}, whatever the segment's dead set says; missing when the segment has no such column or value.
   */
  static void readSegmentValue(final SegmentReader segment, final long position, final ColumnDefinition definition, final int k,
      final RowValues out) {
    readValue(columnReader(segment, definition), position, definition, k, out);
  }

  /** The segment's reader of the column {@code definition} (by index file id, same kind), null when the segment has no such column. */
  static ColumnReader columnReader(final SegmentReader segment, final ColumnDefinition definition) {
    final ColumnReader reader = segment.columnByIndexFileId(definition.indexFileId());
    return reader == null || reader.kind() != definition.kind() ? null : reader;
  }

  /** Reads the value at the position from {@code reader} (null: no such column) into slot {@code k} of {@code out}. */
  static void readValue(final ColumnReader reader, final long position, final ColumnDefinition definition, final int k,
      final RowValues out) {
    if (reader == null || !reader.isPresent(position)) {
      out.present[k] = false;
      return;
    }
    out.present[k] = true;
    switch (definition.kind()) {
    case ORDINAL -> out.ordinal[k] = reader.ordinal(position);
    case LONG -> out.longs[k] = reader.longValue(position);
    case DOUBLE -> out.doubles[k] = reader.doubleValue(position);
    case ORDINAL_LIST -> {
      int count = reader.ordinals(position, listOf(out, k, 0));
      if (count > out.list[k].length)
        count = reader.ordinals(position, listOf(out, k, count));
      out.listCount[k] = count;
    }
    }
  }

  /** The list array of slot {@code k}, grown to at least {@code length} entries. */
  private static int[] listOf(final RowValues out, final int k, final int length) {
    if (out.list[k] == null || out.list[k].length < length)
      out.list[k] = new int[Math.max(length, 8)];
    return out.list[k];
  }

  /**
   * V-B4: adds a column definition. The published state is unchanged and the column reads as missing (no writer dictionary, see
   * {@link #dictionary}) until the next rebuild publishes it. The overlay is re-keyed to the new columns (its blocks rebuilt as NEW
   * blocks with the column missing, R13) under {@link #applyLock()} only, then swapped in under {@link #publishLock()}, so the next apply
   * pass builds blocks the overlay accepts. The frozen overlay of a running flush is re-keyed too, so {@link #liveValueLookup} reads it
   * with the new column indexes; the flush keeps writing the overlay it froze. Every overlay mutation (apply pass, a flush's freeze) must
   * hold {@link #applyLock()}. A column whose index file id is already projected is ignored.
   */
  void addColumn(final ColumnDefinition column) {
    applyLock.lock();
    try {
      final ColumnDefinition[] current = columns;
      for (final ColumnDefinition existing : current)
        if (existing.indexFileId() == column.indexFileId())
          return;
      final ColumnDefinition[] next = Arrays.copyOf(current, current.length + 1);
      next[current.length] = column;
      // rebuilt outside the publish lock, so a release on a query thread never waits for it: the apply lock keeps the overlay still
      final ColumnarOverlay rekeyed = rekey(overlay, current, next);
      final ColumnarOverlay rekeyedFrozen = frozenOverlay == null ? null : rekey(frozenOverlay, current, next);
      publishLock.lock();
      try {
        overlay = rekeyed;
        if (rekeyedFrozen != null)
          frozenOverlay = rekeyedFrozen;
        columns = next;
      } finally {
        publishLock.unlock();
      }
    } finally {
      applyLock.unlock();
    }
  }

  /**
   * A new overlay over {@code next} holding the rows of {@code from}; values are carried by index file id when the column stores the
   * same data (a rename included, {@link ColumnarReconciler#sameData}), other columns are missing.
   */
  private ColumnarOverlay rekey(final ColumnarOverlay from, final ColumnDefinition[] previous, final ColumnDefinition[] next) {
    final ColumnarOverlay result = new ColumnarOverlay(maxRecordsInPage, next, System::currentTimeMillis);
    if (from.rows() == 0)
      return result;
    final int[] source = new int[next.length];
    for (int k = 0; k < next.length; k++) {
      source[k] = -1;
      for (int j = 0; j < previous.length; j++)
        if (ColumnarReconciler.sameData(previous[j], next[k]))
          source[k] = j;
    }
    final RowBuffer rows = new RowBuffer(maxRecordsInPage, next);
    final RowValues read = new RowValues(previous.length);
    final RowValues write = new RowValues(next.length);
    for (final int page : from.pages()) {
      final OverlayPageBlock block = from.page(page);
      rows.clear();
      for (int row = 0; row < block.rowCount(); row++) {
        block.readRow(row, read);
        for (int k = 0; k < next.length; k++) {
          final int j = source[k];
          write.present[k] = j >= 0 && read.present[j];
          if (j < 0)
            continue;
          write.ordinal[k] = read.ordinal[j];
          write.list[k] = read.list[j];
          write.listCount[k] = read.listCount[j];
          write.longs[k] = read.longs[j];
          write.doubles[k] = read.doubles[j];
        }
        rows.put((int) (block.position(row) % maxRecordsInPage), write);
      }
      result.replacePage(OverlayPageBlock.build(page, maxRecordsInPage, next, block.pageVersion(), rows));
    }
    return result;
  }

  /**
   * The reconciler (spec 6.4): makes {@code next} the columns, keyed by index file id. A column of {@code next} that stores the same
   * data as a current one ({@link ColumnarReconciler#sameData}) keeps its values and its dictionary under its new property name
   * (RENAME: the segments of the current state answer under the new names through {@link SegmentReader#withColumnNames}); a current
   * column {@code next} lacks is dropped with its writer dictionary; a column whose type changed (RETYPE) keeps the dictionary of its
   * stored values but is not published ({@link #isPublishedWith}) and its new rows read as missing until a rebuild publishes it; a new
   * column reads as missing until then too. The overlays (live, and frozen by a failed flush) are re-keyed and a state is published with them,
   * under the apply and publish locks. The manifest names the new columns at the next publish ({@link #manifestColumns}).
   * <p>
   * Takes the maintenance slot for the change, so no flush, merge, compaction or rebuild built on the old columns publishes after it.
   *
   * @return false, changing nothing, when another job holds the maintenance slot: the caller retries later
   */
  boolean redefineColumns(final ColumnDefinition[] next) {
    if (!tryBeginMaintenance())
      return false;
    try {
      applyLock.lock();
      try {
        final ColumnDefinition[] previous = columns;
        final ColumnarOverlay rekeyed = rekey(overlay, previous, next);
        final ColumnarOverlay rekeyedFrozen = frozenOverlay == null ? null : rekey(frozenOverlay, previous, next);
        // by index file id: a renamed column's dictionary under its new name; a retyped one keeps the dictionary its segments are
        // encoded with until the schema rebuild replaces it (the column is not published meanwhile, see isPublishedWith)
        final Map<String, GlobalDictionary> carried = new HashMap<>();
        for (final ColumnDefinition column : next)
          for (final ColumnDefinition was : previous)
            if (was.indexFileId() == column.indexFileId() && dictionaries.get(was.property()) != null)
              carried.put(column.property(), dictionaries.get(was.property()));
        publishLock.lock();
        try {
          overlay = rekeyed;
          columns = next.clone();
          final ColumnarSegmentView[] overlays;
          if (rekeyedFrozen != null) {
            frozenOverlay = rekeyedFrozen;
            frozenView = rekeyedFrozen.publish();
            final ColumnarSegmentView frozen = supersededInFrozen.isEmpty() ? frozenView : new MaskedView(frozenView, supersededInFrozen.snapshot());
            supersededChanged = false;
            overlays = new ColumnarSegmentView[] { frozen, rekeyed.publish() };
          } else
            overlays = new ColumnarSegmentView[] { rekeyed.publish() };
          final ProjectionState current = state.get();
          final SegmentReader[] segments = current.segments();
          final SegmentReader[] named = segments.clone();
          for (int i = 0; i < named.length; i++)
            named[i] = segments[i].withColumnNames(columns);
          if (!publish(manifest, named, overlays, Map.copyOf(carried), current.isUsable()))
            for (int i = 0; i < named.length; i++)
              if (named[i] != segments[i])
                named[i].close();
        } finally {
          publishLock.unlock();
        }
      } finally {
        applyLock.unlock();
      }
      return true;
    } finally {
      endMaintenance();
    }
  }

  /**
   * The columns a publish built on {@code current} names: those of {@code current} the projection still has, under the projection's
   * definition when it stores the same data (a RENAME), else as published (a RETYPE is named once its rebuild publishes); a column the
   * reconciler dropped is left out.
   */
  ColumnDefinition[] manifestColumns(final ColumnarManifest current) {
    final ColumnDefinition[] own = columns;
    final List<ColumnDefinition> result = new ArrayList<>(own.length);
    for (final ColumnDefinition published : current.columns())
      for (final ColumnDefinition column : own)
        if (column.indexFileId() == published.indexFileId()) {
          result.add(ColumnarReconciler.sameData(published, column) ? column : published);
          break;
        }
    return result.toArray(new ColumnDefinition[0]);
  }

  /**
   * The reconciler, after {@code ALTER TYPE ... NAME} (the bucket files are renamed in place, the directory keyed by the file id stays,
   * R9): the bucket and type names the next manifest and the stats use. The segment count moves to the new type name.
   */
  void rename(final String newBucketName, final String newTypeName) {
    publishLock.lock();
    try {
      if (!newTypeName.equals(typeName)) {
        stats.addSegments(typeName, -reportedSegments);
        stats.addSegments(newTypeName, reportedSegments);
        stats.addOpenFiles(typeName, -reportedOpenFiles);
        stats.addOpenFiles(newTypeName, reportedOpenFiles);
        typeName = newTypeName;
      }
      bucketName = newBucketName;
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Records the reason ({@code missing} or {@code corrupt}) and stops serving: the current state is republished unusable, so queries
   * take the fallback. On a writable database the rebuild requester is called with the reason, outside the publish lock; on a
   * READ_ONLY one nothing is requested or written (R16).
   */
  void markUnusable(final String rebuildReason) {
    markUnusableWithoutRequest(rebuildReason);
    requestRebuild(rebuildReason);
  }

  /** The first half of {@link #markUnusable}: records the reason and republishes the state unusable, requesting nothing. */
  void markUnusableWithoutRequest(final String rebuildReason) {
    publishLock.lock();
    try {
      unusableReason = rebuildReason;
      final ProjectionState current = state.get();
      if (current.isUsable())
        publish(manifest, current.segments(), current.overlays(), dictionaries, false);
    } finally {
      publishLock.unlock();
    }
  }

  /** The second half of {@link #markUnusable}: calls the rebuild requester, on a writable, open projection only. Takes no lock. */
  void requestRebuild(final String rebuildReason) {
    if (!readOnly && !closed && !dropped && !stopped)
      rebuildRequester.accept(rebuildReason);
  }

  /** The rebuild entry of {@link #markUnusable}; a no-op by default. */
  void setRebuildRequester(final Consumer<String> requester) {
    this.rebuildRequester = requester;
  }

  /**
   * Releases the projection's reference on its state and closes the dictionaries (their reads stay valid for snapshots still held).
   * Files are kept: a release after close deletes nothing, so a projection reopened on the same directory cannot lose a file.
   */
  void close() {
    publishLock.lock();
    try {
      if (closed)
        return;
      closed = true;
      dropOwnReference();
      closeDictionaries();
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * P52: stops publishing and releases the projection's reference; the bucket directory is deleted when the last retained state is
   * released, at once when no snapshot holds one.
   */
  void drop() {
    publishLock.lock();
    try {
      if (dropped)
        return;
      dropped = true;
      dropOwnReference();
      if (retained.isEmpty())
        deleteDirectory();
    } finally {
      publishLock.unlock();
    }
  }

  private void dropOwnReference() {
    if (ownReferenceDropped)
      return;
    ownReferenceDropped = true;
    stats.addSegments(typeName, -reportedSegments);
    reportedSegments = 0;
    state.get().release();
  }

  /** {@code onLastRelease} of every state, on whichever thread released last (R2). */
  private void stateReleased(final ProjectionState released) {
    publishLock.lock();
    try {
      retained.remove(released);
      final Set<SegmentReader> heldReaders = Collections.newSetFromMap(new IdentityHashMap<>());
      final Set<String> heldFiles = new HashSet<>();
      for (final ProjectionState other : retained) {
        heldFiles.addAll(other.files());
        for (final SegmentReader reader : other.segments())
          if (heldReaders.add(reader))
            heldFiles.add(fileName(reader));
      }
      for (final Iterator<SegmentReader> it = openReaders.iterator(); it.hasNext(); ) {
        final SegmentReader reader = it.next();
        if (!heldReaders.contains(reader)) {
          reader.close();
          it.remove();
        }
      }
      reportOpenFiles();

      if (dropped) {
        if (retained.isEmpty())
          deleteDirectory();
        return;
      }
      if (openDictionaries.size() > dictionaries.size())
        closeUnheldDictionaries(); // a replaced generation exists; an overlay publish skips this
      final ColumnarManifest current = manifest;
      if (readOnly || closed || stopped || current == null || !namesUnreferenced(released, current.files(), heldFiles))
        return;
      heldFiles.addAll(inFlightFiles);
      for (final String name : ColumnarStore.deleteUnreferenced(bucketDir, current.files(), heldFiles)) {
        final SegmentFile file = segmentFiles.remove(name);
        if (file != null)
          file.deleted();
      }
    } finally {
      publishLock.unlock();
    }
  }

  /** Closes the dictionary generations neither a retained state nor the writer map holds, before their files can be deleted. */
  private void closeUnheldDictionaries() {
    final Set<GlobalDictionary> held = Collections.newSetFromMap(new IdentityHashMap<>());
    held.addAll(dictionaries.values());
    for (final ProjectionState other : retained)
      held.addAll(other.dictionaries().values());
    for (final Iterator<GlobalDictionary> it = openDictionaries.iterator(); it.hasNext(); ) {
      final GlobalDictionary dictionary = it.next();
      if (!held.contains(dictionary)) {
        dictionary.close();
        it.remove();
      }
    }
  }

  /** Whether the released state read a file nobody references any more, so most releases (overlay publishes) skip the directory scan. */
  private static boolean namesUnreferenced(final ProjectionState released, final Set<String> referenced, final Set<String> held) {
    for (final String name : released.files())
      if (!referenced.contains(name) && !held.contains(name))
        return true;
    for (final SegmentReader reader : released.segments()) {
      final String name = fileName(reader);
      if (!referenced.contains(name) && !held.contains(name))
        return true;
    }
    return false;
  }

  /** P52, V-M11: invalidates the cache of every segment opened, then deletes the bucket directory; once. */
  private void deleteDirectory() {
    if (directoryDeleted)
      return;
    directoryDeleted = true;
    for (final SegmentReader reader : openReaders)
      reader.close();
    openReaders.clear();
    reportOpenFiles();
    for (final SegmentFile file : segmentFiles.values())
      file.deleted();
    segmentFiles.clear();
    closeDictionaries();
    if (!readOnly)
      FileUtils.deleteRecursively(bucketDir.toFile());
  }

  private void closeDictionaries() {
    for (final GlobalDictionary dictionary : openDictionaries)
      dictionary.close();
    openDictionaries.clear();
  }

  private static String fileName(final SegmentReader reader) {
    return reader.file().path().getFileName().toString();
  }

  /** V-B6: claims the projection's single maintenance slot (flush, merge, compaction); false when another job holds it. */
  boolean tryBeginMaintenance() {
    return maintenance.compareAndSet(false, true);
  }

  /** Releases the slot of {@link #tryBeginMaintenance()}. */
  void endMaintenance() {
    maintenance.set(false);
  }

  /** The no-WAL epoch: the number of no-WAL marks so far (R8, V-B3). */
  long noWalEpoch() {
    return noWalEpoch.get();
  }

  /** Task 13, after setting the bits of a no-WAL commit: advances the no-WAL epoch and returns its new value. */
  long bumpNoWalEpoch() {
    return noWalEpoch.incrementAndGet();
  }

  /** Clock time of the last published flush, the creation time before the first: the age a deletes-only flush waits for. */
  long lastFlushMs() {
    return lastFlushMs;
  }

  /** True while a flush froze an overlay and did not publish it yet (running, or failed and to be retried). */
  boolean hasFrozenOverlay() {
    return frozenOverlay != null;
  }

  /**
   * Flush, first step; the caller holds {@link #applyLock()}, so no apply pass runs. Clears {@link #appliedSinceFreeze} and the
   * superseded positions, freezes the live overlay and publishes its view before the now empty live one. A frozen overlay left by a
   * failed flush is returned again, its records kept.
   *
   * @return the frozen overlay, null when there is no row to write
   */
  ColumnarOverlay freezeOverlay() {
    if (!applyLock.isHeldByCurrentThread())
      throw new IllegalStateException("Freezing the overlay of the columnar projection of bucket '" + bucketName + "' needs its apply lock");
    publishLock.lock();
    try {
      if (frozenOverlay != null)
        return frozenOverlay;
      clearFreezeRecords();
      if (overlay.rows() == 0)
        return null;
      final ColumnarOverlay frozen = overlay.freeze();
      frozenOverlay = frozen;
      frozenView = frozen.publish();
      final ProjectionState current = state.get();
      publish(manifest, current.segments(), new ColumnarSegmentView[] { frozenView, overlay.publish() }, dictionaries, current.isUsable());
      return frozen;
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Apply thread (holds {@link #applyLock()}): the pass updated or deleted a position whose live value {@link #liveValueLookup} answered
   * from the frozen overlay ({@link #LOOKUP_FROZEN}). The next {@link #publishOverlay()} hides it in the frozen view and the flushed
   * segment starts with it dead (V-B2). Ignored when no overlay is frozen.
   */
  void supersedeFrozen(final long position) {
    if (frozenOverlay == null)
      return;
    supersededInFrozen.set(position);
    supersededChanged = true;
  }

  /** Flush, under {@link #applyLock()}: a copy of the positions superseded in the frozen overlay, null when there is none. */
  PositionBitmap supersededInFrozenCopy() {
    return supersededInFrozen.isEmpty() ? null : supersededInFrozen.copy();
  }

  /**
   * Apply pass (holds {@link #applyLock()}), after {@code tracker().completed}: records the first {@code count} primary pages as applied
   * since the freeze (V-B2).
   */
  void pagesApplied(final int[] pages, final int count) {
    addApplied(false, pages, count);
  }

  /** {@link #pagesApplied} for pages of the external tracker (R7). */
  void externalPagesApplied(final int[] pages, final int count) {
    addApplied(true, pages, count);
  }

  private void addApplied(final boolean external, final int[] pages, final int count) {
    if (!applyLock.isHeldByCurrentThread())
      throw new IllegalStateException("Recording applied pages of bucket '" + bucketName + "' needs its apply lock");
    publishLock.lock();
    try {
      final IntHashSet target = external ? externalAppliedSinceFreeze : appliedSinceFreeze;
      final IntHashSet duringRebuild = external ? externalAppliedDuringRebuild : appliedDuringRebuild;
      for (int i = 0; i < count; i++) {
        target.add(pages[i]);
        if (duringRebuild != null)
          duringRebuild.add(pages[i]);
      }
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Rebuild, before its scan: from now on every page an apply pass completes is recorded, so {@link #rebuildPublished} marks it dirty
   * again and nothing applied to the old state while the scan ran is lost.
   */
  void beginRebuildTracking() {
    publishLock.lock();
    try {
      appliedDuringRebuild = new IntHashSet();
      externalAppliedDuringRebuild = externalTracker == null ? null : new IntHashSet();
    } finally {
      publishLock.unlock();
    }
  }

  /** Rebuild, at its end whatever the outcome: no page is recorded any more. */
  void endRebuildTracking() {
    publishLock.lock();
    try {
      appliedDuringRebuild = null;
      externalAppliedDuringRebuild = null;
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Rebuild of a projection with no valid manifest (missing or corrupt), before it writes anything: deletes every file of the bucket
   * directory but the held ones, so no orphan of the old files can collide with a name the rebuild publishes. The manifest, its tmp
   * file and the {@code NOWAL} marker are kept ({@link ColumnarStore#deleteUnreferenced}). Nothing when a manifest is current.
   */
  void deleteOrphansForRebuild() {
    publishLock.lock();
    try {
      if (manifest == null && !readOnly && Files.isDirectory(bucketDir))
        ColumnarStore.deleteUnreferenced(bucketDir, Set.of(), inFlightFiles);
    } finally {
      publishLock.unlock();
    }
  }

  /**
   * Rebuild, last step, under {@link #applyLock()} and {@link #publishLock()} once its manifest is renamed: the rebuilt segments,
   * dictionaries and owner maps replace the current ones in one publish (usable), with a new empty live overlay over the current
   * columns. The previous state's files go through the last-release path (R1/R2). Every page an apply pass completed during the scan is
   * marked dirty again, so it is re-applied over the rebuilt state; the other unflushed pages leave the trackers (the rebuilt state holds
   * their committed content). A frozen overlay left by a failed flush is dropped, and the unreadable page retries are forgotten.
   *
   * @return false, publishing nothing, when the projection is closed or dropped
   */
  boolean rebuildPublished(final ColumnarManifest rebuilt, final SegmentReader[] segments, final Map<String, GlobalDictionary> rebuiltDictionaries,
      final ContentOwnerMap rebuiltOwners, final ContentOwnerMap rebuiltExternalOwners) {
    if (!publishLock.isHeldByCurrentThread() || !applyLock.isHeldByCurrentThread())
      throw new IllegalStateException("Publishing the rebuild of bucket '" + bucketName + "' needs its apply and publish locks");
    if (closed || dropped)
      return false;
    ownersLock.writeLock().lock();
    try {
      owners = rebuiltOwners;
    } finally {
      ownersLock.writeLock().unlock();
    }
    if (externalOwnersLock != null) {
      externalOwnersLock.writeLock().lock();
      try {
        externalOwners = rebuiltExternalOwners;
      } finally {
        externalOwnersLock.writeLock().unlock();
      }
    }
    overlay = new ColumnarOverlay(maxRecordsInPage, columns, System::currentTimeMillis);
    frozenOverlay = null;
    frozenView = null;
    clearFreezeRecords();
    publish(rebuilt, segments, new ColumnarSegmentView[] { overlay.publish() }, rebuiltDictionaries, true);
    ownersPublished();
    unreadablePages.clear();
    remarkAndForgetUnflushed(tracker, appliedDuringRebuild);
    if (externalTracker != null)
      remarkAndForgetUnflushed(externalTracker, externalAppliedDuringRebuild);
    appliedDuringRebuild = null;
    externalAppliedDuringRebuild = null;
    lastFlushMs = System.currentTimeMillis();
    return true;
  }

  /** Marks the applied pages dirty first, then clears the unflushed ones: a page is always in one place of the tracker. */
  private static void remarkAndForgetUnflushed(final DirtyPageTracker target, final IntHashSet applied) {
    if (applied != null && applied.size() > 0) {
      final int[] pages = applied.toArray();
      target.mark(pages, pages.length);
    }
    final int[] unflushed = target.unflushedPages();
    target.flushed(unflushed, unflushed.length, page -> false);
  }

  /**
   * Flush, last step, under {@link #applyLock()} and {@link #publishLock()} once the state holding the new segment is published: the
   * pages unflushed at the freeze leave the trackers unless the apply pass completed them again since (R14), and the frozen overlay is
   * gone.
   */
  void flushPublished(final int[] pages, final int[] externalPages) {
    if (!publishLock.isHeldByCurrentThread() || !applyLock.isHeldByCurrentThread())
      throw new IllegalStateException("Completing a flush of bucket '" + bucketName + "' needs its apply and publish locks");
    tracker.flushed(pages, pages.length, appliedSinceFreeze::contains);
    if (externalTracker != null)
      externalTracker.flushed(externalPages, externalPages.length, externalAppliedSinceFreeze::contains);
    frozenOverlay = null;
    frozenView = null;
    clearFreezeRecords();
    lastFlushMs = System.currentTimeMillis();
  }

  private void clearFreezeRecords() {
    appliedSinceFreeze.clear();
    if (externalAppliedSinceFreeze != null)
      externalAppliedSinceFreeze.clear();
    if (!supersededInFrozen.isEmpty())
      supersededInFrozen = new PositionBitmap(maxRecordsInPage);
    supersededChanged = false;
  }

  /**
   * The manifest generation the next flush, merge or compaction publishes, under {@link #applyLock()}: one above the current manifest
   * and above every generation an uncertain publish may have used ({@link #publishUncertain}).
   */
  long nextGeneration() {
    final ColumnarManifest current = manifest;
    return Math.max(current == null ? 0 : current.generation(), generationFloor) + 1;
  }

  /**
   * Under {@link #applyLock()}: a publish failed after it started replacing the manifest and the file on disk is not known to be its own.
   * Its files stay held and the next publish uses a higher generation, so no name it may have published is ever written again.
   */
  void publishUncertain(final long generation) {
    generationFloor = Math.max(generationFloor, generation);
  }

  /** Whether the name is held by {@link #holdFiles}, {@link #reserveSegmentId} or {@link #reserveDictionaryGeneration}. */
  boolean isHeld(final String name) {
    publishLock.lock();
    try {
      return inFlightFiles.contains(name);
    } finally {
      publishLock.unlock();
    }
  }

  Set<String> heldFilesForTesting() {
    publishLock.lock();
    try {
      return Set.copyOf(inFlightFiles);
    } finally {
      publishLock.unlock();
    }
  }

  /** Under {@link #applyLock()}, after a manifest publish (or the load): the owner maps as they are now are the published ones. */
  void ownersPublished() {
    publishedOwners = owners;
    publishedOwnersModifications = owners.modifications();
    if (externalOwnersLock != null) {
      publishedExternalOwners = externalOwners;
      publishedExternalOwnersModifications = externalOwners.modifications();
    }
  }

  /** Under {@link #applyLock()}: whether an owner map changed since {@link #ownersPublished()}. */
  boolean ownersChangedSincePublish() {
    if (owners != publishedOwners || owners.modifications() != publishedOwnersModifications)
      return true;
    return externalOwnersLock != null && (externalOwners != publishedExternalOwners
        || externalOwners.modifications() != publishedExternalOwnersModifications);
  }

  /**
   * True once {@link #close()}, {@link #drop()} or {@link #stopPublishing()} ran; a publisher checks it under {@link #publishLock()}
   * before its manifest.
   */
  boolean isClosedOrDropped() {
    return closed || dropped || stopped;
  }

  /**
   * The frozen overlay's view hiding the positions the apply pass superseded after the freeze (V-B2), so a position counts once: its
   * newer value is in the live overlay, or it was deleted. Point reads answer the stored row, as every {@link ColumnReader} does.
   */
  private static final class MaskedView implements ColumnarSegmentView {
    private final    ColumnarSegmentView inner;
    private final    PositionBitmap      hidden;
    private volatile PositionBitmap      live;

    private MaskedView(final ColumnarSegmentView inner, final PositionBitmap hidden) {
      this.inner = inner;
      this.hidden = hidden;
    }

    @Override
    public PositionBitmap live() {
      PositionBitmap result = live;
      if (result == null) {
        result = inner.live().copy();
        result.andNot(hidden);
        live = result;
      }
      return result;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      final PositionBitmap result = inner.live(fromPage, toPageExclusive);
      result.andNot(hidden);
      return result;
    }

    @Override
    public ColumnReader column(final String property) {
      final ColumnReader column = inner.column(property);
      return column == null ? null : new MaskedColumn(column, hidden);
    }

    @Override
    public long minLong(final String property) {
      return inner.minLong(property);
    }

    @Override
    public long maxLong(final String property) {
      return inner.maxLong(property);
    }
  }

  private record MaskedColumn(ColumnReader inner, PositionBitmap hidden) implements ColumnReader {
    @Override
    public ColumnKind kind() {
      return inner.kind();
    }

    @Override
    public boolean isPresent(final long position) {
      return inner.isPresent(position);
    }

    @Override
    public int ordinal(final long position) {
      return inner.ordinal(position);
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      return inner.ordinals(position, out);
    }

    @Override
    public long longValue(final long position) {
      return inner.longValue(position);
    }

    @Override
    public double doubleValue(final long position) {
      return inner.doubleValue(position);
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      final PositionBitmap visible = matches.copy();
      visible.andNot(hidden);
      inner.forEachOrdinal(visible, consumer);
    }
  }

  /** A held state seen through the {@link ColumnarSnapshot} contract; releases it once. */
  private static final class Snapshot implements ColumnarSnapshot {
    private static final AtomicIntegerFieldUpdater<Snapshot> CLOSED = AtomicIntegerFieldUpdater.newUpdater(Snapshot.class, "closed");

    private final int                       bucketId;
    private final ProjectionState           state;
    private final PositionBitmap            pending;
    private final long                      oldestPendingEpochStartMs;
    private final List<ColumnarSegmentView> views;
    private volatile int                    closed;

    private Snapshot(final int bucketId, final ProjectionState state, final PositionBitmap pending, final long oldestPendingEpochStartMs) {
      this.bucketId = bucketId;
      this.state = state;
      this.pending = pending;
      this.oldestPendingEpochStartMs = oldestPendingEpochStartMs;
      final SegmentReader[] segments = state.segments();
      final ColumnarSegmentView[] overlays = state.overlays();
      final ColumnarSegmentView[] all = Arrays.copyOf(segments, segments.length + overlays.length, ColumnarSegmentView[].class);
      System.arraycopy(overlays, 0, all, segments.length, overlays.length);
      this.views = List.of(all);
    }

    @Override
    public int bucketId() {
      return bucketId;
    }

    @Override
    public List<ColumnarSegmentView> segments() {
      return views;
    }

    @Override
    public PositionBitmap pendingPositions() {
      return pending;
    }

    @Override
    public long oldestPendingEpochStartMs() {
      return oldestPendingEpochStartMs;
    }

    @Override
    public boolean isUsable() {
      return state.isUsable();
    }

    @Override
    public GlobalDictionary dictionary(final String property) {
      return state.dictionary(property);
    }

    @Override
    public void close() {
      if (CLOSED.compareAndSet(this, 0, 1))
        state.release();
    }
  }
}
