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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.utility.FileUtils;

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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
  static final String MISSING         = "missing";
  static final String CORRUPT         = "corrupt";
  /** Reached by {@link #acquire()} between the read of the pending positions and the read of the state. */
  static final String ACQUIRE_BETWEEN = "acquire.pendingRead";

  private static final SegmentReader[] NO_SEGMENTS       = new SegmentReader[0];
  private static final String          SEGMENT_PREFIX    = "seg-";
  private static final String          DICTIONARY_PREFIX = "dict-";
  private static final String          DICTIONARY_SUFFIX = ".dic";

  private final int              bucketId;
  private final String           bucketName;
  private final String           typeName;
  private final Path             bucketDir;
  private final boolean          readOnly;
  private final int              maxRecordsInPage;
  private final ColumnarStats    stats;
  private final DirtyPageTracker tracker;
  private final DirtyPageTracker externalTracker;
  private final int              externalFileId;

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

  // guarded by publishLock
  private final Set<ProjectionState>     retained            = new HashSet<>();
  private final Set<SegmentReader>       openReaders         = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<String, SegmentFile> segmentFiles        = new HashMap<>();
  private final Set<GlobalDictionary>    openDictionaries    = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<String>              inFlightFiles       = new HashSet<>();
  private final Map<Integer, Integer>    nextGeneration      = new HashMap<>();
  private int                            nextSegmentId;
  private int                            reportedSegments;
  private boolean                        ownReferenceDropped;
  private boolean                        directoryDeleted;
  private volatile boolean               closed;
  private volatile boolean               dropped;

  private ColumnarProjection(final Path databasePath, final LocalBucket bucket, final String typeName, final ColumnDefinition[] columns,
      final ColumnarStats stats, final boolean readOnly, final LocalBucket external) {
    this.bucketId = bucket.getFileId();
    this.bucketName = bucket.getName();
    this.typeName = typeName;
    this.bucketDir = ColumnarStore.bucketDir(databasePath, bucketId);
    this.readOnly = readOnly;
    this.maxRecordsInPage = bucket.getMaxRecordsInPage();
    this.stats = stats;
    this.columns = columns.clone();
    this.overlay = new ColumnarOverlay(maxRecordsInPage, this.columns, System::currentTimeMillis);
    this.tracker = new DirtyPageTracker(maxRecordsInPage, System::currentTimeMillis);
    if (external == null) {
      this.externalTracker = null;
      this.externalFileId = -1;
      this.externalOwnersLock = null;
    } else {
      this.externalTracker = new DirtyPageTracker(external.getMaxRecordsInPage(), System::currentTimeMillis);
      this.externalFileId = external.getFileId();
      this.externalOwnersLock = new ReentrantReadWriteLock();
      this.externalOwners = new ContentOwnerMap();
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
        type == null ? bucket.getName() : type.getName(), columns, stats, db.getMode() == ComponentFile.MODE.READ_ONLY, external);
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
        // first, on its own: a manifest that fails to load must still leave the reservations above every existing file
        seedFromDirectory();
        found = ColumnarManifest.load(bucketDir);
        if (found != null) {
          seedFromManifest(found);
          loadFiles(found, readers, loaded);
        }
      } catch (final NoSuchFileException e) {
        reason = MISSING;
      } catch (final IOException | BlockCorruptionException e) {
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
    } catch (final IOException e) {
      for (final GlobalDictionary dictionary : created.values())
        dictionary.close();
      throw new UncheckedIOException("Cannot publish the empty columnar projection of bucket '" + bucketName + "'", e);
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

  private String propertyOf(final int indexFileId) {
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

    while (true) {
      final ProjectionState current = state.get();
      try {
        return new Snapshot(bucketId, current.retain(), pending, oldest);
      } catch (final IllegalStateException e) {
        // released by a publish that already swapped in a newer state: retry on it; released and still current: closed or dropped meanwhile
        if (state.get() == current)
          throw new IllegalStateException("Columnar projection of bucket '" + bucketName + "' is closed", e);
      }
    }
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
    final ProjectionState previous = state.getAndSet(next);
    if (previous != null)
      previous.release();
    return true;
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

  /** Apply thread, end of a pass: publishes the overlay's current content as the state's live overlay view. Nothing after drop. */
  void publishOverlay() {
    publishLock.lock();
    try {
      final ProjectionState current = state.get();
      final ColumnarSegmentView[] overlays = current.overlays().clone();
      overlays[overlays.length - 1] = overlay.publish();
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
   * {@code out}, newest first: the live overlay, then the segments of the current state from the newest, skipping dead rows. A live row
   * without a value for the column (a column the segment does not have, or a missing value) answers with {@code present} false.
   * Segment columns are matched by index file id, so a RENAME keeps its values. The caller holds {@link #applyLock()}, so the segment
   * set it reads cannot change during the pass (every publish changing it holds the same lock).
   *
   * @return {@link #LOOKUP_OVERLAY}, the id of the segment holding the live row, or {@link #LOOKUP_ABSENT} (then {@code present} is
   * false)
   */
  long liveValueLookup(final long position, final int column, final RowValues out) {
    if (position >= 0 && position / maxRecordsInPage <= Integer.MAX_VALUE) {
      final OverlayPageBlock block = overlay.page((int) (position / maxRecordsInPage));
      if (block != null) {
        final int row = block.row((int) (position % maxRecordsInPage));
        if (row >= 0) {
          readOverlayValue(block, row, column, out);
          return LOOKUP_OVERLAY;
        }
      }
      final SegmentReader[] segments = state.get().segments();
      for (int i = segments.length - 1; i >= 0; i--) {
        final SegmentReader segment = segments[i];
        if (segment.row(position) < 0)
          continue;
        final PositionBitmap dead = segment.deadSet();
        if (dead != null && dead.get(position))
          continue;
        readSegmentValue(segment, position, column, out);
        return segment.segmentId();
      }
    }
    out.present[column] = false;
    return LOOKUP_ABSENT;
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

  private void readSegmentValue(final SegmentReader segment, final long position, final int k, final RowValues out) {
    final ColumnDefinition definition = columns[k];
    final ColumnReader reader = segment.columnByIndexFileId(definition.indexFileId());
    if (reader == null || reader.kind() != definition.kind() || !reader.isPresent(position)) {
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
   * pass builds blocks the overlay accepts. Every overlay mutation (apply pass, a flush's freeze) must hold {@link #applyLock()}. A column
   * whose index file id is already projected is ignored.
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
      publishLock.lock();
      try {
        overlay = rekeyed;
        columns = next;
      } finally {
        publishLock.unlock();
      }
    } finally {
      applyLock.unlock();
    }
  }

  /** A new overlay over {@code next} holding the rows of {@code from}; values are carried by index file id, other columns are missing. */
  private ColumnarOverlay rekey(final ColumnarOverlay from, final ColumnDefinition[] previous, final ColumnDefinition[] next) {
    final ColumnarOverlay result = new ColumnarOverlay(maxRecordsInPage, next, System::currentTimeMillis);
    if (from.rows() == 0)
      return result;
    final int[] source = new int[next.length];
    for (int k = 0; k < next.length; k++) {
      source[k] = -1;
      for (int j = 0; j < previous.length; j++)
        if (previous[j].indexFileId() == next[k].indexFileId())
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
   * Records the reason ({@code missing} or {@code corrupt}) and stops serving: the current state is republished unusable, so queries
   * take the fallback. On a writable database the rebuild requester is called with the reason, outside the publish lock; on a
   * READ_ONLY one nothing is requested or written (R16).
   */
  void markUnusable(final String rebuildReason) {
    final Consumer<String> requester;
    publishLock.lock();
    try {
      unusableReason = rebuildReason;
      final ProjectionState current = state.get();
      if (current.isUsable())
        publish(manifest, current.segments(), current.overlays(), dictionaries, false);
      requester = readOnly || closed || dropped ? null : rebuildRequester;
    } finally {
      publishLock.unlock();
    }
    if (requester != null)
      requester.accept(rebuildReason);
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

      if (dropped) {
        if (retained.isEmpty())
          deleteDirectory();
        return;
      }
      if (openDictionaries.size() > dictionaries.size())
        closeUnheldDictionaries(); // a replaced generation exists; an overlay publish skips this
      final ColumnarManifest current = manifest;
      if (readOnly || closed || current == null || !namesUnreferenced(released, current.files(), heldFiles))
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
