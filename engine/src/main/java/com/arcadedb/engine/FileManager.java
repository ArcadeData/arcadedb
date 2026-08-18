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
package com.arcadedb.engine;

import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileManager {
  private final        ComponentFile.MODE                        mode;
  private final        List<ComponentFile>                       files             = new ArrayList<>();
  private final        ConcurrentHashMap<String, ComponentFile>  fileNameMap       = new ConcurrentHashMap<>();
  private final        ConcurrentHashMap<Integer, ComponentFile> fileIdMap         = new ConcurrentHashMap<>();
  private final        AtomicLong                                maxFilesOpened    = new AtomicLong();
  // Bumps on every file registration / drop. Lets callers (e.g. PaginatedSparseVectorEngine's
  // refreshSegmentsFromFileManager) skip the O(total files) walk on the hot query path when the
  // FileManager is unchanged since their last observation - they cache the value here, compare on
  // entry, and only re-walk when it has advanced.
  private final        AtomicLong                                modificationCount = new AtomicLong();
  // Volatile because startRecordingChanges()/stopRecordingChanges() are the handshake HA uses to decide
  // whether a schema change still needs replicating; a stale read there loses the change silently (#5728).
  private volatile     List<FileChange>                          recordedChanges   = null;
  private volatile     Thread                                    recordingThread   = null;
  /**
   * The creates of the active session that no caller has consumed yet, in registration order (issue #6142).
   * <p>
   * Same events as the {@code create} entries of {@link #recordedChanges}, carried as a CONSUMABLE queue rather than
   * as a cumulative log. A replicator that ships a session's payload in instalments has to answer "what was created
   * since my last instalment?" on every one of them, and deriving that from the cumulative list is O(instalments x
   * file changes) - fine for a session that creates one or two files however much WAL it produces, quadratic for a
   * DDL that creates many. An index into the list is NOT the alternative: {@link #dropFile} removes the cancelled
   * create from the MIDDLE of it, so a saved position does not stay meaningful.
   * <p>
   * Kept in step with {@code recordedChanges} at every site that touches a create - registration, the
   * drop-cancels-create rule, and the post-rename name refresh - so the two never tell different stories about the
   * same file id. {@code null} exactly when no session is open.
   */
  private volatile     Map<Integer, String>                      unshippedCreates  = null;
  private final static PaginatedComponentFile                    RESERVED_SLOT     = new PaginatedComponentFile();
  /**
   * Decides whether the physical deletion of a dropped file has to be DEFERRED because a point-in-time snapshot
   * window still needs to read it (issue #6075, challenge C2). Installed by {@code LocalDatabase} and delegating to
   * {@code PageManager}; {@code null} on a file manager not attached to a database (tests, tooling), where nothing
   * can be snapshotting anyway.
   */
  private volatile DroppedFileHandler droppedFileHandler = null;

  @FunctionalInterface
  public interface DroppedFileHandler {
    /** @return true when the caller must NOT delete the file: an open snapshot window took the deletion over. */
    boolean deferDrop(ComponentFile file);
  }

  public static class FileChange {
    public final boolean create;
    public final int     fileId;
    public final String  fileName;

    public FileChange(final boolean create, final int fileId, final String fileName) {
      this.create = create;
      this.fileId = fileId;
      this.fileName = fileName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (!(o instanceof FileChange))
        return false;
      final FileChange that = (FileChange) o;
      return fileId == that.fileId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileId);
    }
  }

  public static class FileManagerStats {
    public long maxOpenFiles;
    public long totalOpenFiles;
  }

  public FileManager(final String path, final ComponentFile.MODE mode, final Set<String> supportedFileExt) {
    this(path, mode, supportedFileExt, null);
  }

  /**
   * @param path          primary database directory; created if missing.
   * @param extraScanPath optional secondary directory to scan for additional component files (e.g. paired
   *                      external-property buckets that have been tiered to a different disk via
   *                      {@code arcadedb.externalPropertyBucketPath}). May be null/empty.
   */
  public FileManager(final String path, final ComponentFile.MODE mode, final Set<String> supportedFileExt,
      final String extraScanPath) {
    this.mode = mode;

    final File dbDirectory = new File(path);
    if (!dbDirectory.exists()) {
      boolean created = dbDirectory.mkdirs();
      if (!created) {
        LogManager.instance().log(this, Level.SEVERE, "Cannot create the directory '%s'", null, dbDirectory);
        throw new IllegalArgumentException(String.format("Cannot create the directory '%s'", dbDirectory));
      }
    } else {
      if (!dbDirectory.canRead()) {
        LogManager.instance().log(this, Level.SEVERE, "The directory '%s' doesn't have the proper permissions", null, dbDirectory);
        throw new IllegalArgumentException(String.format("The directory '%s' doesn't have the proper permissions", dbDirectory));
      }

      scanDirectoryForComponentFiles(dbDirectory, supportedFileExt);
    }

    if (extraScanPath != null && !extraScanPath.isEmpty()) {
      final File extraDir = new File(extraScanPath);
      if (extraDir.exists() && extraDir.canRead() && !extraDir.equals(dbDirectory))
        scanDirectoryForComponentFiles(extraDir, supportedFileExt);
    }
  }

  private void scanDirectoryForComponentFiles(final File dir, final Set<String> supportedFileExt) {
    final File[] entries = dir.listFiles();
    if (entries == null)
      return;
    for (final File f : entries) {
      // Compute the extension from the file name (not the full path) so a database directory containing dots
      // (e.g. /home/u/my.db/bucket1) doesn't accidentally find the dot in the directory name.
      final String fileName = f.getName();
      final int lastDot = fileName.lastIndexOf(".");
      if (lastDot < 0)
        continue;
      final String fileExt = fileName.substring(lastDot + 1);
      if (!supportedFileExt.contains(fileExt))
        continue;
      try {
        final ComponentFile file = new PaginatedComponentFile(f.getAbsolutePath(), mode);
        registerFile(file);
      } catch (final FileNotFoundException e) {
        LogManager.instance().log(this, Level.WARNING, "Cannot load file '%s'", null, f);
      }
    }
  }

  /**
   * Start recording changes in file system. Changes can be returned (before the end of the lock in database) with {@link #getRecordedChanges()}.
   * <p>
   * There is a single session per database and it is not re-entrant, so the check and the claim must be
   * atomic: two callers both told they started would each install their own list, and the loser's recorded
   * file creations would never be shipped to the followers (#5728).
   *
   * @return true if the recorded started and false if it was already started.
   */
  public synchronized boolean startRecordingChanges() {
    if (recordedChanges != null) {
      // Level-guarded because a contended HA caller polls this method until the session frees up.
      if (Logger.getLogger(getClass().getName()).isLoggable(Level.FINE))
        LogManager.instance().log(this, Level.FINE,
            "startRecordingChanges denied: a session is already active with %d entries",
            null, recordedChanges.size());
      return false;
    }

    recordedChanges = new ArrayList<>();
    unshippedCreates = new LinkedHashMap<>();
    recordingThread = Thread.currentThread();
    LogManager.instance().log(this, Level.FINE,
        "startRecordingChanges: new session begun on thread '%s'", null, Thread.currentThread().getName());
    return true;
  }

  /**
   * Tells apart the two reasons {@link #startRecordingChanges()} can refuse. A caller nested inside its own
   * session may proceed, because the frame that opened the session still owns the recorded changes and will
   * act on them; a caller facing a session opened by a different thread has no such guarantee and must wait
   * for its own. Conflating the two let an HA leader apply a schema change locally and replicate nothing
   * (#5728).
   *
   * @return true when the active session was opened by the calling thread
   */
  public synchronized boolean isRecordingChangesOnCurrentThread() {
    return recordedChanges != null && recordingThread == Thread.currentThread();
  }

  public List<FileChange> getRecordedChanges() {
    return recordedChanges;
  }

  /**
   * Hands over the creates recorded since the previous call and starts a fresh batch (issue #6142).
   * <p>
   * The incremental counterpart of {@link #getRecordedChanges()} for a caller that ships the session's payload in
   * instalments: each call answers "created since your last one", so a whole session costs one pass over its file
   * creations rather than one pass per instalment. See {@link #unshippedCreates} for why re-deriving it from the
   * cumulative list, or indexing into that list, is not equivalent.
   * <p>
   * CONSUMING, so exactly one consumer per session may call it - today that is the HA schema-instalment producer -
   * and the returned map is the caller's to keep. What it hands over is what the followers must be told to create
   * BEFORE the pages of this instalment land in them; the cumulative list stays untouched and still describes the
   * whole session, which is what the session's final entry is built from.
   *
   * @return the creates, in registration order; empty when no session is open or nothing was created since the
   *     previous call
   */
  public synchronized Map<Integer, String> drainRecordedCreates() {
    final Map<Integer, String> drained = unshippedCreates;
    if (drained == null || drained.isEmpty())
      // A fresh empty map is not installed on the null branch: no session is open, so there is nothing to collect
      // into and startRecordingChanges will install one when there is.
      return Collections.emptyMap();

    unshippedCreates = new LinkedHashMap<>();
    return drained;
  }

  public synchronized void stopRecordingChanges() {
    if (recordedChanges != null && Logger.getLogger(getClass().getName()).isLoggable(Level.FINE)) {
      final StringBuilder dump = new StringBuilder();
      for (final FileChange c : recordedChanges) {
        if (!dump.isEmpty())
          dump.append(", ");
        dump.append(c.create ? "+" : "-").append(c.fileId).append(":'").append(c.fileName).append("'");
      }
      LogManager.instance().log(this, Level.FINE,
          "stopRecordingChanges on thread '%s': %d entries [%s]",
          null, Thread.currentThread().getName(), recordedChanges.size(), dump.toString());
    }
    recordedChanges = null;
    unshippedCreates = null;
    recordingThread = null;
  }

  /**
   * @return {@code true} when every file was fsynced; {@code false} when any fsync failed (#4934). After a
   *     failed fsync the OS may have DROPPED the dirty pages (fsyncgate semantics), so the callers that were
   *     about to delete the WAL protecting that data must preserve it instead: the clean-close path keeps
   *     the WAL and the lock file so the next open recovers, and the runtime WAL-rotation path skips the
   *     drop and retries on the next pass.
   */
  public boolean syncFiles() {
    boolean allSynced = true;
    for (final ComponentFile f : fileNameMap.values()) {
      if (f instanceof PaginatedComponentFile pcf) {
        try {
          pcf.force(true);
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on syncing file '%s' to disk", e, f.getFileName());
          allSynced = false;
        }
      }
    }
    return allSynced;
  }

  public synchronized void close() {
    for (final ComponentFile f : fileNameMap.values())
      f.close();

    files.clear();
    fileNameMap.clear();
    fileIdMap.clear();
  }

  /** Installs the snapshot-aware deletion policy for dropped files (issue #6075). */
  public void setDroppedFileHandler(final DroppedFileHandler handler) {
    this.droppedFileHandler = handler;
  }

  @FunctionalInterface
  public interface FileSetOperation<T> {
    T execute() throws IOException;
  }

  /**
   * Runs {@code operation} holding the monitor {@link #dropFile} and {@link #getOrCreateFile} take, so the registered
   * file set cannot change underneath it.
   * <p>
   * The snapshot t0 barrier (issue #6075) uses this to close a TOCTOU window that would otherwise defeat the whole
   * deferred-deletion mechanism: it lists the files it is about to snapshot and only THEN publishes the window, and
   * {@code PageManager.deferFileDrop} keeps a dropped file alive only for windows that are already published. A
   * {@code dropFile} landing in that gap - which index compaction can do at any moment, since it takes no database
   * lock - would delete a file the snapshot had just claimed. Holding this monitor across list-and-publish makes the
   * two mutually exclusive. Lock ORDER matters and is the same on both paths: this monitor first, then
   * {@code PageManager}'s snapshot registry lock.
   */
  public synchronized <T> T executeWithFileSetLocked(final FileSetOperation<T> operation) throws IOException {
    return operation.execute();
  }

  /**
   * Drops the file at {@code fileId} and reports whether there was one to drop.
   * <p>
   * The check and the drop happen inside the SAME {@code synchronized} block deliberately (issue #6189 review): a
   * caller that wants to know whether IT was the one that actually removed the file - rather than racing another
   * dropper and finding it already gone - needs that answer atomically with the removal, not from a separate
   * {@code existsFile} check beforehand, which a concurrent dropper could invalidate in the gap between the two
   * calls.
   *
   * @return {@code true} if a file was there and this call removed it; {@code false} if the id already did not
   * resolve to anything, in which case this call did nothing
   */
  public boolean dropFile(final int fileId) throws IOException {
    final ComponentFile file;
    synchronized (this) {
      // Drop the file on disk FIRST, then update the maps. If drop() throws, every map is left untouched
      // so the file id stays fully resolvable and the caller can retry; clearing fileIdMap up front would
      // strand the entry in fileNameMap/files (a partial, unretryable state) on failure (issue #4711).
      // This intentionally holds the monitor across the drop() I/O so the map mutations stay atomic with
      // the delete; dropFile is a rare DDL operation, so briefly blocking concurrent registerFile is fine.
      file = fileIdMap.get(fileId);
      if (file != null) {
        // #6075 (challenge C2): while a snapshot window is open the file is kept alive and its deletion deferred to
        // the close of the last window that needs it, so LSM and vector index compaction can keep dropping files
        // during a backup instead of being postponed for its whole duration. The file leaves this manager either
        // way - the LIVE database must see it as gone immediately.
        final DroppedFileHandler handler = droppedFileHandler;
        if (handler == null || !handler.deferDrop(file))
          file.drop();

        fileIdMap.remove(fileId);
        fileNameMap.remove(file.getComponentName());
        files.set(fileId, null);
        modificationCount.incrementAndGet();

        final FileChange entry = new FileChange(false, fileId, file.getFileName());
        if (recordedChanges != null) {
          // Mirrors the cancellation below on the consumable queue (issue #6142). Unconditional because the two
          // outcomes need the same thing: a create still queued is cancelled, and one already drained - or one from
          // before the session - is simply not there. What an instalment ALREADY announced is not this map's
          // business; the shipper keeps its own record of that so the session's final entry can retire it.
          if (unshippedCreates != null)
            unshippedCreates.remove(fileId);

          if (recordedChanges.remove(entry)) {
            LogManager.instance().log(this, Level.FINE,
                "dropFile fileId=%d cancels prior CREATE entry (componentName='%s', fileName='%s')",
                null, fileId, file.getComponentName(), file.getFileName());
            // JUST ADDED: REMOVE THE ENTRY
          } else {
            recordedChanges.add(entry);
            LogManager.instance().log(this, Level.FINE,
                "recorded DROP fileId=%d fileName='%s' componentName='%s'",
                null, fileId, file.getFileName(), file.getComponentName());
          }
        }
      }
    }
    return file != null;
  }

  /**
   * <b>Must stay lock-free, and must stay callable after {@link #close} (#5636.)</b> {@code Profiler.toJSON()} reads
   * this while holding its own monitor, which a closing database can be waiting on, so a lock taken here would sit on
   * the other side of that wait. Note this is deliberately NOT {@code synchronized}, unlike {@link #getFiles()} just
   * below.
   * <p>
   * The close-tolerance half is not theoretical: {@code LocalDatabase.kill()} closes this file manager well before it
   * reaches {@code Profiler.unregisterDatabase}, so a scrape landing in that window reads a CLOSED manager on a
   * still-registered database. Returning zeros there is fine - these feed gauges, so the reading dips for one scrape -
   * but throwing would take the whole snapshot down, and {@code toJSON()} has no try/catch by design.
   */
  public FileManagerStats getStats() {
    final FileManagerStats stats = new FileManagerStats();
    stats.maxOpenFiles = maxFilesOpened.get();
    stats.totalOpenFiles = fileIdMap.size();
    return stats;
  }

  public synchronized List<ComponentFile> getFiles() {
    return Collections.unmodifiableList(new ArrayList<>(files));
  }

  /**
   * Monotonically increasing counter that bumps on every file registration and drop. Callers can
   * cache the value, compare on later entries, and skip the {@link #getFiles()} walk when it is
   * unchanged since the last observation. This is a content-version proxy, not a wall-clock value
   * - rollback is not possible, but a wraparound after 2^63 mutations is not a concern in practice.
   */
  public long getModificationCount() {
    return modificationCount.get();
  }

  public boolean existsFile(final int fileId) {
    return fileIdMap.containsKey(fileId);
  }

  public ComponentFile getFile(final int fileId) {
    final ComponentFile f = fileIdMap.get(fileId);
    if (f == null)
      throw new IllegalArgumentException("File with id " + fileId + " was not found");

    return f;
  }

  /**
   * Looks up an already-registered file by the component name it is registered under, creating nothing.
   * This is the read-only half of {@link #getOrCreateFile(String, String, ComponentFile.MODE)}, and the
   * difference matters to a caller that has to decide which <em>file id</em> to build a component on:
   * {@code getOrCreateFile} is keyed by component name, so a component that allocated a fresh id and only
   * then asked for its file would be handed this one instead, and would go on addressing pages of an id
   * that is not the file it holds.
   *
   * @return the registered file, or {@code null} when no file is registered under that component name
   */
  public ComponentFile getFileByComponentName(final String componentName) {
    return fileNameMap.get(componentName);
  }

  /**
   * Returns the file registered under {@code fileName}, opening and registering one at {@code filePath} when there
   * is none. This is the by-<em>name</em> half of the pair; {@link #getOrCreateFile(int, String)} is the by-id one.
   * <p>
   * <b>The mode is a request the caller is entitled to, not a hint (issue #6340.)</b> On a hit this used to hand
   * the registered file back whatever mode it carried, which is the third member of the family the by-name id
   * check (#6283) and the by-id name check (#6314) closed: the caller asks for one thing and is handed another.
   * The direction that matters is the quiet one - a caller asking for {@code READ_ONLY} and being given a
   * {@code READ_WRITE} file gets a WEAKER guarantee than it asked for, with nothing anywhere saying so, and
   * mode is the one file property whose whole purpose is to be a guarantee.
   * <p>
   * Reopening the file to satisfy the request is deliberately not what happens instead. The mode selects the
   * {@code RandomAccessFile} open string ({@code PaginatedComponentFile.open}) and a registered file is shared by
   * every component addressing it, so "upgrading" one would change the channel under readers that had asked for
   * the narrower one - trading a loud refusal for a silent widening, which is the very shape being removed here.
   * <p>
   * <b>Nothing reaches this today, and that is a statement rather than an assumption.</b> This overload has
   * exactly one caller, {@code PaginatedComponent}'s constructor, and a hit on it survives the file-id guard
   * there only when the component was deliberately built on the registered file's own id - which today means
   * {@code TimeSeriesTagDictionary}'s build-on-an-existing-file constructor, and that one now takes the mode
   * from the file too ({@link ComponentFile#getMode()}). Every other component reaches this on the miss path,
   * where the file is opened with the mode asked for and agrees by construction. The guard is what keeps the
   * next caller from having to re-derive that.
   *
   * @throws IllegalStateException when a file is already registered under {@code fileName} in a different mode
   */
  public ComponentFile getOrCreateFile(final String fileName, final String filePath, final ComponentFile.MODE mode)
      throws IOException {
    ComponentFile file = fileNameMap.get(fileName);
    if (file != null)
      return checkModeMatches(file, mode);

    synchronized (this) {
      file = fileNameMap.get(fileName);
      if (file != null)
        return checkModeMatches(file, mode);

      file = new PaginatedComponentFile(filePath, mode);
      registerFile(file);
      recordCreate(file);

      return file;
    }
  }

  /**
   * The by-<em>id</em> mirror of {@link #getOrCreateFile(String, String, ComponentFile.MODE)}, and it has the same
   * hazard: it is keyed by the id alone, so an id that is already registered hands that file back whatever
   * {@code filePath} the caller asked for, and the caller then goes on using a file that is not the one it named.
   * The by-name half of that pair was closed at the component layer by issue #6283; this one is closed here
   * (issue #6314), so the guarantee "the file you get back is the file you asked for" belongs to
   * {@code FileManager} for both keys rather than to whichever caller remembered to check.
   * <p>
   * A caller that has to tell "already applied" apart from "diverged" still needs its own branch on the two, and
   * the HA follower's {@code ArcadeStateMachine.createNewFiles} has one: a matching name is an idempotent replay
   * it skips, a differing one is a file-id space that has diverged from the leader's, which it turns into the
   * quarantine-and-resync path (issue #6063). This check therefore fires for nobody today; it is what makes the
   * next caller not have to re-derive that reasoning. It throws the same {@link SchemaException} that caller does,
   * so a caller that does reach it is classified exactly as it would have classified itself.
   *
   * @throws SchemaException when {@code fileId} is already registered under a different file name
   */
  public ComponentFile getOrCreateFile(final int fileId, final String filePath) throws IOException {
    ComponentFile file = fileIdMap.get(fileId);
    if (file != null)
      return checkFileNameMatches(file, filePath);

    synchronized (this) {
      file = fileIdMap.get(fileId);
      if (file == null) {
        file = new PaginatedComponentFile(filePath, mode);
        registerFile(file);
        recordCreate(file);
      } else
        checkFileNameMatches(file, filePath);

      return file;
    }
  }

  /**
   * Asserts that an already-registered file is the one the caller named. The comparison is on the file name and
   * not on the whole path: the id, page size, version and extension a component addresses its file through all
   * live in that name, while the directory prefix is the database path both sides already share.
   */
  private static ComponentFile checkFileNameMatches(final ComponentFile file, final String filePath) {
    final String requestedName = new File(filePath).getName();
    if (!file.getFileName().equals(requestedName))
      throw new SchemaException(
          "File id " + file.getFileId() + " is already registered as '" + file.getFileName() + "' but was requested as '"
              + requestedName + "' ('" + filePath + "')");
    return file;
  }

  /**
   * Asserts that an already-registered file is open in the mode the caller asked for (issue #6340). Thrown as an
   * {@link IllegalStateException} and not as the {@link SchemaException} its by-id sibling above raises, because the
   * two say different things: a file name that does not match is a file-id space that has diverged from the
   * leader's, which the HA follower turns into a quarantine-and-resync, while a mode that does not match is a caller
   * asking for a guarantee the shared file cannot give it - a programming error on the same footing as the file-id
   * and page-size guards in {@code PaginatedComponent}, which is this overload's only caller and which throws
   * exactly this.
   */
  private static ComponentFile checkModeMatches(final ComponentFile file, final ComponentFile.MODE mode) {
    if (file.getMode() != mode)
      throw new IllegalStateException(
          "File '" + file.getFileName() + "' is already open in mode " + file.getMode() + " but was requested in mode "
              + mode + " ('" + file.getFilePath() + "')");
    return file;
  }

  /**
   * Records a file creation in the active session, in both of the forms a consumer can ask for it: the cumulative
   * {@link #recordedChanges} log and the consumable {@link #unshippedCreates} queue (issue #6142). Called from the
   * two {@code getOrCreateFile} overloads while they hold this monitor, so the two views cannot disagree.
   * <p>
   * EVERY CREATE MUST COME THROUGH HERE. A new creation path that appends to {@code recordedChanges} directly would
   * leave the queue short by exactly that file, and the symptom is not local: the instalment that should have
   * announced it never does, the followers never create it, and the pages that land in it afterwards are applied
   * against a file only the leader has. Adding an entry to the cumulative log is therefore not a substitute for
   * calling this method, and there is deliberately no other writer of either structure ({@link #dropFile} records
   * the matching removal, and nothing outside this class mutates them).
   */
  private void recordCreate(final ComponentFile file) {
    if (recordedChanges == null)
      return;

    recordedChanges.add(new FileChange(true, file.getFileId(), file.getFileName()));
    if (unshippedCreates != null)
      unshippedCreates.put(file.getFileId(), file.getFileName());

    LogManager.instance().log(this, Level.FINE,
        "recorded CREATE fileId=%d fileName='%s' componentName='%s'",
        null, file.getFileId(), file.getFileName(), file.getComponentName());
  }

  public synchronized int newFileId() {
//    // LOOK FOR AN HOLE
//    for (int i = 0; i < files.size(); ++i) {
//      if (files.get(i) == null) {
//        files.set(i, RESERVED_SLOT);
//        return i;
//      }
//    }
//
    files.add(RESERVED_SLOT);
    return files.size() - 1;
  }

  public void renameFile(final String oldName, final String newName) {
    final ComponentFile file = fileNameMap.remove(oldName);
    if (file == null) {
      // Lookup miss: callers may pass a full file path (e.g. PaginatedComponent.removeTempSuffix)
      // even though fileNameMap is keyed by component name. The fileNameMap stays consistent in
      // that case (component name unchanged) but recordedChanges entries that snapshotted the
      // pre-rename file name are NOT updated.
      LogManager.instance().log(this, Level.FINE,
          "renameFile: lookup miss oldName='%s' newName='%s' (recordedChanges%s)",
          null, oldName, newName, recordedChanges != null ? " ACTIVE" : " inactive");
      return;
    }

    fileNameMap.put(newName, file);
    LogManager.instance().log(this, Level.FINE,
        "renameFile: oldName='%s' newName='%s' fileId=%d (recordedChanges%s, file.fileName='%s')",
        null, oldName, newName, file.getFileId(),
        recordedChanges != null ? " ACTIVE" : " inactive", file.getFileName());
  }

  /**
   * Updates the {@link FileChange} entry for {@code file} in the active recording session so it
   * carries the file's current OS file name. {@link com.arcadedb.engine.PaginatedComponent#removeTempSuffix}
   * renames the underlying file from the {@code temp_*} extension to the final extension AFTER
   * {@link #getOrCreateFile(int, String)} has snapshotted the pre-rename name into
   * {@code recordedChanges}. Without re-syncing, the SCHEMA_ENTRY shipped by the Raft compaction
   * path carries the temp-suffixed file name in {@code addFiles} while the schema JSON captured
   * post-rename references the stripped name; the follower then creates the file under the wrong
   * name and emits "Cannot find indexes ..." warnings on schema reload (issue #4083).
   * <p>
   * SYNCHRONIZED like every other mutation of the session's two structures. It used to rely on "only the recording
   * thread ever reaches here", which is true of its single caller ({@code PaginatedComponent.removeTempSuffix}) but
   * is not something the next caller has to notice - and {@code unshippedCreates} (issue #6142) is a plain
   * {@link LinkedHashMap}, so getting that wrong would corrupt a map rather than merely read one stale. Taking the
   * monitor here cannot deadlock: this method touches maps only, the paths that hold this monitor across I/O
   * ({@link #dropFile}) take file locks UNDER it, and the caller holds none of its own - {@code rename} released
   * the component's channel lock before returning.
   */
  public synchronized void refreshRecordedFileName(final ComponentFile file) {
    if (recordedChanges == null || file == null)
      return;
    final int fileId = file.getFileId();
    final String currentFullName = file.getFileName();
    if (currentFullName == null)
      return;

    // The consumable queue carries the same names and is read at flush time, so it needs the rename too or an
    // instalment would announce the pre-rename name for a file the schema JSON names post-rename - the divergence
    // this method exists to prevent (issue #4083), just on the incremental path (issue #6142).
    final Map<Integer, String> queued = unshippedCreates;
    if (queued != null)
      queued.replace(fileId, currentFullName);

    for (int i = 0; i < recordedChanges.size(); i++) {
      final FileChange c = recordedChanges.get(i);
      if (c.fileId == fileId && !currentFullName.equals(c.fileName)) {
        LogManager.instance().log(this, Level.FINE,
            "refreshRecordedFileName: fileId=%d updating '%s' -> '%s'",
            null, fileId, c.fileName, currentFullName);
        recordedChanges.set(i, new FileChange(c.create, c.fileId, currentFullName));
      }
    }
  }

  private void registerFile(final ComponentFile file) {
    final int pos = file.getFileId();
    while (files.size() < pos + 1)
      files.add(null);
    final ComponentFile prev = files.get(pos);
    if (prev != null && prev != RESERVED_SLOT)
      throw new IllegalArgumentException(
          "Cannot register file '" + file + "' at position " + pos + " because already occupied by file '" + prev + "'");

    files.set(pos, file);
    fileNameMap.put(file.getComponentName(), file);
    fileIdMap.put(pos, file);
    maxFilesOpened.incrementAndGet();
    modificationCount.incrementAndGet();
  }

}
