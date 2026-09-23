/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Manifest and checksum helpers for snapshot-based resync in Raft HA.
 * <p>
 * A follower that falls behind the compacted Raft log downloads the WHOLE database as a ZIP from
 * {@code SnapshotHttpHandler} and installs it through {@code SnapshotInstaller}; this class carries the manifest
 * that transfer is verified with, and the checksum computation behind the {@code /checksums} endpoint.
 * <p>
 * <b>There is deliberately no file-level diff here</b> (#6125). One used to be - a {@code findDifferingFiles}
 * helper, called from nothing but its own unit test, whose presence suggested resync could ship only what changed.
 * It was removed rather than wired in, for two reasons. Granularity: an ArcadeDB database is usually dominated by
 * one bucket file, so a whole-file comparison saves nothing the moment a single byte of it changes. Consistency:
 * the checksums come from one point-in-time window and the ZIP from another, so a file that matched when it was
 * compared can be rewritten before the transfer starts, and a follower that kept its local copy on the strength of
 * that match would hold a database torn across two instants. Incremental resync therefore belongs at the PAGE
 * level, on the page-version manifest of phase 3 (#6115), where both halves come from the same window. Until then
 * {@code /checksums} is an operator diagnostic - "do these two nodes hold the same bytes?" without moving a
 * database - and nothing more.
 */
public final class SnapshotManager {

  /**
   * Reserved name of the manifest entry the leader appends as the <b>final</b> entry of a snapshot
   * ZIP (issue #4831). The manifest lists every file shipped (name + uncompressed size + CRC32) so a
   * follower can detect a download truncated at an entry boundary: a {@link java.util.zip.ZipInputStream}
   * that hits EOF cleanly between entries returns {@code null} from {@code getNextEntry()} without throwing,
   * so a short archive would otherwise be accepted as complete. Because the manifest is written last, any
   * truncation drops it, and the follower fails the install rather than opening a structurally-incomplete
   * database. The '.'-prefix keeps it out of the way of real database files; the installer reads it into
   * memory and never writes it to disk.
   */
  public static final String MANIFEST_ENTRY_NAME = ".arcadedb-snapshot-manifest.json";

  /**
   * Response header the leader sets on the snapshot stream to advertise that it emits a
   * {@link #MANIFEST_ENTRY_NAME manifest}. A follower that sees this header requires the manifest to be
   * present (a missing manifest then means a truncated download). When the header is absent - a leader
   * predating issue #4831 during a rolling upgrade - the follower falls back to the legacy behaviour and
   * skips manifest verification, preserving backward compatibility.
   */
  public static final String MANIFEST_HEADER = "X-ArcadeDB-Snapshot-Manifest";
  /**
   * Response header carrying the uncompressed byte count of the files the snapshot ships (issue #7037), so the
   * follower can refuse the download up front when its volume cannot hold them instead of failing with {@code No
   * space left on device} halfway through the extraction. A leader predating #7037 omits it and the check is skipped.
   */
  public static final String UNCOMPRESSED_BYTES_HEADER = "X-ArcadeDB-Snapshot-Uncompressed-Bytes";

  /**
   * One file recorded in a snapshot manifest: the entry name, its uncompressed byte size and its CRC32.
   */
  public record ManifestEntry(String name, long size, long crc) {
  }

  private SnapshotManager() {
  }

  /**
   * Serialises the given manifest entries to the JSON written into {@link #MANIFEST_ENTRY_NAME}.
   * Shape: {@code {"version":1,"files":[{"name":..,"size":..,"crc":..}, ...]}}.
   */
  public static String buildManifest(final List<ManifestEntry> entries) {
    final JSONArray files = new JSONArray();
    for (final ManifestEntry e : entries) {
      final JSONObject f = new JSONObject();
      f.put("name", e.name());
      f.put("size", e.size());
      f.put("crc", e.crc());
      files.put(f);
    }
    final JSONObject root = new JSONObject();
    root.put("version", 1);
    root.put("files", files);
    return root.toString();
  }

  /**
   * Parses the JSON produced by {@link #buildManifest} back into the list of manifest entries.
   *
   * @throws IOException if the JSON is malformed (treated like a truncated/corrupt manifest)
   */
  public static List<ManifestEntry> parseManifest(final String json) throws IOException {
    try {
      final JSONObject root = new JSONObject(json);
      final JSONArray files = root.getJSONArray("files");
      final List<ManifestEntry> result = new ArrayList<>(files.length());
      for (int i = 0; i < files.length(); i++) {
        final JSONObject f = files.getJSONObject(i);
        result.add(new ManifestEntry(f.getString("name"), f.getLong("size"), f.getLong("crc")));
      }
      return result;
    } catch (final RuntimeException e) {
      throw new IOException("Malformed snapshot manifest: " + e.getMessage(), e);
    }
  }

  /**
   * Computes CRC32 checksums for all regular files in the given directory, reading them live off the disk.
   *
   * @param directory the directory to scan
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory) throws IOException {
    return computeFileChecksums(directory, null);
  }

  /**
   * Computes the checksums of a database directory, taking the content of every page file from a point-in-time
   * snapshot window instead of reading it live (#6116).
   * <p>
   * This is what lets the {@code /checksums} endpoint stop freezing the data files with
   * {@code PageManager.suspendFlushAndExecute}, which was the last writer-throttling reader left in the product
   * after #6075 migrated the backup, the HA verify and the HA snapshot ship. It needs a directory-oriented shape
   * rather than the verify handler's file-list one because the endpoint's contract is "every non-transient file in
   * the database directory", which includes files the page snapshot does not cover at all - {@code database.json},
   * {@code schema.json}, the {@code .ts.sealed} time-series stores, the {@code last-tx-id.bin} marker. Those are
   * read raw, as before; the database read lock the caller holds is what makes that safe, and is unchanged.
   * <p>
   * A page file the window does not carry was created after t0, so it has no point-in-time content to report: it is
   * skipped rather than read live, which is the same rule {@code PostVerifyDatabaseHandler} follows by iterating the
   * window's own file list. Reading it live would put a torn CRC of a file being actively written into a map whose
   * whole purpose is to be compared with another node's.
   * <p>
   * "Is this a page file" is decided from the NAME ({@link LocalDatabase#isComponentFileName}) rather than by asking
   * the {@code FileManager} what it currently has registered. The registry is a moving target even under the
   * database read lock this runs beneath: index compaction creates and drops component files without the write
   * lock, so a name set captured a moment before the directory listing can miss a file that is already on disk -
   * and that file would then be CRC'd live, which is precisely the case being excluded.
   *
   * @param directory the database directory to scan
   * @param snapshot  the open window to serve page files from, or {@code null} to read everything live
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory, final PageSnapshot snapshot)
      throws IOException {
    return computeFileChecksums(directory, snapshot, null);
  }

  /**
   * The full form of {@link #computeFileChecksums(File, PageSnapshot)}, which additionally reports the files the
   * answer does NOT cover because they were gone by the time it tried to read them (#7956).
   * <p>
   * {@code listFiles} produces a name and the {@code FileInputStream} below finds nothing there a moment later. The
   * database read lock the caller holds does not prevent it: a TimeSeries sealed store dropped by retention is
   * unregistered raw-{@code FileChannel} I/O that takes no write lock at all, and on the
   * {@code pageSnapshotEnabled=false} path so is a component file dropped by index compaction, which this class's
   * other javadoc already notes happens WITHOUT the database write lock. The {@code IOException} used to leave the
   * loop, so the endpoint answered 500 and the cluster comparison reported the node as ERROR - the whole answer lost
   * to one file that no longer exists, at the moment an operator is using it to decide whether a follower diverged.
   * <p>
   * A vanished file is therefore dropped from the map instead, and its name is handed to {@code unreadableFiles} so
   * the caller can say "this answer does not cover these" rather than imply it is complete. Silently shortening the
   * map is the one option that is not available: a leader compares its OWN keys, so a short map rolls up as
   * agreement - the trap {@code PostVerifyDatabaseHandler.collectSealedStores} documents and avoids for the same
   * reason (#7338). The name is logged at WARNING whether or not a sink was passed, so the two-argument overload
   * above does not turn it into silence.
   * <p>
   * Only a file that is GONE is survivable. One that is still on disk and still cannot be opened is a genuine fault
   * - a permission problem, a failing disk - and is rethrown, because degrading that to a 200 would hide a broken
   * node behind the very diagnostic that exists to find broken nodes. {@code FileInputStream} reports both as
   * {@link FileNotFoundException}, so the two are told apart by asking whether the file is still there.
   *
   * @param directory       the database directory to scan
   * @param snapshot        the open window to serve page files from, or {@code null} to read everything live
   * @param unreadableFiles collects the names listed but no longer present when read, or {@code null} when the
   *                        caller does not report them
   *
   * @return a map of file name to CRC32 checksum value
   *
   * @throws IOException if a file that is still present cannot be read
   */
  public static Map<String, Long> computeFileChecksums(final File directory, final PageSnapshot snapshot,
      final Collection<String> unreadableFiles) throws IOException {
    final Map<String, Long> checksums = new HashMap<>();
    final File[] files = directory.listFiles(File::isFile);
    if (files == null)
      return checksums;

    final Map<String, Integer> snapshotFileIds = new HashMap<>();
    if (snapshot != null)
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        snapshotFileIds.put(file.fileName(), file.fileId());

    final byte[] buffer = new byte[8192];
    for (final File file : files) {
      final String name = file.getName();
      if (isNodeLocalScratchFileName(name))
        continue;

      final Integer snapshotFileId = snapshotFileIds.get(name);
      if (snapshotFileId != null) {
        checksums.put(name, snapshot.calculateChecksum(snapshotFileId));
        continue;
      }

      if (snapshot != null && LocalDatabase.isComponentFileName(name))
        // A PAGE FILE THE WINDOW DOES NOT CARRY WAS CREATED AFTER t0 (INDEX COMPACTION DOES THIS DURING A BACKUP):
        // IT HAS NO POINT-IN-TIME CONTENT, SO IT IS ABSENT RATHER THAN TORN
        continue;

      final CRC32 crc = new CRC32();
      try (final FileInputStream fis = new FileInputStream(file)) {
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1)
          crc.update(buffer, 0, bytesRead);
      } catch (final FileNotFoundException e) {
        // STILL ON DISK: THE OPEN FAILED FOR A REAL REASON (PERMISSIONS, A FAILING DEVICE) AND MUST STILL FAIL THE
        // ENDPOINT, WHOSE 500 BODY REPORTS THE DEEPEST CAUSE PRECISELY SO IT CAN NAME IT
        if (file.exists())
          throw e;

        LogManager.instance().log(SnapshotManager.class, Level.WARNING,
            "File '%s' disappeared from '%s' while its checksum was being computed: it is left out of the answer and "
                + "reported as uncovered", null, name, directory.getName());
        if (unreadableFiles != null)
          unreadableFiles.add(name);
        continue;
      }
      checksums.put(name, crc.getValue());
    }

    return checksums;
  }

  /**
   * True when {@code name} is node-local working state that lives in a database directory without being part of the
   * database, so a checksum scan whose only purpose is to be compared with another node's must leave it out.
   * <p>
   * The first five entries are the long-standing ones: WAL logs, the {@code schema.prev.json} backup, the lock file,
   * WAL files preserved as {@code .corrupt} evidence after an aborted recovery (#4958), and the copy-on-write scratch
   * spill of an open snapshot window (#6075).
   * <p>
   * The rest are #7459. They are all published by an ATOMIC RENAME or consumed by one, which is what makes them the
   * same defect: the scan either CRCs a file that no longer exists a moment later - so two nodes compared by
   * {@code /api/v1/cluster/checksums} disagree over a file neither of them really has - or the
   * {@code FileInputStream} below fails outright and the endpoint answers 500.
   * <ul>
   * <li>{@code .tmp} - the staging name of every atomic publisher that writes into a database directory. The
   * producers found by grepping {@code '\.tmp"'} over {@code src/main/java} are {@code FileUtils.atomicWriteFile}
   * and {@code atomicCopyFile} ({@code schema.json}, {@code schema.prev.json}, {@code configuration.json}, since
   * #6114), {@code TransactionManager}, {@code TimeSeriesSealedStore} (seal, compaction, retention and
   * downsampling), {@code LSMVectorIndexGraphManifest}, {@code LSMVectorIndexOrdinalMapFile} and
   * {@code GraphAnalyticalViewCSRPersistence}. No file ArcadeDB keeps ends in {@code .tmp}: the component
   * extensions are the {@code SUPPORTED_FILE_EXT} set in {@code LocalDatabase}, and the rest of the directory is
   * {@code .json}, {@code .bin} and {@code .ts.sealed}.</li>
   * <li>{@code .ts.sealed.incoming} - where {@code ArcadeStateMachine.repairEngineWithSealedBlob} and
   * {@code TimeSeriesSealedStore.installSealedFileBytes} stage a sealed store shipped whole, before moving it into
   * place. A crashed install leaves it on disk until the next open cleans it up.</li>
   * <li>{@code .ts.sealed.parts} - where a sealed store too large for one Raft entry is reassembled slice by slice
   * (#4416), so it is present for the whole of a multi-gigabyte transfer.</li>
   * <li>{@code .snapshot-pending} - the marker saying this node has a half-installed snapshot. Its companions
   * {@code .snapshot-new} and {@code .snapshot-backup} are directories, which the {@code File::isFile} listing
   * above already excludes.</li>
   * <li>{@code .snapshot-swap-state} - the durable phase of that install's file swap (#7769). It is cleared after
   * the marker, so one crash can leave it beside a serving database until the next install; its temporary
   * sibling is covered by the {@code .tmp} rule.</li>
   * </ul>
   * The last four exist only on a FOLLOWER, and only while it is catching up, which is the worst possible
   * combination for a divergence detector: the node being interrogated is the one carrying a key the leader cannot
   * have, and the endpoint reports that as a difference in the data.
   * <p>
   * The final entry is #7955, and it is the odd one out: an index-compaction temporary is a fully REGISTERED
   * component file rather than unregistered scratch, so unlike everything above it also reaches the page snapshot
   * window and {@code FileManager.getFiles()}. It is node-local all the same - only the node that happens to be
   * compacting has one - and it needs the skip for a second reason too: its extension ({@code temp_umtidx} and
   * friends) is not in {@code LocalDatabase.SUPPORTED_FILE_EXT}, so the post-t0 page-file guard below never
   * recognised it, and on the branch where no window carries it the file was CRC'd live WHILE COMPACTION WAS
   * WRITING IT - the torn checksum {@link #computeFileChecksums(File, PageSnapshot)} exists to prevent. Skipping it
   * here settles both, before either branch is reached. See {@link PaginatedComponent#isTemporaryFileName(String)}
   * for why the test is on the extension and not on the name.
   */
  private static boolean isNodeLocalScratchFileName(final String name) {
    return name.endsWith(".wal")
        || name.endsWith(".prev.json")
        || name.endsWith(".lock")
        || name.endsWith(".corrupt")
        || name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT)
        || name.endsWith(".tmp")
        || name.endsWith(TimeSeriesSealedStore.FILE_EXTENSION + ".incoming")
        || name.endsWith(TimeSeriesSealedStore.FILE_EXTENSION + ArcadeStateMachine.SEALED_STAGING_SUFFIX)
        || name.equals(ArcadeDBServer.SNAPSHOT_PENDING_FILE)
        || name.equals(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)
        || PaginatedComponent.isTemporaryFileName(name);
  }
}
