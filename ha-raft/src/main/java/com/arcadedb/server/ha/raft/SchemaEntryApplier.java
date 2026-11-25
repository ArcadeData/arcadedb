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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.timeseries.TimeSeriesSealedInstallLock;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.ContinuousAggregateImpl;
import com.arcadedb.schema.FullSchemaLoadReason;
import com.arcadedb.schema.IncrementalLoadRefusal;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.SchemaEntryRecorder;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Applies the SCHEMA entries of the Raft log on this node: the schema delta or the full reload, the files the entry
 * creates, renames or drops, and the sealed blobs and chunks that repair a columnar or vector engine. The WAL changes the
 * entry carries go through the state machine's transaction path.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which dispatches each entry to the class that applies it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SchemaEntryApplier {
  private final ArcadeStateMachine sm;

  SchemaEntryApplier(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Applies a committed DDL (schema change) entry to the local database.
   * <p>
   * <b>Three-phase application order</b> (order matters for correctness):
   * <ol>
   *   <li><b>Create/remove physical files.</b> WAL pages reference file IDs that must already
   *       exist on the replica. File-existence guards make this idempotent on replay.</li>
   *   <li><b>Apply buffered WAL entries.</b> Index page writes that occurred during DDL on the
   *       leader are embedded in the schema entry. These target the files created in step 1.
   *       Page-version guards make this idempotent on replay.</li>
   *   <li><b>Update schema JSON and reload.</b> Writes the schema configuration and reloads
   *       types, buckets, and file IDs into memory. Naturally idempotent (overwrites with
   *       same content on replay).</li>
   * </ol>
   * <p>
   * Like {@link ArcadeStateMachine#applyTxEntry}, the originator skips this because schema changes were already
   * applied locally during the transaction.
   */
  // @VisibleForTesting - the database is resolved through the databaseFor() seam below so a test can drive this
  // against a database it opened itself, the way applyTxEntry's null-server harness already can (issue #7695).
  void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final boolean originatedLocally) {
    // Same origin-tracking as applyTxEntry: skip if this node originated the entry in the
    // current lifecycle (schema changes were already applied locally during the transaction).
    if (originatedLocally) {
      HALog.log(sm, HALog.TRACE, "Skipping schema apply on originator for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = sm.databaseFor(decoded.databaseName());

    // Issue #11946: an entry the previous session of this node already applied to the copy on disk, coming back with the replay
    // that follows a restart. Its schema document is older than the copy's, and writing it took out of the schema what later
    // entries added - an index, which a transaction committed through this node meanwhile then left out of its pages, on every
    // node - until the replay reached them again. Its files, its renames and its document are already there, or superseded:
    // only its pages are applied, which their versions keep idempotent.
    final boolean appliedBefore = sm.appliedIndexes.appliedByPreviousSession(decoded.databaseName(), entryIndex)
        && !createsAFileTheCopyHoldsUnderAnotherName(db, decoded.filesToAdd());
    if (appliedBefore)
      HALog.log(sm, HALog.DETAILED,
          "Schema entry %d of database '%s' was applied by the previous session: keeping the schema and the files of the copy",
          entryIndex, decoded.databaseName());

    HALog.log(sm, HALog.DETAILED,
        "Applying schema entry to database '%s' (entryIndex=%d): filesToAdd=%d, filesToRemove=%d, schemaPayload=%s",
        decoded.databaseName(), entryIndex,
        decoded.filesToAdd() != null ? decoded.filesToAdd().size() : 0,
        decoded.filesToRemove() != null ? decoded.filesToRemove().size() : 0,
        decoded.schemaDelta() != null ? "delta" :
            decoded.schemaJson() != null && !decoded.schemaJson().isEmpty() ? "document" : "none");

    if (HALog.isEnabled(HALog.DETAILED)) {
      HALog.log(sm, HALog.DETAILED, "Received SCHEMA_ENTRY filesToAdd=%s", decoded.filesToAdd());
      HALog.log(sm, HALog.DETAILED, "Received SCHEMA_ENTRY filesToRemove=%s", decoded.filesToRemove());
      logFollowerSchemaPayloadDiagnostics(decoded.databaseName(), decoded.schemaJson(),
          decoded.filesToAdd());
    }

    // A TimeSeries compaction/maintenance entry carries only sealed-store blobs (+ the mutable-bucket
    // clear WAL) and never changes the schema or creates/removes paginated files. For such entries we
    // MUST NOT re-update + reload the schema: load() re-instantiates every TimeSeries engine (closing
    // shard executors with a 30s awaitTermination) on the Raft apply thread, stalling replication.
    // installSealedFileBytes already reopened the sealed store and the clear WAL applies to the live
    // mutable-bucket pages, so neither the schema update nor the reload is needed.
    // A schema delta (issue #6989) disqualifies an entry from this shortcut for the same reason it disqualifies
    // it from walOnlyEntry below: the change is carried OUTSIDE schemaJson, so an entry holding one does publish
    // a schema change and does need the reload. No producer ships both today - the compaction path always
    // carries the whole document - and this keeps that from becoming a silent skip if one ever does.
    // Issue #10677: a delete of sealed samples ships the same shape, and DOES change the schema - the range it invalidated on the
    // continuous aggregates over the type (issue #10428), which a follower promoted to leader refreshes them with. Taken for a
    // compaction, the entry's schema was never applied and the follower never held the record
    final boolean sealedOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && isEmptyMap(decoded.filesToRename()) && decoded.schemaDelta() == null
        && (isNotEmpty(decoded.sealedFileBlobs()) || isNotEmpty(decoded.sealedFileChunks()))
        && !carriesAContinuousAggregateRecordNotHeldHere(db, decoded.schemaJson());

    // A non-final chunk of a schema change split across several entries (see
    // RaftTransactionBroker.splitSchemaEntry) only DELIVERS pages: the change is published by the last
    // chunk. Reloading the schema on such a chunk re-instantiates every component from a state that is
    // still half-delivered - and that is not merely wasted work on the single Raft apply thread, it is
    // STICKY: a compacted sub-index that cannot be resolved yet gets detached, and the later publication
    // reuses the same in-memory component, so the follower keeps serving only its mutable pages for good
    // (#5443: ~1897 of 60000 entries).
    //
    // The producer marks these chunks explicitly. Inferring them from "no schema JSON" was tried and is
    // WRONG: the first chunk carries filesToAdd and no schema JSON, which is indistinguishable from a
    // standalone DDL that adds files without changing the schema version - and skipping the reload for
    // that would leave the new files unregistered in the schema.
    final boolean deliveryOnlyEntry = decoded.moreChunksFollow();

    final SchemaEntryRecorder schemaEntryRecorder = ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER;
    if (schemaEntryRecorder != null && !deliveryOnlyEntry)
      schemaEntryRecorder.record(entryIndex);

    // A commit that ran inside a recordFileChanges() callback but created no file and left the schema
    // version untouched ships as a SCHEMA_ENTRY carrying nothing but WAL, because the buffering in
    // RaftReplicatedDatabase.commit() is what preserves ordering against the enclosing DDL. Such an
    // entry has nothing for load() to pick up - applyChanges below already updates page counts through
    // getFileByIdIfExists() - so the reload is pure cost on the single Raft apply thread, where it
    // re-instantiates every TimeSeries engine and closes shard executors with a 30s awaitTermination.
    // Same reasoning as sealedOnlyEntry above.
    //
    // A delta entry (issue #6989) carries its schema change OUTSIDE schemaJson, so it must not be mistaken for
    // one of these: it publishes a schema change and the reload below is exactly what registers it.
    final boolean walOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && isEmptyMap(decoded.filesToRename())
        && (decoded.schemaJson() == null || decoded.schemaJson().isEmpty()) && decoded.schemaDelta() == null
        && !isNotEmpty(decoded.sealedFileBlobs()) && !isNotEmpty(decoded.sealedFileChunks())
        && decoded.walEntries() != null && !decoded.walEntries().isEmpty();

    // Hold the compaction write lock of every shard this entry installs sealed bytes for, from before the
    // install until after the WAL that clears the matching mutable bucket (issue #7337). The leader ships the
    // two together so they are atomic with respect to each other, but the follower applying them is also a node
    // a backup or a snapshot ship can be running on, and THAT pairing was unguarded: TimeSeriesCompactionPause
    // holds each shard's compaction READ lock, which excludes a local compaction and excluded nothing here,
    // because installSealedFile takes only the store's own directoryLock. A copy taken across this window could
    // capture a pre-clear page image with a post-install sealed image and restore with every one of those
    // samples twice, silently. Taking the same lock a local compaction takes is what makes the pause mean on a
    // follower what it already means on a standalone database.
    try (final TimeSeriesSealedInstallLock sealedInstallLock = TimeSeriesSealedInstallLock.acquire(db,
        sealedShardsOf(decoded), ArcadeStateMachine.SEALED_INSTALL_LOCK_TIMEOUT_MS)) {
      // Before anything that reads the schema this entry publishes: it names the renamed files by their new names
      // (issue #9762)
      if (!appliedBefore) {
        renameFiles(db, decoded.filesToRename());

        if (decoded.filesToAdd() != null)
          sm.createNewFiles(db, decoded.filesToAdd());
      }

      // Install any TimeSeries sealed-store blobs BEFORE applying the WAL (issue #4382). The WAL
      // below carries the mutable-bucket clear; installing the sealed file first guarantees a query
      // never observes "cleared mutable + stale sealed" (the data-loss window).
      applySealedBlobs(db, decoded.sealedFileBlobs());

      // A sealed store too large for one entry arrives as an ordered sequence of slices (issue #4416). Every
      // slice but the last only stages bytes; the last one installs the reassembled file, and it rides THIS
      // entry - the publishing one - so the install still happens before the clear WAL below, closing the same
      // data-loss window applySealedBlobs closes for a store that fits inline.
      applySealedChunks(db, decoded.sealedFileChunks());

      // The copy of an entry applied before holds this document or a newer one
      if (!appliedBefore) {
        if (decoded.schemaDelta() != null)
          applySchemaDelta(db, decoded);
        else if (!sealedOnlyEntry && decoded.schemaJson() != null && !decoded.schemaJson().isEmpty())
          db.getSchema().getEmbedded().update(new JSONObject(decoded.schemaJson()));
      }

      // File ids this entry wrote pages into. The incremental refresh below re-runs the load hooks of the
      // already-registered components among them, which is what re-reads an LSM mutable index' page 0 - the full
      // rebuild used to get that for free by re-instantiating every component in the database (#6988).
      final Set<Integer> walTouchedFileIds = new HashSet<>();

      // Apply WAL entries BEFORE the schema reload. New files created above are initially empty;
      // reloading before writing pages would see empty files and silently ignore them, leaving
      // compaction indexes unregistered in the schema after this method returns. Writing the
      // page content first ensures load() finds valid data and registers the files properly.
      // applyChanges() uses getFileByIdIfExists() so it safely skips the page-count update for
      // files not yet registered in the schema (they will be registered by the load() below).
      final List<byte[]> walEntries = decoded.walEntries();
      if (walEntries != null && !walEntries.isEmpty()) {
        final List<Map<Integer, Integer>> bucketDeltas = decoded.bucketDeltas();
        for (int i = 0; i < walEntries.size(); i++) {
          final byte[] walData = walEntries.get(i);
          final Map<Integer, Integer> bucketDelta = bucketDeltas != null && i < bucketDeltas.size()
              ? bucketDeltas.get(i)
              : Collections.emptyMap();
          // Wrapped, not bare (issue #7695): a ReplicationException from a misaligned page count or delta range,
          // or a BufferUnderflowException from a payload shorter than the header, would otherwise leave
          // applyWithRetry through its ReplicationException arm - which rethrows unchanged as a resync signal -
          // and skip the per-database quarantine entirely, so the corrupt entry was dropped on this node and
          // nothing said so. Named per buffered entry: a schema entry carries a batch of them.
          final WALFile.WALTransaction walTx = CommittedEntryDecoding.walTransactionOfCommittedEntry(decoded, walData, entryIndex,
              RaftLogEntryType.SCHEMA_ENTRY, "buffered WAL entry " + (i + 1) + " of " + walEntries.size());
          if (walTx.pages != null)
            for (final WALFile.WALPage page : walTx.pages)
              walTouchedFileIds.add(page.fileId);
          // STRICT, as for a transaction entry: a page more than one version ahead is a write this node never
          // received. Skipping it (ignoreErrors=true, as this used to do) applied every OTHER page of the same
          // transaction, leaving the transaction half on this node - records pointing at content that never landed -
          // with nothing but a WARNING to say so. A replay after a restart is unaffected: a page already at or past
          // the entry's version is skipped or re-applied by applyChanges whatever this flag says.
          try {
            db.getTransactionManager().applyChanges(walTx, bucketDelta, false);
          } catch (final WALVersionGapException e) {
            throw sm.divergedOnWalVersionGap(decoded.databaseName(), walTx.txId, entryIndex, e);
          }
        }
        HALog.log(sm, HALog.DETAILED,
            "Applied %d buffered WAL entries from schema entry to database '%s'",
            walEntries.size(), decoded.databaseName());
      }

      // RELEASED HERE AND NOT AT THE END OF THE BLOCK: the span that has to be indivisible is [sealed image
      // installed, mutable bucket cleared], and it closes with the WAL above. What follows - retiring superseded
      // files, reloading the schema - touches no sealed store, and schema.load() re-instantiates every TimeSeries
      // engine and closes its shard executors, which is not work to be doing while holding a shard's own
      // compaction lock (issue #7337). Idempotent, so the try-with-resources below is still the safety net on
      // every path out of here.
      sealedInstallLock.close();

      // Retire the superseded files only AFTER the WAL (issue #4743). This used to run first, before the
      // schema update - and the schema update re-instantiates the affected components, so an LSM index
      // whose page 0 still named the file just deleted (the WAL that repoints it at the new compacted
      // file is applied above, i.e. later) resolved a file id that no longer existed. The follower logged
      // "Invalid sub-index for index '...' (error=File with id 'NNN' was not found)" over a state that was
      // purely transient, and the old self-repair in LSMTreeIndexMutable.onAfterLoad then tried to DROP
      // the index - a schema write a replica may not perform - which failed the apply and escalated the
      // whole database to a snapshot resync.
      //
      // And only AFTER the reload below has published the generation that no longer reads them (issue #12358). The
      // previous generation stays published for the whole of a full load, so the queries running meanwhile keep
      // resolving its indexes (issue #7963) - and those indexes read through the files this entry retires. Deleted
      // first, every lookup on them failed for the length of the load (up to 1.4 s on a database with many vector
      // indexes) with "File with id NNN was not found". The load leaves them out of the components it builds instead,
      // as if they were already gone, which keeps #4743 closed.
      final Set<Integer> retiring = !appliedBefore && decoded.filesToRemove() != null ?
          decoded.filesToRemove().keySet() :
          Collections.emptySet();

      // Reload schema after WAL pages are on disk so new index files have valid content
      // and are correctly registered (page counts, type links, in-memory structures).
      // Skipped for sealed-only TimeSeries compaction entries (see sealedOnlyEntry above), for
      // delivery-only chunks of a split schema change (see deliveryOnlyEntry above) and for WAL-only
      // entries (see walOnlyEntry above).
      //
      // The refresh is INCREMENTAL whenever the entry can be expressed that way (#6988): load() re-instantiates a
      // Component for every file in the database and reads page 0 of each, so running it once per DDL statement
      // costs O(entries x total files) - quadratic in the number of types, and all of it serialized on the single
      // Ratis apply thread (a 1209-type schema took ~2h53m to replicate, issue #6982). loadIncremental() touches
      // only the files this entry created or wrote to, and refuses - returning false, having changed nothing - for
      // every entry whose effect it cannot express (a retired file, a compacted index, a bloom filter, a new
      // dictionary), which is what keeps the ordering guarantees of #4743 and #5443 on the full-rebuild path.
      if (!appliedBefore && !sealedOnlyEntry && !deliveryOnlyEntry && !walOnlyEntry) {
        final LocalSchema schema = db.getSchema().getEmbedded();
        final IncrementalLoadRefusal refusal;
        if (!incrementalSchemaApplyEnabled())
          refusal = IncrementalLoadRefusal.of(FullSchemaLoadReason.INCREMENTAL_DISABLED,
              GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getKey() + "=false");
        else if (!isEmptyMap(decoded.filesToRename()))
          // A rename re-keys components the incremental refresh would look up by their old names (issue #9762): it takes
          // the full rebuild, which re-reads every component under the name its file now carries
          refusal = IncrementalLoadRefusal.of(FullSchemaLoadReason.FILE_RENAMED,
              decoded.filesToRename().size() + " file(s) renamed");
        else
          refusal = schema.tryLoadIncremental(ComponentFile.MODE.READ_WRITE, keysOrNull(decoded.filesToRemove()),
              walTouchedFileIds);

        if (refusal != null)
          loadSchemaInFull(schema, decoded, entryIndex, refusal, retiring);

        // The reload read the index and dictionary files again, on the apply thread (issue #11518): a copy that turns up
        // damaged here is quarantined before the next entry is applied to it, as the startup scan does for one found at open
        sm.quarantine.quarantineIfLocalCopyDamaged(decoded.databaseName(), db);
      }

      // Whichever path published above: the incremental refresh refuses every entry that retires a file (FILE_RETIRED), so a
      // non-empty set here always follows a full load that left these ids out. A load that throws leaves them in place: the
      // entry is not applied, so its retry or replay (appliedBefore false) retires them after its own load. Scans of the previous generation
      // that still read them end with a retryable IndexReplacedException rather than a bad request.
      for (final Integer fileId : retiring) {
        db.getPageManager().deleteFile(db, fileId);
        db.getFileManager().dropFile(fileId);
        db.getSchema().getEmbedded().removeFile(fileId);
      }

    } catch (final IOException e) {
      throw new RuntimeException("Failed to apply schema entry for database '" + decoded.databaseName() + "'", e);
    }

    HALog.log(sm, HALog.DETAILED, "Applied schema change to database '%s'", decoded.databaseName());
  }

  /**
   * The full {@code LocalSchema.load()} a replica falls back to when the incremental refresh declines an entry (issue
   * #10020). It re-instantiates every component of the database on the single Ratis apply thread, so it is the event behind
   * an apply-lag spike after DDL, and behind every vector index searching cold afterwards (issue #10021). It is therefore
   * never silent: the reason and the entry go to the log at INFO with the duration, and the reason-tagged counter and timer
   * ({@code arcadedb.ha.schema.full_load}) are what a dashboard alerts on.
   */
  private void loadSchemaInFull(final LocalSchema schema, final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final IncrementalLoadRefusal refusal, final Set<Integer> retiringFileIds) throws IOException {
    final SchemaFullLoadRecorder recorder = sm.schemaFullLoadRecorder;
    // Counted before the load, so one that throws is still seen
    recorder.started(refusal.reason());

    final long startNanos = System.nanoTime();
    schema.load(ComponentFile.MODE.READ_WRITE, true, false, retiringFileIds);
    final long elapsedNanos = System.nanoTime() - startNanos;
    recorder.completed(refusal.reason(), elapsedNanos);

    LogManager.instance().log(sm, Level.INFO,
        "Applied schema entry %d of database '%s' with a FULL schema load instead of the incremental refresh, which was "
            + "declined because %s [%s] (filesToAdd=%d, filesToRemove=%d, filesToRename=%d): every component of the database "
            + "was re-instantiated on the apply thread in %d ms",
        entryIndex, decoded.databaseName(), refusal.reason().getDescription(), refusal, sizeOf(decoded.filesToAdd()),
        sizeOf(decoded.filesToRemove()), sizeOf(decoded.filesToRename()), TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
  }

  private static int sizeOf(final Map<?, ?> map) {
    return map == null ? 0 : map.size();
  }

  /**
   * The shards a schema entry installs sealed bytes for, inline or sliced (issue #7337). A slice that only
   * stages bytes is included along with the one that installs: locking a shard that turns out not to be replaced
   * by this entry costs one uncontended lock, and deciding it from {@code last} would make the lock depend on a
   * flag the decoder could mis-set.
   * <p>
   * Package-private so a test can pin that BOTH carriers are covered: an inline blob and a slice sequence
   * install the same file, and covering only the first would leave every sealed store too large for one Raft
   * entry - the ones a tear costs most on - unguarded.
   */
  static List<TimeSeriesSealedInstallLock.ShardRef> sealedShardsOf(
      final RaftLogEntryCodec.DecodedEntry decoded) {
    final List<TimeSeriesSealedInstallLock.ShardRef> shards = new ArrayList<>();
    if (decoded.sealedFileBlobs() != null)
      for (final RaftLogEntryCodec.TsSealedBlob blob : decoded.sealedFileBlobs())
        shards.add(new TimeSeriesSealedInstallLock.ShardRef(blob.typeName(), blob.shardIndex()));
    if (decoded.sealedFileChunks() != null)
      for (final RaftLogEntryCodec.TsSealedChunk chunk : decoded.sealedFileChunks())
        shards.add(new TimeSeriesSealedInstallLock.ShardRef(chunk.typeName(), chunk.shardIndex()));
    return shards;
  }

  /**
   * Applies a schema change that arrived as a DELTA rather than as a whole document (issue #6989): merges it
   * into this node's own schema and hands the result to {@code LocalSchema.update()}, which is the same call the
   * whole-document path makes, so everything downstream of it - the file rewrite, the plan-cache invalidation
   * and the {@code load()} below - is unchanged.
   * <p>
   * <b>{@code baseVersion} is a diagnostic, not a gate.</b> A follower's {@code versionSerial} legitimately runs
   * AHEAD of the document the leader shipped: the {@code load()} that follows every schema entry re-saves the
   * schema whenever it repaired anything, and each save increments the counter. Refusing on a mismatch would
   * therefore refuse the common case - and there is nothing to refuse INTO, because a delta entry carries no
   * whole document to fall back to.
   * <p>
   * What makes that safe is the delta itself rather than the version: it carries the leader's authoritative key
   * sets, so the merged document has the leader's structure whatever the receiver started from, and only the
   * content of children the leader considered unchanged is inherited locally. Genuine divergence stays the
   * business of the WAL-version-gap detection and {@code checkDatabase}, which is where it was before this
   * entry type existed.
   */
  // @VisibleForTesting
  void applySchemaDelta(final DatabaseInternal db, final RaftLogEntryCodec.DecodedEntry decoded)
      throws IOException {
    final SchemaDelta.Payload delta = decoded.schemaDelta();
    final LocalSchema schema = db.getSchema().getEmbedded();

    HALog.log(sm, HALog.DETAILED,
        "Applying a %d-char schema delta to database '%s' (leader base version %d, local version %d)",
        delta.deltaJson().length(), db.getName(), delta.baseVersion(), schema.getVersion());

    final JSONObject merged = SchemaDelta.apply(schema.toJSON(), new JSONObject(delta.deltaJson()));

    // The MERGED document is what this entry publishes, so it - not the empty schemaJson slot - is what the
    // #4083 cross-reference has to look at. Only reached at DETAILED.
    if (HALog.isEnabled(HALog.DETAILED))
      logFollowerSchemaPayloadDiagnostics(db.getName(), merged.toString(), decoded.filesToAdd());

    schema.update(merged);
  }

  /**
   * Symmetric counterpart of {@code RaftReplicatedDatabase.logSchemaPayloadDiagnostics} on the
   * follower side: enumerates the {@code indexes} keys present in the inbound schema JSON and
   * flags those whose backing file is not in {@code filesToAdd}. Such names will fail to load
   * when {@code LocalSchema.load()} runs and surface as "Cannot find indexes [...]" warnings
   * (issue #4083).
   */
  private void logFollowerSchemaPayloadDiagnostics(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd) {
    if (schemaJson == null || schemaJson.isEmpty())
      return;

    try {
      final JSONObject root = new JSONObject(schemaJson);
      if (!root.has("types"))
        return;
      final JSONObject types = root.getJSONObject("types");

      final Set<String> shippedIndexNames = new HashSet<>();
      if (filesToAdd != null) {
        for (final String fullName : filesToAdd.values()) {
          final int firstDot = fullName.indexOf('.');
          shippedIndexNames.add(firstDot > 0 ? fullName.substring(0, firstDot) : fullName);
        }
      }

      for (final String typeName : types.keySet()) {
        if (!(types.get(typeName) instanceof JSONObject type))
          continue;
        if (!type.has("indexes"))
          continue;
        final JSONObject indexes = type.getJSONObject("indexes");
        for (final String idxName : indexes.keySet()) {
          final boolean shipped = shippedIndexNames.contains(idxName);
          HALog.log(sm, HALog.DETAILED,
              "[%s.applySchema] schemaJson.types.%s.indexes['%s'] %s",
              dbName, typeName, idxName,
              shipped ? "= matched in filesToAdd" : "= NOT in filesToAdd (will likely 'Cannot find indexes')");
        }
      }
    } catch (final RuntimeException e) {
      HALog.log(sm, HALog.DETAILED,
          "[%s.applySchema] schema JSON parse failed for diagnostics: %s", dbName, e.getMessage());
    }
  }

  /**
   * Whether {@code filesToAdd} names a file id the copy holds under another name (issue #11946). A replayed entry at or below the
   * position the previous session reached is then not one that session applied to this copy: it is an entry of a previous
   * incarnation of the database, dropped and created again since, and its pages name by file id files the current copy uses for
   * others. Skipping its file creation and applying its pages would write them into those files whenever their versions line up,
   * so such an entry takes the full apply, whose {@link ArcadeStateMachine#createNewFiles} refuses the id and sends the database
   * to the quarantine and the resync, before any of its pages or its document is applied.
   */
  private static boolean createsAFileTheCopyHoldsUnderAnotherName(final DatabaseInternal db, final Map<Integer, String> filesToAdd) {
    if (filesToAdd == null)
      return false;
    for (final Map.Entry<Integer, String> file : filesToAdd.entrySet())
      if (db.getFileManager().existsFile(file.getKey()) && !db.getFileManager().getFile(file.getKey()).getFileName()
          .equals(file.getValue()))
        return true;
    return false;
  }

  /**
   * Moves the component files a schema entry renamed (issue #9762): a type rename on the leader moved its bucket and
   * index files under the new type name, and the schema this entry publishes names them so. Each file is renamed through
   * its component, the way the leader renamed it, so the component, the file manager and the file on disk all follow.
   * <p>
   * Idempotent: a file already carrying the new name - a replay after a restart that came after the move - is skipped. A
   * file id this node does not have, or one that is not a paginated component, is not a replay of anything: this node's
   * file-id space has diverged from the leader's, and the throw sends the database to a resync rather than load a schema
   * naming files it does not have (the same reasoning as {@link ArcadeStateMachine#createNewFiles}).
   */
  // @VisibleForTesting
  static void renameFiles(final DatabaseInternal db, final Map<Integer, String> filesToRename) throws IOException {
    if (filesToRename == null || filesToRename.isEmpty())
      return;

    for (final Map.Entry<Integer, String> rename : filesToRename.entrySet()) {
      final int fileId = rename.getKey();
      final String newComponentName = rename.getValue();
      final ComponentFile file = db.getFileManager().getFileIfExists(fileId);
      if (file == null)
        throw new SchemaException("File id " + fileId + " of database '" + db.getName() + "' does not exist locally, but a "
            + "committed schema change renames it to '" + newComponentName + "' - this node's file-id space has diverged "
            + "from the leader's");
      if (newComponentName.equals(file.getComponentName()))
        continue;

      final Component component = db.getSchema().getEmbedded().getFileByIdIfExists(fileId);
      if (!(component instanceof PaginatedComponent paginated))
        throw new SchemaException("File id " + fileId + " of database '" + db.getName() + "' ('" + file.getComponentName()
            + "') is not a paginated component locally, but a committed schema change renames it to '" + newComponentName
            + "' - this node's file-id space has diverged from the leader's");

      paginated.rename(newComponentName);
      HALog.log(ArcadeStateMachine.class, HALog.DETAILED, "Renamed file %d of database '%s' to component '%s'", fileId,
          db.getName(), newComponentName);
    }
  }

  /**
   * Installs TimeSeries sealed-store blobs shipped by the leader (issue #4382): for each blob the
   * full {@code .ts.sealed} file is replaced atomically and the in-memory sealed store reopened.
   * Idempotent: re-applying the same blob (crash/restart replay) simply rewrites the identical file.
   * <p>
   * <b>A blob this node cannot put in place raises</b> (issues #8070 and #8172), after every other blob in the
   * entry has been attempted. See {@link SealedStoreNotInstalledException} for why it must: a Raft entry is
   * applied once and never re-shipped, so logging the failure and carrying on consumed the one payload that could
   * have repaired the type and left it engine-less for good. That covers all three ways a blob can fail to land -
   * a failed engine repair (#8070), a type this node does not have, and a type that exists but is not a
   * TIMESERIES one. The last two are guarded by a Raft-ordering invariant (the type-creation entry carries a
   * lower index and is applied first), so they should not happen; #8172 is about what it costs when the invariant
   * does not hold - for example a rolling upgrade shipping a type this node's build cannot construct - which is a
   * silent, permanent divergence on replicated data against one SEVERE log line.
   */
  // Package-private rather than private so Issue6839TsSealedBlobRecoveryTest can drive the apply path directly:
  // the recovery it pins is entirely inside this method, and a 3-node IT would only add flakiness to prove it.
  void applySealedBlobs(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedBlob> blobs)
      throws IOException {
    if (blobs == null || blobs.isEmpty())
      return;
    // Accumulated rather than thrown on the spot: the entry may carry blobs for other types, and one unrepairable
    // type must not stop the ones that CAN be installed from being installed (issue #8070).
    final List<String> unrepaired = new ArrayList<>();
    for (final RaftLogEntryCodec.TsSealedBlob blob : blobs) {
      final LocalSchema schema = db.getSchema().getEmbedded();
      if (!schema.existsType(blob.typeName())) {
        // Should not happen: the type-creation entry has a lower Raft index and is applied first. When it DOES
        // happen the payload is consumed and never re-shipped, so the entry is refused rather than checkpointed
        // over a sealed store this node never installed (issue #8172, the rule #7602 wrote down).
        LogManager.instance().log(sm, Level.SEVERE,
            "Received TimeSeries sealed blob for unknown type '%s' (db=%s); refusing the entry", null,
            blob.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "unknown type");
        continue;
      }
      if (!(schema.getType(blob.typeName()) instanceof LocalTimeSeriesType tsType)) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Received TimeSeries sealed blob for non-timeseries type '%s' (db=%s); refusing the entry", null,
            blob.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "not a TIMESERIES type");
        continue;
      }
      if (tsType.getEngine() == null) {
        // The type is registered with no engine (issue #6356), which used to end here: the blob was logged away,
        // and since a Raft entry is applied once and never re-shipped, that made every one of them a permanent
        // divergence - on the very state LocalSchema.readConfiguration() justifies keeping by pointing at HA as
        // the thing that would repair it. This blob IS that repair, so install it and retry (issue #6839).
        if (repairEngineWithSealedBlob(db, tsType, blob))
          HALog.log(sm, HALog.DETAILED,
              "Repaired TimeSeries type %s shard %d from a replicated sealed blob (%d bytes) on db '%s'",
              blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
        else
          addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "engine repair failed");
        continue;
      }
      tsType.getEngine().getShard(blob.shardIndex()).getSealedStore().installSealedFileBytes(blob.bytes());
      HALog.log(sm, HALog.DETAILED, "Installed TimeSeries sealed blob for %s shard %d (%d bytes) on db '%s'",
          blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
    }

    if (!unrepaired.isEmpty())
      throw sealedStoreNotInstalled(db, unrepaired, "blob");
  }

  /**
   * Records one (type, shard) the entry could not put in place, tagged with WHY, for the refusal raised after the
   * loop. De-duplicated because one entry can carry several slices of the same (type, shard) and an operator
   * reading the refusal needs the list of what is missing, not a list of how many payloads named it.
   */
  private static void addUnrepaired(final List<String> unrepaired, final String typeName, final int shardIndex,
      final String reason) {
    final String entry = typeName + " shard " + shardIndex + " (" + reason + ")";
    if (!unrepaired.contains(entry))
      unrepaired.add(entry);
  }

  /**
   * The refusal a consumed-and-unusable sealed payload is reported with (issue #8070).
   * <p>
   * Names every (type, shard) the entry could not put in place, because the entry is refused as a whole and an
   * operator reading one line has to see the whole of what this node is missing. The individual failures have
   * already been logged at SEVERE with their stack traces by {@code repairEngineWithSealedFile}; this is the
   * sentence that says what it COSTS, which is the half the per-failure log cannot know.
   */
  private static SealedStoreNotInstalledException sealedStoreNotInstalled(final DatabaseInternal db,
      final List<String> unrepaired, final String shipment) {
    return new SealedStoreNotInstalledException(
        "The replicated TimeSeries sealed store shipped as a " + shipment + " could not be installed on database '"
            + decodedDbName(db) + "' for " + String.join(", ", unrepaired)
            + ". A Raft entry is applied once and never re-shipped, so this entry must NOT be recorded as applied:"
            + " quarantining the database and resyncing it from the leader is the only path back (issue #8070)");
  }

  /**
   * Reassembles and installs a sealed store the leader shipped SLICED because it does not fit one Raft entry
   * (issue #4416), the counterpart of {@link #applySealedBlobs}.
   * <p>
   * WHY THE STAGING FILE IS ON DISK and not a buffer in this state machine. The slices arrive as separate Raft
   * entries, and this node persists its applied index between them, so a follower that restarts mid-sequence must
   * not need the earlier slices again - it will never be sent them. A file survives that restart; a field does
   * not. It is also what keeps a multi-gigabyte sealed store from having to fit in heap on the way in.
   * <p>
   * WHAT MAKES THE SEQUENCE SAFE. Raft applies entries in index order on every node, so the slices arrive in the
   * order the leader cut them. {@code offset == 0} TRUNCATES the staging file - through the write handle, so the
   * truncation cannot be silently skipped the way a failed delete could - which is what makes a sequence abandoned
   * by a leader that died mid-shipment cost nothing: the next leader's first slice discards whatever it left. Any
   * other slice must find the staging file exactly {@code offset} bytes long - anything else means the
   * sequence this node holds is not the sequence the leader is sending, and the only honest answer is to stop
   * trusting local state and resync, which is what {@link ReplicationException} asks for (it is caught in
   * {@code applyWithRetry} and turned into a targeted snapshot resync rather than halting the node).
   * <p>
   * The last slice is verified against the WHOLE file the leader hashed before it is installed: length first,
   * then CRC32. Per-slice CRCs are already checked by the decoder, but they only prove each piece survived the
   * wire, not that what this node assembled is what the leader had.
   * <p>
   * ONLY THE LAST SLICE IS FSYNCED, and the earlier ones deliberately are not. A power loss between applying a
   * slice and the OS flushing it can therefore leave a staging file SHORTER than this node's persisted applied
   * index implies. That is not a hole, it is what the checks above are for: the next slice finds a staging file
   * that is not {@code offset} bytes long, or the last one finds the wrong length or CRC, and either way the
   * sequence is refused and the database resyncs rather than installing a truncated store. Paying an fsync per
   * slice would buy a faster recovery from an event that already costs a restart, at the price of one flush per
   * entry on the single Raft apply thread for every sliced compaction. The final slice IS synced because after it
   * the file is moved into place and nothing checks it again.
   * <p>
   * The install itself is the one {@code applySealedBlobs} performs - an atomic move onto the store's file - and
   * the target path is derived from THIS node's schema, never from the file name in the payload: a name arriving
   * over the wire has no business selecting a path here.
   */
  // Package-private for the same reason as applySealedBlobs: the whole mechanism lives in this method, and a
  // 3-node IT can prove it end to end but cannot pin the broken-sequence arms without contriving a failure.
  void applySealedChunks(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedChunk> chunks)
      throws IOException {
    if (chunks == null || chunks.isEmpty())
      return;
    // Same accumulation as applySealedBlobs, for the same reason (issue #8070).
    final List<String> unrepaired = new ArrayList<>();
    for (final RaftLogEntryCodec.TsSealedChunk chunk : chunks) {
      final LocalSchema schema = db.getSchema().getEmbedded();
      if (!schema.existsType(chunk.typeName())) {
        // Should not happen: the type-creation entry has a lower Raft index and is applied first. Refused, not
        // stepped over, for the reason the blob path gives (issue #8172).
        LogManager.instance().log(sm, Level.SEVERE,
            "Received TimeSeries sealed slice for unknown type '%s' (db=%s); refusing the entry", null,
            chunk.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "unknown type");
        continue;
      }
      if (!(schema.getType(chunk.typeName()) instanceof LocalTimeSeriesType tsType)) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Received TimeSeries sealed slice for non-timeseries type '%s' (db=%s); refusing the entry", null,
            chunk.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "not a TIMESERIES type");
        continue;
      }

      final File target = new File(db.getDatabasePath(),
          TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), chunk.shardIndex()));
      final File staging = new File(target.getPath() + ArcadeStateMachine.SEALED_STAGING_SUFFIX);

      if (chunk.offset() > 0L && (!staging.exists() || staging.length() != chunk.offset()))
        throw new ReplicationException(String.format(
            "TimeSeries sealed slice for '%s' shard %d (db=%s) starts at offset %d but this node has staged %s; "
                + "the slice sequence is broken, resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), chunk.offset(),
            staging.exists() ? staging.length() + " bytes" : "nothing"));

      try (final RandomAccessFile out = new RandomAccessFile(staging, "rw")) {
        // The first slice truncates, and it does so through the open handle rather than by deleting the file
        // first. Deleting was the obvious way to write this and it is the wrong one: a delete that fails - a
        // handle another process still holds on Windows, a permissions hiccup - can only be logged and stepped
        // over, and RandomAccessFile.write never SHRINKS a file, so the tail of a longer abandoned sequence
        // would survive underneath the new one. The guards below still catch that (the next slice's offset
        // check, or the final length check), but only by forcing a full resync - which is precisely the cost
        // "an abandoned sequence costs nothing" says a restart does not pay. setLength cannot be stepped over:
        // it truncates or it throws, and throwing here IS the resync signal.
        if (chunk.offset() == 0L)
          out.setLength(0);
        out.seek(chunk.offset());
        out.write(chunk.bytes());
        if (chunk.last())
          FileUtils.sync(out.getFD());
      }

      if (!chunk.last()) {
        HALog.log(sm, HALog.DETAILED,
            "Staged TimeSeries sealed slice for %s shard %d at offset %d (%d bytes of %d) on db '%s'",
            chunk.typeName(), chunk.shardIndex(), chunk.offset(), chunk.bytes().length, chunk.fileLength(),
            decodedDbName(db));
        continue;
      }

      final long stagedLength = staging.length();
      if (stagedLength != chunk.fileLength()) {
        deleteSealedStagingFile(staging);
        throw new ReplicationException(String.format(
            "TimeSeries sealed store for '%s' shard %d (db=%s) reassembled to %d bytes but the leader shipped %d; "
                + "resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), stagedLength, chunk.fileLength()));
      }

      final long assembledCrc = crc32Of(staging);
      if (assembledCrc != chunk.fileCrc()) {
        deleteSealedStagingFile(staging);
        throw new ReplicationException(String.format(
            "TimeSeries sealed store for '%s' shard %d (db=%s) reassembled with CRC %d, the leader shipped %d; "
                + "resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), assembledCrc, chunk.fileCrc()));
      }

      if (tsType.getEngine() == null) {
        // Same repair as a whole-file blob performs (issue #6839): the slices ARE the authoritative copy of the
        // file whose failure to open left this type without an engine, so they go down first and initEngine()
        // opens the store over them.
        if (repairEngineWithSealedFile(db, tsType, chunk.shardIndex(), staging))
          HALog.log(sm, HALog.DETAILED,
              "Repaired TimeSeries type %s shard %d from a %d-byte replicated sealed store shipped in slices on db '%s'",
              chunk.typeName(), chunk.shardIndex(), stagedLength, decodedDbName(db));
        else
          addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "engine repair failed");
        continue;
      }

      tsType.getEngine().getShard(chunk.shardIndex()).getSealedStore().installSealedFile(staging);
      HALog.log(sm, HALog.DETAILED,
          "Installed TimeSeries sealed store for %s shard %d (%d bytes, shipped in slices) on db '%s'",
          chunk.typeName(), chunk.shardIndex(), stagedLength, decodedDbName(db));
    }

    // The sliced path had the identical defect, and it is the worse half of it: the slices were assembled, the
    // whole file was verified against the leader's length and CRC, and the last entry of the sequence was then
    // checkpointed over a repair that did not happen (issue #8070).
    if (!unrepaired.isEmpty())
      throw sealedStoreNotInstalled(db, unrepaired, "slice sequence");
  }

  private static long crc32Of(final File file) throws IOException {
    final CRC32 crc = new CRC32();
    final byte[] buffer = new byte[64 * 1024];
    try (final RandomAccessFile in = new RandomAccessFile(file, "r")) {
      for (int read = in.read(buffer); read > 0; read = in.read(buffer))
        crc.update(buffer, 0, read);
    }
    return crc.getValue();
  }

  /**
   * Removes a staging file a refused reassembly left behind. BEST EFFORT, and unlike the first slice's truncation
   * that is fine here: nothing downstream depends on this succeeding. The next sequence starts with an
   * {@code offset == 0} slice, which truncates through its own write handle whether or not this delete worked, so
   * a failure costs disk until the next compaction and never correctness. That is exactly the difference from the
   * first-slice case, where a silently skipped delete DID leave a longer sequence's tail under a shorter one.
   */
  private void deleteSealedStagingFile(final File staging) {
    if (staging.exists() && !staging.delete())
      LogManager.instance().log(sm, Level.WARNING,
          "Failed to delete stale TimeSeries sealed staging file '%s'; it costs disk until the next slice "
              + "sequence truncates it, and nothing else depends on it being gone", null, staging.getAbsolutePath());
  }

  /**
   * Puts the leader's sealed bytes in place for a type registered with no engine and re-runs {@code initEngine()}.
   * Returns whether the type now has one.
   * <p>
   * The order is the point. Retrying {@code initEngine()} FIRST recovers nothing in the reported case: the
   * engine failed because this shard's {@code .ts.sealed} could not be opened, so re-opening the same file fails
   * for the same reason and there is still no store to install the blob through. The blob is the authoritative
   * copy of exactly that file, so it goes down first and {@code initEngine()} then opens the store over it -
   * which is also why nothing is installed afterwards: the file already IS the blob.
   * <p>
   * Written through a temporary and moved with {@code REPLACE_EXISTING}, the same way
   * {@code TimeSeriesSealedStore.installSealedFileBytes} does it, so a crash mid-write cannot leave a half-file
   * where a whole one used to be. The target name is derived from the type and shard rather than taken from
   * {@code blob.fileName()}: a path from a replicated payload must not select a file on this node.
   * <p>
   * A failure here is logged and REPORTED, never thrown: one unrepairable type must not abort the apply of an
   * entry that may carry blobs for others, and the state it leaves behind is the state it started from - still
   * visible, still reported by {@code CHECK DATABASE}, still failing loudly on every read and write.
   * <p>
   * Reporting it is not the same as tolerating it. The caller collects what could not be repaired, finishes the
   * entry's other blobs, and then raises {@link SealedStoreNotInstalledException} so the entry is not checkpointed
   * as applied over a repair that did not happen (issue #8070) - a Raft entry is applied once and never
   * re-shipped, so the blob that was the repair would otherwise be consumed with nothing left to resend it.
   */
  private boolean repairEngineWithSealedBlob(final DatabaseInternal db, final LocalTimeSeriesType tsType,
      final RaftLogEntryCodec.TsSealedBlob blob) {
    // tsType.getName(), not blob.typeName(): the two are equal here - tsType came from getType(blob.typeName()) -
    // but taking it from the resolved schema type means the name selecting a file on this node provably came from
    // the local schema and not from the wire, rather than only doing so as long as the caller keeps that lookup.
    final File target = new File(db.getDatabasePath(),
        TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), blob.shardIndex()));
    final File incoming = new File(target.getPath() + ".incoming");
    try (final FileOutputStream out = new FileOutputStream(incoming)) {
      out.write(blob.bytes());
      FileUtils.sync(out.getFD());
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Received TimeSeries sealed blob for type '%s' shard %d (db=%s) whose storage engine is unavailable, and "
              + "the engine could not be initialised over it: %s", e, blob.typeName(), blob.shardIndex(),
          decodedDbName(db), e.getMessage());
      return false;
    }
    return repairEngineWithSealedFile(db, tsType, blob.shardIndex(), incoming);
  }

  /**
   * The half of {@link #repairEngineWithSealedBlob} that runs once the leader's copy of the sealed file is a file
   * on this node: move it into place and re-run {@code initEngine()} over it.
   * <p>
   * Shared with the sliced path (issue #4416), which reassembles the leader's copy on disk rather than in heap and
   * so arrives here with a path instead of an array. {@code source} is CONSUMED on success.
   * <p>
   * A repaired type is also re-scheduled for maintenance (issue #6948). The only other place that schedules an
   * existing type is {@code LocalSchema.readConfiguration()}, and it skips precisely the types this method
   * repairs: at schema load their engine was unavailable, so the gate there never fired for them. Without this
   * call the type comes back readable and writable yet permanently unmaintained for the life of the process -
   * {@code compactAll()}, {@code applyRetention()} and {@code applyDownsampling()} have no other caller, so the
   * mutable bucket grows unbounded and configured retention and downsampling silently stop being applied. The
   * leader-only skip that makes this harmless on a follower lives INSIDE the recurring task, not in
   * {@code schedule()}, so a healthy follower keeps a ticking task ready for the moment it is elected; a repaired
   * one would have none. {@code schedule()} replaces any existing task for the type name, so a repeated repair is
   * safe.
   */
  private boolean repairEngineWithSealedFile(final DatabaseInternal db, final LocalTimeSeriesType tsType,
      final int shardIndex, final File source) {
    final File target = new File(db.getDatabasePath(),
        TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), shardIndex));
    // The move and the engine creation are one step a copy of the database must not straddle (issue #7475): with no
    // engine there is no shard compaction lock for TimeSeriesCompactionPause to exclude this through, so the type's
    // own lifecycle lock stands in for it. Already held when the entry's install lock named this type - it is
    // reentrant, and the repair is only reached for types the entry names - and taken here for callers without one. A timeout
    // returns false, which the caller turns into SealedStoreNotInstalledException: the entry is refused, not consumed (issue #8070).
    final Lock lifecycleLock = tsType.getEngineLifecycleLock().writeLock();
    boolean locked = false;
    try {
      locked = lifecycleLock.tryLock(ArcadeStateMachine.SEALED_INSTALL_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (!locked) {
        LogManager.instance().log(sm, Level.SEVERE,
            "Received TimeSeries sealed store for type '%s' shard %d (db=%s) whose storage engine is unavailable, but the type "
                + "stayed locked for %dms (a backup or snapshot in flight?), so it was not installed", null, tsType.getName(),
            shardIndex, decodedDbName(db), ArcadeStateMachine.SEALED_INSTALL_LOCK_TIMEOUT_MS);
        return false;
      }
      Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      tsType.initEngine();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LogManager.instance().log(sm, Level.SEVERE,
          "Interrupted while locking TimeSeries type '%s' shard %d (db=%s) for a sealed-store repair", e,
          tsType.getName(), shardIndex, decodedDbName(db));
      return false;
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Received TimeSeries sealed store for type '%s' shard %d (db=%s) whose storage engine is unavailable, and "
              + "the engine could not be initialised over it: %s", e, tsType.getName(), shardIndex,
          decodedDbName(db), e.getMessage());
      return false;
    } finally {
      if (locked)
        lifecycleLock.unlock();
    }

    if (!tsType.isEngineAvailable()) {
      LogManager.instance().log(sm, Level.SEVERE,
          "TimeSeries type '%s' (db=%s) still has no storage engine after installing the replicated sealed store "
              + "for shard %d; skipping", null, tsType.getName(), decodedDbName(db), shardIndex);
      return false;
    }

    scheduleMaintenanceAfterRepair(db, tsType);
    return true;
  }

  /**
   * Re-arms automatic compaction, retention and downsampling for a type that has just been repaired (issue #6948).
   * <p>
   * Scheduled with {@code schema.getDatabase()} rather than the {@code db} parameter, so this task holds the same
   * instance the two pre-existing {@code schedule()} call sites hold: the one {@code LocalSchema} was built with,
   * which lives exactly as long as the schema does. {@code db} here is the server's wrapper, and wrappers are
   * replaceable - a task holding a superseded one through the scheduler's {@code WeakReference} would cancel
   * itself the moment that wrapper became garbage, which is this very bug again by another route. The replication
   * flags the recurring task needs are NOT taken from this reference: {@code runMaintenance} resolves
   * {@code getWrappedDatabaseInstance()} on every tick, for the same reason the compaction path underneath it
   * does.
   * <p>
   * Kept off the success path's error handling on purpose: the repair itself has already succeeded and the type is
   * usable again, so failing to ALSO schedule it must be logged and swallowed rather than turned into "the repair
   * failed" - the data is in place either way, and the state a thrown exception would leave is strictly worse than
   * the one it would be reporting.
   * <p>
   * The catch is deliberately wider than the {@code RejectedExecutionException} that
   * {@code LocalSchema.readConfiguration()} catches at its own {@code schedule()} call site, and the difference is
   * the caller, not the callee. That one runs during a database open, where an escaping runtime exception fails the
   * open and says so. This one runs inside the Raft apply path, whose whole contract here is that one type must not
   * abort the apply of an entry that may carry blobs for others - the same reason
   * {@link #repairEngineWithSealedBlob} reports failure rather than throwing. So no <em>exception</em> may escape,
   * a programming error included.
   * <p>
   * {@code Exception} and not {@code Throwable}, deliberately: an {@code Error} says the JVM itself is no longer in
   * a state this node can reason about, and the apply path's "do not let one type abort the entry" contract is not
   * a licence to keep applying Raft entries through one. Errors still propagate.
   * <p>
   * What IS swallowed is logged at SEVERE, with its stack trace and its exception class, and names the consequence
   * precisely, so it does not disappear: swallowing it here must not also hide it, and what it leaves behind - a
   * type maintained by nothing - is the very defect this method exists to prevent. That is louder than the sibling
   * catch in {@code LocalSchema.readConfiguration()} on purpose, and for the same reason the catch is wider: there
   * the alternative was the open failing loudly on its own, here nothing else will ever say a word.
   */
  private void scheduleMaintenanceAfterRepair(final DatabaseInternal db, final LocalTimeSeriesType tsType) {
    try {
      final LocalSchema schema = db.getSchema().getEmbedded();
      schema.getTimeSeriesMaintenanceScheduler().schedule(schema.getDatabase(), tsType);
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Repaired TimeSeries type '%s' (db=%s) but could not re-schedule its automatic maintenance; compaction, "
              + "retention and downsampling stay off for it until the database is reopened: %s: %s", e,
          tsType.getName(), decodedDbName(db), e.getClass().getSimpleName(), e.getMessage());
    }
  }

  private static String decodedDbName(final DatabaseInternal db) {
    return db != null ? db.getName() : "?";
  }

  /**
   * Whether {@code schemaJson} records, on a continuous aggregate, a range of invalidated samples that differs from the one this node
   * holds, or that this node does not hold at all (issue #10677). A TimeSeries delete writes that record inside the replication session
   * that ships the sealed bytes it rewrote, so its entry looks like a compaction's, which changes no schema; the record is what tells
   * them apart. A compaction's schema carries one only while a range is pending, and then this node holds it already, so it still
   * skips the reload. Any difference counts, a narrower range included: the reload it costs is the one a delete over mutable samples
   * always takes, and the leader's record is the one to hold.
   * <p>
   * The common case - no record anywhere in the document - costs one scan of the string and no parse. The document is parsed only
   * for a sealed entry while a range is pending on the leader, which lasts until its next refresh.
   */
  static boolean carriesAContinuousAggregateRecordNotHeldHere(final DatabaseInternal db, final String schemaJson) {
    if (schemaJson == null || !schemaJson.contains(ArcadeStateMachine.INVALIDATED_FROM_KEY))
      return false;
    final JSONObject aggregates = new JSONObject(schemaJson).getJSONObject("continuousAggregates", null);
    // the key elsewhere in the document (a property or type named after it) is no record: the entry stays a compaction's
    if (aggregates == null)
      return false;
    final Schema schema = db.getSchema();
    for (final String name : aggregates.keySet()) {
      final JSONObject definition = aggregates.getJSONObject(name, null);
      if (definition == null || !definition.has(ArcadeStateMachine.INVALIDATED_FROM_KEY) || !definition.has(ArcadeStateMachine.INVALIDATED_TO_KEY))
        continue;
      if (!schema.existsContinuousAggregate(name)
          || !(schema.getContinuousAggregate(name) instanceof ContinuousAggregateImpl held))
        return true;
      final ContinuousAggregateImpl.Invalidation record = held.getInvalidation();
      if (record == null || record.fromTs() != definition.getLong(ArcadeStateMachine.INVALIDATED_FROM_KEY, Long.MIN_VALUE)
          || record.toTs() != definition.getLong(ArcadeStateMachine.INVALIDATED_TO_KEY, Long.MIN_VALUE))
        return true;
    }
    // A record this node holds and the document does not is left alone: the leader cleared it with a refresh, whose plain schema save
    // is not shipped (issue #10634), and a promoted leader holding it only recomputes those buckets once more
    return false;
  }

  private static boolean isEmptyMap(final Map<?, ?> map) {
    return map == null || map.isEmpty();
  }

  static boolean isNotEmpty(final List<?> list) {
    return list != null && !list.isEmpty();
  }

  private static Set<Integer> keysOrNull(final Map<Integer, String> map) {
    return map == null ? null : map.keySet();
  }

  /**
   * Safety valve for {@link LocalSchema#loadIncremental} (issue #6988). Read per entry rather than cached so an
   * operator can turn the incremental refresh off on a running server and have the very next applied entry go back
   * to the full rebuild, without a restart.
   */
  // @VisibleForTesting - Issue6988SchemaIncrementalApplySettingTest pins the "read per entry, never cached" half,
  // which is the half a reader cannot tell from the call site and which HaSchemaDeltaReplicationFullRebuildIT cannot show
  // (it sets the value once, at server start).
  boolean incrementalSchemaApplyEnabled() {
    // The server-scoped value when this state machine is wired to one, the global default otherwise: tests drive
    // applySchemaEntry with no server attached, and they must exercise the same path production does.
    final ArcadeDBServer currentServer = sm.server;
    return currentServer != null ?
        currentServer.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY) :
        GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getValueAsBoolean();
  }
}
