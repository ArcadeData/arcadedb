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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.IntFunction;
import java.util.logging.Level;

/**
 * Sidecar of a persisted JVector graph recording <b>which</b> vectors that graph was built over, not merely how
 * many.
 * <p>
 * The graph itself stores topology addressed by ordinal; the mapping from an ordinal to a record lives outside it,
 * recomputed from the location index every time the graph is loaded. Reusing a persisted graph is therefore only
 * safe while that mapping is the one the graph was built with, and until issue #6106 the only thing checked was the
 * node count. A count cannot tell two generations apart: since the renumbering compaction of issue #5870 every
 * generation's live ids are densely {@code [0, N)}, so a graph built over one set of records and a live set holding
 * a different set of the same size look identical to a count comparison. The graph is then reused with ordinals
 * resolving to records its nodes were never built from, and searches answer with wrong-but-plausible neighbours
 * rather than failing.
 * <p>
 * What the fingerprint covers is the ordinal &rarr; record correspondence: the vector id <b>and</b> the RID at each
 * ordinal, in ordinal order. Fingerprinting the ids alone would not help, because dense {@code [0, N)} is exactly
 * what two different generations both produce; it is the records behind those ids that differ.
 * <p>
 * Like {@link LSMVectorIndexPQFile} this is a plain file rather than a paginated component: it is a few dozen bytes
 * read once per graph load and rewritten once per graph persist, and keeping it out of the page system means it can
 * be removed before the graph pages are touched and written only after they are committed. That order is what makes
 * the check safe under a crash: a persist that fails observably replaces the manifest with one that refuses the
 * pages ({@link #markUnusable}), and a process killed outright leaves none at all, which the load path treats as
 * "cannot be verified" rather than as "the graph matches".
 * <p>
 * <b>This class holds no lock of its own, and callers must not write one manifest concurrently.</b> Each call is
 * individually atomic - the file is written under a per-write temporary name and moved into place - so a reader can
 * never see a half-written manifest. What is not defended here is the ORDER of two concurrent persists of the same
 * index: whichever moves last wins, and if that is not the one whose pages are on disk, the manifest certifies the
 * wrong generation. The engine serialises those persists today ({@code LSMVectorIndex.graphBuildLock} on the
 * rebuild path, the index write lock plus the {@code INDEX_STATUS} gate on the {@code build()} path), and anything
 * that changes either has to keep that true - moving the serialisation in here would only turn an ordering problem
 * into a shorter ordering problem, since the pages and the manifest are written at different times by design.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class LSMVectorIndexGraphManifest {
  public static final  String FILE_EXT         = "vecgraphfp";
  static final         int    FORMAT_VERSION   = 1;
  private static final long   FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long   FNV_PRIME        = 0x100000001b3L;

  /**
   * {@code vectorCount} of a manifest that deliberately describes nothing. No live set can ever have a negative
   * size, so a manifest carrying this always fails the comparison and always forces a rebuild - which is how a
   * failed graph persist says "whatever is on these pages, do not trust it" without relying on the count
   * comparison the absence of a manifest falls back to.
   */
  static final int UNUSABLE_VECTOR_COUNT = -1;

  /**
   * What a persisted manifest says about the graph next to it.
   *
   * @param formatVersion       version of this file's own layout
   * @param vectorCount         number of ordinals the graph was built over, or {@link #UNUSABLE_VECTOR_COUNT}
   * @param fingerprint         fingerprint of the ordinal &rarr; (vector id, RID) correspondence
   * @param closeDeferredRebuild {@code true} when the pages this manifest describes are known stale because the
   *                             most recent {@code close()} chose to defer the rebuild that would otherwise have
   *                             brought them up to date (issue #6657), rather than run it synchronously. Always
   *                             {@code false} again once a build actually completes - {@link #write(int, long)} and
   *                             {@link #markUnusable(String)} both clear it - so it answers specifically "did the
   *                             last close skip a rebuild", not the broader "is a rebuild owed" that
   *                             {@code vectorCount}/{@code fingerprint} against the live index already answers.
   */
  public record Content(int formatVersion, int vectorCount, long fingerprint, boolean closeDeferredRebuild) {
  }

  private final Path path;

  LSMVectorIndexGraphManifest(final String graphFilePath) {
    this.path = Path.of(graphFilePath + "." + FILE_EXT);
  }

  /**
   * Fingerprint of the ordinal &rarr; record correspondence a graph is built over: for every ordinal, the vector id
   * and the RID it resolves to. FNV-1a over the whole sequence, the same construction
   * {@code LocalBucket.offPageContentFingerprint} uses.
   * <p>
   * An ordinal whose RID cannot be resolved contributes a distinct marker rather than being skipped: skipping would
   * make an unresolvable ordinal invisible, so a live set that lost exactly that record would fingerprint the same.
   *
   * @param vectorIds    the ordinal &rarr; vector id array, in ordinal order
   * @param ridOfVector  resolves a vector id to its RID, or {@code null} when the location is gone
   */
  public static long fingerprintOf(final int[] vectorIds, final IntFunction<RID> ridOfVector) {
    long hash = FNV_OFFSET_BASIS;
    hash = mixInt(hash, vectorIds.length);
    for (final int vectorId : vectorIds) {
      hash = mixInt(hash, vectorId);
      final RID rid = ridOfVector.apply(vectorId);
      if (rid == null) {
        hash = mixInt(hash, -1);
        hash = mixLong(hash, -1L);
      } else {
        hash = mixInt(hash, rid.getBucketId());
        hash = mixLong(hash, rid.getPosition());
      }
    }
    return hash;
  }

  private static long mixInt(long hash, final int value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
      hash ^= (value >>> shift) & 0xFFL;
      hash *= FNV_PRIME;
    }
    return hash;
  }

  private static long mixLong(long hash, final long value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      hash ^= (value >>> shift) & 0xFFL;
      hash *= FNV_PRIME;
    }
    return hash;
  }

  public Path getFilePath() {
    return path;
  }

  public boolean exists() {
    return Files.exists(path);
  }

  /**
   * Drops the manifest. Called before the graph pages are rewritten, so that a persist interrupted half way leaves
   * a graph nothing vouches for instead of a graph the previous manifest still appears to describe.
   */
  public void invalidate() {
    try {
      Files.deleteIfExists(path);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Could not remove the vector graph manifest '%s': %s", path,
          e.getMessage());
    }
  }

  /**
   * Says that whatever is on the graph pages describes nothing this index can use, so the next load rebuilds
   * instead of judging the pages by their node count.
   * <p>
   * This is what a FAILED graph persist leaves behind. Deleting the manifest would not do: the load path reads an
   * absent manifest as "persisted by an older version" and falls back to the count comparison - the very
   * comparison this class exists to replace - so a persist that died after {@link #invalidate()} but before it
   * damaged anything would silently downgrade a perfectly good index to the weak check. Refusing the pages
   * outright costs one rebuild, and only after an event that is already logged as SEVERE.
   *
   * @param reason human-readable note stored in the file; nothing reads it back
   */
  public void markUnusable(final String reason) {
    write(UNUSABLE_VECTOR_COUNT, 0L, reason, false);
  }

  /**
   * Records the correspondence the just-committed graph was built over. Written through a temporary file so a
   * crash mid-write cannot leave a truncated manifest that reads as a valid one. Always clears
   * {@link Content#closeDeferredRebuild()}: a build that reached this call completed, so whatever a previous
   * {@code close()} deferred has now been paid for.
   */
  public void write(final int vectorCount, final long fingerprint) {
    write(vectorCount, fingerprint, null, false);
  }

  /**
   * Marks the currently-described graph as stale because {@code close()} deferred the rebuild that would have
   * refreshed it (issue #6657), rather than running it. Preserves whatever {@code vectorCount}/{@code fingerprint}
   * are already on disk - this is not a new build, just a note that the existing pages are now known outdated -
   * or falls back to {@link #UNUSABLE_VECTOR_COUNT} when nothing has ever been persisted here, so a first-ever
   * build that a large index's close deferred is recorded too.
   * <p>
   * Cleared automatically the next time {@link #write(int, long)} or {@link #markUnusable(String)} runs, which is
   * exactly when a rebuild - deferred or not - actually completes.
   * <p>
   * {@code read() == null} is treated as "nothing persisted yet" and takes the {@link #UNUSABLE_VECTOR_COUNT}
   * fallback, which is also what a transient read failure or a format-version mismatch report (both logged as
   * WARNING by {@link #read()}, not thrown) - indistinguishable from here. Accepted rather than plumbed through:
   * the fallback only downgrades an otherwise-valid manifest to "not usable", which forces one extra rebuild on
   * the next load and self-heals from there, the same cost a genuinely corrupt manifest would already pay.
   * <p>
   * Short-circuits to a single {@link #read()} (no write at all) when the flag already reads {@code true}: a
   * large index left with pending mutations and closed repeatedly with no intervening search - each close still
   * seeing the same {@code needsGraphBuild()} - would otherwise re-persist an identical manifest (temp file,
   * write, atomic rename) on every single one of those closes for no observable change.
   */
  public void markCloseDeferred() {
    final Content existing = read();
    if (existing != null && existing.closeDeferredRebuild())
      return;
    final int vectorCount = existing != null ? existing.vectorCount() : UNUSABLE_VECTOR_COUNT;
    final long fingerprint = existing != null ? existing.fingerprint() : 0L;
    write(vectorCount, fingerprint, null, true);
  }

  /**
   * The temporary file carries a per-write suffix rather than a fixed {@code .tmp} name. Graph persists for one
   * index are serialised today - by {@code LSMVectorIndex.graphBuildLock} on the rebuild path, and by the index
   * write lock plus the {@code INDEX_STATUS} gate on the {@code build()} path - but that is two invariants held in
   * two different places, and a shared temp name would turn a future change to either of them into a corrupted
   * manifest. A unique name costs nothing and removes the assumption.
   */
  private void write(final int vectorCount, final long fingerprint, final String reason,
      final boolean closeDeferredRebuild) {
    final Path temporary = path.resolveSibling(
        path.getFileName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");
    try {
      final Path parent = path.getParent();
      if (parent != null && !Files.exists(parent))
        Files.createDirectories(parent);

      // A process killed between the write and the move leaves its temporary behind. Nothing reads one - the
      // extension is not a component one, so no scan opens it - but nothing would ever remove it either, and an
      // index rebuilt often enough would quietly litter the database directory. Sweeping here keeps at most one
      // generation of leftovers around, without a lifecycle of its own. It is one directory listing per graph
      // persist, an operation that has just walked every live vector twice.
      deleteLeftoverTemporaries(parent);

      final JSONObject json = new JSONObject();
      json.put("formatVersion", FORMAT_VERSION);
      json.put("vectorCount", vectorCount);
      // As a string: a 64-bit fingerprint is not representable in the double a JSON number decodes to.
      json.put("fingerprint", Long.toString(fingerprint));
      json.put("closeDeferredRebuild", closeDeferredRebuild);
      if (reason != null)
        json.put("reason", reason);

      Files.writeString(temporary, json.toString(), StandardCharsets.UTF_8);
      try {
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final Exception e) {
      // A missing manifest only costs a rebuild on the next load, so this must never fail a graph persist.
      LogManager.instance().log(this, Level.WARNING, "Could not write the vector graph manifest '%s': %s", path,
          e.getMessage());
      try {
        Files.deleteIfExists(temporary);
      } catch (final IOException ignored) {
        // NOTHING ELSE TO DO
      }
    }
  }

  /**
   * Removes the temporaries of earlier writes of THIS manifest, matched by name so no other file can be caught.
   *
   * @param parent directory holding the manifest, or {@code null} when it has none
   */
  private void deleteLeftoverTemporaries(final Path parent) {
    if (parent == null)
      return;

    try (final DirectoryStream<Path> leftovers = Files.newDirectoryStream(parent,
        path.getFileName() + ".*.tmp")) {
      for (final Path leftover : leftovers)
        Files.deleteIfExists(leftover);
    } catch (final Exception e) {
      // Housekeeping only: a manifest that cannot be written is what matters, and that is reported below.
      LogManager.instance().log(this, Level.FINE,
          "Could not remove leftover vector graph manifest temporaries next to '%s': %s", path, e.getMessage());
    }
  }

  /**
   * @return what the manifest on disk says, or {@code null} when there is none, it cannot be read, or it was
   * written by a layout this build does not know
   */
  public Content read() {
    if (!Files.exists(path))
      return null;

    try {
      final JSONObject json = new JSONObject(Files.readString(path, StandardCharsets.UTF_8));
      final int formatVersion = json.getInt("formatVersion", -1);
      if (formatVersion != FORMAT_VERSION) {
        LogManager.instance().log(this, Level.WARNING,
            "Vector graph manifest '%s' has format version %d, expected %d: ignoring it", path, formatVersion,
            FORMAT_VERSION);
        return null;
      }
      return new Content(formatVersion, json.getInt("vectorCount", -1),
          Long.parseLong(json.getString("fingerprint", "0")),
          json.getBoolean("closeDeferredRebuild", false));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Could not read the vector graph manifest '%s': %s", path,
          e.getMessage());
      return null;
    }
  }
}
