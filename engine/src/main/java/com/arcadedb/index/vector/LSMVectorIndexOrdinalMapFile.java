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

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.logging.Level;

/**
 * The ordinal &rarr; (vector id, RID) correspondence of a persisted JVector graph, written out in full rather than
 * re-derived on load.
 * <p>
 * The graph stores topology addressed by ordinal and nothing that says what an ordinal means; until issue #7842 the
 * meaning was recomputed at load time as "the live vector ids, ascending", which is only the same array the graph was
 * built with while the live set has not lost a member. Delete one vector and every ordinal past it shifts by one, so
 * the graph and the recomputed array describe different records - the corruption issue #3135 fixed by refusing the
 * graph outright and rebuilding from scratch, an O(N) rebuild for a single tombstone.
 * <p>
 * Recording the array removes the need for that recomputation, and with it the need for the rebuild: an ordinal whose
 * vector id has since been tombstoned is simply a dead node, which {@link LiveVectorBitsFilter} already refuses to
 * return from a search (issue #5558) while the beam keeps walking through it. Tombstones are then paid for on a
 * schedule - the ordinary mutation threshold, through the admission-controlled async rebuild - instead of by whichever
 * query first touches the index after a delete.
 * <p>
 * The RIDs are recorded next to the ids because a vector id alone cannot prove the graph still matches: a compaction
 * renumbers the whole live set densely from 0 (issue #5870), so an id that survives a crash between the renumbering
 * and the graph persist is live, in range, and answers for a different record. Checking the RID at each ordinal is
 * what separates "this graph is behind" from "this graph describes something else", and it is the same question
 * {@link LSMVectorIndexGraphManifest}'s fingerprint answers for the no-deletions case - except that the fingerprint
 * cannot survive a deletion, since a tombstoned id no longer resolves to a RID at all.
 * <p>
 * Layout: a varint header ({@code FORMAT_VERSION}, entry count), then one entry per ordinal - the vector id as a
 * delta against the previous one (the array is ascending by construction, so the deltas are small and positive), the
 * RID bucket id, and the RID position - and finally an FNV-1a hash of the payload bytes. A truncated or corrupted
 * file therefore fails the hash and is ignored rather than trusted, which costs one rebuild.
 * <p>
 * Like {@link LSMVectorIndexGraphManifest} and {@link LSMVectorIndexPQFile} this is a plain file rather than a
 * paginated component, for the same reason: it is written once per graph persist and read once per graph load, and
 * keeping it outside the page system is what lets it be removed before the graph pages are touched and written only
 * after they are committed. {@link LSMVectorIndexGraphManifest} owns its lifecycle so that the two can never
 * disagree about which generation of pages they describe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexOrdinalMapFile {
  public static final  String FILE_EXT         = "vecordmap";
  static final         int    FORMAT_VERSION   = 1;
  private static final long   FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long   FNV_PRIME        = 0x100000001b3L;

  /**
   * A bucket id no RID can carry, written in place of one for an ordinal whose vector id had already lost its
   * location when the graph was persisted. Such an ordinal is dead by definition, and the load path requires it to
   * be tombstoned rather than trying to match a RID that was never recorded.
   */
  private static final int NO_RID_BUCKET = -1;

  /**
   * What a persisted ordinal map says, once its hash has verified it.
   *
   * @param vectorIds  the ordinal &rarr; vector id array, ascending, exactly as the graph was built with
   * @param bucketIds  the RID bucket id at each ordinal, or {@link #NO_RID_BUCKET}
   * @param positions  the RID position at each ordinal
   */
  record Content(int[] vectorIds, int[] bucketIds, long[] positions) {
    int size() {
      return vectorIds.length;
    }

    /**
     * @return whether a RID was recorded at all for this ordinal - it was not when the vector id had already lost
     * its location by the time the graph was persisted, which makes the ordinal dead by definition
     */
    boolean hasRid(final int ordinal) {
      return bucketIds[ordinal] != NO_RID_BUCKET;
    }
  }

  private final Path path;

  LSMVectorIndexOrdinalMapFile(final String graphFilePath) {
    this.path = Path.of(graphFilePath + "." + FILE_EXT);
  }

  public Path getFilePath() {
    return path;
  }

  public boolean exists() {
    return Files.exists(path);
  }

  /**
   * Drops the map. Called wherever {@link LSMVectorIndexGraphManifest#invalidate()} is, and for the same reason: a
   * map that outlives the pages it describes is worse than no map at all.
   */
  void invalidate() {
    try {
      Files.deleteIfExists(path);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Could not remove the vector graph ordinal map '%s': %s", path,
          e.getMessage());
    }
  }

  /**
   * Records the ordinal map of the graph that has just been committed. Written through a temporary file and moved
   * into place, so a crash mid-write leaves either the previous map or none - never a truncated one that reads as
   * whole.
   * <p>
   * A failure here is logged and swallowed: the map is an optimisation over rebuilding, so the worst an absent one
   * costs is the pre-issue-#7842 behaviour.
   *
   * @param ordinalToVectorId the array the graph was built with, ascending
   * @param ridOfVector       resolves a vector id to its RID, or {@code null} when the location is already gone
   */
  void write(final int[] ordinalToVectorId, final IntFunction<RID> ridOfVector) {
    final Path temporary = path.resolveSibling(
        path.getFileName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");
    try {
      final Path parent = path.getParent();
      if (parent != null && !Files.exists(parent))
        Files.createDirectories(parent);

      // Same housekeeping as the manifest's, and for the same reason: a process killed between the write and the
      // move leaves a temporary nothing else would ever remove.
      deleteLeftoverTemporaries(parent);

      // 14 bytes per entry is the worst case for the three varints below; the common case is a third of that, and
      // the buffer grows on its own if this estimate is short. Computed in long arithmetic and clamped: an index
      // large enough to overflow the int would otherwise ask for a NEGATIVE buffer and fail the write outright.
      final Binary payload = new Binary((int) Math.min(Integer.MAX_VALUE - 8L, 16L + ordinalToVectorId.length * 14L));
      payload.putUnsignedNumber(FORMAT_VERSION);
      payload.putUnsignedNumber(ordinalToVectorId.length);

      int previousVectorId = 0;
      for (final int vectorId : ordinalToVectorId) {
        // Delta-encoded because the array is ascending by construction (LSMVectorIndex feeds it from
        // VectorLocationIndex.getAllVectorIds(), which is sorted). putNumber is signed, so an array that ever
        // stopped being ascending would still round-trip - it would only stop being compact.
        payload.putNumber(vectorId - previousVectorId);
        previousVectorId = vectorId;

        final RID rid = ridOfVector.apply(vectorId);
        if (rid == null) {
          payload.putNumber(NO_RID_BUCKET);
          payload.putUnsignedNumber(0);
        } else {
          payload.putNumber(rid.getBucketId());
          payload.putUnsignedNumber(rid.getPosition());
        }
      }

      final byte[] bytes = payload.toByteArray();
      final byte[] file = Arrays.copyOf(bytes, bytes.length + 8);
      long hash = hashOf(bytes, bytes.length);
      for (int i = file.length - 1; i >= bytes.length; i--) {
        file[i] = (byte) hash;
        hash >>>= 8;
      }

      Files.write(temporary, file);
      try {
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Could not write the vector graph ordinal map '%s': %s", path,
          e.getMessage());
      try {
        Files.deleteIfExists(temporary);
      } catch (final IOException ignored) {
        // NOTHING ELSE TO DO
      }
      invalidate();
    }
  }

  /**
   * @return the recorded map, or {@code null} when there is none, it cannot be read, its hash does not verify, or it
   * was written by a layout this build does not know
   */
  Content read() {
    if (!Files.exists(path))
      return null;

    try {
      final byte[] bytes = Files.readAllBytes(path);
      if (bytes.length < 8)
        return null;

      final int payloadLength = bytes.length - 8;
      final Binary file = new Binary(bytes, bytes.length);
      file.position(payloadLength);
      if (file.getLong() != hashOf(bytes, payloadLength)) {
        LogManager.instance().log(this, Level.WARNING,
            "Vector graph ordinal map '%s' fails its own checksum: ignoring it", path);
        return null;
      }

      file.position(0);
      final int formatVersion = (int) file.getUnsignedNumber();
      if (formatVersion != FORMAT_VERSION) {
        LogManager.instance().log(this, Level.WARNING,
            "Vector graph ordinal map '%s' has format version %d, expected %d: ignoring it", path, formatVersion,
            FORMAT_VERSION);
        return null;
      }

      final int count = (int) file.getUnsignedNumber();
      if (count < 0)
        return null;

      final int[] vectorIds = new int[count];
      final int[] bucketIds = new int[count];
      final long[] positions = new long[count];
      int previousVectorId = 0;
      for (int ordinal = 0; ordinal < count; ordinal++) {
        previousVectorId += (int) file.getNumber();
        vectorIds[ordinal] = previousVectorId;
        bucketIds[ordinal] = (int) file.getNumber();
        positions[ordinal] = file.getUnsignedNumber();
      }
      return new Content(vectorIds, bucketIds, positions);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Could not read the vector graph ordinal map '%s': %s", path,
          e.getMessage());
      return null;
    }
  }

  /** FNV-1a over the payload bytes, the same construction the manifest fingerprint uses. */
  private static long hashOf(final byte[] bytes, final int length) {
    long hash = FNV_OFFSET_BASIS;
    for (int i = 0; i < length; i++) {
      hash ^= bytes[i] & 0xFFL;
      hash *= FNV_PRIME;
    }
    return hash;
  }

  private void deleteLeftoverTemporaries(final Path parent) {
    if (parent == null)
      return;

    try (final DirectoryStream<Path> leftovers = Files.newDirectoryStream(parent, path.getFileName() + ".*.tmp")) {
      for (final Path leftover : leftovers)
        Files.deleteIfExists(leftover);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE,
          "Could not remove leftover vector graph ordinal map temporaries next to '%s': %s", path, e.getMessage());
    }
  }
}
