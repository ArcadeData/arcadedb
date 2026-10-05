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
package com.arcadedb.engine.timeseries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A TimeSeries sealed store as it was LISTED at a point in time {@code t0}: its path, and the identity of the file that
 * path named then (issue #8738).
 * <p>
 * The HA verify, the HA snapshot ship and the full backup all list the sealed stores in the same read-locked frame as
 * their page window's t0, and read the bytes later, after that lock is released (#7634, #7671, #7705). The sealed
 * stores are not page files, so the window cannot carry them. A path is not a file: a TimeSeries type
 * dropped and recreated under the same name between the listing and the read leaves the listed path naming the NEW
 * {@code .ts.sealed}, which reads cleanly and describes nothing at t0. A store that is merely GONE was already caught;
 * this catches the one that came back.
 * <p>
 * The identity is the {@link BasicFileAttributes#fileKey()} (device and inode on POSIX), the size, the last-modified
 * time and the creation time. The file key alone is not enough: an inode freed by the drop can be handed straight to
 * the recreated file, and on Windows there is no file key at all. Together they make a replacement that matches on
 * every one of them require a file of the same size written at the same instant into a recycled inode. The creation
 * time adds nothing where the platform cannot report one: Linux reports the birth time only through {@code statx},
 * and otherwise {@code creationTime()} falls back to the last-modified time, so the effective identity there is file
 * key, size and last-modified time. The check is
 * deliberately conservative, so any doubt reads as "changed", which costs a retried ship or a verify reporting
 * incomplete coverage or a failed backup, never a silent "covered".
 * <p>
 * {@link #open()} checks the identity AFTER opening, so a swap before the open is seen. {@link #verifyUnchanged(long)}
 * is called again after the read, which catches the file being rewritten in place while it was read - and, as a
 * deliberate false positive, also a swap landing after the open: on POSIX the open handle kept reading the t0 bytes,
 * but telling that apart from an in-place rewrite would need the identity of the handle rather than of the path, and
 * Java exposes only the latter. Failing closed there costs a retry, never a wrong "covered".
 * <p>
 * Nothing legitimate writes a sealed store while any of the three callers reads it, on either of their paths: each
 * holds the {@link TimeSeriesCompactionPause} until the last sealed byte is read, which excludes compaction,
 * retention and downsampling (the shard compaction write lock), a follower's sealed install
 * ({@link TimeSeriesSealedInstallLock}) and the header rewrite, which only follows a block append. The verify does
 * not read the sealed stores at all when it could not take the pause. What is left - a hand-edited file, a
 * {@code touch} - is exactly what a check like this should not pass silently.
 *
 * @param file     the listed path
 * @param identity the file's attributes at t0; {@code null} when they could not be read, in which case the store is
 *                 reported as changed on every read rather than trusted unchecked
 */
public record ListedSealedStore(File file, BasicFileAttributes identity) {

  /** Raised when a listed sealed store's path no longer names the file that was listed at t0. */
  public static final class ChangedException extends IOException {
    public ChangedException(final String message) {
      super(message);
    }
  }

  /**
   * Lists the sealed stores of a database directory and captures each one's identity, or returns {@code null} when the
   * directory cannot be listed (the distinction {@link TimeSeriesSealedStore#listSealedFilesOrNull(File)} exists for).
   * Call it in the frame that fixes t0.
   */
  public static List<ListedSealedStore> listOrNull(final File databaseDirectory) {
    final File[] files = TimeSeriesSealedStore.listSealedFilesOrNull(databaseDirectory);
    if (files == null)
      return null;
    final List<ListedSealedStore> listed = new ArrayList<>(files.length);
    for (final File file : files)
      listed.add(capture(file));
    return List.copyOf(listed);
  }

  /**
   * Captures one file's identity now. A file whose attributes cannot be read - it vanished between the directory
   * listing and this call - is still returned, with no identity, so the reader reports it rather than the listing
   * silently dropping it.
   */
  public static ListedSealedStore capture(final File file) {
    BasicFileAttributes identity;
    try {
      identity = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    } catch (final IOException e) {
      identity = null;
    }
    return new ListedSealedStore(file, identity);
  }

  public String name() {
    return file.getName();
  }

  /** The size at t0, which is the size a read that passes {@link #verifyUnchanged(long)} has read. 0 when unknown. */
  public long size() {
    return identity != null ? identity.size() : 0L;
  }

  /**
   * Opens the listed path for reading and checks it still names the t0 file.
   *
   * @throws FileNotFoundException when the path names no file any more
   * @throws ChangedException      when it names a different file, or one changed since t0
   */
  public FileInputStream open() throws IOException {
    final FileInputStream in = new FileInputStream(file);
    try {
      verifyUnchanged(-1L);
      return in;
    } catch (final IOException | RuntimeException e) {
      try {
        in.close();
      } catch (final IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  /**
   * Checks the listed path still names the t0 file, unchanged.
   *
   * @param bytesRead the number of bytes a completed read consumed, checked against the t0 size; {@code -1} to skip
   *                  that comparison (before the read)
   *
   * @throws FileNotFoundException when the path names no file any more
   * @throws ChangedException      when it names a different file, or one changed since t0
   */
  public void verifyUnchanged(final long bytesRead) throws IOException {
    if (identity == null) {
      // GONE BEFORE ITS IDENTITY COULD BE TAKEN, AND STILL GONE: REPORTED AS THE VANISHED STORE IT IS, SO EVERY CALLER
      // KEEPS ITS OWN "WENT AWAY" WORDING FOR IT
      if (!file.exists())
        throw new FileNotFoundException(file.getPath() + " (no such file)");
      throw new ChangedException("TimeSeries sealed store '" + name()
          + "' could not be identified when it was listed, so what is read now cannot be tied to that point in time");
    }

    final BasicFileAttributes now;
    try {
      now = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    } catch (final NoSuchFileException e) {
      throw new FileNotFoundException(file.getPath() + " (no such file)");
    }

    final String difference = differenceFrom(now);
    if (difference != null)
      throw new ChangedException("TimeSeries sealed store '" + name()
          + "' is not the file that was listed at the point in time being read (" + difference
          + "): it was replaced or rewritten since, for instance by its type being dropped and recreated under the "
          + "same name");

    if (bytesRead >= 0 && bytesRead != identity.size())
      throw new ChangedException("TimeSeries sealed store '" + name() + "' read " + bytesRead + " bytes, but held "
          + identity.size() + " when it was listed");
  }

  /** What differs between the t0 identity and {@code now}, or {@code null} when nothing does. */
  private String differenceFrom(final BasicFileAttributes now) {
    if (!Objects.equals(identity.fileKey(), now.fileKey()))
      return "file key " + identity.fileKey() + " -> " + now.fileKey();
    if (identity.size() != now.size())
      return "size " + identity.size() + " -> " + now.size();
    if (!sameTime(identity.lastModifiedTime(), now.lastModifiedTime()))
      return "last modified " + identity.lastModifiedTime() + " -> " + now.lastModifiedTime();
    if (!sameTime(identity.creationTime(), now.creationTime()))
      return "created " + identity.creationTime() + " -> " + now.creationTime();
    return null;
  }

  private static boolean sameTime(final FileTime a, final FileTime b) {
    return a == null ? b == null : a.equals(b);
  }
}
