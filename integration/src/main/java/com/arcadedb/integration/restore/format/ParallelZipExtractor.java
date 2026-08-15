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
package com.arcadedb.integration.restore.format;

import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreException;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Multi-threaded extractor for the full restore (issue #6086).
 * <p>
 * <b>Why.</b> Once the backup became parallel (#6072) the restore was the slow half of a recovery: on the same 1.25 GB
 * database the backup took 0.68 s and the restore that undoes it 2.9 s. The restore was a single thread alternating
 * between inflating into an 8 KB buffer and writing that buffer out, for the whole archive.
 * <p>
 * <b>How the parallelism works.</b> A ZIP entry is one deflate stream and has to be inflated serially, and the chunk
 * boundaries the backup writer used are not recorded anywhere in the archive - recording them would be an archive
 * format change, which is out of scope precisely because old backups have to keep restoring. So the split is
 * <i>between</i> entries, which needs no format change at all: every entry becomes its own output file, so N entries
 * can be inflated and written at once. Entries are handed out largest first, because the makespan of a set of
 * independent tasks of very different sizes is decided by when the biggest one starts.
 * <p>
 * <b>What this cannot do.</b> Per-entry parallelism needs random access to the archive ({@link ZipFile}), which the
 * two remote-ish input sources cannot provide: an archive read over http(s) is a one-shot stream, and an encrypted one
 * is a single cipher stream that only decrypts front to back. Both keep the sequential walk in
 * {@link FullRestoreFormat}, which is therefore not legacy code but the fallback for those two cases - and the
 * escape hatch for anyone who sets the thread count to 0.
 * <p>
 * <b>Concurrency.</b> Per the {@code engine-concurrency} skill this never touches {@code ForkJoinPool.commonPool()}.
 * It uses a dedicated {@link ThreadPoolExecutor} whose lifetime is exactly one restore: a restore is a rare, one-off
 * operation, so a permanently resident pool would hold idle threads for the whole life of the process, and there is no
 * steady-state pool for {@code PoolMetrics} to report on. The queue holds nothing but entry references, so it cannot
 * be the thing that runs the heap up; peak heap is bounded by construction at one copy buffer per pool thread, taken
 * from a pool of exactly that many, plus the JDK inflater's own buffer per entry in flight.
 * <p>
 * <b>Note on {@code ZipFile} and threads.</b> Concurrent {@code getInputStream} readers serialize only on the raw read
 * of compressed bytes (the JDK synchronizes on the shared source), which is the cheap part: inflating and writing, the
 * two that dominate, stay parallel. Opening one {@code ZipFile} per thread would not remove even that, because the JDK
 * caches and shares the underlying source between instances that name the same file.
 */
public class ParallelZipExtractor {
  /**
   * Copy buffer per worker thread, also used as the read-ahead buffer of the sequential path.
   * <p>
   * Its size is worth almost nothing on the write side and a great deal on the read side, which is the opposite of
   * what the issue expected: raising the copy buffer from 8 KB to 256 KB moved a 1.25 GB restore from 3.96 s to
   * 3.92 s, while putting a buffer of that size <i>under</i> the {@code ZipInputStream} of the sequential path moved
   * it from 5.16 s to 3.96 s. A restore is inflate-bound - measured at 2.96 s of inflating against 0.93 s of writing
   * for that database - and the write side was never the problem.
   */
  public static final int BUFFER_SIZE = 256 * 1024;

  /**
   * How long a failed extraction waits for the workers that are still writing an entry. A worker cannot be
   * interrupted out of a file write, so this is sized for the largest single entry a database can hold rather than
   * for a typical one; it is a degradation bound, not a timeout anyone should reach.
   */
  private static final int TERMINATION_TIMEOUT_SECONDS = 60;

  /** What the restore needs back to log the same summary line the sequential path logs. */
  public record ExtractStats(int files, long uncompressedSize) {
  }

  /**
   * An entry and the file it will become. The target is resolved once, in the validation pass, and carried from
   * there: the checks that produce it are the ones that decide whether the archive is acceptable at all, so
   * recomputing them in the worker would mean the bytes are written against a second, later answer to a question
   * already settled.
   */
  private record PlannedEntry(ZipEntry entry, File target) {
  }

  private final int                        threads;
  private final ConsoleLogger              logger;
  private final ArrayBlockingQueue<byte[]> bufferPool;

  public ParallelZipExtractor(final int threads, final ConsoleLogger logger) {
    if (threads < 1)
      throw new IllegalArgumentException("At least one restore thread is required");
    this.threads = threads;
    this.logger = logger;
    this.bufferPool = new ArrayBlockingQueue<>(threads);
  }

  public ExtractStats extract(final File archive, final File databaseDirectory) throws IOException {
    try (final ZipFile zipFile = new ZipFile(archive, StandardCharsets.UTF_8)) {
      final List<ZipEntry> entries = new ArrayList<>();
      final Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
      while (enumeration.hasMoreElements())
        entries.add(enumeration.nextElement());

      // VALIDATE EVERY NAME BEFORE ANY THREAD HAS WRITTEN ANYTHING, SO A HOSTILE ARCHIVE IS REFUSED WITHOUT LEAVING
      // BEHIND THE FILES THAT PRECEDED THE BAD ENTRY. THE SEQUENTIAL PATH CANNOT DO THIS - IT ONLY LEARNS AN ENTRY'S
      // NAME WHEN IT REACHES IT - BUT THE CENTRAL DIRECTORY GIVES THIS ONE EVERY NAME UP FRONT.
      //
      // THE DUPLICATE CHECK IS PART OF THAT AND IS NOT PEDANTRY: TWO ENTRIES OF THE SAME NAME ARE HARMLESS WHEN THEY
      // ARE WRITTEN ONE AFTER THE OTHER (THE SECOND WINS, WHICH IS WHAT THE SEQUENTIAL WALK DOES) AND ARE TWO
      // THREADS WRITING ONE FILE HERE. NO ARCADEDB BACKUP CAN PRODUCE ONE - A DATABASE DIRECTORY HAS UNIQUE FILE
      // NAMES - SO REFUSING IS BETTER THAN INVENTING A LOCKING RULE FOR AN ARCHIVE NOBODY SHOULD BE RESTORING
      final Set<String> names = new HashSet<>(entries.size());
      final List<PlannedEntry> plan = new ArrayList<>(entries.size());
      for (final ZipEntry entry : entries) {
        plan.add(new PlannedEntry(entry, resolveTarget(entry, databaseDirectory)));
        if (!names.add(entry.getName()))
          throw new IOException("The backup archive contains two entries named '%s'".formatted(entry.getName()));
      }

      // LARGEST FIRST: WITH ENTRIES THIS UNEVEN (A DATABASE IS A FEW BIG PAGE FILES AND A HANDFUL OF TINY ONES) THE
      // DURATION IS DECIDED BY HOW LATE THE BIGGEST ENTRY STARTS
      plan.sort(Comparator.comparingLong(planned -> -entryWeight(planned.entry())));

      final int poolSize = Math.min(threads, plan.size());
      if (poolSize < 1)
        return new ExtractStats(0, 0L);

      final AtomicInteger threadId = new AtomicInteger();
      // THE QUEUE IS UNBOUNDED, WHERE THE #6072 BACKUP WRITER THIS OTHERWISE MIRRORS USES A BOUNDED ONE WITH
      // CallerRunsPolicy, AND THE DIFFERENCE IS DELIBERATE. THERE, WORK IS PRODUCED CONTINUOUSLY WHILE THE FILE IS
      // READ, SO THE QUEUE IS THE BACKPRESSURE THAT KEEPS CHUNK BUFFERS - MEGABYTES EACH - FROM PILING UP. HERE THE
      // WHOLE WORKLIST IS KNOWN BEFORE THE POOL EXISTS AND IS SUBMITTED IN ONE CLOSED LOOP: ITS LENGTH IS THE NUMBER
      // OF FILES IN A DATABASE, AND WHAT IS QUEUED IS AN ENTRY REFERENCE, NOT A BUFFER. A BOUNDED QUEUE WOULD ALSO
      // COST SOMETHING REAL - CallerRunsPolicy WOULD MAKE THE SUBMITTING THREAD EXTRACT AN ENTRY, TAKING A BUFFER
      // POOLED FOR THE WORKERS AND BREAKING THE "ONE BUFFER PER POOL THREAD" HEAP BOUND
      final ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(), r -> {
        final Thread thread = new Thread(r, "arcadedb-restore-inflater-" + threadId.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      });

      try {
        final List<Future<Long>> results = new ArrayList<>(plan.size());
        for (final PlannedEntry planned : plan)
          results.add(executor.submit(uncompressEntry(zipFile, planned)));

        long databaseOrigSize = 0L;
        for (final Future<Long> result : results)
          databaseOrigSize += drain(result);

        return new ExtractStats(plan.size(), databaseOrigSize);
      } finally {
        // shutdownNow() ALONE IS NOT ENOUGH ON THE FAILURE PATH. IT DRAINS THE QUEUE, SO NO FURTHER ENTRY STARTS, BUT
        // THE INTERRUPT IT SENDS DOES NOT COME BACK OUT OF A FileOutputStream.write() OR A ZipFile READ - NEITHER IS
        // INTERRUPTIBLE - SO UP TO poolSize-1 WORKERS WOULD STILL BE WRITING FILES AFTER THIS METHOD HAD ALREADY
        // THROWN TO ITS CALLER. THAT MATTERS BECAUSE OF WHAT CALLERS DO NEXT: THE SERVER'S RESTORE HANDLER DELETES
        // THE DESTINATION DIRECTORY WHEN A RESTORE FAILS, AND DELETING A TREE THAT THREADS ARE STILL WRITING INTO IS
        // A RACE THAT LEAVES FILES BEHIND. WAITING HERE MAKES ONE GUARANTEE THE CALLER CAN RELY ON: WHEN extract()
        // RETURNS OR THROWS, NOTHING IS STILL WRITING. THE WAIT IS BOUNDED ONLY SO A PATHOLOGICAL CASE DEGRADES TO A
        // WARNING RATHER THAN A HANG; ON THE SUCCESS PATH EVERY TASK IS ALREADY DONE AND IT RETURNS AT ONCE
        executor.shutdownNow();
        awaitTermination(executor, databaseDirectory);
      }
    }
  }

  private void awaitTermination(final ThreadPoolExecutor executor, final File databaseDirectory) {
    try {
      if (!executor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        logger.errorLine("Restore workers did not stop within %d seconds: files may still be written into '%s'",
            TERMINATION_TIMEOUT_SECONDS, databaseDirectory.getAbsolutePath());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private Callable<Long> uncompressEntry(final ZipFile zipFile, final PlannedEntry planned) {
    return () -> {
      final ZipEntry entry = planned.entry();
      final File uncompressedFile = planned.target();

      // THE FILESYSTEM'S OWN ANSWER TO "IS THIS THE SAME FILE?", WHICH COMPARING NAMES CANNOT GIVE: ON A
      // CASE-INSENSITIVE FILESYSTEM - THE DEFAULT ON macOS AND WINDOWS - 'Doc_0.bucket' AND 'doc_0.bucket' ARE TWO
      // DISTINCT ENTRY NAMES AND ONE FILE, WHICH IS THE VERY RACE THE DUPLICATE-NAME CHECK EXISTS TO PREVENT. IT
      // CANNOT BE DECIDED UP FRONT WITHOUT GUESSING AT THE FILESYSTEM (AND GUESSING WRONG WOULD REFUSE AN ARCHIVE
      // THAT RESTORES PERFECTLY WELL ON A CASE-SENSITIVE ONE), SO IT IS ASKED HERE, WHERE THE ANSWER IS AUTHORITATIVE.
      // THE DESTINATION IS ALWAYS A FRESHLY CREATED DIRECTORY, SO NOTHING ELSE CAN MAKE THIS FAIL
      if (!uncompressedFile.createNewFile())
        throw new IOException(
            "Cannot restore entry '%s': '%s' already exists".formatted(entry.getName(), uncompressedFile));

      final byte[] buffer = acquireBuffer();
      long origSize = 0L;
      try (final InputStream in = zipFile.getInputStream(entry);
           final OutputStream out = new FileOutputStream(uncompressedFile)) {
        int len;
        while ((len = in.read(buffer)) > 0) {
          out.write(buffer, 0, len);
          origSize += len;
        }
      } finally {
        bufferPool.offer(buffer);
      }

      final long compressedSize = entry.getCompressedSize();
      // ONE CALL, NOT THE log()+logLine() PAIR THE SEQUENTIAL PATH USES: SEVERAL THREADS LOG HERE AND A HALF-LINE
      // FOLLOWED LATER BY ITS OTHER HALF WOULD INTERLEAVE INTO NONSENSE
      if (compressedSize > -1)
        logger.logLine(2, "- File '%s'... %s -> %s (%,d%% compression)", entry.getName(),
            FileUtils.getSizeAsString(compressedSize), FileUtils.getSizeAsString(origSize),
            origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
      else
        logger.logLine(2, "- File '%s'... uncompressed to %s", entry.getName(), FileUtils.getSizeAsString(origSize));

      return origSize;
    };
  }

  private byte[] acquireBuffer() {
    final byte[] buffer = bufferPool.poll();
    return buffer != null ? buffer : new byte[BUFFER_SIZE];
  }

  private static long drain(final Future<Long> result) throws IOException {
    try {
      return result.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RestoreException("Interrupted while restoring the database", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException ioException)
        throw ioException;
      if (cause instanceof RuntimeException runtimeException)
        throw runtimeException;
      throw new RestoreException("Error while restoring the database", cause);
    }
  }

  /**
   * The same two checks the sequential path makes, in the same order: the name must be a plain file name, and the
   * resolved path must stay inside the database directory.
   */
  private static File resolveTarget(final ZipEntry entry, final File databaseDirectory) throws IOException {
    final String fileName = entry.getName();

    FileUtils.checkValidName(fileName);

    final File uncompressedFile = new File(databaseDirectory, fileName);
    if (!uncompressedFile.toPath().normalize().startsWith(databaseDirectory.toPath().normalize()))
      throw new IOException("Bad zip entry");

    return uncompressedFile;
  }

  /** Uncompressed size when the central directory carries it, the compressed size otherwise. */
  private static long entryWeight(final ZipEntry entry) {
    final long size = entry.getSize();
    return size >= 0 ? size : Math.max(entry.getCompressedSize(), 0);
  }
}
