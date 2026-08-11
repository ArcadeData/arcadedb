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
package com.arcadedb.integration.backup.format;

import com.arcadedb.integration.backup.BackupException;
import com.arcadedb.integration.backup.IoThrottler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Multi-threaded ZIP writer for the full backup (issue #6072, phase 1).
 * <p>
 * <b>Why not {@code ZipOutputStream}.</b> The historical writer deflates the whole archive on the caller thread, which
 * caps a backup at the ~20-40 MB/s a single core sustains at level 9, so on a large database the backup is CPU bound
 * long before it is I/O bound - and its whole duration is a window during which page flushing is suspended and writers
 * are throttled. {@code ZipOutputStream} owns its {@code Deflater} and offers no way to hand it data already
 * compressed, so parallelism is impossible without emitting the container ourselves. This class does exactly that, and
 * nothing more: the bytes it produces are an ordinary ZIP archive, read by the unchanged restore path
 * ({@code FullRestoreFormat}, a plain {@code ZipInputStream} walk) and by any standard unzip tool.
 * <p>
 * <b>How the parallelism works.</b> Splitting the archive by entry would not help - a database is often a handful of
 * very large files, sometimes a single dominant one - so the split is <i>inside</i> each entry. The reader thread cuts
 * the file into fixed-size chunks and hands them to a worker pool; every chunk is deflated by its own
 * {@link Deflater} and terminated with {@link Deflater#SYNC_FLUSH}, which ends it on a byte boundary with a non-final
 * empty stored block. Deflate streams ended that way concatenate, so the writer thread simply appends the compressed
 * chunks back in order and closes the entry with one final empty stored block. This is the same construction pigz uses
 * for parallel gzip. The only cost is that each chunk starts from an empty dictionary: at the 1 MB default chunk size
 * that is well under 1% of ratio, and it is measured rather than assumed by the benchmark in
 * {@code BackupCompressionBenchmark}.
 * <p>
 * <b>Sizes are not known when the entry header is written</b>, since the compressed size only exists once the last
 * chunk comes back. The writer therefore uses the streaming form the ZIP specification provides for exactly this case:
 * general-purpose bit 3 set, zeroed sizes in the local header, and a data descriptor after the entry data. That is
 * what {@code ZipOutputStream} itself does for a {@code DEFLATED} entry of unknown size, so the layout is not a new
 * dialect, and it removes any need to seek backwards - which an encrypted backup, written through a
 * {@code CipherOutputStream}, could not do anyway.
 * <p>
 * The combination that is worth naming, because it is the one a hand-written ZIP writer most often gets wrong, is a
 * streaming data descriptor on an entry above 4 GB: the descriptor's fields become 8 bytes wide, and nothing in the
 * local header says so, so a reader has to infer it from the byte counts it has actually inflated (which is exactly
 * what {@code ZipInputStream} does). Archives produced here have been verified against four independent readers -
 * {@code ZipInputStream}, {@code ZipFile}, Info-ZIP {@code unzip -t} and Python's {@code zipfile} - with a 4 GB entry
 * as well as small ones.
 * <p>
 * <b>Concurrency.</b> Per the {@code engine-concurrency} skill this never touches {@code ForkJoinPool.commonPool()}.
 * It uses a dedicated {@link ThreadPoolExecutor} whose lifetime is exactly one backup: a backup is a rare, bursty
 * operation, so a permanently resident pool would hold idle threads for the 99.99% of the time no backup is running,
 * and there is no steady-state pool for {@code PoolMetrics} to report on. Saturation is handled by construction rather
 * than by a rejection policy: the reader keeps at most {@code threads * 2} chunks in flight, which both bounds peak
 * heap (that many input plus output buffers, ~32 MB of buffers at 8 threads - the benchmark's ~45 MB figure is the
 * process heap, this on top of its baseline) and applies backpressure to the reader, so the queue
 * cannot fill and the caller-runs policy installed as a backstop never has to fire.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ParallelZipArchiveWriter implements BackupArchiveWriter {
  /** Default size of the unit of work handed to a compression thread. */
  public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

  // ZIP FORMAT CONSTANTS - APPNOTE.TXT 6.3.x, SAME VALUES java.util.zip USES INTERNALLY
  private static final long LOCSIG           = 0x04034b50L;
  private static final long EXTSIG           = 0x08074b50L;
  private static final long CENSIG           = 0x02014b50L;
  private static final long ENDSIG           = 0x06054b50L;
  private static final long ZIP64_ENDSIG     = 0x06064b50L;
  private static final long ZIP64_LOCSIG     = 0x07064b50L;
  private static final long ZIP64_MAGICVAL   = 0xFFFFFFFFL;
  private static final int  ZIP64_MAGICCOUNT = 0xFFFF;
  private static final int  METHOD_DEFLATED  = 8;
  /** Bit 3: sizes and CRC follow the data in a descriptor. Bit 11: the entry name is UTF-8. */
  private static final int  FLAG             = 0x08 | 0x0800;
  private static final int  VERSION_DEFLATE  = 20;
  private static final int  VERSION_ZIP64    = 45;

  /** Final empty stored block: BFINAL=1, BTYPE=stored, LEN=0, NLEN=~0. Terminates a chunked deflate stream. */
  private static final byte[] FINAL_EMPTY_BLOCK   = { 0x01, 0x00, 0x00, (byte) 0xFF, (byte) 0xFF };
  /** Canonical deflate encoding of an empty input: one final fixed-Huffman block holding only the end-of-block code. */
  private static final byte[] EMPTY_DEFLATE_STREAM = { 0x03, 0x00 };

  private record CentralEntry(byte[] name, int dosTime, long crc, long compressedSize, long uncompressedSize,
                              long localHeaderOffset) {
  }

  /** A chunk on its way back to the writer thread, carrying the two buffers so both can be recycled. */
  private record CompressedChunk(byte[] input, byte[] output, int outputLength) {
  }

  private final OutputStream                    out;
  private final int                             compressionLevel;
  private final int                             chunkSize;
  private final int                             outputBufferSize;
  private final int                             maxChunksInFlight;
  private final IoThrottler                     throttler;
  private final ThreadPoolExecutor              executor;
  // THREAD CONFINEMENT, AND THE REASON THE TWO KINDS OF POOL ARE DIFFERENT TYPES: THE BUFFER POOLS ARE TOUCHED ONLY BY
  // THE SINGLE THREAD THAT CALLS addFile - IT TAKES A BUFFER BEFORE SUBMITTING A CHUNK AND PUTS IT BACK IN drainChunk,
  // NEVER FROM A WORKER - SO A PLAIN ArrayDeque IS CORRECT AND CHEAPER. THE DEFLATERS ARE THE OPPOSITE: WORKERS TAKE
  // AND RETURN THEM, SO THAT ONE HAS TO BE CONCURRENT. DO NOT "FIX" THE ArrayDeques INTO CONCURRENT COLLECTIONS, AND
  // DO NOT START TOUCHING THEM FROM A WORKER - THE CONFINEMENT IS THE INVARIANT, NOT THE COLLECTION TYPE
  private final ConcurrentLinkedQueue<Deflater> deflaterPool     = new ConcurrentLinkedQueue<>();
  private final ArrayDeque<byte[]>              inputBufferPool  = new ArrayDeque<>();
  private final ArrayDeque<byte[]>              outputBufferPool = new ArrayDeque<>();
  private final List<CentralEntry>              entries          = new ArrayList<>();
  private final byte[]                          numberBuffer     = new byte[8];

  private long    written = 0L;
  private boolean closed  = false;

  public ParallelZipArchiveWriter(final OutputStream out, final int compressionLevel, final int threads,
      final IoThrottler throttler) {
    this(out, compressionLevel, threads, throttler, DEFAULT_CHUNK_SIZE);
  }

  public ParallelZipArchiveWriter(final OutputStream out, final int compressionLevel, final int threads,
      final IoThrottler throttler, final int chunkSize) {
    if (threads < 1)
      throw new IllegalArgumentException("At least one compression thread is required");
    if (chunkSize < 1024)
      throw new IllegalArgumentException("Chunk size must be at least 1KB");

    // THE COMPRESSED CHUNKS ARE WRITTEN IN ONE CALL EACH AND GO STRAIGHT THROUGH (BufferedOutputStream BYPASSES ITS
    // BUFFER FOR A WRITE AT LEAST AS LARGE AS IT), SO THIS ONLY COALESCES THE HANDFUL OF 2-TO-8 BYTE HEADER FIELDS
    // PER ENTRY, WHICH WOULD OTHERWISE BE ONE WRITE EACH ON A DATABASE WITH THOUSANDS OF FILES
    this.out = new BufferedOutputStream(out, 8192);
    this.compressionLevel = compressionLevel;
    this.chunkSize = chunkSize;
    this.outputBufferSize = deflateBound(chunkSize);
    this.maxChunksInFlight = threads * 2;
    this.throttler = throttler;

    final AtomicInteger threadId = new AtomicInteger();
    this.executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(maxChunksInFlight), r -> {
      final Thread t = new Thread(r, "arcadedb-backup-compressor-" + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public EntryStats addFile(final File inputFile) throws IOException {
    if (closed)
      throw new IllegalStateException("Backup archive already closed");

    final byte[] nameBytes = inputFile.getName().getBytes(StandardCharsets.UTF_8);
    final int dosTime = toDosTime(inputFile.lastModified());
    final long localHeaderOffset = written;

    writeLocalFileHeader(nameBytes, dosTime);

    final CRC32 crc = new CRC32();
    final ArrayDeque<Future<CompressedChunk>> inFlight = new ArrayDeque<>();

    long uncompressedSize = 0L;
    long compressedSize = 0L;
    try {
      try (final FileInputStream fileIn = new FileInputStream(inputFile)) {
        while (true) {
          final byte[] input = acquireInputBuffer();
          final int read = fileIn.readNBytes(input, 0, chunkSize);
          if (read <= 0) {
            inputBufferPool.push(input);
            break;
          }

          throttler.throttle(read);
          crc.update(input, 0, read);
          uncompressedSize += read;

          inFlight.add(submitChunk(input, read));

          while (inFlight.size() >= maxChunksInFlight)
            compressedSize += drainChunk(inFlight.poll());
        }
      }

      while (!inFlight.isEmpty())
        compressedSize += drainChunk(inFlight.poll());
    } finally {
      for (final Future<CompressedChunk> pending : inFlight)
        pending.cancel(true);
    }

    if (uncompressedSize == 0) {
      writeRaw(EMPTY_DEFLATE_STREAM);
      compressedSize += EMPTY_DEFLATE_STREAM.length;
    } else {
      writeRaw(FINAL_EMPTY_BLOCK);
      compressedSize += FINAL_EMPTY_BLOCK.length;
    }

    writeDataDescriptor(crc.getValue(), compressedSize, uncompressedSize);

    entries.add(new CentralEntry(nameBytes, dosTime, crc.getValue(), compressedSize, uncompressedSize, localHeaderOffset));

    return new EntryStats(uncompressedSize, compressedSize);
  }

  @Override
  public void close() throws IOException {
    if (closed)
      return;
    closed = true;

    try {
      final long centralDirectoryOffset = written;
      for (final CentralEntry entry : entries)
        writeCentralDirectoryEntry(entry);
      writeEndOfCentralDirectory(centralDirectoryOffset, written - centralDirectoryOffset);
      out.flush();
    } finally {
      release();
    }
  }

  @Override
  public void abort() {
    closed = true;
    release();
  }

  /**
   * Gives back the pool threads and the native memory the deflaters hold. Never throws.
   * <p>
   * The wait matters on the abort path: a worker interrupted mid-chunk still returns its {@link Deflater} to the pool
   * in its own {@code finally}, so draining the pool without waiting would race with that and leave one deflater for
   * the JDK's cleaner to reclaim instead of freeing it here. Chunks are at most a megabyte, so termination is prompt;
   * the timeout only exists so a pathological case degrades to deferred reclamation rather than a hang.
   */
  private void release() {
    executor.shutdownNow();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    for (Deflater deflater = deflaterPool.poll(); deflater != null; deflater = deflaterPool.poll())
      deflater.end();
  }

  // ------------------------------------------------------------------------------------------------- COMPRESSION

  private Future<CompressedChunk> submitChunk(final byte[] input, final int inputLength) {
    final byte[] output = acquireOutputBuffer();
    return executor.submit(new DeflateChunk(input, inputLength, output));
  }

  private long drainChunk(final Future<CompressedChunk> future) throws IOException {
    final CompressedChunk chunk;
    try {
      chunk = future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BackupException("Interrupted while compressing the backup archive", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException ioException)
        throw ioException;
      throw new BackupException("Error while compressing the backup archive", cause);
    }

    writeRaw(chunk.output(), 0, chunk.outputLength());

    inputBufferPool.push(chunk.input());
    if (chunk.output().length == outputBufferSize)
      outputBufferPool.push(chunk.output());

    return chunk.outputLength();
  }

  private final class DeflateChunk implements Callable<CompressedChunk> {
    private final byte[] input;
    private final int    inputLength;
    private       byte[] output;

    private DeflateChunk(final byte[] input, final int inputLength, final byte[] output) {
      this.input = input;
      this.inputLength = inputLength;
      this.output = output;
    }

    @Override
    public CompressedChunk call() {
      final Deflater deflater = acquireDeflater();
      try {
        deflater.setInput(input, 0, inputLength);

        int produced = 0;
        while (true) {
          if (produced == output.length) {
            final byte[] larger = new byte[output.length + (output.length >> 1)];
            System.arraycopy(output, 0, larger, 0, produced);
            output = larger;
          }
          final int available = output.length - produced;
          // SYNC_FLUSH ENDS THE CHUNK ON A BYTE BOUNDARY WITH A NON-FINAL EMPTY STORED BLOCK, WHICH IS WHAT MAKES
          // THE INDEPENDENTLY COMPRESSED CHUNKS CONCATENABLE INTO ONE VALID DEFLATE STREAM
          final int n = deflater.deflate(output, produced, available, Deflater.SYNC_FLUSH);
          produced += n;
          if (n < available)
            break;
        }
        return new CompressedChunk(input, output, produced);
      } finally {
        releaseDeflater(deflater);
      }
    }
  }

  private Deflater acquireDeflater() {
    final Deflater deflater = deflaterPool.poll();
    if (deflater == null)
      // nowrap=true: RAW DEFLATE, NO ZLIB HEADER, WHICH IS WHAT A ZIP ENTRY STORES
      return new Deflater(compressionLevel, true);
    deflater.reset();
    return deflater;
  }

  private void releaseDeflater(final Deflater deflater) {
    deflaterPool.offer(deflater);
  }

  private byte[] acquireInputBuffer() {
    final byte[] buffer = inputBufferPool.poll();
    return buffer != null ? buffer : new byte[chunkSize];
  }

  private byte[] acquireOutputBuffer() {
    final byte[] buffer = outputBufferPool.poll();
    return buffer != null ? buffer : new byte[outputBufferSize];
  }

  /**
   * zlib's {@code deflateBound}: the largest output deflate can produce for an input of {@code n} bytes, when it falls
   * back to stored blocks because the data is incompressible. The extra 128 bytes cover the trailing SYNC_FLUSH marker.
   */
  private static int deflateBound(final int n) {
    return n + (n >> 12) + (n >> 14) + (n >> 25) + 13 + 128;
  }

  // ------------------------------------------------------------------------------------------------ ZIP CONTAINER

  private void writeLocalFileHeader(final byte[] name, final int dosTime) throws IOException {
    writeInt(LOCSIG);
    writeShort(VERSION_DEFLATE);
    writeShort(FLAG);
    writeShort(METHOD_DEFLATED);
    writeInt(dosTime & 0xFFFFFFFFL);
    // CRC AND BOTH SIZES ARE UNKNOWN UNTIL THE ENTRY IS COMPLETE: THEY TRAVEL IN THE DATA DESCRIPTOR INSTEAD
    writeInt(0);
    writeInt(0);
    writeInt(0);
    writeShort(name.length);
    writeShort(0);
    writeRaw(name);
  }

  private void writeDataDescriptor(final long crc, final long compressedSize, final long uncompressedSize)
      throws IOException {
    writeInt(EXTSIG);
    writeInt(crc);
    if (compressedSize >= ZIP64_MAGICVAL || uncompressedSize >= ZIP64_MAGICVAL) {
      writeLong(compressedSize);
      writeLong(uncompressedSize);
    } else {
      writeInt(compressedSize);
      writeInt(uncompressedSize);
    }
  }

  private void writeCentralDirectoryEntry(final CentralEntry entry) throws IOException {
    final boolean zip64 = entry.compressedSize() >= ZIP64_MAGICVAL || entry.uncompressedSize() >= ZIP64_MAGICVAL
        || entry.localHeaderOffset() >= ZIP64_MAGICVAL;

    writeInt(CENSIG);
    writeShort(zip64 ? VERSION_ZIP64 : VERSION_DEFLATE);
    writeShort(zip64 ? VERSION_ZIP64 : VERSION_DEFLATE);
    writeShort(FLAG);
    writeShort(METHOD_DEFLATED);
    writeInt(entry.dosTime() & 0xFFFFFFFFL);
    writeInt(entry.crc());
    // EACH FIELD IS REPLACED BY ITS SENTINEL ONLY IF IT INDIVIDUALLY OVERFLOWS, AND THE EXTRA FIELD THEN CARRIES
    // EXACTLY THOSE, IN THE ORDER THE SPECIFICATION FIXES - WHICH IS WHAT THE SPECIFICATION REQUIRES ("the fields MUST
    // only appear if the corresponding record field is set to 0xFFFFFFFF") AND WHAT ZipOutputStream.writeCEN DOES.
    // AN ENTRY PAST 4GB IN A SMALL ARCHIVE THEREFORE CARRIES ONE 8-BYTE VALUE, NOT THREE
    final boolean size64 = entry.uncompressedSize() >= ZIP64_MAGICVAL;
    final boolean compressedSize64 = entry.compressedSize() >= ZIP64_MAGICVAL;
    final boolean offset64 = entry.localHeaderOffset() >= ZIP64_MAGICVAL;
    final int zip64Fields = (size64 ? 1 : 0) + (compressedSize64 ? 1 : 0) + (offset64 ? 1 : 0);

    writeInt(compressedSize64 ? ZIP64_MAGICVAL : entry.compressedSize());
    writeInt(size64 ? ZIP64_MAGICVAL : entry.uncompressedSize());
    writeShort(entry.name().length);
    writeShort(zip64 ? 4 + 8 * zip64Fields : 0);
    writeShort(0);
    writeShort(0);
    writeShort(0);
    writeInt(0);
    writeInt(offset64 ? ZIP64_MAGICVAL : entry.localHeaderOffset());
    writeRaw(entry.name());

    if (zip64) {
      writeShort(0x0001);
      writeShort(8 * zip64Fields);
      if (size64)
        writeLong(entry.uncompressedSize());
      if (compressedSize64)
        writeLong(entry.compressedSize());
      if (offset64)
        writeLong(entry.localHeaderOffset());
    }
  }

  private void writeEndOfCentralDirectory(final long centralDirectoryOffset, final long centralDirectorySize)
      throws IOException {
    final int count = entries.size();
    final boolean zip64 = count >= ZIP64_MAGICCOUNT || centralDirectoryOffset >= ZIP64_MAGICVAL
        || centralDirectorySize >= ZIP64_MAGICVAL;

    if (zip64) {
      final long zip64EndOffset = written;

      writeInt(ZIP64_ENDSIG);
      writeLong(44);                 // SIZE OF THIS RECORD FROM HERE ON
      writeShort(VERSION_ZIP64);
      writeShort(VERSION_ZIP64);
      writeInt(0);                   // THIS DISK
      writeInt(0);                   // DISK HOLDING THE CENTRAL DIRECTORY
      writeLong(count);
      writeLong(count);
      writeLong(centralDirectorySize);
      writeLong(centralDirectoryOffset);

      writeInt(ZIP64_LOCSIG);
      writeInt(0);
      writeLong(zip64EndOffset);
      writeInt(1);                   // TOTAL NUMBER OF DISKS
    }

    writeInt(ENDSIG);
    writeShort(0);
    writeShort(0);
    writeShort(Math.min(count, ZIP64_MAGICCOUNT));
    writeShort(Math.min(count, ZIP64_MAGICCOUNT));
    writeInt(Math.min(centralDirectorySize, ZIP64_MAGICVAL));
    writeInt(Math.min(centralDirectoryOffset, ZIP64_MAGICVAL));
    writeShort(0);
  }

  /**
   * MS-DOS date and time, the only timestamp a ZIP header carries. Anything outside the representable
   * 1980-2107 range is clamped rather than wrapping around into a bogus date.
   */
  private static int toDosTime(final long javaTime) {
    final LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(javaTime), ZoneId.systemDefault());
    final int year = time.getYear();
    if (year < 1980)
      return (1 << 21) | (1 << 16);
    if (year > 2107)
      return (127 << 25) | (12 << 21) | (31 << 16) | (23 << 11) | (59 << 5) | 29;
    return (year - 1980) << 25 | time.getMonthValue() << 21 | time.getDayOfMonth() << 16 | time.getHour() << 11
        | time.getMinute() << 5 | time.getSecond() >> 1;
  }

  // ------------------------------------------------------------------------------------------------- RAW OUTPUT

  private void writeRaw(final byte[] bytes) throws IOException {
    writeRaw(bytes, 0, bytes.length);
  }

  private void writeRaw(final byte[] bytes, final int offset, final int length) throws IOException {
    out.write(bytes, offset, length);
    written += length;
  }

  private void writeShort(final int value) throws IOException {
    numberBuffer[0] = (byte) value;
    numberBuffer[1] = (byte) (value >>> 8);
    writeRaw(numberBuffer, 0, 2);
  }

  private void writeInt(final long value) throws IOException {
    numberBuffer[0] = (byte) value;
    numberBuffer[1] = (byte) (value >>> 8);
    numberBuffer[2] = (byte) (value >>> 16);
    numberBuffer[3] = (byte) (value >>> 24);
    writeRaw(numberBuffer, 0, 4);
  }

  private void writeLong(final long value) throws IOException {
    for (int i = 0; i < 8; i++)
      numberBuffer[i] = (byte) (value >>> (i * 8));
    writeRaw(numberBuffer, 0, 8);
  }
}
