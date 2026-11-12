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
package com.arcadedb.integration.misc;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;

/**
 * Chunked, multi-threaded raw deflate: the compression core shared by the full backup's ZIP writer
 * ({@code ParallelZipArchiveWriter}, issue #6072) and the export's gzip stream ({@link ParallelGzipOutputStream}, issue
 * #11539). It produces a raw deflate stream only; the container around it (a ZIP entry, a gzip member) and its checksum
 * are the caller's.
 * <p>
 * <b>How the parallelism works.</b> The caller cuts its input into chunks and hands each one to {@link #submit}; every
 * chunk is deflated by its own {@link Deflater} on a worker and terminated with {@link Deflater#SYNC_FLUSH}, which ends it
 * on a byte boundary with a non-final empty stored block. Deflate streams ended that way concatenate, so the compressed
 * chunks are written back in submission order and the caller closes the stream with {@link #FINAL_EMPTY_BLOCK} (or writes
 * {@link #EMPTY_DEFLATE_STREAM} when nothing was submitted). This is the construction pigz uses for parallel gzip: the
 * result is ONE deflate stream, not a series of independent members, so any inflater reads it. The only cost is that each
 * chunk starts from an empty dictionary: at the 1 MB default chunk size that is well under 1% of ratio.
 * <p>
 * <b>Concurrency.</b> Per the {@code engine-concurrency} skill this never touches {@code ForkJoinPool.commonPool()}. It
 * uses a dedicated {@link ThreadPoolExecutor} whose lifetime is exactly one operation (a backup, an export): those are
 * rare, bursty operations, so a permanently resident pool would hold idle threads for the time none is running, and there
 * is no steady-state pool for {@code PoolMetrics} to report on. Saturation is handled by construction rather than by a
 * rejection policy: at most {@code threads * 2} chunks are in flight, which both bounds peak heap (that many input plus
 * output buffers, roughly 4 MB per thread at the default chunk size) and applies backpressure to the producer, so the
 * queue cannot fill and the caller-runs policy installed as a backstop never has to fire.
 * <p>
 * <b>Thread confinement.</b> Everything but the workers' {@link Deflater} pool is touched only by the single thread that
 * calls {@link #submit} and {@link #drainAll}: it takes a buffer before submitting a chunk and puts it back when the chunk
 * is drained, never from a worker, so plain {@link ArrayDeque}s are correct and cheaper. The deflaters are the opposite:
 * workers take and return them, so that pool has to be concurrent. Do not "fix" the deques into concurrent collections,
 * and do not start touching them from a worker - the confinement is the invariant, not the collection type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ParallelDeflater implements AutoCloseable {
  /** Default size of the unit of work handed to a compression thread. */
  public static final int    DEFAULT_CHUNK_SIZE   = 1024 * 1024;
  /** Final empty stored block: BFINAL=1, BTYPE=stored, LEN=0, NLEN=~0. Terminates a chunked deflate stream. */
  public static final byte[] FINAL_EMPTY_BLOCK    = { 0x01, 0x00, 0x00, (byte) 0xFF, (byte) 0xFF };
  /** Canonical deflate encoding of an empty input: one final fixed-Huffman block holding only the end-of-block code. */
  public static final byte[] EMPTY_DEFLATE_STREAM = { 0x03, 0x00 };

  /** A chunk on its way back to the producer thread, carrying the two buffers so both can be recycled. */
  private record CompressedChunk(byte[] input, byte[] output, int outputLength) {
  }

  private final int                                 compressionLevel;
  private final int                                 chunkSize;
  private final int                                 outputBufferSize;
  private final int                                 maxChunksInFlight;
  private final String                              subject;
  private final ThreadPoolExecutor                  executor;
  private final ConcurrentLinkedQueue<Deflater>     deflaterPool     = new ConcurrentLinkedQueue<>();
  private final ArrayDeque<byte[]>                  inputBufferPool  = new ArrayDeque<>();
  private final ArrayDeque<byte[]>                  outputBufferPool = new ArrayDeque<>();
  private final ArrayDeque<Future<CompressedChunk>> inFlight         = new ArrayDeque<>();

  /**
   * @param compressionLevel deflate level, 0 (stored) to 9 (smallest)
   * @param threads          compression threads, at least 1
   * @param chunkSize        uncompressed bytes per unit of work, at least 1 KB
   * @param threadName       prefix of the worker thread names
   * @param subject          what is being compressed, for the error messages ("the backup archive", "the export file")
   */
  public ParallelDeflater(final int compressionLevel, final int threads, final int chunkSize, final String threadName,
      final String subject) {
    if (threads < 1)
      throw new IllegalArgumentException("At least one compression thread is required");
    if (chunkSize < 1024)
      throw new IllegalArgumentException("Chunk size must be at least 1KB");
    if (compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION)
      throw new IllegalArgumentException("Compression level must be between 0 and 9, found " + compressionLevel);

    this.compressionLevel = compressionLevel;
    this.chunkSize = chunkSize;
    this.outputBufferSize = deflateBound(chunkSize);
    this.maxChunksInFlight = threads * 2;
    this.subject = subject;

    final AtomicInteger threadId = new AtomicInteger();
    this.executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(maxChunksInFlight), r -> {
      final Thread t = new Thread(r, threadName + "-" + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  /**
   * The automatic thread count: half the available processors, capped at 8, never below 1. A backup or an export runs
   * alongside the live workload, so claiming every core would buy its own speed with the workload's CPU. The cap matters
   * because scaling is close to linear - without it a 64-core machine would put 32 threads on a job that saturates the
   * disk long before that. Taking the core count as an argument so the boundaries can be pinned by a test.
   */
  public static int autoThreads(final int availableProcessors) {
    return Math.max(1, Math.min(availableProcessors / 2, 8));
  }

  public int getChunkSize() {
    return chunkSize;
  }

  /** A buffer of {@link #getChunkSize()} bytes to fill and hand to {@link #submit}, or to give back with {@link #recycle}. */
  public byte[] acquireInputBuffer() {
    final byte[] buffer = inputBufferPool.poll();
    return buffer != null ? buffer : new byte[chunkSize];
  }

  /** Gives back an input buffer that was acquired and not submitted. */
  public void recycle(final byte[] inputBuffer) {
    inputBufferPool.push(inputBuffer);
  }

  /**
   * Queues {@code length} bytes of {@code input} for compression, then writes to {@code out} the chunks that complete, in
   * order, until fewer than {@code threads * 2} are in flight. {@code input} belongs to this deflater from here on: it comes
   * back through {@link #acquireInputBuffer()}.
   *
   * @return the compressed bytes written to {@code out} by this call
   */
  public long submit(final byte[] input, final int length, final OutputStream out) throws IOException {
    final byte[] output = acquireOutputBuffer();
    inFlight.add(executor.submit(new DeflateChunk(input, length, output)));

    long written = 0L;
    while (inFlight.size() >= maxChunksInFlight)
      written += drainOne(out);
    return written;
  }

  /**
   * Writes every chunk still in flight to {@code out}, in order. The stream is not terminated: the caller writes
   * {@link #FINAL_EMPTY_BLOCK} after it.
   *
   * @return the compressed bytes written to {@code out} by this call
   */
  public long drainAll(final OutputStream out) throws IOException {
    long written = 0L;
    while (!inFlight.isEmpty())
      written += drainOne(out);
    return written;
  }

  /**
   * Writes to {@code out}, in order, the chunks at the head of the queue whose compression is complete, without waiting for
   * the others: what a {@code flush()} can promise without stalling the producer on the workers.
   *
   * @return the compressed bytes written to {@code out} by this call
   */
  public long drainCompleted(final OutputStream out) throws IOException {
    long written = 0L;
    while (!inFlight.isEmpty() && inFlight.peek().isDone())
      written += drainOne(out);
    return written;
  }

  /** Cancels the chunks still in flight, after a failure: their output is never wanted. */
  public void cancelPending() {
    for (Future<CompressedChunk> pending = inFlight.poll(); pending != null; pending = inFlight.poll())
      pending.cancel(true);
  }

  /**
   * Cancels what is in flight, gives back the pool threads and the native memory the deflaters hold. Never throws.
   * <p>
   * The wait matters on the abort path: a worker interrupted mid-chunk still returns its {@link Deflater} to the pool in its
   * own {@code finally}, so draining the pool without waiting would race with that and leave one deflater for the JDK's
   * cleaner to reclaim instead of freeing it here. Chunks are about a megabyte, so termination is prompt; the timeout only
   * exists so a pathological case degrades to deferred reclamation rather than a hang.
   */
  @Override
  public void close() {
    cancelPending();
    executor.shutdownNow();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    for (Deflater deflater = deflaterPool.poll(); deflater != null; deflater = deflaterPool.poll())
      deflater.end();
    // ROUGHLY 4 MB PER THREAD: NOT KEPT REACHABLE THROUGH A CLOSED STREAM OR WRITER THE CALLER STILL HOLDS
    inputBufferPool.clear();
    outputBufferPool.clear();
  }

  private long drainOne(final OutputStream out) throws IOException {
    final Future<CompressedChunk> future = inFlight.poll();
    final CompressedChunk chunk;
    try {
      chunk = future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      final InterruptedIOException interrupted = new InterruptedIOException("Interrupted while compressing " + subject);
      interrupted.initCause(e);
      throw interrupted;
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException ioException)
        throw ioException;
      if (cause instanceof Error error)
        throw error;
      throw new IOException("Error while compressing " + subject, cause);
    }

    out.write(chunk.output(), 0, chunk.outputLength());

    inputBufferPool.push(chunk.input());
    // AN OUTPUT BUFFER A WORKER HAD TO GROW (INCOMPRESSIBLE INPUT PAST THE BOUND) IS NOT RECYCLED: THE POOL KEEPS ONE SIZE
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
          // SYNC_FLUSH ENDS THE CHUNK ON A BYTE BOUNDARY WITH A NON-FINAL EMPTY STORED BLOCK, WHICH IS WHAT MAKES THE
          // INDEPENDENTLY COMPRESSED CHUNKS CONCATENABLE INTO ONE VALID DEFLATE STREAM
          final int n = deflater.deflate(output, produced, available, Deflater.SYNC_FLUSH);
          produced += n;
          if (n < available)
            break;
        }
        return new CompressedChunk(input, output, produced);
      } finally {
        deflaterPool.offer(deflater);
      }
    }
  }

  private Deflater acquireDeflater() {
    final Deflater deflater = deflaterPool.poll();
    if (deflater == null)
      // nowrap=true: RAW DEFLATE, NO ZLIB HEADER, WHICH IS WHAT BOTH A ZIP ENTRY AND A GZIP MEMBER CARRY
      return new Deflater(compressionLevel, true);
    deflater.reset();
    return deflater;
  }

  private byte[] acquireOutputBuffer() {
    final byte[] buffer = outputBufferPool.poll();
    return buffer != null ? buffer : new byte[outputBufferSize];
  }

  /**
   * zlib's {@code deflateBound}: the largest output deflate can produce for an input of {@code n} bytes, when it falls back
   * to stored blocks because the data is incompressible. The extra 128 bytes cover the trailing SYNC_FLUSH marker.
   */
  private static int deflateBound(final int n) {
    return n + (n >> 12) + (n >> 14) + (n >> 25) + 13 + 128;
  }
}
