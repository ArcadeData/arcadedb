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
package com.arcadedb.bench.workload.soak;

import com.arcadedb.bench.server.ServerUnderTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Samples the server under test every {@link #PERIOD} on a thread of its own while a soak runs: the heap in use (the sum of the
 * {@code jvm_memory_used_bytes} series of area {@code heap} in the {@code /prometheus} scrape, which the JVM and the native image
 * both publish), the thread count and the open file descriptors of the server process. A reading that fails is left out of its
 * series, never recorded as zero.
 * <p>
 * The heap is summarized as the minimum of each floor window ({@link #heapFloors()}): the heap right after a collection, without
 * asking the server to collect, so a heap that only saws up and down has a flat floor and a leak has a rising one. Threads and
 * files are summarized as per-minute medians.
 * <p>
 * Every summary leaves out the samples of the warm-up, the first stretch of the soak: a server that has just started grows its
 * thread pools, its open files and its heap to their working size there, which is no leak. The windows start where it ends.
 */
public final class SoakSampler implements AutoCloseable {
  public static final Duration PERIOD        = Duration.ofSeconds(10);
  public static final Duration FLOOR_WINDOW  = Duration.ofMinutes(5);
  public static final Duration MEDIAN_WINDOW = Duration.ofMinutes(1);

  private static final Logger   LOGGER         = LoggerFactory.getLogger(SoakSampler.class);
  private static final String   HEAP_SERIES    = "jvm_memory_used_bytes{";
  private static final String   HEAP_AREA      = "area=\"heap\"";
  private static final Duration SCRAPE_TIMEOUT = Duration.ofSeconds(5);
  private static final double   NANOS_PER_HOUR = 3_600e9;
  private static final int      INITIAL        = 1_024;
  /** How long stopping waits for a sample under way: a scrape and two reads inside the container, each with its own deadline. */
  private static final Duration JOIN_BOUND     = Duration.ofSeconds(60);

  /** What one sample reads from the server; the seam that lets a test sample without one. */
  interface Source extends AutoCloseable {
    /** The text of a {@code /prometheus} scrape; null when it failed. */
    String scrape();

    /** The thread count of the server process; negative when it cannot be read. */
    int threads();

    /** The open file descriptors of the server process; negative when they cannot be read. */
    int files();

    @Override
    default void close() {
    }
  }

  private final Source         source;
  private final long           periodNanos;
  private final long           floorWindowNanos;
  private final long           warmupNanos;
  private final long           soakNanos;
  private final CountDownLatch stop = new CountDownLatch(1);
  private final Object         lock = new Object();
  private long[]               offsets = new long[INITIAL];
  private long[]               heaps   = new long[INITIAL];
  private int[]                threads = new int[INITIAL];
  private int[]                files   = new int[INITIAL];
  private int                  count;
  private Thread               thread;

  /**
   * Samples {@code server} every {@link #PERIOD}. Its summaries keep the samples from the end of {@code warmup} to {@code window}
   * after {@link #start()}: started just before the phase, whose clock starts once its clients are open, the cut falls just before
   * the end of the window, so the drain after it (loops stopped, an unloaded server) never reaches a slope.
   */
  public SoakSampler(final ServerUnderTest server, final Duration floorWindow, final Duration warmup, final Duration window) {
    this(new RemoteSource(server), PERIOD, floorWindow, warmup, window);
  }

  SoakSampler(final Source source, final Duration period, final Duration floorWindow, final Duration warmup, final Duration window) {
    this.source = source;
    this.periodNanos = period.toNanos();
    this.floorWindowNanos = floorWindow.toNanos();
    this.warmupNanos = warmup.toNanos();
    this.soakNanos = window.toNanos();
  }

  /**
   * A sampler that samples nothing, over samples recorded elsewhere (a soak read back, or a test): sample {@code i} was taken
   * {@code offsetsNanos[i]} after the start, in time order, a negative reading standing for one that failed. Starting and closing
   * it do nothing.
   */
  public static SoakSampler replay(final Duration period, final Duration floorWindow, final Duration warmup, final Duration window,
      final long[] offsetsNanos, final long[] heaps, final int[] threads, final int[] files) {
    final SoakSampler sampler = new SoakSampler(null, period, floorWindow, warmup, window);
    for (int i = 0; i < offsetsNanos.length; i++)
      sampler.add(offsetsNanos[i], heaps[i], threads[i], files[i]);
    return sampler;
  }

  /** Starts sampling, the first sample at once. */
  public void start() {
    if (source == null)
      return;
    if (thread != null)
      throw new IllegalStateException("The sampler was started already");
    final long origin = System.nanoTime();
    thread = new Thread(() -> loop(origin), "bench-soak-sampler");
    thread.setDaemon(true);
    thread.start();
  }

  private void loop(final long origin) {
    long next = origin;
    try {
      while (true) {
        final long at = System.nanoTime() - origin;
        final String scrape = source.scrape();
        final long heap = scrape != null ? heapUsed(scrape) : -1L;
        add(at, heap, source.threads(), source.files());
        // a sample slower than the period skips the slots it overran rather than bunching samples up
        final long now = System.nanoTime();
        next += periodNanos;
        while (next <= now)
          next += periodNanos;
        if (stop.await(next - now, TimeUnit.NANOSECONDS))
          return;
      }
    } catch (final InterruptedException e) {
      // stopped
    } catch (final RuntimeException e) {
      LOGGER.warn("The soak sampler stopped on a failure, the series end here", e);
    }
  }

  /**
   * Stops sampling and waits up to {@link #JOIN_BOUND} for the sampling thread to end (a sample under way finishes first). An
   * interrupt of the caller, before or during the wait, does not cut the wait short and is restored when this returns: a soak
   * cancelled at its deadline still has a stopped sampler to read its series from, and its caller still sees the interrupt.
   */
  public void stop() {
    stop.countDown();
    if (thread == null)
      return;
    boolean interrupted = Thread.interrupted();
    final long deadline = System.nanoTime() + JOIN_BOUND.toNanos();
    try {
      while (thread.isAlive()) {
        final long left = (deadline - System.nanoTime()) / 1_000_000L;
        if (left <= 0) {
          LOGGER.warn("The soak sampler did not stop within {} s, its last sample is left out", JOIN_BOUND.toSeconds());
          return;
        }
        try {
          thread.join(left);
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted)
        Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    try {
      stop();
    } finally {
      if (source != null)
        source.close();
    }
  }

  /**
   * Records one sample taken {@code offsetNanos} after the start; a negative reading is left out of its series. Samples come in
   * time order.
   */
  void add(final long offsetNanos, final long heap, final int threadCount, final int fileCount) {
    synchronized (lock) {
      if (count == offsets.length) {
        final int size = count * 2;
        offsets = Arrays.copyOf(offsets, size);
        heaps = Arrays.copyOf(heaps, size);
        threads = Arrays.copyOf(threads, size);
        files = Arrays.copyOf(files, size);
      }
      offsets[count] = offsetNanos;
      heaps[count] = heap;
      threads[count] = threadCount;
      files[count] = fileCount;
      count++;
    }
  }

  /** The samples taken so far. */
  public int samples() {
    synchronized (lock) {
      return count;
    }
  }

  /**
   * The minimum heap of each complete floor window after the warm-up as {@code {hourOffset, bytes}} pairs, the hour offset (from
   * the start of the soak) being the middle of the window: the heap after collection, on the JVM and the native image alike. A
   * window with no heap reading is left out.
   */
  public double[][] heapFloors() {
    synchronized (lock) {
      return summarize(Arrays.copyOf(heaps, count), floorWindowNanos, true);
    }
  }

  /** The median thread count of each complete minute after the warm-up as {@code {hourOffset, threads}} pairs. */
  public double[][] threads() {
    synchronized (lock) {
      return summarize(widen(threads), MEDIAN_WINDOW.toNanos(), false);
    }
  }

  /** The median count of open files of each complete minute after the warm-up as {@code {hourOffset, files}} pairs. */
  public double[][] files() {
    synchronized (lock) {
      return summarize(widen(files), MEDIAN_WINDOW.toNanos(), false);
    }
  }

  private long[] widen(final int[] series) {
    final long[] values = new long[count];
    for (int i = 0; i < count; i++)
      values[i] = series[i];
    return values;
  }

  /**
   * One pair per complete window of {@code windowNanos} after the warm-up holding a reading: the window minimum when
   * {@code minimum}, the median otherwise. The windows start where the warm-up ends and end by the end of the soak window; a window
   * is complete when the samples cover it to its end, the last sample standing for one period. Samples past the soak window are
   * never read.
   */
  private double[][] summarize(final long[] values, final long windowNanos, final boolean minimum) {
    final long covered = count == 0 ? 0 : Math.min(offsets[count - 1] + periodNanos, soakNanos) - warmupNanos;
    if (covered <= 0)
      return new double[0][];
    final int windows = (int) (covered / windowNanos);
    final double[][] pairs = new double[windows][];
    final long[] window = new long[count];
    int found = 0;
    int i = 0;
    while (i < count && offsets[i] < warmupNanos)
      i++;
    for (int w = 0; w < windows; w++) {
      final long end = warmupNanos + (w + 1) * windowNanos;
      int n = 0;
      while (i < count && offsets[i] < end) {
        if (values[i] >= 0)
          window[n++] = values[i];
        i++;
      }
      if (n == 0)
        continue;
      Arrays.sort(window, 0, n);
      final double value = minimum ? window[0] : (n & 1) == 1 ? window[n / 2] : (window[n / 2 - 1] + window[n / 2]) / 2.0;
      pairs[found++] = new double[] { (warmupNanos + (w + 0.5) * windowNanos) / NANOS_PER_HOUR, value };
    }
    return found == windows ? pairs : Arrays.copyOf(pairs, found);
  }

  /** The heap in use of a {@code /prometheus} scrape: the sum of its {@code jvm_memory_used_bytes} lines of area heap; -1 when it has none. */
  static long heapUsed(final String scrape) {
    double sum = 0;
    boolean any = false;
    for (final String line : scrape.split("\\R")) {
      if (!line.startsWith(HEAP_SERIES))
        continue;
      final int close = line.lastIndexOf('}');
      if (close < 0 || !line.substring(0, close).contains(HEAP_AREA))
        continue;
      final String rest = line.substring(close + 1).trim();
      final int space = rest.indexOf(' ');
      try {
        sum += Double.parseDouble(space < 0 ? rest : rest.substring(0, space));
        any = true;
      } catch (final NumberFormatException e) {
        // a value the scrape could not express is no reading
      }
    }
    return any ? (long) sum : -1L;
  }

  /** The readings of the server under test: the scrape with the root credentials, the threads and files from its process. */
  private static final class RemoteSource implements Source {
    private final ServerUnderTest server;
    private final HttpClient      client;
    private final String          authorization;

    private RemoteSource(final ServerUnderTest server) {
      this.server = server;
      this.client = HttpClient.newBuilder().connectTimeout(SCRAPE_TIMEOUT).build();
      this.authorization =
          "Basic " + Base64.getEncoder().encodeToString(("root:" + ServerUnderTest.PASSWORD).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String scrape() {
      try {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(server.httpUrl() + "/prometheus")).timeout(SCRAPE_TIMEOUT)
            .header("Authorization", authorization).GET().build();
        final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200 ? response.body() : null;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      } catch (final IOException | RuntimeException e) {
        return null;
      }
    }

    @Override
    public int threads() {
      return server.threadCount();
    }

    @Override
    public int files() {
      return server.openFiles();
    }

    @Override
    public void close() {
      client.close();
    }
  }
}
