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
package com.arcadedb.bench.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.model.MemoryStatsConfig;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.api.model.StatsConfig;
import org.testcontainers.DockerClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The ArcadeDB server a workload runs against: one Testcontainers container, pinned to a cpuset and a memory limit, with the
 * databases directory bind-mounted from a host temp dir. Nothing runs inside the container (the native image is built FROM
 * scratch, so it has no shell): peak memory comes from the Docker stats stream, disk size from the host directory.
 */
public final class ServerUnderTest implements AutoCloseable {
  public static final String PASSWORD = ContainersTestTemplate.PASSWORD;

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerUnderTest.class);
  /** Same pin as ContainersTestTemplate: a native image has no rm, so the cleanup never runs the image under test. */
  private static final String CLEANUP_IMAGE =
      "busybox:1.37.0-musl@sha256:222ad6d973c0d198014546a65cd02c5fdedcc172123c5b4c2bf0af636550bd94";

  private static final int  HTTP_PORT         = 2480;
  private static final int  GRPC_PORT         = 50051;
  private static final long GIB               = 1024L * 1024 * 1024;
  private static final long FRESH_SAMPLE_WAIT = 1_500L;
  /** How long the liveness check waits for {@code /api/v1/ready}. */
  private static final Duration READY_TIMEOUT = Duration.ofSeconds(5);

  private final GenericContainer<?> container;
  private final String              image;
  private final boolean             nativeImage;
  private final String              cpuset;
  private final int                 memLimitGb;
  private final String              heap;
  private final Map<String, String> settings;
  private final Path                hostDir;
  private final DockerClient        docker;
  private final ServerWrapper       wrapper;

  private final Object sampleLock = new Object();
  private long         peak;
  private boolean      closed;

  private ResultCallback.Adapter<Statistics> statsCallback;

  private ServerUnderTest(final GenericContainer<?> container, final String image, final String cpuset, final int memLimitGb,
      final String heap, final Map<String, String> settings, final Path hostDir) {
    this.container = container;
    this.image = image;
    this.nativeImage = isNativeImage(image);
    this.cpuset = cpuset;
    this.memLimitGb = memLimitGb;
    this.heap = heap;
    this.settings = settings;
    this.hostDir = hostDir;
    this.docker = DockerClientFactory.instance().client();
    this.wrapper = new ServerWrapper(container);
  }

  /**
   * Whether the tag of an image reference (what follows the last ':' after the last '/', a digest suffix from '@' left out)
   * names a native build.
   */
  static boolean isNativeImage(final String image) {
    final int at = image.indexOf('@');
    final String name = at >= 0 ? image.substring(0, at) : image;
    final int colon = name.lastIndexOf(':');
    return colon > name.lastIndexOf('/') && name.substring(colon + 1).contains("native");
  }

  /** Read timeout of the server under test: the default case timeout, so no statement of a case outlives its connection. */
  static final int SOCKET_TIMEOUT_MS = 30 * 60 * 1000;

  /**
   * The settings of the server under test, the given ones winning. The socket timeout is raised from its 30 s default because
   * the remote client talks HTTP/2 and the server closes an HTTP/2 connection one socket timeout into a statement (issue
   * #12606): an index build longer than that fails with "EOF reached while reading" although the server completes it.
   */
  static Map<String, String> settings(final Map<String, String> settings) {
    final Map<String, String> all = new LinkedHashMap<>();
    all.put("arcadedb.server.rootPassword", PASSWORD);
    all.put("arcadedb.backup.enabled", "false");
    all.put("arcadedb.server.plugins", "PrometheusMetricsPlugin");
    all.put("arcadedb.network.socketTimeout", String.valueOf(SOCKET_TIMEOUT_MS));
    all.putAll(settings);
    return all;
  }

  /**
   * Starts the server and waits for {@code /api/v1/ready}. Settings are environment variables named after the setting, as
   * {@code ContainersTestTemplate.configureServer} does: the heap goes to {@code ARCADEDB_OPTS_MEMORY} for the JVM image and to
   * the command line for the native one.
   */
  public static ServerUnderTest start(final String image, final String cpuset, final int memLimitGb, final String heap,
      final Map<String, String> settings) {
    final Map<String, String> all = settings(settings);

    final String memoryOpts = "-Xms" + heap + " -Xmx" + heap;
    final long memory = memLimitGb * GIB;

    final Path hostDir;
    try {
      hostDir = Files.createTempDirectory("bench-db");
      // the container runs as the unprivileged "arcadedb" user
      Files.setPosixFilePermissions(hostDir, PosixFilePermissions.fromString("rwxrwxrwx"));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    final GenericContainer<?> container = new GenericContainer<>(image);
    all.forEach(container::withEnv);
    if (!isNativeImage(image))
      container.withEnv("ARCADEDB_OPTS_MEMORY", memoryOpts);
    else
      container.withCommand(memoryOpts.split("\\s+"));
    container.withExposedPorts(HTTP_PORT, GRPC_PORT)
        .withStartupTimeout(Duration.ofSeconds(120))
        .withFileSystemBind(hostDir.toString(), "/home/arcadedb/databases")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withCpusetCpus(cpuset).withMemory(memory).withMemorySwap(memory))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(HTTP_PORT).forStatusCode(204));

    ServerUnderTest server = null;
    try {
      container.start();
      server = new ServerUnderTest(container, image, cpuset, memLimitGb, heap, all, hostDir);
      server.startSampler();
      return server;
    } catch (final Throwable e) {
      final List<Throwable> failures = release(server != null ? server.statsCallback : null, container, hostDir);
      failures.forEach(e::addSuppressed);
      throw e;
    }
  }

  public ServerWrapper wrapper() {
    return wrapper;
  }

  public String httpUrl() {
    return "http://" + wrapper.host() + ":" + wrapper.httpPort();
  }

  /** Copies a host file into the container, readable by the server user. */
  public void copyToContainer(final Path host, final String containerPath) {
    container.copyFileToContainer(MountableFile.forHostPath(host, 0644), containerPath);
  }

  public String containerId() {
    return container.getContainerId();
  }

  public boolean isNative() {
    return nativeImage;
  }

  /**
   * The image reference, its digest, its variant and the git revision label ({@code "unknown"} when the image has none). The
   * image inspected is the one the container runs, by id: the tag may have been pulled again since the server started.
   */
  public JSONObject imageInfo() {
    final InspectContainerResponse running = container.getContainerInfo();
    final String imageId = running != null && running.getImageId() != null ? running.getImageId() : image;
    final InspectImageResponse inspect = docker.inspectImageCmd(imageId).exec();
    final List<String> repoDigests = inspect.getRepoDigests();
    String digest = inspect.getId();
    if (repoDigests != null && !repoDigests.isEmpty()) {
      final String first = repoDigests.getFirst();
      digest = first.substring(first.indexOf('@') + 1);
    }
    final Map<String, String> labels = inspect.getConfig() != null ? inspect.getConfig().getLabels() : null;
    final String revision = labels != null ? labels.getOrDefault("org.opencontainers.image.revision", "unknown") : "unknown";

    return new JSONObject().put("image", image)
        .put("imageDigest", digest)
        .put("revision", revision)
        .put("variant", nativeImage ? "native" : "jvm");
  }

  /**
   * Null while {@code /api/v1/ready} answers within {@link #READY_TIMEOUT}, else why the server does not: {@code "OOMKilled"} or
   * {@code "exit code <n>"} when Docker says the container stopped, {@code "not answering /api/v1/ready"} when it still runs
   * (or cannot be inspected) but does not answer.
   */
  public String deathReason() {
    if (answersReady())
      return null;
    try {
      final InspectContainerResponse.ContainerState state = docker.inspectContainerCmd(container.getContainerId()).exec().getState();
      if (state != null) {
        if (Boolean.TRUE.equals(state.getOOMKilled()))
          return "OOMKilled";
        if (Boolean.FALSE.equals(state.getRunning()))
          return "exit code " + state.getExitCodeLong();
      }
    } catch (final RuntimeException e) {
      LOGGER.warn("Cannot inspect the container of the server under test: {}", e.toString());
    }
    return "not answering /api/v1/ready";
  }

  private boolean answersReady() {
    try (final HttpClient client = HttpClient.newBuilder().connectTimeout(READY_TIMEOUT).build()) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create(httpUrl() + "/api/v1/ready")).timeout(READY_TIMEOUT).GET()
          .build();
      final int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
      return status >= 200 && status < 300;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (final IOException | RuntimeException e) {
      return false;
    }
  }

  /** The rows of the class histogram kept in {@link #diagnostics()}: its header lines and the biggest classes. */
  static final int HISTOGRAM_ROWS = 30;

  /**
   * Writes a thread dump of the server ({@code jcmd 1 Thread.print}, run inside the container) to {@code file}. The native image
   * has no jcmd and no attach mechanism, and a SIGQUIT could end it: it gets no dump and no signal, and this returns false.
   * False also when jcmd fails.
   */
  public boolean threadDump(final Path file) {
    if (nativeImage)
      return false;
    final String text = jcmd("Thread.print");
    if (text == null)
      return false;
    try {
      Files.writeString(file, text, StandardCharsets.UTF_8);
      return true;
    } catch (final IOException e) {
      LOGGER.warn("Cannot write the thread dump {}: {}", file, e.toString());
      return false;
    }
  }

  /**
   * What the run file keeps about the server at the end of a workload: {@code threads}, its thread count read from
   * {@code /proc/<pid>/status} on the host (absent where the host has no such file, as under Docker Desktop), and, for the JVM
   * image only, {@code classHistogram}, the first {@value #HISTOGRAM_ROWS} lines of {@code jcmd 1 GC.class_histogram}. Nothing
   * here sends the native image a signal.
   */
  public JSONObject diagnostics() {
    final JSONObject json = new JSONObject();
    final int threads = threadCount();
    if (threads >= 0)
      json.put("threads", threads);
    if (!nativeImage) {
      final String histogram = jcmd("GC.class_histogram");
      if (histogram != null)
        json.put("classHistogram", firstLines(histogram, HISTOGRAM_ROWS));
    }
    return json;
  }

  /** The first {@code n} non-empty lines of a text. */
  static JSONArray firstLines(final String text, final int n) {
    final JSONArray rows = new JSONArray();
    for (final String line : text.split("\\R")) {
      if (rows.length() >= n)
        break;
      if (!line.isBlank())
        rows.put(line);
    }
    return rows;
  }

  /** The {@code Threads:} value of a {@code /proc/<pid>/status} text; -1 when it has none. */
  static int threadsOf(final String status) {
    for (final String line : status.split("\\R"))
      if (line.startsWith("Threads:"))
        try {
          return Integer.parseInt(line.substring("Threads:".length()).trim());
        } catch (final NumberFormatException e) {
          return -1;
        }
    return -1;
  }

  /** The thread count of the server process as the host sees it; -1 when it cannot be read. */
  private int threadCount() {
    try {
      final InspectContainerResponse.ContainerState state = docker.inspectContainerCmd(container.getContainerId()).exec().getState();
      final Long pid = state != null ? state.getPidLong() : null;
      if (pid == null || pid <= 0)
        return -1;
      final Path status = Path.of("/proc", String.valueOf(pid), "status");
      if (!Files.isReadable(status))
        return -1;
      return threadsOf(Files.readString(status, StandardCharsets.UTF_8));
    } catch (final IOException | RuntimeException e) {
      return -1;
    }
  }

  /** The output of {@code jcmd 1 <command>} inside the container (the JVM is PID 1, server.sh execs it); null on failure. */
  private String jcmd(final String command) {
    try {
      final Container.ExecResult result = container.execInContainer("jcmd", "1", command);
      if (result.getExitCode() != 0) {
        LOGGER.warn("jcmd {} exited with {}: {}", command, result.getExitCode(), result.getStderr());
        return null;
      }
      return result.getStdout();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (final IOException | RuntimeException e) {
      LOGGER.warn("jcmd {} failed: {}", command, e.toString());
      return null;
    }
  }

  public JSONObject config() {
    return new JSONObject().put("heap", heap)
        .put("cpuset", cpuset)
        .put("memLimitGb", memLimitGb)
        .put("settings", redact(settings));
  }

  /** The settings as recorded in a results file: the value of every key containing "password" (any case) becomes "***". */
  static JSONObject redact(final Map<String, String> settings) {
    final JSONObject json = new JSONObject();
    for (final Map.Entry<String, String> e : settings.entrySet())
      json.put(e.getKey(), e.getKey().toLowerCase(Locale.ROOT).contains("password") ? "***" : e.getValue());
    return json;
  }

  /** Forgets the peak seen so far; a workload calls it at the start of each case. */
  public void resetPeak() {
    synchronized (sampleLock) {
      peak = 0;
    }
  }

  /**
   * The highest working-set size of the container since {@link #resetPeak()}. A case shorter than the sampling period would
   * otherwise report nothing, so this waits up to 1.5 s for one sample if none has arrived yet.
   */
  public long peakRssBytes() {
    synchronized (sampleLock) {
      final long deadline = System.nanoTime() + FRESH_SAMPLE_WAIT * 1_000_000L;
      while (peak == 0 && !closed) {
        final long remaining = (deadline - System.nanoTime()) / 1_000_000L;
        if (remaining <= 0)
          break;
        try {
          sampleLock.wait(remaining);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      return peak;
    }
  }

  /** The size of the files under the database directory, computed on the host; {@code -1} when it cannot be read. */
  public long diskBytes(final String database) {
    final Path dir = hostDir.resolve(database);
    if (!Files.isDirectory(dir))
      return -1L;
    try (final Stream<Path> files = Files.walk(dir)) {
      return files.filter(Files::isRegularFile).mapToLong(p -> {
        try {
          return Files.size(p);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }).sum();
    } catch (final IOException | UncheckedIOException e) {
      return -1L;
    }
  }

  private void startSampler() {
    // docker-java runs the stream on its own daemon threads; each frame is one sample per second
    statsCallback = docker.statsCmd(container.getContainerId()).exec(new ResultCallback.Adapter<Statistics>() {
      @Override
      public void onNext(final Statistics stats) {
        record(stats);
      }
    });
  }

  private void record(final Statistics stats) {
    final MemoryStatsConfig memory = stats.getMemoryStats();
    if (memory == null || memory.getUsage() == null)
      return;
    long inactive = 0;
    final StatsConfig detail = memory.getStats();
    if (detail != null) {
      // cgroup v2 reports "inactive_file", v1 "total_inactive_file"; docker stats subtracts it from the usage
      final Long value = detail.getInactiveFile() != null ? detail.getInactiveFile() : detail.getTotalInactiveFile();
      if (value != null)
        inactive = value;
    }
    final long sample = Math.max(0, memory.getUsage() - inactive);
    synchronized (sampleLock) {
      if (sample > peak)
        peak = sample;
      sampleLock.notifyAll();
    }
  }

  @Override
  public void close() {
    synchronized (sampleLock) {
      if (closed)
        return;
      closed = true;
      sampleLock.notifyAll();
    }
    final List<Throwable> failures = release(statsCallback, container, hostDir);
    if (!failures.isEmpty())
      LOGGER.warn("Server under test closed with {} cleanup failure(s), first: {}", failures.size(), failures.getFirst().toString());
  }

  /** Hands the databases directory of the host to the test, which checks that {@link #close()} removed it. */
  Path hostDatabasesDir() {
    return hostDir;
  }

  /** Every step runs in its own try, so one failure cannot skip the others or mask the original exception. */
  private static List<Throwable> release(final ResultCallback.Adapter<Statistics> statsCallback,
      final GenericContainer<?> container, final Path dir) {
    final List<Throwable> failures = new ArrayList<>();
    if (statsCallback != null) {
      try {
        statsCallback.close();
      } catch (final Throwable e) {
        failures.add(e);
      }
    }
    try {
      container.stop();
    } catch (final Throwable e) {
      failures.add(e);
    }
    try {
      deleteDirectory(dir);
    } catch (final Throwable e) {
      failures.add(e);
    }
    return failures;
  }

  /**
   * Removes the bind-mounted directory. On Linux the container user owns what it created there (mode 755), so the host user
   * cannot unlink it: a short-lived root container does that first (as {@code ContainersTestTemplate} does), not the image
   * under test, which may have no {@code rm}.
   */
  private static void deleteDirectory(final Path dir) {
    try (final GenericContainer<?> cleaner = new GenericContainer<>(CLEANUP_IMAGE)
        .withFileSystemBind(dir.toString(), "/cleanup", BindMode.READ_WRITE)
        .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
        .withCommand("sh", "-c", "rm -rf /cleanup/* /cleanup/.[!.]*")
        .withStartupCheckStrategy(new OneShotStartupCheckStrategy())) {
      cleaner.start();
    } catch (final Exception e) {
      LOGGER.warn("Privileged cleanup container failed for {}: {}", dir, e.getMessage());
    }
    try {
      Files.walkFileTree(dir, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
          Files.deleteIfExists(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path d, final IOException exc) throws IOException {
          Files.deleteIfExists(d);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      LOGGER.warn("Could not delete the databases directory {}, remove it by hand: {}", dir, e.getMessage());
    }
  }
}
