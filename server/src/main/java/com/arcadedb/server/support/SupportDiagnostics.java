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
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ServerPlugin;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Builds {@code diagnostics.json} (schema v1 of SUPPORT-API.md, section 4) and the optional thread dump. Everything is
 * redacted before it is returned. Never included: database contents, user names or hashes, keys, tokens, the values of masked
 * settings, addresses of HA peers.
 */
public class SupportDiagnostics {
  /** The plugin names the portal knows: only these are ever reported. */
  static final List<String> PLUGIN_NAMES = List.of("gremlin", "postgresql", "mongodb", "redis", "grpc", "bolt", "graphql", "mcp");

  static final int MAX_THREAD_DUMP_BYTES = 5 * 1024 * 1024;

  private final ArcadeDBServer server;

  public SupportDiagnostics(final ArcadeDBServer server) {
    this.server = server;
  }

  public JSONObject build(final SupportRedactor.Session redaction) {
    final JSONObject json = new JSONObject();
    json.put("schema", 1);
    json.put("generatedAt", Instant.now().toString());
    if (server.getInstanceId() != null)
      json.put("instanceId", server.getInstanceId());

    json.put("server", buildServer());
    json.put("os", buildOs());
    json.put("jvm", buildJvm(redaction));
    json.put("runtime", SupportRuntime.detect());
    json.put("configuration", buildConfiguration(redaction));
    json.put("plugins", new JSONArray(detectPlugins()));
    json.put("databases", buildDatabases());
    json.put("ha", buildHA());
    json.put("metrics", buildMetrics());
    return json;
  }

  private JSONObject buildServer() {
    final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    return new JSONObject().put("version", Constants.getVersion()).put("build", Constants.getBuildNumber())
        .put("name", server.getServerName())
        .put("startedAt", Instant.ofEpochMilli(runtime.getStartTime()).toString())
        .put("uptimeSeconds", runtime.getUptime() / 1000);
  }

  private JSONObject buildOs() {
    final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    final JSONObject json = new JSONObject().put("name", os.getName()).put("version", os.getVersion()).put("arch", os.getArch())
        .put("cpuCores", os.getAvailableProcessors());
    // The com.sun.management types are fully qualified: they share their simple names with the java.lang.management ones imported above
    if (os instanceof com.sun.management.OperatingSystemMXBean sun)
      json.put("totalMemoryBytes", sun.getTotalMemorySize());
    return json;
  }

  private JSONObject buildJvm(final SupportRedactor.Session redaction) {
    final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    final Runtime rt = Runtime.getRuntime();
    final JSONArray gc = new JSONArray();
    for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans())
      gc.put(bean.getName());
    return new JSONObject().put("vendor", runtime.getVmVendor()).put("version", System.getProperty("java.version"))
        .put("vmName", runtime.getVmName()).put("maxHeapBytes", rt.maxMemory()).put("usedHeapBytes", rt.totalMemory() - rt.freeMemory())
        .put("inputArguments", new JSONArray(redaction.redactArguments(runtime.getInputArguments()))).put("gc", gc);
  }

  private JSONObject buildConfiguration(final SupportRedactor.Session redaction) {
    return buildConfiguration(server.getConfiguration(), server::getOperatorSettingSource, System::getenv, redaction);
  }

  /**
   * The settings that differ from their default, in two lists: {@code nonDefault} is what the OPERATOR supplied (a system
   * property, an environment variable, the server configuration file, a SET SERVER SETTING: {@code source} says which),
   * {@code computed} is what the server worked out itself (a default fitted to the heap or the cores, a key it puts in
   * its own configuration at startup). Without the split a vanilla server looks customised in the portal. The origin is
   * not guessed from the value: the server records the keys the operator supplied (see
   * {@link ArcadeDBServer#getOperatorSettingSource(String)}).
   */
  static JSONObject buildConfiguration(final ContextConfiguration configuration, final Function<String, String> operatorSource,
      final Function<String, String> environment, final SupportRedactor.Session redaction) {
    final JSONArray nonDefault = new JSONArray();
    final JSONArray computed = new JSONArray();
    final JSONArray masked = new JSONArray();

    for (final GlobalConfiguration cfg : GlobalConfiguration.values()) {
      final Object effective = configuration.getValue(cfg);
      final Object defValue = cfg.getDefValue();
      final String effectiveText = effective == null ? "" : String.valueOf(effective);
      final String defaultText = defValue == null ? "" : String.valueOf(defValue);
      if (effectiveText.equals(defaultText))
        continue;

      final boolean jvm = System.getProperty(cfg.getKey()) != null;
      final boolean env = environment.apply(cfg.getKey()) != null || environment.apply(cfg.getKey().replace('.', '_').toUpperCase(Locale.ROOT)) != null;
      final String operator = operatorSource.apply(cfg.getKey());
      // A default written as a template ("${arcadedb.server.rootPath}/databases") always differs from its resolved value
      if (defaultText.contains("${") && !jvm && !env && operator == null)
        continue;

      if (cfg.isHidden()) {
        // The name says it is set; the value is never read into the bundle
        masked.put(cfg.getKey());
        continue;
      }

      final Object publishable = cfg.publishableValue(effective);
      final JSONObject entry = new JSONObject().put("key", cfg.getKey()).put("value", redaction.redact(String.valueOf(publishable)));
      if (jvm || env || operator != null)
        nonDefault.put(entry.put("source", jvm ? "jvm" : env ? "env" : operator));
      else
        computed.put(entry);
    }
    return new JSONObject().put("nonDefault", nonDefault).put("computed", computed).put("masked", masked);
  }

  /** The plugins that are running, by the names the portal knows. */
  List<String> detectPlugins() {
    final Set<String> found = new TreeSet<>();
    for (final ServerPlugin plugin : server.getPlugins()) {
      final String text = (String.valueOf(plugin.getName()) + " " + plugin.getClass().getSimpleName()).toLowerCase(Locale.ROOT);
      addIfContains(found, text, "gremlin", "gremlin");
      addIfContains(found, text, "postgres", "postgresql");
      addIfContains(found, text, "mongo", "mongodb");
      addIfContains(found, text, "redis", "redis");
      addIfContains(found, text, "grpc", "grpc");
      addIfContains(found, text, "bolt", "bolt");
      addIfContains(found, text, "graphql", "graphql");
      addIfContains(found, text, "mcp", "mcp");
    }
    try {
      if (QueryEngineManager.getInstance().getAvailableLanguages().contains("graphql"))
        found.add("graphql");
    } catch (final RuntimeException ignored) {
      // the query engine list is informative only
    }
    final List<String> ordered = new ArrayList<>();
    for (final String name : PLUGIN_NAMES)
      if (found.contains(name))
        ordered.add(name);
    return ordered;
  }

  private static void addIfContains(final Set<String> found, final String text, final String needle, final String name) {
    if (text.contains(needle))
      found.add(name);
  }

  private JSONArray buildDatabases() {
    final JSONArray databases = new JSONArray();
    final String directory = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    for (final String name : new TreeSet<>(server.getDatabaseNames())) {
      if (ArcadeDBServer.isReservedDatabaseName(name))
        continue;
      final JSONObject db = new JSONObject().put("name", name);
      try {
        final ServerDatabase database = server.getDatabase(name, false, false);
        final Database embedded = database.getEmbedded();
        if (embedded != null) {
          db.put("mode", embedded.getMode() == ComponentFile.MODE.READ_ONLY ? "read_only" : "read_write");
          db.put("sizeBytes", directorySize(Paths.get(embedded.getDatabasePath())));
        } else
          db.put("sizeBytes", directorySize(Paths.get(directory, name)));
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.FINE, "Support: cannot read the size of database '%s': %s", name, e.getMessage());
        db.put("sizeBytes", directorySize(Paths.get(directory, name)));
      }
      databases.put(db);
    }
    return databases;
  }

  static long directorySize(final Path directory) {
    if (!Files.isDirectory(directory))
      return 0L;
    final long[] total = { 0L };
    try {
      Files.walkFileTree(directory, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
          total[0] += attrs.size();
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      // what was summed so far
    }
    return total[0];
  }

  private JSONObject buildHA() {
    final HAServerPlugin ha = server.getHA();
    final JSONObject json = new JSONObject().put("enabled", ha != null);
    if (ha != null) {
      json.put("clusterName", ha.getClusterName());
      json.put("nodes", ha.getConfiguredServers());
      json.put("role", ha.isLeader() ? "leader" : "replica");
    }
    return json;
  }

  private JSONObject buildMetrics() {
    final JSONObject json = new JSONObject();
    final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
      final double load = sun.getProcessCpuLoad();
      if (load >= 0)
        json.put("cpuLoadPercent", Math.round(load * 1000) / 10.0);
    }
    if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix)
      json.put("openFiles", unix.getOpenFileDescriptorCount());
    json.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());

    try {
      final String directory = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      final Path path = Paths.get(directory);
      if (Files.exists(path)) {
        final FileStore store = Files.getFileStore(path);
        json.put("diskFreeBytes", store.getUsableSpace());
        json.put("diskTotalBytes", store.getTotalSpace());
      }
    } catch (final IOException | RuntimeException e) {
      // no disk figures
    }
    return json;
  }

  /**
   * A thread dump as text, redacted, at most 5 MB: when it is longer the dump ends with a line saying so.
   */
  public String threadDump(final SupportRedactor.Session redaction) {
    final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] infos = bean.dumpAllThreads(true, true);
    final StringBuilder out = new StringBuilder(64 * 1024);
    out.append("Thread dump of ").append(server.getServerName()).append(" at ").append(Instant.now()).append(", ").append(infos.length)
        .append(" threads\n\n");

    for (final ThreadInfo info : infos) {
      final StringBuilder t = new StringBuilder();
      t.append('"').append(info.getThreadName()).append("\" #").append(info.getThreadId());
      if (info.isDaemon())
        t.append(" daemon");
      t.append(" ").append(info.getThreadState());
      if (info.getLockName() != null)
        t.append(" on ").append(info.getLockName());
      if (info.getLockOwnerName() != null)
        t.append(" owned by \"").append(info.getLockOwnerName()).append("\" #").append(info.getLockOwnerId());
      t.append('\n');
      for (final StackTraceElement frame : info.getStackTrace())
        t.append("    at ").append(frame).append('\n');
      for (final MonitorInfo monitor : info.getLockedMonitors())
        t.append("    - locked ").append(monitor).append('\n');
      for (final LockInfo lock : info.getLockedSynchronizers())
        t.append("    - locked synchronizer ").append(lock).append('\n');
      t.append('\n');

      if (out.length() + t.length() > MAX_THREAD_DUMP_BYTES) {
        out.append("... the thread dump was cut here: it exceeds ").append(MAX_THREAD_DUMP_BYTES / 1024 / 1024).append(" MB\n");
        break;
      }
      out.append(t);
    }
    return redaction.redact(out.toString());
  }
}
