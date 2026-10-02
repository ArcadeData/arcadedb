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

import java.net.http.HttpClient;
import com.arcadedb.server.http.handler.LeaderDial;
import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * The support feature of a server: registration with the ArcadeData customer portal, previews of the redacted bundle, and
 * the calls to the portal. The Client key stays inside this package: nothing this class returns carries it.
 */
public class SupportService implements AutoCloseable {
  public static final String BUY_URL = "https://arcadedb.com/pricing.html";

  private static final long WHOAMI_CACHE_MS   = 60_000L;
  private static final long FAILURE_CACHE_MS  = 5_000L;
  private static final Set<String> KINDS = Set.of("bug", "question", "performance", "other");
  private static final Set<String> SEVERITIES = Set.of("S1", "S2", "S3", "S4");
  private static final int MAX_GITHUB_SUMMARY = 3000;

  private final ArcadeDBServer         server;
  private final SupportConfiguration   configuration;
  private final SupportBundleManager   bundles;
  private final SupportScreenshots     screenshots = new SupportScreenshots();
  private final SupportConnector       connector   = new SupportConnector(this);
  private final ZoneId                 zone;
  private volatile long                maxZipBytes;
  private volatile Supplier<List<Path>> logFiles;
  private final long                   retryDelayMs;
  private volatile long                lastRegisteredAt;
  private SupportAutoRegistration      autoRegistration;

  /** {@code fingerprint}: the portal, the client id and a hash of the key, so a changed key or portal never reads a stale answer. */
  private record CachedWhoami(String fingerprint, String json, long at) {
  }

  private record CachedFailure(String fingerprint, SupportException exception, long at) {
  }

  private volatile CachedWhoami  whoami;
  private volatile CachedFailure failure;
  private final     Semaphore   previewSlot = new Semaphore(1);
  private           SupportPeerQuery peerQuery;
  private           HttpClient       peerClient;

  public SupportService(final ArcadeDBServer server, final Path configDirectory) {
    this(server, new SupportConfiguration(configDirectory, server.getConfiguration()), new SupportBundleManager(),
        ZoneId.systemDefault(), SupportLogCollector.DEFAULT_MAX_ZIP_BYTES, 500L);
  }

  SupportService(final ArcadeDBServer server, final SupportConfiguration configuration, final SupportBundleManager bundles,
      final ZoneId zone, final long maxZipBytes, final long retryDelayMs) {
    this.server = server;
    this.configuration = configuration;
    this.bundles = bundles;
    this.zone = zone;
    this.maxZipBytes = maxZipBytes;
    this.retryDelayMs = retryDelayMs;
    this.logFiles = () -> SupportLogCollector.locate(server.getConfiguration());
  }

  /** For tests: where the log files are. */
  void setLogFiles(final Supplier<List<Path>> logFiles) {
    this.logFiles = logFiles;
  }

  /** For tests: the cap of the zipped logs. */
  void setMaxZipBytes(final long maxZipBytes) {
    this.maxZipBytes = maxZipBytes;
  }

  public SupportConfiguration getConfiguration() {
    return configuration;
  }

  public SupportBundleManager getBundles() {
    return bundles;
  }

  public SupportScreenshots getScreenshots() {
    return screenshots;
  }

  public SupportConnector getConnector() {
    return connector;
  }

  /** The fan-out of a support request over the other nodes of the cluster (see {@link SupportPeerQuery}). */
  public synchronized SupportPeerQuery getPeerQuery() {
    if (peerQuery == null) {
      peerClient = LeaderDial.newConnectTimeoutBoundedClient(server.getConfiguration());
      peerQuery = new SupportPeerQuery(SupportPeerQuery.clusterOf(server, () -> peerClient));
    }
    return peerQuery;
  }

  /** For tests: stands the cluster in with something else. */
  synchronized void setPeerQuery(final SupportPeerQuery peerQuery) {
    this.peerQuery = peerQuery;
  }

  /**
   * Starts the daemon that registers this server as an installation without Studio (see {@link SupportAutoRegistration}), if it
   * is registered with a key and {@code arcadedb.support.autoRegister} is not turned off. Never throws: a server must start
   * whatever happens here.
   */
  public synchronized void startAutoRegistration() {
    if (autoRegistration == null)
      startAutoRegistration(SupportAutoRegistration.Timing.defaults());
  }

  /** For tests: starts the loop with these timings, replacing one that is running. */
  synchronized void startAutoRegistration(final SupportAutoRegistration.Timing timing) {
    try {
      if (autoRegistration != null)
        autoRegistration.close();
      autoRegistration = new SupportAutoRegistration(timing,
          () -> server.getConfiguration().getValueAsBoolean(GlobalConfiguration.SUPPORT_AUTO_REGISTER),
          () -> configuration.get() != null, this::registerInstallation, () -> lastRegisteredAt,
          message -> LogManager.instance().log(this, Level.WARNING, "Support: %s", message));
      autoRegistration.start();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE, "Automatic registration in the support portal was not started: %s",
          e.getClass().getSimpleName());
    }
  }

  @Override
  public void close() {
    final SupportAutoRegistration auto;
    synchronized (this) {
      auto = autoRegistration;
      autoRegistration = null;
    }
    if (auto != null)
      auto.close();
    connector.close();
    bundles.close();
    final HttpClient client;
    synchronized (this) {
      client = peerClient;
      peerClient = null;
      peerQuery = null;
    }
    LeaderDial.releaseBounded(client);
  }

  // ---------------------------------------------------------------------------------------------- registration

  /** The state shown by Studio. The key is never part of it, only its last four characters. */
  public JSONObject status(final boolean refresh) {
    final JSONObject json = new JSONObject();
    final SupportConfiguration.Registration registration = configuration.get();
    json.put("registered", registration != null);
    json.put("portalUrl", configuration.getPortalUrl(registration));
    json.put("canWriteConfig", configuration.canWriteConfig());
    json.put("instanceId", server.getInstanceId());
    json.put("buyUrl", BUY_URL);
    json.put("logTimeZone", describeZone());
    if (registration == null)
      return json;

    json.put("clientId", registration.getClientId());
    json.put("keyHint", registration.getKeyHint());
    json.put("fromSettings", registration.isFromSettings());
    if (!registration.getRegisteredAt().isEmpty())
      json.put("registeredAt", registration.getRegisteredAt());

    try {
      describeWhoami(json, parseWhoami(whoamiCached(registration, refresh)));
    } catch (final SupportException e) {
      // The portal is unreachable or refuses the key: Studio still shows the registration and why the portal did not answer
      json.put("portalError", new JSONObject().put("error", e.getCode()).put("message", e.getMessage()));
    } catch (final IllegalArgumentException e) {
      // A hand-edited support.json (or a setting) with a portal URL that is not acceptable: validated when it is used, not only
      // when it is saved
      json.put("portalError", new JSONObject().put("error", "portal_url_invalid").put("message",
          "The portal address in the support configuration is not valid: " + e.getMessage()));
    }
    return json;
  }

  private static void describeWhoami(final JSONObject json, final JSONObject who) {
    json.put("workspaceName", who.has("workspace") ? who.getJSONObject("workspace").getString("name", "") : "");
    json.put("workspaceId", who.has("workspace") ? who.getJSONObject("workspace").getString("id", "") : "");
    if (who.has("key")) {
      final JSONObject key = who.getJSONObject("key");
      json.put("keyLabel", key.getString("label", ""));
      json.put("scopes", key.has("scopes") ? key.getJSONArray("scopes") : new JSONArray());
    }
    json.put("plan", who.has("plan") && !who.isNull("plan") ? who.getJSONObject("plan") : JSONObject.NULL);
    json.put("sla", who.has("sla") && !who.isNull("sla") ? who.getJSONObject("sla") : JSONObject.NULL);
    json.put("buyUrl", who.getString("buyUrl", BUY_URL));
  }

  /**
   * Checks a Client ID and key with the portal without storing anything (the "Verify" button of Studio).
   *
   * @return {@code {verified: true, workspaceName, plan, sla, keyLabel, scopes, ...}}
   */
  public JSONObject verify(final String clientId, final String key) {
    final SupportConfiguration.Registration candidate = candidate(clientId, key);
    final JSONObject json = new JSONObject().put("verified", true).put("clientId", clientId.trim()).put("keyHint",
        SupportConfiguration.keyHint(key.trim()));
    describeWhoami(json, parseWhoami(client(candidate).whoami()));
    return json;
  }

  /**
   * The body of {@code whoami} as a JSON object that {@link #describeWhoami} can read. A captive proxy, a maintenance page that
   * answers 200 or a change of the portal's contract is reported as {@code portal_error}, which Studio renders, instead of
   * escaping as a server error.
   */
  private static JSONObject parseWhoami(final String body) {
    try {
      final JSONObject who = new JSONObject(body);
      describeWhoami(new JSONObject(), who);
      return who;
    } catch (final RuntimeException e) {
      throw new SupportException("portal_error",
          "The portal answered with something this server does not understand: check the portal address and try again later");
    }
  }

  private SupportConfiguration.Registration candidate(final String clientId, final String key) {
    try {
      SupportConfiguration.validateClientId(clientId);
      SupportConfiguration.validateKey(key);
      final String url = configuration.getPortalUrl();
      SupportConfiguration.validatePortalUrl(url);
      return new SupportConfiguration.Registration(url, clientId.trim(), key.trim(), "", false);
    } catch (final IllegalArgumentException e) {
      throw new SupportException("bad_request", e.getMessage());
    }
  }

  private String whoamiCached(final SupportConfiguration.Registration registration, final boolean refresh) {
    final CachedWhoami cached = whoami;
    final long now = System.currentTimeMillis();
    final String fingerprint = fingerprint(registration.getPortalUrl(), registration.getClientId(), registration.getKey());
    if (!refresh && cached != null && cached.fingerprint.equals(fingerprint) && now - cached.at < WHOAMI_CACHE_MS)
      return cached.json;
    // A portal that is down is remembered for a few seconds, so that every load of the tab does not cost two 30 s attempts on a
    // worker thread; the Refresh button always asks again
    final CachedFailure failed = failure;
    if (!refresh && failed != null && failed.fingerprint.equals(fingerprint) && now - failed.at < FAILURE_CACHE_MS)
      throw failed.exception;
    try {
      final String json = client(registration).whoami();
      parseWhoami(json);
      whoami = new CachedWhoami(fingerprint, json, now);
      failure = null;
      return json;
    } catch (final SupportException e) {
      failure = new CachedFailure(fingerprint, e, now);
      throw e;
    }
  }

  private static String fingerprint(final String portalUrl, final String clientId, final String key) {
    try {
      final byte[] hash = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
      return portalUrl + "|" + clientId + "|" + HexFormat.of().formatHex(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  /**
   * Verifies the Client ID and key with the portal ({@code whoami}) and stores them.
   *
   * @throws SupportException the registration cannot be saved ({@code registered_by_settings}, {@code config_not_writable}) or
   *                          the portal refuses it
   */
  public JSONObject register(final String clientId, final String key) {
    final SupportConfiguration.Registration existing = configuration.get();
    if (existing != null && existing.isFromSettings())
      throw new SupportException("registered_by_settings", "This server is registered through the settings "
          + "arcadedb.support.clientId and arcadedb.support.clientKey: change them in the server configuration");

    final SupportConfiguration.Registration candidate = candidate(clientId, key);
    final String url = candidate.getPortalUrl();
    if (!configuration.canWriteConfig())
      throw new SupportException("config_not_writable", "The configuration directory of the server is not writable: set the "
          + "settings arcadedb.support.clientId and arcadedb.support.clientKey instead");

    final String who = client(candidate).whoami();
    // Not saved unless the portal's answer is one this server can read
    parseWhoami(who);

    try {
      configuration.save(url, clientId, key);
    } catch (final IOException e) {
      // The message of an IOException names a path, never the content
      throw new SupportException("config_not_writable", "Cannot write the registration file: " + e.getClass().getSimpleName());
    }
    whoami = new CachedWhoami(fingerprint(url, clientId.trim(), key.trim()), who, System.currentTimeMillis());
    return status(false);
  }

  public void unregister() {
    try {
      configuration.clear();
    } catch (final IllegalStateException e) {
      throw new SupportException("registered_by_settings", e.getMessage());
    } catch (final IOException e) {
      throw new SupportException("config_not_writable", "Cannot remove the registration file: " + e.getClass().getSimpleName());
    }
    whoami = null;
    failure = null;
  }

  private SupportPortalClient client(final SupportConfiguration.Registration registration) {
    return new SupportPortalClient(registration, server.getInstanceId(), retryDelayMs);
  }

  private SupportPortalClient requireClient() {
    final SupportConfiguration.Registration registration = configuration.get();
    if (registration == null)
      throw new SupportException("not_registered", "This server is not registered with the support portal: enter the Client ID and "
          + "key of your workspace in the Support tab");
    return client(registration);
  }

  // ---------------------------------------------------------------------------------------------- installation

  /**
   * Registers this server as an installation of the key's workspace in the portal, or completes the one it already has: the
   * portal answers {@code {status: created|updated|unchanged, installationId, name, filled, differs}}. What is sent is the same
   * redacted diagnostics document an issue carries (never logs, never a thread dump).
   */
  public String registerInstallation() {
    final SupportPortalClient client = requireClient();
    final JSONObject diagnostics = new SupportDiagnostics(server).build(new SupportRedactor.Session());
    final String answer = client.registerInstallation(new JSONObject().put("diagnostics", diagnostics).toString());
    // Shared with the automatic registration, so a registration by Studio or the connect flow is not repeated at once
    lastRegisteredAt = System.currentTimeMillis();
    return answer;
  }

  // ---------------------------------------------------------------------------------------------- preview

  /**
   * Builds the bundle in a temporary directory, redacted, and describes it.
   *
   * @param request {@code includeLogs, window, includeDiagnostics, includeThreads}
   */
  public JSONObject preview(final JSONObject request) {
    // One scan at a time: collecting and zipping logs reads every file of the window, and two of them at once on a large log
    // would double the disk and CPU spent on a server that is probably already in trouble
    if (!previewSlot.tryAcquire())
      throw new SupportException("preview_busy", "Another preview is being built: wait for it to finish, then try again");
    try {
      return buildPreview(request);
    } finally {
      previewSlot.release();
    }
  }

  private JSONObject buildPreview(final JSONObject request) {
    final boolean includeLogs = request.getBoolean("includeLogs", false);
    final boolean includeDiagnostics = request.getBoolean("includeDiagnostics", true);
    final boolean includeThreads = request.getBoolean("includeThreads", false);
    if (!includeLogs && !includeDiagnostics && !includeThreads)
      throw new SupportException("bad_request", "Select at least one of the logs, the diagnostics snapshot or the thread dump");

    final Instant now = Instant.now();
    final SupportLogWindow window = includeLogs ? parseWindow(request, now) : null;

    final SupportBundleManager.Bundle bundle;
    try {
      bundle = bundles.create();
    } catch (final IOException e) {
      throw new SupportException("internal_error", "Cannot create the temporary directory of the preview: " + e.getClass().getSimpleName());
    }

    boolean success = false;
    try {
      final JSONArray files = new JSONArray();
      final List<String> warnings = new ArrayList<>();
      JSONObject diagnostics = null;
      JSONObject summary = null;

      if (includeDiagnostics) {
        final SupportRedactor.Session session = new SupportRedactor.Session();
        diagnostics = new SupportDiagnostics(server).build(session);
        final Path file = bundle.getDirectory().resolve("diagnostics.json");
        final String text = diagnostics.toString(2);
        Files.writeString(file, text, StandardCharsets.UTF_8);
        bundle.setDiagnostics(file);
        files.put(fileEntry("diagnostics.json", Files.size(file), countLines(text), session.getCount()));
      }

      if (includeThreads) {
        final SupportRedactor.Session session = new SupportRedactor.Session();
        final String dump = new SupportDiagnostics(server).threadDump(session);
        final Path file = bundle.getDirectory().resolve("threads.txt");
        Files.writeString(file, dump, StandardCharsets.UTF_8);
        bundle.setThreads(file);
        files.put(fileEntry("threads.txt", Files.size(file), countLines(dump), session.getCount()));
      }

      if (includeLogs) {
        final SupportLogCollector collector = new SupportLogCollector(zone);
        final Path zip = bundle.getDirectory().resolve("logs.zip");
        final SupportLogCollector.Result result = collector.collect(logFiles.get(), window, zip,
            maxZipBytes);
        warnings.addAll(result.getWarnings());
        if (!result.isEmpty()) {
          bundle.setLogs(zip);
          final JSONObject logs = fileEntry("logs.zip", result.getZipBytes(), result.getLines(), result.getRedactions());
          final JSONArray entries = new JSONArray();
          for (final SupportLogCollector.FileStat stat : result.getFiles())
            entries.put(stat.toJSON());
          logs.put("entries", entries);
          files.put(logs);

          summary = result.getSummary();
          final Path summaryFile = bundle.getDirectory().resolve("summary.json");
          Files.writeString(summaryFile, summary.toString(2), StandardCharsets.UTF_8);
          bundle.setSummary(summaryFile);
          files.put(fileEntry("summary.json", Files.size(summaryFile), countLines(summary.toString(2)), 0));
        }
      }

      final JSONObject description = new JSONObject();
      description.put("previewId", bundle.getId());
      description.put("expiresAt", Instant.ofEpochMilli(bundle.getExpiresAt()).toString());
      description.put("files", files);
      description.put("warnings", new JSONArray(warnings));
      description.put("empty", bundle.isEmpty());
      if (window != null)
        description.put("window", new JSONObject().put("from", window.from().toString()).put("to", window.to().toString()));
      description.put("logTimeZone", describeZone());
      final String github = githubSummary(diagnostics, summary);
      bundle.setGithubSummary(github);
      description.put("githubSummary", github);
      bundle.setDescription(description);
      success = true;
      return description;
    } catch (final IOException e) {
      throw new SupportException("bad_request", "Cannot build the preview: " + e.getClass().getSimpleName());
    } finally {
      if (!success)
        bundles.remove(bundle.getId());
    }
  }

  private SupportLogWindow parseWindow(final JSONObject request, final Instant now) {
    try {
      return SupportLogWindow.parse(request.has("window") ? request.getJSONObject("window") : null, now, zone);
    } catch (final IllegalArgumentException e) {
      throw new SupportException("bad_request", e.getMessage());
    }
  }

  /** The zone the log timestamps are written in, and how it is applied to the window. */
  JSONObject describeZone() {
    final ZoneOffset offset = zone.getRules().getOffset(Instant.now());
    return new JSONObject().put("id", zone.getId()).put("name", zone.getDisplayName(TextStyle.FULL, Locale.ENGLISH))
        .put("offset", offset.getId())
        .put("note", "Log lines carry no time zone: they are written in the time zone of the server JVM (" + zone.getId() + ", UTC"
            + (offset.getId().equals("Z") ? "" : offset.getId()) + "). The window is converted to it.");
  }

  private static JSONObject fileEntry(final String name, final long size, final long lines, final int redactions) {
    return new JSONObject().put("name", name).put("sizeBytes", size).put("lines", lines).put("redactions", redactions);
  }

  private static long countLines(final String text) {
    long lines = 1;
    for (int i = 0; i < text.length(); i++)
      if (text.charAt(i) == '\n')
        lines++;
    return lines;
  }

  /**
   * The environment and the log summary as Markdown for a public GitHub issue: no logs, no instance id, no host names.
   * At most {@value #MAX_GITHUB_SUMMARY} characters.
   */
  static String githubSummary(final JSONObject diagnostics, final JSONObject summary) {
    final StringBuilder out = new StringBuilder();
    if (diagnostics != null) {
      out.append("### Environment (from ArcadeDB Studio)\n");
      final JSONObject server = diagnostics.getJSONObject("server");
      out.append("- ArcadeDB: ").append(server.getString("version", "?")).append('\n');
      final JSONObject os = diagnostics.getJSONObject("os");
      out.append("- OS: ").append(os.getString("name", "?")).append(' ').append(os.getString("version", "")).append(' ')
          .append(os.getString("arch", "")).append(", ").append(os.getInt("cpuCores", 0)).append(" cores");
      if (os.has("totalMemoryBytes"))
        out.append(", ").append(os.getLong("totalMemoryBytes", 0) >> 30).append(" GB RAM");
      out.append('\n');
      final JSONObject jvm = diagnostics.getJSONObject("jvm");
      out.append("- Java: ").append(jvm.getString("version", "?")).append(" (").append(jvm.getString("vmName", "")).append("), max heap ")
          .append(jvm.getLong("maxHeapBytes", 0) >> 20).append(" MB\n");
      out.append("- Runtime: ").append(diagnostics.getJSONObject("runtime").getString("container", "none")).append('\n');
      final JSONArray plugins = diagnostics.getJSONArray("plugins");
      if (plugins.length() > 0) {
        out.append("- Plugins: ");
        for (int i = 0; i < plugins.length(); i++)
          out.append(i > 0 ? ", " : "").append(plugins.getString(i));
        out.append('\n');
      }
      final JSONObject ha = diagnostics.getJSONObject("ha");
      if (ha.getBoolean("enabled", false))
        out.append("- HA: ").append(ha.getInt("nodes", 0)).append(" nodes, ").append(ha.getString("role", "")).append('\n');
      out.append("- Databases: ").append(diagnostics.getJSONArray("databases").length()).append('\n');
    }
    if (summary != null) {
      out.append("\n### Logs (window ").append(summary.getJSONObject("window").getString("from", "")).append(" to ")
          .append(summary.getJSONObject("window").getString("to", "")).append(")\n");
      final JSONObject levels = summary.getJSONObject("levels");
      out.append("- Lines: ").append(summary.getLong("lines", 0));
      for (final String level : levels.keySet())
        out.append(", ").append(level).append(' ').append(levels.getLong(level, 0));
      out.append('\n');
      final JSONArray top = summary.getJSONArray("topExceptions");
      for (int i = 0; i < top.length() && i < 5; i++) {
        final JSONObject e = top.getJSONObject(i);
        // Class and count only: an exception message may carry record data, SQL text or database names, and this text goes into a
        // public issue. The messages stay in summary.json, which is sent to the portal and never to GitHub.
        out.append("- `").append(e.getString("class", "").replace('`', '\'')).append("` x").append(e.getLong("count", 0)).append('\n');
      }
    }
    String text = out.toString();
    if (text.length() > MAX_GITHUB_SUMMARY)
      text = text.substring(0, MAX_GITHUB_SUMMARY) + "\n...";
    return text;
  }

  // ---------------------------------------------------------------------------------------------- portal calls

  /**
   * Opens an issue with the files of a preview (if any).
   *
   * @return {@code {number, url}} as answered by the portal
   */
  public String createIssue(final JSONObject request) {
    final SupportPortalClient client = requireClient();

    final String title = request.getString("title", "").strip();
    final String body = request.getString("body", "");
    final String severity = request.getString("severity", "S3");
    final String kind = request.getString("kind", null);
    if (title.isEmpty() || title.length() > 200)
      throw new SupportException("bad_request", "The title is required and at most 200 characters");
    if (body.length() > 20000)
      throw new SupportException("bad_request", "The description is at most 20000 characters");
    if (!SEVERITIES.contains(severity))
      throw new SupportException("bad_request", "The severity is S1, S2, S3 or S4");
    if (kind != null && !kind.isEmpty() && !KINDS.contains(kind))
      throw new SupportException("bad_request", "The kind is bug, question, performance or other");

    final JSONObject metadata = new JSONObject().put("title", title).put("body", body).put("severity", severity)
        .put("studioVersion", Constants.getRawVersion()).put("source", "studio");
    if (kind != null && !kind.isEmpty())
      metadata.put("kind", kind);

    final String previewId = request.getString("previewId", null);
    final List<SupportScreenshots.Shot> shots = screenshots.peek(screenshotIds(request));

    // The preview is leased for the whole upload: a slow one may outlive the preview's 15 minutes
    try (final SupportBundleManager.Lease lease = previewId == null || previewId.isEmpty() ? null : bundles.lease(previewId)) {
      final SupportBundleManager.Bundle bundle = lease == null ? null : lease.bundle();
      final String response = client.createIssue(metadata, bundle == null ? null : bundle.getLogs(),
          bundle == null ? null : bundle.getDiagnostics(), bundle == null ? null : bundle.getSummary(),
          bundle == null ? null : bundle.getThreads(), shots);
      if (bundle != null)
        bundles.remove(bundle.getId());
      screenshots.remove(shots);
      return response;
    } catch (final IOException e) {
      throw new SupportException("internal_error", "Cannot read the preview files: " + e.getClass().getSimpleName());
    } catch (final SupportPortalException e) {
      SupportPortalClient.logFailure(this, "create issue", e);
      throw e;
    }
  }

  /** Sends the files of a preview and/or the screenshots the user added to an existing issue. */
  public String addAttachments(final long number, final String previewId, final List<String> screenshotIds) {
    final SupportPortalClient client = requireClient();
    final List<SupportScreenshots.Shot> shots = screenshots.peek(screenshotIds);
    final boolean withPreview = previewId != null && !previewId.isEmpty();
    if (!withPreview && shots.isEmpty())
      throw new SupportException("bad_request", "There is nothing to send: prepare the files or add a screenshot");
    try (final SupportBundleManager.Lease lease = withPreview ? bundles.lease(previewId) : null) {
      final SupportBundleManager.Bundle bundle = lease == null ? null : lease.bundle();
      if (bundle != null && bundle.isEmpty() && shots.isEmpty())
        throw new SupportException("bad_request", "The preview has no files to send");
      final String response = client.addAttachments(number, bundle == null ? null : bundle.getLogs(),
          bundle == null ? null : bundle.getDiagnostics(), bundle == null ? null : bundle.getSummary(),
          bundle == null ? null : bundle.getThreads(), shots);
      if (bundle != null)
        bundles.remove(bundle.getId());
      screenshots.remove(shots);
      return response;
    } catch (final IOException e) {
      throw new SupportException("internal_error", "Cannot read the preview files: " + e.getClass().getSimpleName());
    } catch (final SupportPortalException e) {
      SupportPortalClient.logFailure(this, "add attachments", e);
      throw e;
    }
  }

  /** Sends the files of a preview to an existing issue. */
  public String addAttachments(final long number, final String previewId) {
    return addAttachments(number, previewId, List.of());
  }

  // ------------------------------------------------------------------------------------------------ screenshots

  /**
   * Holds a screenshot the user added until Send, and answers what the browser needs to show it back: {@code {id, type, size}}.
   * The picture travels as base64 in {@code data} so it rides the JSON the rest of the API speaks.
   */
  public JSONObject stageScreenshot(final JSONObject request) {
    final String data = request.getString("data", "");
    if (data.isEmpty())
      throw new SupportException("bad_request", "The screenshot is empty");
    // Bound BEFORE decoding: a base64 string decodes to three quarters of its length
    if (data.length() > SupportScreenshots.MAX_BYTES / 3 * 4 + 8)
      throw new SupportException("too_large", "A screenshot is at most " + SupportScreenshots.MAX_BYTES / (1024 * 1024) + " MB");
    final byte[] bytes;
    try {
      bytes = java.util.Base64.getDecoder().decode(data);
    } catch (final IllegalArgumentException e) {
      throw new SupportException("bad_request", "The screenshot is not valid base64");
    }
    final SupportScreenshots.Shot shot = screenshots.stage(bytes);
    return new JSONObject().put("id", shot.id()).put("type", shot.mediaType()).put("size", shot.bytes().length);
  }

  public void discardScreenshot(final String id) {
    screenshots.discard(id);
  }

  /** {@code screenshots} of a request: a list of ids, at most five. */
  private static List<String> screenshotIds(final JSONObject request) {
    if (!request.has("screenshots") || request.isNull("screenshots"))
      return List.of();
    if (!(request.get("screenshots") instanceof JSONArray array))
      throw new SupportException("bad_request", "screenshots must be a list of screenshot ids");
    if (array.length() > SupportScreenshots.MAX_PER_SEND)
      throw new SupportException("bad_request", "At most " + SupportScreenshots.MAX_PER_SEND + " screenshots can be sent at a time");
    final List<String> ids = new ArrayList<>();
    for (int i = 0; i < array.length(); i++)
      if (array.get(i) instanceof String id && !id.isBlank())
        ids.add(id);
      else
        throw new SupportException("bad_request", "screenshots must be a list of screenshot ids");
    return ids;
  }

  public static List<String> screenshotIdsOf(final JSONObject request) {
    return screenshotIds(request);
  }

  public String listIssues(final String status) {
    try {
      return requireClient().listIssues(status);
    } catch (final IllegalArgumentException e) {
      throw new SupportException("bad_request", e.getMessage());
    }
  }

  public String getIssue(final long number) {
    return requireClient().getIssue(number);
  }

  public String addComment(final long number, final String body) {
    return addComment(number, body, List.of());
  }

  /**
   * A reply, optionally showing screenshots the user added: they are attached to the issue first, then the reply names them.
   * If the reply itself fails afterwards the pictures stay attached to the issue (they were sent) and are not held here any more.
   */
  public String addComment(final long number, final String body, final List<String> screenshotIds) {
    if (body == null || body.isBlank())
      throw new SupportException("bad_request", "The comment is empty");
    if (body.length() > 20000)
      throw new SupportException("bad_request", "The comment is at most 20000 characters");
    final SupportPortalClient client = requireClient();
    final List<SupportScreenshots.Shot> shots = screenshots.peek(screenshotIds);
    if (shots.isEmpty())
      return client.addComment(number, body);
    try {
      final JSONObject uploaded = new JSONObject(client.addScreenshots(number, shots));
      screenshots.remove(shots);
      final List<String> names = new ArrayList<>();
      if (uploaded.has("added") && uploaded.get("added") instanceof JSONArray added)
        for (int i = 0; i < added.length(); i++)
          if (added.get(i) instanceof String name)
            names.add(name);
      if (names.size() != shots.size())
        throw new SupportException("portal_error", "The portal did not confirm the screenshots; send the reply again without them");
      return client.addComment(number, body, names);
    } catch (final IOException e) {
      throw new SupportException("internal_error", "Cannot send the screenshots: " + e.getClass().getSimpleName());
    } catch (final SupportPortalException e) {
      SupportPortalClient.logFailure(this, "reply with screenshots", e);
      throw e;
    }
  }

  public void setOpen(final long number, final boolean open) {
    requireClient().setOpen(number, open);
  }

  // ------------------------------------------------------------------------------------- support requests

  private static final Pattern REQUEST_ID = Pattern.compile("^rq_[0-9a-f]{8}$");
  private static final int    MAX_RESPONSES = 20;

  /**
   * Forwards the answer to ONE support request (the client ran what staff asked, declined, or it failed). Only what the portal
   * reads is forwarded; the portal validates again and rebuilds the result, so this is the shape check, not the safety.
   */
  public String answerRequest(final long number, final String requestId, final JSONObject body) {
    if (requestId == null || !REQUEST_ID.matcher(requestId).matches())
      throw new SupportException("bad_request", "The request id is not valid");
    return requireClient().answerRequest(number, requestId, answerOf(body, null).toString());
  }

  /** Forwards several answers ("Run all") as one call, so the portal writes one comment. */
  public String answerRequests(final long number, final JSONObject body) {
    final JSONArray list = body.has("responses") && body.get("responses") instanceof JSONArray array ? array : null;
    if (list == null || list.isEmpty() || list.length() > MAX_RESPONSES)
      throw new SupportException("bad_request", "responses must be a list of 1 to " + MAX_RESPONSES + " answers");
    final JSONArray answers = new JSONArray();
    for (int i = 0; i < list.length(); i++) {
      final JSONObject one = list.get(i) instanceof JSONObject object ? object : null;
      if (one == null)
        throw new SupportException("bad_request", "Every answer must be an object");
      final String id = one.getString("requestId", "");
      if (!REQUEST_ID.matcher(id).matches())
        throw new SupportException("bad_request", "The request id is not valid");
      answers.put(answerOf(one, id));
    }
    return requireClient().answerRequests(number, new JSONObject().put("responses", answers).toString());
  }

  private static JSONObject answerOf(final JSONObject body, final String requestId) {
    final String outcome = body.getString("outcome", "");
    if (!outcome.equals("answered") && !outcome.equals("declined") && !outcome.equals("failed"))
      throw new SupportException("bad_request", "outcome must be answered, declined or failed");
    final JSONObject out = new JSONObject().put("outcome", outcome);
    if (requestId != null)
      out.put("requestId", requestId);
    if (body.has("reason")) {
      if (!(body.get("reason") instanceof String reason) || reason.length() > 500)
        throw new SupportException("bad_request", "reason must be text of at most 500 characters");
      out.put("reason", reason);
    }
    if (outcome.equals("answered")) {
      final boolean hasResult = body.has("result");
      final boolean hasText = body.has("text");
      if (hasResult == hasText)
        throw new SupportException("bad_request", "An answer carries either a result or text");
      if (hasResult) {
        final JSONObject result = body.get("result") instanceof JSONObject object ? object : null;
        if (result == null)
          throw new SupportException("bad_request", "result must be an object");
        out.put("result", result);
        if (body.has("durationMs") && body.get("durationMs") instanceof Number duration && duration.longValue() >= 0)
          out.put("durationMs", duration.longValue());
      } else {
        if (!(body.get("text") instanceof String text) || text.isBlank() || text.length() > 20000)
          throw new SupportException("bad_request", "text must be a non-empty string of at most 20000 characters");
        out.put("text", text);
      }
    }
    return out;
  }

  // ---------------------------------------------------------------------------------------------- download

  /**
   * The redacted bundle as one zip file for the public path and offline sharing: the logs (one entry per log file, under
   * {@code logs/}), {@code diagnostics.json}, {@code summary.json}, {@code threads.txt}. Built from the preview's files, so the
   * download is what the user reviewed.
   */
  public Path buildDownload(final String previewId) throws IOException {
    // The lease ends with this call: a caller that streams the file afterwards holds its own (see SupportHandler)
    try (final SupportBundleManager.Lease lease = bundles.lease(previewId)) {
      return buildDownload(lease.bundle());
    }
  }

  /** As {@link #buildDownload(String)} for a preview the caller already holds with {@link SupportBundleManager#lease}. */
  public Path buildDownload(final SupportBundleManager.Bundle bundle) throws IOException {
    if (bundle.isEmpty())
      throw new SupportException("bad_request", "The preview has no files");

    // One build at a time per preview: two downloads of it would otherwise write the same partial file
    synchronized (bundle.getBuildLock()) {
      final Path target = bundle.getDirectory().resolve("arcadedb-support-bundle.zip");
      if (Files.exists(target))
        return target;
      writeZip(bundle, target);
      return target;
    }
  }

  private static void writeZip(final SupportBundleManager.Bundle bundle, final Path target) throws IOException {
    final Path partial = bundle.getDirectory().resolve("arcadedb-support-bundle.zip.partial");
    try (final ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(partial))) {
      addFile(zip, "diagnostics.json", bundle.getDiagnostics());
      addFile(zip, "summary.json", bundle.getSummary());
      addFile(zip, "threads.txt", bundle.getThreads());
      if (bundle.getLogs() != null)
        try (final ZipFile logs = new ZipFile(bundle.getLogs().toFile())) {
          final var entries = logs.entries();
          while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            zip.putNextEntry(new ZipEntry("logs/" + entry.getName()));
            try (final InputStream in = logs.getInputStream(entry)) {
              in.transferTo(zip);
            }
            zip.closeEntry();
          }
        }
    }
    Files.move(partial, target);
  }

  private static void addFile(final ZipOutputStream zip, final String name, final Path file) throws IOException {
    if (file == null)
      return;
    zip.putNextEntry(new ZipEntry(name));
    Files.copy(file, zip);
    zip.closeEntry();
  }
}
