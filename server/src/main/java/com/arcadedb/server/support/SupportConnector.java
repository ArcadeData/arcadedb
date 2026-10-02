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

import com.arcadedb.Constants;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.BoundedHttpExchange;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.logging.Level;

/**
 * "Connect to ArcadeDB Portal": the device authorization flow of the customer portal (portal docs/STUDIO-CONNECT.md). The server
 * asks the portal for a code ({@code start}), Studio shows it and opens the portal in a new tab, a person who administers a
 * workspace approves it there, and this class, polling in a daemon thread, receives the workspace key ONCE and stores it exactly
 * as a manual registration does ({@link SupportService#register}), then registers this server as an installation.
 * <p>
 * The key travels only between the portal and this server: the browser gets the user code, the portal address and the state.
 * The device code and the key are never logged, never returned and only held in local variables for the length of the exchange.
 * The start request carries {@code attributes}, a flat map of short texts about this server (see {@link #attributesOf}): host, version,
 * server name and, in HA, the cluster name and size. The portal interprets them; the platform in between only keeps them.
 * One connect runs at a time. The state of the last one stays readable (so Studio can show the outcome) until the next one starts
 * or it is cancelled.
 * <p>
 * Without Studio the same routes are used from a shell ({@code root} basic authentication, anywhere the HTTP port is reachable) or
 * from the console ({@code connect portal remote:<host> root}):
 * <pre>
 * curl -s -u root:PASSWORD -X POST -H 'Content-Type: application/json' -d '{"label":"prod-1"}' http://localhost:2480/api/v1/server/support/connect
 *   -> 200 {"userCode":"WDJB-MJHT","verifyUrl":"https://portal.arcadedb.com/#/connect?code=WDJB-MJHT","expiresIn":600}
 *   (open verifyUrl in any browser, check the code, approve)
 * curl -s -u root:PASSWORD http://localhost:2480/api/v1/server/support/connect
 *   -> 200 {"status":"pending",...}, then {"status":"connected","workspaceName":"Acme Corp","registration":{"status":"created",...}}
 *      (other final states: expired, denied, error {error,message}, cancelled; none when nothing was started or it was cancelled)
 * curl -s -u root:PASSWORD -X DELETE http://localhost:2480/api/v1/server/support/connect
 *   -> 204 (ends the wait; a key already received stays registered)
 * </pre>
 * Errors are {@code {error, detail, message}}: 401 no credentials, 403 not root, 404 {@code not_supported} (the portal has no such
 * flow yet), 409 {@code connect_in_progress} | {@code registered_by_settings} | {@code config_not_writable} | {@code key_limit},
 * 429 {@code rate_limited} (with Retry-After), 503 {@code portal_unreachable}.
 */
public class SupportConnector implements AutoCloseable {
  static final String START_PATH = "/public/v1/support/connect/start";
  static final String POLL_PATH  = "/public/v1/support/connect/poll";

  private static final long MIN_INTERVAL_MS     = 1_000L;
  private static final long MAX_INTERVAL_MS     = 30_000L;
  private static final long SLOW_DOWN_STEP_MS   = 5_000L;
  /** The portal's expiry plus the time the last poll may take; the portal answers expired_token first in practice. */
  private static final long EXPIRY_SLACK_MS     = 15_000L;
  private static final int  MAX_TRANSPORT_FAILS = 8;
  private static final long CALL_TIMEOUT_MS     = 20_000L;
  private static final long MAX_RESPONSE_BYTES  = 64 * 1024L;

  public enum State {
    PENDING, CONNECTED, EXPIRED, DENIED, ERROR, CANCELLED
  }

  private final SupportService service;
  private final long           pollSleepOverrideMs;

  private volatile Session session;

  /** What Studio may read; the device code is deliberately not a field of anything returned. */
  private static final class Session {
    final    String     userCode;
    final    String     verifyUrl;
    final    long       expiresAtMs;
    volatile State      state = State.PENDING;
    volatile String     workspaceName;
    volatile JSONObject registration;
    volatile JSONObject error;
    volatile Thread     thread;

    Session(final String userCode, final String verifyUrl, final long expiresAtMs) {
      this.userCode = userCode;
      this.verifyUrl = verifyUrl;
      this.expiresAtMs = expiresAtMs;
    }
  }

  public SupportConnector(final SupportService service) {
    this(service, -1L);
  }

  /** For tests: a fixed wait between polls instead of the portal's interval. */
  SupportConnector(final SupportService service, final long pollSleepOverrideMs) {
    this.service = service;
    this.pollSleepOverrideMs = pollSleepOverrideMs;
  }

  /**
   * Asks the portal for a code and starts waiting for the approval.
   *
   * @return {@code {userCode, verifyUrl, expiresIn}}
   */
  public synchronized JSONObject start(final String label, final String instanceId, final JSONObject attributes) {
    final Session running = session;
    if (running != null && running.state == State.PENDING)
      throw new SupportException("connect_in_progress", "A connection is already waiting for approval: finish it or cancel it first");

    final SupportConfiguration configuration = service.getConfiguration();
    final SupportConfiguration.Registration existing = configuration.get();
    if (existing != null && existing.isFromSettings())
      throw new SupportException("registered_by_settings", "This server is registered through the settings "
          + "arcadedb.support.clientId and arcadedb.support.clientKey: change them in the server configuration");
    if (!configuration.canWriteConfig())
      throw new SupportException("config_not_writable", "The configuration directory of the server is not writable: set the "
          + "settings arcadedb.support.clientId and arcadedb.support.clientKey instead");

    final String portalUrl = configuration.getPortalUrl(existing);
    try {
      SupportConfiguration.validatePortalUrl(portalUrl);
    } catch (final IllegalArgumentException e) {
      throw new SupportException("bad_request", e.getMessage());
    }

    // host and version are also sent at the top level, which the portal still accepts, for a portal that predates "attributes"
    final JSONObject body = new JSONObject().put("host", truncate(attributes.getString("host", ""), 100)).put("version", Constants.getRawVersion())
        .put("attributes", attributes);
    if (instanceId != null && !instanceId.isBlank())
      body.put("instanceId", instanceId);
    if (label != null && !label.isBlank())
      body.put("label", truncate(label.trim(), 60));

    final JSONObject answer = post(portalUrl, START_PATH, body);
    final String deviceCode = answer.getString("deviceCode", "");
    final String userCode = answer.getString("userCode", "");
    final String verifyUrl = answer.getString("verifyUrl", "");
    final long expiresIn = answer.getLong("expiresIn", 600L);
    final long interval = answer.getLong("interval", 3L);
    if (deviceCode.isEmpty() || userCode.isEmpty() || !isHttpUrl(verifyUrl))
      throw new SupportException("portal_error", "The portal answered in a form this server does not understand");

    final Session created = new Session(userCode, verifyUrl, System.currentTimeMillis() + expiresIn * 1000L);
    session = created;
    // The device code goes to the thread and nowhere else
    final Thread thread = new Thread(() -> waitForApproval(created, portalUrl, deviceCode, instanceId, interval * 1000L),
        "ArcadeDB-SupportConnect");
    thread.setDaemon(true);
    created.thread = thread;
    thread.start();

    LogManager.instance().log(this, Level.INFO, "Support connect: waiting for the approval of code %s", userCode);
    return new JSONObject().put("userCode", userCode).put("verifyUrl", verifyUrl).put("expiresIn", expiresIn);
  }

  /** {@code {status: none|pending|connected|expired|denied|error|cancelled, ...}}, never any secret. */
  public JSONObject status() {
    final Session s = session;
    if (s == null)
      return new JSONObject().put("status", "none");
    final JSONObject json = new JSONObject().put("status", s.state.name().toLowerCase(Locale.ROOT));
    if (s.state == State.PENDING)
      json.put("userCode", s.userCode).put("verifyUrl", s.verifyUrl).put("expiresOn", s.expiresAtMs);
    if (s.workspaceName != null)
      json.put("workspaceName", s.workspaceName);
    if (s.registration != null)
      json.put("registration", s.registration);
    if (s.error != null)
      json.put("error", s.error);
    return json;
  }

  /** Stops waiting. A key that was already received stays registered: this only ends the wait. */
  public synchronized void cancel() {
    final Session s = session;
    if (s == null)
      return;
    if (s.state == State.PENDING) {
      s.state = State.CANCELLED;
      final Thread t = s.thread;
      if (t != null)
        t.interrupt();
    }
    session = null;
  }

  @Override
  public void close() {
    cancel();
  }

  // ---------------------------------------------------------------------------------------------- the waiting thread

  private void waitForApproval(final Session s, final String portalUrl, final String deviceCode, final String instanceId,
      final long firstIntervalMs) {
    long interval = clamp(firstIntervalMs);
    int transportFailures = 0;
    final JSONObject pollBody = new JSONObject().put("deviceCode", deviceCode);
    if (instanceId != null && !instanceId.isBlank())
      pollBody.put("instanceId", instanceId);

    try {
      while (s.state == State.PENDING) {
        if (System.currentTimeMillis() > s.expiresAtMs + EXPIRY_SLACK_MS) {
          finish(s, State.EXPIRED, null);
          return;
        }
        Thread.sleep(pollSleepOverrideMs >= 0 ? pollSleepOverrideMs : interval);
        if (s.state != State.PENDING)
          return;

        final JSONObject answer;
        try {
          answer = rawPost(portalUrl, POLL_PATH, pollBody);
          transportFailures = 0;
        } catch (final SupportException e) {
          if (e.getCode().equals("portal_unreachable") && ++transportFailures < MAX_TRANSPORT_FAILS)
            continue;
          finish(s, State.ERROR, error(e.getCode(), e.getMessage()));
          return;
        }

        if (answer.getInt("httpStatus", 0) == 200) {
          approved(s, answer);
          return;
        }
        switch (answer.getString("error", "")) {
        case "authorization_pending" -> {
          // keep waiting
        }
        case "slow_down" -> interval = Math.min(MAX_INTERVAL_MS,
            Math.max(interval, clamp(answer.getLong("interval", 0L) * 1000L)) + SLOW_DOWN_STEP_MS);
        case "access_denied" -> {
          finish(s, State.DENIED, null);
          return;
        }
        case "expired_token" -> {
          finish(s, State.EXPIRED, null);
          return;
        }
        case "key_limit" -> {
          finish(s, State.ERROR, error("key_limit",
              "The workspace already has 25 active keys: revoke one in the portal (Studio keys) and connect again."));
          return;
        }
        case "rate_limited" -> interval = Math.min(MAX_INTERVAL_MS, interval * 2);
        default -> {
          finish(s, State.ERROR, error("portal_error", "The portal answered with something this server does not understand"));
          return;
        }
        }
      }
    } catch (final InterruptedException e) {
      // cancel() or shutdown
      Thread.currentThread().interrupt();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING, "Support connect failed: %s", e.getClass().getSimpleName());
      finish(s, State.ERROR, error("internal_error", "The connection failed on this server: " + e.getClass().getSimpleName()));
    }
  }

  private void approved(final Session s, final JSONObject answer) {
    final String clientId = answer.getString("clientId", "");
    final String key = answer.getString("key", "");
    s.workspaceName = answer.getString("workspaceName", null);
    try {
      // The same path as pasting the two values: whoami must answer before anything is saved
      service.register(clientId, key);
    } catch (final SupportException e) {
      finish(s, State.ERROR, error(e.getCode(), e.getMessage()));
      return;
    }
    LogManager.instance().log(this, Level.INFO, "Support connect: approved, the workspace key was stored");

    // Registering the installation is part of connecting, but its failure does not undo the connection
    try {
      s.registration = new JSONObject(service.registerInstallation());
    } catch (final SupportException e) {
      s.registration = error(e.getCode(), e.getMessage());
    } catch (final RuntimeException e) {
      s.registration = error("portal_error",
          "The server could not be registered as an installation: " + e.getClass().getSimpleName());
    }
    s.state = State.CONNECTED;
  }

  private static void finish(final Session s, final State state, final JSONObject error) {
    s.error = error;
    s.state = state;
  }

  // ---------------------------------------------------------------------------------------------- the portal

  private JSONObject post(final String portalUrl, final String path, final JSONObject body) {
    final JSONObject answer = rawPost(portalUrl, path, body);
    final int status = answer.getInt("httpStatus", 0);
    if (status >= 200 && status < 300)
      return answer;
    final String code = answer.getString("error", "");
    if (status == 429)
      throw new SupportException("rate_limited", "The portal refuses too many connection attempts from this server: retry later");
    if (status == 404)
      throw new SupportException("not_supported", "This portal cannot connect servers from Studio yet");
    if (status == 400)
      throw new SupportException("bad_request", "The portal refused the request" + (code.isEmpty() ? "" : " (" + code + ")"));
    throw new SupportException("portal_error", "The support portal answered with an error (HTTP " + status + ")");
  }

  /** One anonymous POST; the answer is the JSON body plus {@code httpStatus}. Never sends a key (there is none yet). */
  private JSONObject rawPost(final String portalUrl, final String path, final JSONObject body) {
    final HttpRequest request = HttpRequest.newBuilder().uri(URI.create(portalUrl + path)).timeout(Duration.ofMillis(CALL_TIMEOUT_MS))
        .header("Content-Type", "application/json").header("Accept", "application/json")
        .header("User-Agent", "ArcadeDB/" + Constants.getRawVersion() + " support-client")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build();
    final HttpResponse<String> response;
    try {
      response = BoundedHttpExchange.send(SupportPortalClient.HTTP_CLIENT, request,
          info -> new SupportPortalClient.LimitedStringBody(MAX_RESPONSE_BYTES), CALL_TIMEOUT_MS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SupportException("portal_unreachable", "The request to the portal was interrupted");
    } catch (final IOException e) {
      throw new SupportException("portal_unreachable", "Cannot reach the support portal at " + portalUrl + " ("
          + e.getClass().getSimpleName() + "). Check the network connection and the proxy settings of this server");
    }
    final JSONObject json;
    try {
      json = new JSONObject(response.body() == null || response.body().isBlank() ? "{}" : response.body());
    } catch (final RuntimeException e) {
      throw new SupportException("portal_error",
          "The portal answered with something this server does not understand: check the portal address and try again later");
    }
    return json.put("httpStatus", response.statusCode());
  }

  // ---------------------------------------------------------------------------------------------- helpers

  /**
   * What this server tells the portal about itself when it asks to connect: a flat map of short texts the portal shows to the
   * approver and interprets as it needs (the platform keeps them as they are). The cluster name and size are only sent by a
   * server that runs in HA. Never a secret or a path; empty values are left out and long ones cut to 200 characters.
   *
   * @param ha the number of servers configured in the cluster, or 0 when not known; ignored when {@code clusterName} is empty
   */
  static JSONObject attributesOf(final String host, final String serverName, final String clusterName, final int ha) {
    final JSONObject attributes = new JSONObject();
    put(attributes, "host", host);
    put(attributes, "version", Constants.getRawVersion());
    put(attributes, "serverName", serverName);
    if (clusterName != null && !clusterName.isBlank()) {
      put(attributes, "clusterName", clusterName);
      if (ha > 0)
        put(attributes, "haNodes", Integer.toString(ha));
    }
    return attributes;
  }

  private static void put(final JSONObject attributes, final String key, final String value) {
    if (value != null && !value.isBlank())
      attributes.put(key, truncate(value.trim(), 200));
  }

  /** The host name this server reports to the portal (shown to the approver, who is told it is not verified). */
  public static String localHost(final String serverName) {
    try {
      final String host = InetAddress.getLocalHost().getHostName();
      if (host != null && !host.isBlank())
        return host;
    } catch (final IOException | RuntimeException e) {
      // fall through to the server name
    }
    return serverName;
  }

  private static JSONObject error(final String code, final String message) {
    return new JSONObject().put("error", code).put("message", message);
  }

  private static long clamp(final long ms) {
    return Math.max(MIN_INTERVAL_MS, Math.min(MAX_INTERVAL_MS, ms));
  }

  private static String truncate(final String s, final int max) {
    return s == null ? "" : s.length() <= max ? s : s.substring(0, max);
  }

  private static boolean isHttpUrl(final String url) {
    final String lower = url.toLowerCase(Locale.ROOT);
    return lower.startsWith("https://") || lower.startsWith("http://");
  }

  /** For tests: waits for the polling thread of the current connection to end. */
  void join(final long millis) throws InterruptedException {
    final Session s = session;
    if (s != null && s.thread != null)
      s.thread.join(millis);
  }
}
