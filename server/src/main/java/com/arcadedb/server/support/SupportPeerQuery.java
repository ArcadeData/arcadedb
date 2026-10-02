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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.HAServerPlugin.ClusterPeer;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.handler.LeaderDial;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * Runs ONE read-only query of a support request on the OTHER members of the cluster, for the "all nodes" and "one named node"
 * targets of a support request (docs/SUPPORT-REQUESTS.md). The node Studio talks to runs the query on itself the ordinary way;
 * this class only reaches the peers.
 * <p>
 * Where the safety is, because a relay is exactly where it goes wrong:
 * <ul>
 * <li><b>The engine is the read-only gate, on each peer.</b> The query is sent to the peer's ordinary
 * {@code POST /api/v1/query/{database}}, which executes only idempotent statements and refuses everything else, in SQL and in
 * OpenCypher. Nothing here parses the statement for safety and nothing trusts the caller about it. There is no way to ask a peer
 * for a command, a script or a write through this class: the path, the method and the endpoint are fixed.</li>
 * <li><b>Peers come from the HA configuration only.</b> {@link HAServerPlugin#getClusterPeers()} hands out the guarded address of
 * each cluster member (the guard every peer-to-peer dial uses); a request names a node by NAME to pick among them, never by
 * address, so this cannot be turned into a way to reach any other host.</li>
 * <li><b>The caller's identity, not the server's.</b> The peer authenticates the hop with the cluster token and resolves the
 * forwarded user by name on ITS OWN user list, so the query runs with that user's permissions on that peer, like a forwarded
 * write does. Nobody's password travels. The token is never in an answer, a log line or an error.</li>
 * <li><b>Bounded.</b> At most {@link #MAX_PEERS} peers, {@link #MAX_PARALLEL} at once, {@link #DEADLINE_MS} each, and
 * {@link #MAX_RESPONSE_BYTES} read from each; one peer failing is that peer's row, not the request's failure.</li>
 * <li><b>One hop.</b> The one-hop marker the cluster uses everywhere is sent, so a peer answers for itself and relays nothing.</li>
 * </ul>
 */
public final class SupportPeerQuery {
  public static final int MAX_PEERS = 16;
  public static final int MAX_PARALLEL = 8;
  public static final long DEADLINE_MS = 35_000L;
  public static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
  public static final int MAX_STATEMENT = 2000;
  /** Rows asked of a peer: one more than Studio shows, so "truncated" can be told. */
  public static final int ROW_LIMIT = 1001;

  private static final Pattern DATABASE = Pattern.compile("[A-Za-z0-9_.\\-]{1,128}");
  private static final Pattern LANGUAGE = Pattern.compile("sql|opencypher");
  private static final Pattern NODE_NAME = Pattern.compile("[^\\p{Cntrl}]{1,120}");

  /** What a run needs from the server, so a test can stand in the cluster with plain HTTP servers. */
  public interface Cluster {
    List<ClusterPeer> peers();

    /** The effective cluster token, or null/blank when this server has none. */
    String clusterToken();

    boolean useSsl();

    /** The truststore-carrying client for HTTPS peers, or null. */
    HttpClient httpsClient() throws IOException;

    HttpClient plainClient();
  }

  private final Cluster cluster;
  private final long    deadlineMs;

  public SupportPeerQuery(final Cluster cluster) {
    this(cluster, DEADLINE_MS);
  }

  /** For tests: a shorter deadline. */
  SupportPeerQuery(final Cluster cluster, final long deadlineMs) {
    this.cluster = cluster;
    this.deadlineMs = deadlineMs;
  }

  /** The peers of the cluster by name, for the node selector of Studio. */
  public JSONObject peers() {
    final JSONArray names = new JSONArray();
    for (final ClusterPeer peer : cluster.peers())
      names.put(peer.name());
    return new JSONObject().put("ha", !cluster.peers().isEmpty()).put("peers", names);
  }

  /**
   * @param user      the authenticated caller; the peers resolve this name on their own user list
   * @param nodes     {@code "all"} or the name of ONE peer
   * @return {@code {ha, nodes:[{node, status:"ok", records, truncated} | {node, status:"failed", error}]}}
   */
  public JSONObject run(final String user, final String database, final String language, final String statement,
      final String nodes) {
    if (database == null || !DATABASE.matcher(database).matches())
      throw new SupportException("bad_request", "the database name is not valid");
    if (language == null || !LANGUAGE.matcher(language).matches())
      throw new SupportException("bad_request", "the language must be 'sql' or 'opencypher'");
    if (statement == null || statement.isBlank() || statement.length() > MAX_STATEMENT || hasControl(statement))
      throw new SupportException("bad_request", "the statement must be 1 to " + MAX_STATEMENT + " characters of text");
    if (nodes == null || (!"all".equals(nodes) && !NODE_NAME.matcher(nodes).matches()))
      throw new SupportException("bad_request", "nodes must be 'all' or the name of one node");
    if (user == null || user.isBlank())
      throw new SupportException("bad_request", "no authenticated user");

    final List<ClusterPeer> all = cluster.peers();
    final JSONObject out = new JSONObject().put("ha", !all.isEmpty());
    final JSONArray results = new JSONArray();
    out.put("nodes", results);
    if (all.isEmpty())
      return out;

    final List<ClusterPeer> targets = new ArrayList<>();
    if ("all".equals(nodes))
      targets.addAll(all.size() > MAX_PEERS ? all.subList(0, MAX_PEERS) : all);
    else {
      for (final ClusterPeer peer : all)
        if (peer.name().equals(nodes)) {
          targets.add(peer);
          break;
        }
      if (targets.isEmpty()) {
        results.put(failed(nodes, "this node is not a member of the cluster"));
        return out;
      }
    }

    final String token = cluster.clusterToken();
    final String body = new JSONObject().put("language", language).put("command", statement).put("limit", ROW_LIMIT).toString();
    final ExecutorService pool = Executors.newFixedThreadPool(Math.min(MAX_PARALLEL, targets.size()), r -> {
      final Thread t = new Thread(r, "support-peer-query");
      t.setDaemon(true);
      return t;
    });
    try {
      final List<CompletableFuture<JSONObject>> futures = new ArrayList<>();
      for (final ClusterPeer peer : targets)
        futures.add(CompletableFuture.supplyAsync(() -> queryPeer(peer, token, user, database, body), pool)
            .completeOnTimeout(failed(peer.name(), "timed out"), deadlineMs + 5_000L, TimeUnit.MILLISECONDS));
      for (final CompletableFuture<JSONObject> f : futures)
        results.put(f.join());
    } finally {
      pool.shutdownNow();
    }
    return out;
  }

  private JSONObject queryPeer(final ClusterPeer peer, final String token, final String user, final String database,
      final String body) {
    try {
      if (peer.httpAddress() == null)
        return failed(peer.name(), "not reachable: " + (peer.refusal() == null ? "no address" : peer.refusal()));
      if (token == null || token.isBlank())
        return failed(peer.name(), "the cluster has no token, so a peer cannot be asked on behalf of a user");

      String endpoint = peer.httpAddress();
      boolean https = false;
      HttpClient client = cluster.plainClient();
      if (cluster.useSsl() && peer.httpsAddress() != null) {
        // A cluster that named an HTTPS endpoint said where this belongs: it is not sent in the clear if no client can be built
        final HttpClient secure = cluster.httpsClient();
        if (secure == null)
          return failed(peer.name(), "not reachable: the encrypted connection to the peer cannot be set up");
        client = secure;
        endpoint = peer.httpsAddress();
        https = true;
      }

      final HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create((https ? "https" : "http") + "://" + endpoint + "/api/v1/query/"
              + URLEncoder.encode(database, StandardCharsets.UTF_8)))
          .version(HttpClient.Version.HTTP_1_1)
          .header("Content-Type", "application/json")
          .header("X-ArcadeDB-Cluster-Token", token)
          .header("X-ArcadeDB-Forwarded-User", user)
          .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true")
          .timeout(Duration.ofMillis(deadlineMs))
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .build();

      final HttpResponse<InputStream> response = LeaderDial.sendBounded(client, request,
          HttpResponse.BodyHandlers.ofInputStream(), deadlineMs);
      final String text;
      try (final InputStream in = response.body()) {
        final byte[] bytes = in.readNBytes(MAX_RESPONSE_BYTES + 1);
        if (bytes.length > MAX_RESPONSE_BYTES)
          return failed(peer.name(), "the answer is larger than " + (MAX_RESPONSE_BYTES / 1024 / 1024) + " MB");
        text = new String(bytes, StandardCharsets.UTF_8);
      }

      if (response.statusCode() != 200)
        return failed(peer.name(), "the peer refused the query (HTTP " + response.statusCode() + ")" + detailOf(text));

      final JSONObject answer = new JSONObject(text);
      final JSONArray records = answer.getJSONArray("result", new JSONArray());
      final boolean truncated = records.length() >= ROW_LIMIT;
      final JSONArray kept = new JSONArray();
      for (int i = 0; i < records.length() && i < ROW_LIMIT; i++)
        kept.put(records.get(i));
      return new JSONObject().put("node", peer.name()).put("status", "ok").put("records", kept).put("truncated", truncated);
    } catch (final HttpConnectTimeoutException | java.net.ConnectException e) {
      return failed(peer.name(), "not reachable");
    } catch (final HttpTimeoutException e) {
      return failed(peer.name(), "timed out");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return failed(peer.name(), "interrupted");
    } catch (final Exception e) {
      // Class name and no message: a message can carry an address, a token echoed by a proxy, or text from the peer
      LogManager.instance().log(this, Level.FINE, "Support peer query to '%s' failed: %s", peer.name(), e.getClass().getName());
      return failed(peer.name(), "failed (" + e.getClass().getSimpleName() + ")");
    }
  }

  /** The peer's own error text, shortened and without control characters: "detail" first, then "error". */
  private static String detailOf(final String text) {
    try {
      final JSONObject json = new JSONObject(text);
      final String detail = json.getString("detail", json.getString("error", ""));
      if (!detail.isBlank())
        return ": " + detail.replaceAll("\\p{Cntrl}", " ").trim().substring(0, Math.min(300, detail.trim().length()));
    } catch (final Exception ignored) {
      // not JSON
    }
    return "";
  }

  private static JSONObject failed(final String node, final String error) {
    return new JSONObject().put("node", node).put("status", "failed").put("error", error);
  }

  private static boolean hasControl(final String text) {
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if ((c < 0x20 && c != '\n' && c != '\r' && c != '\t') || c == 0x7F)
        return true;
    }
    return false;
  }

  /** The supplier form of {@link Cluster#peers()} for the production wiring; kept here so the handler stays thin. */
  public static Cluster clusterOf(final com.arcadedb.server.ArcadeDBServer server, final Supplier<HttpClient> plain) {
    return new Cluster() {
      @Override
      public List<ClusterPeer> peers() {
        final HAServerPlugin ha = server.getHA();
        return ha == null ? List.of() : ha.getClusterPeers();
      }

      @Override
      public String clusterToken() {
        return HAServerPlugin.effectiveClusterToken(server);
      }

      @Override
      public boolean useSsl() {
        return server.getConfiguration().getValueAsBoolean(com.arcadedb.GlobalConfiguration.NETWORK_USE_SSL);
      }

      @Override
      public HttpClient httpsClient() throws IOException {
        final HAServerPlugin ha = server.getHA();
        return ha == null ? null : ha.getPeerHttpsClient();
      }

      @Override
      public HttpClient plainClient() {
        return plain.get();
      }
    };
  }
}
