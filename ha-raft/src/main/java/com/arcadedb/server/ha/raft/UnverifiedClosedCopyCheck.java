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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * The leader's side of issue #8605. A copy carrying the {@link ArcadeDBServer#UNVERIFIED_CLOSED_COPY_FILE} marker was
 * closed on this node while it was a follower, and a resync could not verify it because the leader of the time did not
 * hold it (issue #8589). A follower refuses to reopen it; a leader that reopens it makes it the cluster's copy, the one
 * every follower then installs from. So the leader asks every peer of the committed configuration about its own copy
 * first, and reopens only when none of them holds one this copy is behind.
 * <p>
 * The recency signal is the per-database applied Raft index each node persists ({@link
 * ArcadeStateMachine#readPersistedAppliedIndex(String)}): a Raft log position is the same number on every node, unlike
 * a database's last transaction id, which a follower's replicated apply never advances. A copy with no recorded
 * position, or one this node holds quarantined (its position may be overstated), cannot be ordered, so it refuses.
 * <p>
 * Every refusal is recorded for the {@code unverified-closed-copy-refused} cluster alert, and dropped once the copy is
 * reopened or its marker is gone.
 */
final class UnverifiedClosedCopyCheck {

  /** Request flag on {@code POST /api/v1/cluster/bootstrap-state}: answer with this node's copy of the named database. */
  static final String COPY_OF = "copyOf";
  /** Response member carrying the {@link CopyState} of the database {@link #COPY_OF} named. */
  static final String COPY    = "copy";
  /**
   * Response member beside {@link #COPY}: whether the answering node holds the database {@link #COPY_OF} named in its
   * server registry, which is exactly when its snapshot endpoint serves it rather than answering 404
   * ({@code SnapshotHttpHandler} gates on {@code existsDatabase}). A follower asks its leader before reinstalling a copy
   * it keeps closed and unverified (issue #8606). Absent from the answer of a server that predates it, which reads as
   * {@code false}: no install is attempted on it.
   */
  static final String REGISTERED = "registered";

  /**
   * Budget of one round: every peer is asked at once, and the round ends when all have answered or this much time has
   * passed, whichever comes first. A peer still silent then counts as unanswered.
   */
  static final long ROUND_TIMEOUT_MS = 5_000L;

  /**
   * How long a refusal is handed back without asking the peers again. Every request that names the database reaches
   * {@link #check(String)}, and a client retrying or a dashboard polling it would otherwise run one round - and hold a
   * worker thread for up to {@link #ROUND_TIMEOUT_MS} - per request.
   */
  static final long REFUSAL_REUSE_MS = 5_000L;

  private static final long LOG_INTERVAL_MS = 60_000L;

  /**
   * One node's copy of a database: whether it holds one at all (registered, or a directory on disk) and the last Raft
   * index applied to it there, or {@code -1} when it cannot be ordered.
   */
  record CopyState(boolean present, long appliedIndex) {
    JSONObject toJSON(final String databaseName) {
      return new JSONObject().put("name", databaseName).put("present", present).put("appliedIndex", appliedIndex);
    }

    static CopyState fromJSON(final JSONObject json) {
      return new CopyState(json.getBoolean("present", true), json.getLong("appliedIndex", -1L));
    }
  }

  /** How a peer is asked, without blocking the caller; an asynchronous HTTP round trip in production. */
  @FunctionalInterface
  interface PeerQuestion {
    CompletableFuture<CopyState> ask(String url, String databaseName);
  }

  private record Refusal(String reason, long atMs) {
  }

  private final RaftHAServer          raftHAServer;
  private final ArcadeDBServer        server;
  private final Map<String, Refusal>  refusals   = new ConcurrentHashMap<>();
  private final Map<String, Long>     lastLogged = new ConcurrentHashMap<>();
  private final Map<String, Object>   rounds     = new ConcurrentHashMap<>();
  // Instance fields so a test can shorten them; production never changes either.
  long                                refusalReuseMs = REFUSAL_REUSE_MS;
  long                                roundTimeoutMs = ROUND_TIMEOUT_MS;

  UnverifiedClosedCopyCheck(final RaftHAServer raftHAServer, final ArcadeDBServer server) {
    this.raftHAServer = raftHAServer;
    this.server = server;
  }

  /**
   * Asks every peer of the committed configuration about its copy of {@code databaseName}.
   *
   * @return {@code null} when the leader may reopen its copy, otherwise why not
   */
  String check(final String databaseName) {
    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    final BootstrapElection election = raftHAServer.getBootstrapElection();
    // A transient state of a starting node: refused, but neither logged at SEVERE nor raised as the cluster alert.
    if (stateMachine == null || election == null)
      return "the HA layer of this server has not started yet";

    // A retrying client inside the reuse window costs a map lookup, not URL resolution and index reads. The check with
    // the round lock held below still decides; this only skips the preparation when a refusal is already standing.
    final Refusal standing = refusals.get(databaseName);
    if (standing != null && System.currentTimeMillis() - standing.atMs() < refusalReuseMs)
      return standing.reason();

    final boolean useSSL = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final Map<RaftPeerId, String> urls = election.peerProbeUrls(useSSL);
    if (useSSL && urls.values().stream().anyMatch(url -> url != null && url.startsWith("http://")))
      PlainHttpFallbackNotice.sayOnce(UnverifiedClosedCopyCheck.class, "asking about an unverified closed copy");

    final String clusterToken = raftHAServer.getClusterToken();
    return check(databaseName, localCopyState(server, stateMachine, databaseName), urls, (url, name) -> {
      try {
        // The node's cached peer clients (issue #7301): this runs on the request path, so no client is built per call.
        final HttpClient client = url.startsWith("https://") ?
            raftHAServer.getHttpsClients().clientFor(server) :
            BootstrapElection.HTTP;
        return askOverHttp(client, url, name, clusterToken);
      } catch (final IOException e) {
        return CompletableFuture.failedFuture(e);
      }
    });
  }

  /**
   * {@link #check(String)} with this node's copy, the peers' URLs and the way to ask them given. Package-private for
   * tests.
   * <p>
   * One round at a time per database: a request arriving while a round runs waits for it and takes its verdict, and a
   * refusal younger than {@link #refusalReuseMs} is handed back without a round at all.
   */
  String check(final String databaseName, final CopyState local, final Map<RaftPeerId, String> peerUrls,
      final PeerQuestion question) {
    synchronized (rounds.computeIfAbsent(databaseName, k -> new Object())) {
      // A round that just passed let its caller reopen the copy and drop the mark: a request that waited behind it has
      // nothing left to verify, and would otherwise run a round of its own.
      if (!hasMarker(databaseName))
        return null;
      final Refusal recent = refusals.get(databaseName);
      if (recent != null && System.currentTimeMillis() - recent.atMs() < refusalReuseMs)
        return recent.reason();
      return round(databaseName, local, peerUrls, question);
    }
  }

  private String round(final String databaseName, final CopyState local, final Map<RaftPeerId, String> peerUrls,
      final PeerQuestion question) {
    final Map<String, CopyState> answered = new TreeMap<>();
    final List<String> unanswered = new ArrayList<>();
    final Map<String, CompletableFuture<CopyState>> pending = new LinkedHashMap<>();
    for (final Map.Entry<RaftPeerId, String> entry : peerUrls.entrySet()) {
      final String peer = entry.getKey().toString();
      if (entry.getValue() == null)
        unanswered.add(peer + " (no HTTP address this node may dial)");
      else
        pending.put(peer, question.ask(entry.getValue(), databaseName));
    }

    // Every peer is asked at once and the round shares ONE deadline, so the worst case is one budget rather than one
    // per peer (nanoTime: immune to wall-clock steps).
    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(roundTimeoutMs);
    for (final Map.Entry<String, CompletableFuture<CopyState>> entry : pending.entrySet()) {
      final String peer = entry.getKey();
      final CompletableFuture<CopyState> answer = entry.getValue();
      try {
        final long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        answered.put(peer, answer.get(remainingNanos, TimeUnit.NANOSECONDS));
      } catch (final TimeoutException e) {
        // Stops waiting on this peer; the HTTP exchange behind the stage ends on its own request timeout.
        answer.cancel(true);
        unanswered.add(peer + " (no answer within " + roundTimeoutMs + " ms)");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        answer.cancel(true);
        unanswered.add(peer + " (interrupted)");
      } catch (final ExecutionException e) {
        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        unanswered.add(peer + " (" + cause.getMessage() + ")");
      } catch (final CancellationException e) {
        unanswered.add(peer + " (cancelled)");
      }
    }

    final String refusal = verdict(local, answered, unanswered);
    if (refusal == null) {
      refusals.remove(databaseName);
      lastLogged.remove(databaseName);
      LogManager.instance().log(this, Level.INFO,
          "Database '%s': this leader's copy, which the last resync could not verify, is at applied index %d and no "
              + "other server holds a newer one (%s), so it is reopened as the cluster's copy (issue #8605)", null,
          databaseName, local.appliedIndex(), answered);
      return null;
    }
    return record(databaseName, refusal);
  }

  /**
   * The rule, free of I/O: every peer must have answered, and every copy a peer holds must be ordered at or below this
   * node's. Package-private for tests.
   *
   * @return {@code null} when the leader may reopen its copy, otherwise why not
   */
  static String verdict(final CopyState local, final Map<String, CopyState> answered, final List<String> unanswered) {
    if (!unanswered.isEmpty())
      return "the other servers could not all be asked about their copy, and one of them may hold a newer one: "
          + String.join(", ", unanswered);
    final List<String> newer = new ArrayList<>();
    final List<String> unordered = new ArrayList<>();
    for (final Map.Entry<String, CopyState> entry : answered.entrySet()) {
      final CopyState peer = entry.getValue();
      if (!peer.present())
        continue;
      if (peer.appliedIndex() < 0 || local.appliedIndex() < 0)
        unordered.add(entry.getKey() + " (applied index " + peer.appliedIndex() + ")");
      else if (peer.appliedIndex() > local.appliedIndex())
        newer.add(entry.getKey() + " (applied index " + peer.appliedIndex() + ")");
    }
    if (!newer.isEmpty())
      return "a newer copy is held by " + String.join(", ", newer) + ", while this node's is at applied index "
          + local.appliedIndex();
    if (!unordered.isEmpty())
      return "it cannot be ordered against the copy held by " + String.join(", ", unordered)
          + ", because one of the two has no recorded applied index (this node's is " + local.appliedIndex() + ")";
    return null;
  }

  /**
   * This node's copy of {@code databaseName}: present when it is registered or has a directory on disk, ordered by the
   * applied index this node persisted for it. A copy this node holds quarantined is reported unordered: the entries
   * skipped while it waits for its resync still advanced the position. Never opens anything.
   * <p>
   * The position is trusted for a closed copy that is not quarantined. An entry for a database closed here reaches it
   * through {@code ArcadeStateMachine.databaseFor -> getDatabase}, which reopens an unmarked copy and applies to it, and
   * refuses a marked one on a follower, failing the apply rather than skipping it silently. A failed apply that still
   * advanced the recorded position without quarantining the database would overstate it; that is the residual risk of
   * the #8454 family, not something this check can see.
   */
  static CopyState localCopyState(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final String databaseName) {
    final boolean present = server.existsDatabase(databaseName) || Files.isDirectory(
        Path.of(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), databaseName));
    if (!present)
      return new CopyState(false, -1L);
    if (stateMachine == null || stateMachine.isDatabaseDiverged(databaseName))
      return new CopyState(true, -1L);
    return new CopyState(true, stateMachine.readPersistedAppliedIndex(databaseName));
  }

  /**
   * The databases a refusal is standing for, with its reason, for the cluster alert. A refusal whose marker has gone
   * since - an install replaced the copy, a drop removed it, an operator accepted it - no longer stands.
   */
  Map<String, String> getRefusals() {
    if (refusals.isEmpty())
      return Collections.emptyMap();
    refusals.keySet().removeIf(name -> !hasMarker(name));
    lastLogged.keySet().retainAll(refusals.keySet());
    final Map<String, String> reasons = new TreeMap<>();
    for (final Map.Entry<String, Refusal> entry : refusals.entrySet())
      reasons.put(entry.getKey(), entry.getValue().reason());
    return reasons;
  }

  private boolean hasMarker(final String databaseName) {
    return Files.exists(Path.of(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        databaseName, ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE));
  }

  private String record(final String databaseName, final String refusal) {
    final long now = System.currentTimeMillis();
    refusals.put(databaseName, new Refusal(refusal, now));
    // Every request that names the database can ask again, so the SEVERE is throttled per database, not per request.
    final Long previous = lastLogged.get(databaseName);
    if (previous == null || now - previous >= LOG_INTERVAL_MS) {
      lastLogged.put(databaseName, now);
      LogManager.instance().log(this, Level.SEVERE,
          "Database '%s' is NOT reopened on this leader: its copy was closed here and the last resync could not verify "
              + "it, and %s. Transfer the leadership to the server holding the newer copy, or remove this node's '%s' "
              + "marker to accept this copy as it is (issue #8605)", null, databaseName, refusal,
          ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
    }
    return refusal;
  }

  /**
   * One peer's answer, asynchronously. The request carries {@link #ROUND_TIMEOUT_MS} as its own timeout, which on JDK
   * 21-25 stops at the response headers; the round's deadline in {@link #round} bounds a body that stalls after them.
   * Package-private for tests.
   */
  static CompletableFuture<CopyState> askOverHttp(final HttpClient client, final String url, final String databaseName,
      final String clusterToken) {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(url, clusterToken, ROUND_TIMEOUT_MS,
        new JSONObject().put(COPY_OF, databaseName).toString());
    return client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
      try {
        return parseAnswer(response.statusCode(), response.body());
      } catch (final IOException e) {
        throw new CompletionException(e);
      }
    });
  }

  /**
   * Asks one peer, expected to be {@code expectedPeerId}, whether it holds {@code databaseName} registered: see
   * {@link #REGISTERED}. Answered from the peer's registry alone, nothing opened or hashed there. Package-private for
   * tests.
   */
  static CompletableFuture<Boolean> askWhetherRegistered(final HttpClient client, final String expectedPeerId,
      final String url, final String databaseName, final String clusterToken) {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(url, clusterToken, ROUND_TIMEOUT_MS,
        new JSONObject().put(COPY_OF, databaseName).toString());
    return client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
      try {
        return parseRegistered(response.statusCode(), response.body(), expectedPeerId, url);
      } catch (final IOException e) {
        throw new CompletionException(e);
      }
    });
  }

  /**
   * The {@link #REGISTERED} member of a peer's answer, refused unless the answer names {@code expectedPeerId} as its
   * author (issue #8658): the address may reach a node other than the one this follower would install from.
   * Package-private for tests.
   */
  static boolean parseRegistered(final int statusCode, final String body, final String expectedPeerId,
      final String url) throws IOException {
    if (statusCode != 200)
      throw new IOException("HTTP " + statusCode);
    final JSONObject json = new JSONObject(body);
    LeaderDatabaseQuery.requireAnsweredBy(json, expectedPeerId, url);
    return json.getBoolean(REGISTERED, false);
  }

  /**
   * A peer's answer. One that predates issue #8605 ignores {@link #COPY_OF} and lists its open databases instead, which
   * says nothing about a closed copy, so an answer without {@link #COPY} is not an answer. Package-private for tests.
   */
  static CopyState parseAnswer(final int statusCode, final String body) throws IOException {
    if (statusCode != 200)
      throw new IOException("HTTP " + statusCode);
    final JSONObject copy = new JSONObject(body).getJSONObject(COPY, null);
    if (copy == null)
      throw new IOException("the server does not report a single database's copy (older version)");
    return CopyState.fromJSON(copy);
  }
}
