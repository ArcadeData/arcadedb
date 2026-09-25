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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.handler.LeaderDial;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.server.security.ServerSecurity;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Asks the leader to seed the cluster security documents, and reads back what it could not commit
 * (issues #7833 and #7834). The server half is {@link PostSecuritySeedHandler}, which explains why there is one
 * endpoint for the two callers.
 * <p>
 * <b>The local short circuit is not an optimisation.</b> When this node IS the leader there is no dial at all:
 * the seeder is right here, and going out over HTTP to reach it would make the one thing this class exists to
 * establish - that a seed runs on the leader, in one JVM, under one {@code ServerSecurity} monitor - depend on
 * a socket to ourselves.
 * <p>
 * <b>Transport</b> is the one every other peer-to-peer dial in this module uses, through {@link LeaderDial}:
 * the leader's HTTPS endpoint when {@code arcadedb.ssl.enabled} is set and one resolves, the plain listener
 * otherwise, with the {@code X-ArcadeDB-Cluster-Token} + {@code X-ArcadeDB-Forwarded-User} pair as the
 * credential. It carries no security document in either direction - a request sends fingerprints, an answer
 * sends document NAMES - so the documents themselves travel only as replicated Raft entries, exactly as every
 * other security change does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ClusterSecuritySeedQuery {

  /**
   * How much longer than the seed's own retry budget a caller waits for the answer.
   * <p>
   * The seed retries the documents that fail for {@code arcadedb.ha.securitySeedRetryTimeout}; a deadline
   * shorter than that would time out callers while the seed was still doing exactly what it was configured to
   * do, and report a failure the cluster had not had. This is the margin on top: the Raft round trip of the
   * last attempt, plus the request itself.
   */
  private static final long SEED_REPORT_MARGIN_MS = 30_000L;

  /**
   * Attempts a request whose only failure is "you are not the leader any more" (issue #7834).
   * <p>
   * The address this dials was resolved a moment earlier, so a 409 is an election that landed in between - a
   * transient condition, and one that resolves into a different address rather than the same one succeeding.
   * Every attempt is a fresh resolve and a round trip, so the count stays small; what makes the WAIT long
   * enough is {@link #notLeaderBackoffMs}, not the count.
   */
  private static final int NOT_LEADER_ATTEMPTS = 3;
  /**
   * Floor for the wait between those attempts, for a configuration that names no election timeout.
   * <p>
   * The wait itself is derived from {@code arcadedb.ha.electionTimeoutMax} rather than fixed
   * (code review on PR #7854). A fixed 500ms gave the whole retry ~1.5s, while that timeout defaults to
   * <b>10 seconds</b>: an election that took as long as the cluster is configured to allow would outlast the
   * retry, and the admitting node would then report a failed seed to an operator for an admission whose seed
   * was about to succeed. Since issue #7521 that report is a 503 with a {@code failedSeeds} array, so a
   * premature one is not cosmetic - it asks an operator to chase a peer that is converging.
   */
  private static final long NOT_LEADER_MIN_BACKOFF_MS = 500L;
  /** The wait between attempts when what failed was the socket rather than the leader's identity. */
  private static final long TRANSIENT_BACKOFF_MS      = 500L;

  /**
   * The plain-HTTP client every dial from this class shares, built once.
   * <p>
   * {@code LeaderDial.newConnectTimeoutBoundedClient}'s own javadoc says a caller builds its client once
   * rather than per request, and every other site in the module holds one in a field
   * ({@code RaftHAServer.forwardHttpClient}, {@code PostBatchHandler}, {@code RaftReplicatedDatabase}). This
   * class is a stateless utility, so it rebuilt one per attempt - and each build starts a selector thread that
   * nothing closes, on a path that runs on every node restart and every snapshot install
   * (code review on PR #7854, the leak PR #7650 fixed at those three sites).
   * <p>
   * A static with a fixed connect timeout, the shape {@code LeaderDatabaseQuery} uses for the same reason: the
   * timeout is a property of a built client and cannot follow configuration afterwards, and this dial is an
   * administrative round trip rather than one whose latency an operator tunes.
   */
  private static final HttpClient PLAIN_HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private ClusterSecuritySeedQuery() {
  }

  /**
   * What the leader answered a seed request with.
   *
   * @param upToDate    {@code true} only when the leader compared the caller's fingerprints against its own live
   *                    documents, found all three equal and submitted nothing (issue #8346). That is the one answer
   *                    that proves the caller's documents are the cluster's at the moment of the comparison; a
   *                    SEEDED document converges only once the caller applies the seed entry, and an answer that does
   *                    not carry the field - an admission, a leader that predates it, the leader's own local seed -
   *                    says nothing about a match
   * @param failedSeeds the names of the documents that could not be seeded, empty when all of them committed or when
   *                    nothing needed seeding
   */
  public record SeedAnswer(boolean upToDate, List<String> failedSeeds) {
  }

  /**
   * The deadline a seed request is given, derived from the seed's own retry budget so the two cannot drift
   * apart. See {@link #SEED_REPORT_MARGIN_MS}.
   */
  public static long reportTimeoutMs(final ContextConfiguration configuration) {
    return Math.max(0L, configuration.getValueAsLong(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT))
        + SEED_REPORT_MARGIN_MS;
  }

  /**
   * How long to wait for an election to name a new leader before re-resolving, taken from the cluster's own
   * {@code arcadedb.ha.electionTimeoutMax} so the two cannot drift apart: a deployment that widens its election
   * timeout for a WAN link or a bulk-load workload widens this with it, without a second setting to remember.
   */
  private static long notLeaderBackoffMs(final ContextConfiguration configuration) {
    return Math.max(NOT_LEADER_MIN_BACKOFF_MS,
        configuration.getValueAsLong(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));
  }

  /**
   * Seeds for an admission: the peer named by {@code admittedPeer} has just become a committed member, and the
   * node that admitted it has an operator waiting for the result (issue #7834).
   * <p>
   * No fingerprints, because the admitting node does not hold the joining peer's documents and has nothing to
   * compare - every admission is seeded.
   *
   * @return the names of the documents that could not be seeded, empty when all of them committed
   *
   * @throws IOException when the leader could not be reached or did not answer the seed's outcome, which the
   *                     caller reports rather than swallowing: a join whose seed outcome is unknown is not a
   *                     join whose seed succeeded
   */
  public static List<String> seedForAdmission(final ArcadeDBServer server, final RaftHAPlugin plugin,
      final String admittedPeer) throws IOException {
    return seed(server, plugin, "the admission of peer '" + admittedPeer + "'", null, false).failedSeeds();
  }

  /**
   * Seeds this node back into step after it rejoined without a membership change (issue #7833), sending the
   * fingerprints of the documents it holds so the leader can answer without submitting anything when they
   * already match.
   *
   * @return the names of the documents that could not be seeded, empty when all of them committed - or when the
   * leader answered that this node is already up to date
   */
  public static List<String> seedForCatchUp(final ArcadeDBServer server, final RaftHAPlugin plugin,
      final String reason) throws IOException {
    return seedForCatchUpAnswer(server, plugin, reason).failedSeeds();
  }

  /**
   * {@link #seedForCatchUp} with the whole answer, so the caller can tell a leader that found this node already in
   * step from one that seeded it (issue #8346). The fingerprints are read inside this call, so a caller that wants to
   * say which applied index the compared documents reflect reads that index BEFORE calling.
   */
  public static SeedAnswer seedForCatchUpAnswer(final ArcadeDBServer server, final RaftHAPlugin plugin,
      final String reason) throws IOException {
    return seed(server, plugin, reason, localFingerprints(server.getSecurity()), true);
  }

  /** The three digests the leader compares against its own; {@code null} when there is no security store. */
  private static JSONObject localFingerprints(final ServerSecurity security) {
    if (security == null)
      return null;
    return new JSONObject()
        .put(ReplicatedSecurityFingerprintRepository.USERS, security.usersFingerprint())
        .put(ReplicatedSecurityFingerprintRepository.GROUPS, security.groupsFingerprint())
        .put(ReplicatedSecurityFingerprintRepository.API_TOKENS, security.apiTokensFingerprint());
  }

  private static SeedAnswer seed(final ArcadeDBServer server, final RaftHAPlugin plugin, final String reason,
      final JSONObject fingerprints, final boolean catchUp) throws IOException {
    for (int attempt = 1; ; attempt++) {
      try {
        return seedOnce(server, plugin, reason, fingerprints, catchUp);
      } catch (final NotLeaderException e) {
        if (attempt >= NOT_LEADER_ATTEMPTS)
          throw new IOException("the security seed could not be requested: " + e.getMessage());
        LogManager.instance().log(ClusterSecuritySeedQuery.class, Level.FINE,
            "The node dialled for the security seed is no longer the leader; re-resolving (attempt %d of %d)",
            attempt, NOT_LEADER_ATTEMPTS);
        backOff(notLeaderBackoffMs(server.getConfiguration()));
      } catch (final IOException e) {
        // A refused connection, a reset, a timeout: the leader address was resolvable a moment ago and the
        // condition may clear before the next attempt (code review on PR #7854). The seed this replaced ran
        // through a RaftClient with a retry policy of its own, so failing an admission on the first blip would
        // be a step back from what #7521 established - a transient failure must not read as a failed seed.
        //
        // Bounded by the same attempt count, and with a short wait rather than the election-sized one: what is
        // being waited out here is a socket, not a leader election.
        if (attempt >= NOT_LEADER_ATTEMPTS)
          throw e;
        LogManager.instance().log(ClusterSecuritySeedQuery.class, Level.FINE,
            "The security seed request to the leader failed (%s); retrying (attempt %d of %d)", e.getMessage(),
            attempt, NOT_LEADER_ATTEMPTS);
        backOff(TRANSIENT_BACKOFF_MS);
      }
    }
  }

  /** Sleeps between attempts, turning an interrupt into the failure this class reports. */
  private static void backOff(final long millis) throws IOException {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while waiting to re-request the cluster security seed", interrupted);
    }
  }

  /**
   * One attempt: resolve the leader, and either seed here or ask it.
   *
   * @param catchUp whether this is a node repairing ITSELF (issue #7833) rather than an admission reporting for
   *                a peer. Carried explicitly rather than inferred from {@code fingerprints == null}, because a
   *                catch-up on a node with no security store has no fingerprints to send and would otherwise be
   *                read as an admission - and answered by a recently completed seed it did not cause, which is
   *                the reuse hole this distinction exists to keep shut (CodeRabbit on PR #7854)
   */
  private static SeedAnswer seedOnce(final ArcadeDBServer server, final RaftHAPlugin plugin, final String reason,
      final JSONObject fingerprints, final boolean catchUp) throws IOException {
    final RaftHAServer raft = plugin.getRaftHAServer();
    if (raft == null)
      throw new IOException("Raft HA is not started on this node, so no security seed can be requested");

    if (plugin.isLeader())
      // No dial: the seeder is in this JVM. See the class note - this is the invariant, not a shortcut.
      // An admission may be answered by the membership change's own seed; a catch-up is repairing this node and
      // must actually seed.
      // Never an up-to-date answer: this path always seeds, it compares nothing.
      return new SeedAnswer(false, raft.getStateMachine().seedSecurityNowAndReport(reason,
          reportTimeoutMs(server.getConfiguration()), !catchUp));

    final LeaderDial dial = LeaderDial.resolve(plugin, PLAIN_HTTP);
    if (dial == null)
      throw new IOException("the cluster leader address is unknown, so the security seed cannot be requested");
    if (dial.refused())
      throw new IOException("the security seed cannot be requested from the leader: " + dial.refusal());
    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL) && !dial.https())
      PlainHttpFallbackNotice.sayOnce(ClusterSecuritySeedQuery.class, "requesting the cluster security seed");

    final JSONObject body = new JSONObject().put("reason", reason);
    // The request TYPE, not a consequence of what else the body happens to carry. A catch-up on a node with no
    // security store sends no fingerprints, and inferring the type from their absence read it as an admission.
    if (catchUp)
      body.put("catchUp", true);
    if (fingerprints != null)
      body.put("fingerprints", fingerprints);

    final long timeoutMs = reportTimeoutMs(server.getConfiguration());
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(dial.url(PostSecuritySeedHandler.ROUTE)))
        .timeout(Duration.ofMillis(Math.max(timeoutMs, LeaderDial.MIN_FORWARD_TIMEOUT_MS)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8));

    PeerCredentials.attach(builder, raft.getClusterToken());

    try {
      return parseAnswer(dial.client().send(builder.build(), HttpResponse.BodyHandlers.ofString()), dial.address());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while requesting the cluster security seed from the leader", e);
    }
  }

  /**
   * The documents the leader reported as not committed.
   * <p>
   * Package-private so the answers this has to tell apart can be pinned without a live leader to produce them
   * (code review on PR #7854).
   * <p>
   * A 503 carrying a {@code failedSeeds} array is the seed's own partial failure and is returned as such, so
   * the caller reports the same list whether it ran the seed itself or asked for it. Every other non-2xx is an
   * {@link IOException}: the outcome is unknown, and "unknown" must not be reported as "nothing failed".
   */
  // @VisibleForTesting
  static List<String> parse(final HttpResponse<String> response, final String address) throws IOException {
    return parseAnswer(response, address).failedSeeds();
  }

  /**
   * {@link #parse} with the whole answer: whether the leader found the caller already in step, as well as what it
   * could not commit (issue #8346). Only a 200 can carry {@code upToDate}; a partial failure is never a match.
   */
  // @VisibleForTesting
  static SeedAnswer parseAnswer(final HttpResponse<String> response, final String address) throws IOException {
    final JSONObject json;
    try {
      json = new JSONObject(response.body());
    } catch (final RuntimeException e) {
      throw new IOException("the leader at " + address + " answered the security seed with HTTP "
          + response.statusCode() + " and a body that is not JSON", e);
    }

    if (response.statusCode() == 409)
      // The node this resolved to is no longer the leader. Retried with a fresh resolve rather than reported.
      throw new NotLeaderException("the node at " + address + " is no longer the Raft leader");

    if (response.statusCode() == 200) {
      final boolean upToDate = json.getBoolean("upToDate", false);
      if (upToDate)
        LogManager.instance().log(ClusterSecuritySeedQuery.class, Level.FINE,
            "The leader at %s reports this node already holds every cluster security document", address);
      return new SeedAnswer(upToDate, List.of());
    }

    if (response.statusCode() == 503 && json.has("failedSeeds")) {
      final JSONArray failed = json.getJSONArray("failedSeeds");
      final List<String> names = new ArrayList<>(failed.length());
      for (int i = 0; i < failed.length(); i++)
        names.add(String.valueOf(failed.get(i)));
      // An EMPTY list on a 503 deliberately falls through to the throw below rather than being returned
      // (code review on PR #7854). The route answers 503 with names when it knows which documents failed and
      // with an `error` when it does not; a 503 naming nothing is neither, and returning it as "no failures"
      // would tell an admitting node that everything committed. Absent and empty are the same answer here -
      // "this response does not say what failed" - and both have to be raised.
      if (!names.isEmpty())
        return new SeedAnswer(false, names);
    }

    throw new IOException("the leader at " + address + " answered the security seed with HTTP "
        + response.statusCode() + ": " + json.getString("error", response.body()));
  }

  /** The one answer worth re-resolving for, rather than reporting. Never leaves this class. */
  private static final class NotLeaderException extends IOException {
    private NotLeaderException(final String message) {
      super(message);
    }
  }
}
