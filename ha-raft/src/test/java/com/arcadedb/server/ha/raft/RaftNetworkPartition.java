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

import com.arcadedb.log.LogManager;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.CodeInjectionForTesting;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * Cuts one in-process Raft peer off from the rest of a {@link BaseRaftHATest} cluster, and holds or drops the client
 * requests its own Raft client sends to the other peers (issue #9934).
 * <p>
 * Every server of the fixture runs in this JVM, so the cut is made where Ratis hands a request to its server code,
 * through Ratis's own {@link CodeInjectionForTesting} points, rather than on the network:
 * <ul>
 *   <li>the <b>server-to-server</b> traffic of the isolated peer, both ways ({@code appendEntries},
 *       {@code requestVote}, {@code installSnapshot}, {@code startLeaderElection}), through Ratis's
 *       {@link BlockRequestHandlingInjection}: a request is parked until the partition heals, then answered as the
 *       stale request it has become. The isolated leader keeps appending entries it can never replicate, steps down
 *       once it has not heard from a majority for an election timeout, and the rest of the cluster elects a leader of
 *       its own;</li>
 *   <li>the <b>client</b> traffic of the isolated peer's Raft client, which the server-to-server cut does not reach.
 *       Ratis's client retries a request its leader failed with {@code NotLeaderException} against the other peers,
 *       long after ArcadeDB's own wait for it gave up, so without this the cluster on the other side of the cut
 *       commits the entry the isolated leader could not: a client request is matched at
 *       {@code RaftServerImpl.appendTransaction}, on any peer but the isolated one, and is either parked until
 *       {@link #releaseClientRequests()} lets it through - a request delayed in flight - or refused for good
 *       ({@link #isolateDroppingClientRequests}, and every request still parked at {@link #close()}).</li>
 * </ul>
 * Both injection points are JVM-wide singletons, so {@link #close()} must run after the test whatever its outcome, and
 * the tests that use this class must not run concurrently in one JVM (ha-raft ITs run serially in one fork).
 * {@link #isolate} resets any block a test that did not close its partition left behind, and {@link #resetAll()}
 * clears both injections when no partition object is at hand.
 */
final class RaftNetworkPartition implements AutoCloseable {

  /** Package-private in Ratis, hence named rather than imported. */
  private static final String RATIS_SERVER_IMPL = "org.apache.ratis.server.impl.RaftServerImpl";

  /**
   * {@code RaftServerImpl.APPEND_TRANSACTION}, the point a leader appends a client request. Package-private in Ratis, so
   * it is read reflectively rather than copied: a Ratis upgrade that renames it fails here, at once, rather than as a
   * timed-out wait for a request nothing intercepted.
   */
  static final String APPEND_TRANSACTION = readRatisInjectionPoint("APPEND_TRANSACTION");

  private enum ClientTraffic {PASS, HOLD, DROP}

  private final    String                isolatedPeerId;
  private final    Predicate<ByteString> intercepted;
  private final    AtomicInteger         held    = new AtomicInteger();
  private final    AtomicInteger         dropped = new AtomicInteger();
  private volatile ClientTraffic         clientTraffic;

  /**
   * @param isolatedPeerId the Raft peer id cut off, as {@link BaseRaftHATest#peerIdForIndex(int)} spells it
   * @param intercepted    which client requests reaching another peer to hold or drop, by their log-entry payload
   */
  private RaftNetworkPartition(final String isolatedPeerId, final Predicate<ByteString> intercepted) {
    this.isolatedPeerId = isolatedPeerId;
    this.intercepted = intercepted;
  }

  /**
   * Isolates {@code isolatedPeerId} now and parks every {@code intercepted} client request that reaches another peer
   * until {@link #releaseClientRequests()}, or refuses them at {@link #close()}.
   */
  static RaftNetworkPartition isolateHoldingClientRequests(final String isolatedPeerId,
      final Predicate<ByteString> intercepted) {
    return isolate(isolatedPeerId, intercepted, ClientTraffic.HOLD);
  }

  /** Isolates {@code isolatedPeerId} now and refuses every {@code intercepted} client request that reaches another peer. */
  static RaftNetworkPartition isolateDroppingClientRequests(final String isolatedPeerId,
      final Predicate<ByteString> intercepted) {
    return isolate(isolatedPeerId, intercepted, ClientTraffic.DROP);
  }

  private static RaftNetworkPartition isolate(final String isolatedPeerId, final Predicate<ByteString> intercepted,
      final ClientTraffic clientTraffic) {
    final RaftNetworkPartition partition = new RaftNetworkPartition(isolatedPeerId, intercepted);
    partition.clientTraffic = clientTraffic;
    CodeInjectionForTesting.put(APPEND_TRANSACTION, partition::onAppendTransaction);
    final BlockRequestHandlingInjection serverTraffic = BlockRequestHandlingInjection.getInstance();
    serverTraffic.unblockAll();
    // Nobody handles what the isolated peer sends, and the isolated peer handles nothing anybody sends
    serverTraffic.blockRequestor(isolatedPeerId);
    serverTraffic.blockReplier(isolatedPeerId);
    LogManager.instance().log(RaftNetworkPartition.class, Level.INFO, "TEST: isolated Raft peer %s (client requests: %s)",
        isolatedPeerId, clientTraffic);
    return partition;
  }

  private static String readRatisInjectionPoint(final String fieldName) {
    try {
      final Field field = Class.forName(RATIS_SERVER_IMPL).getDeclaredField(fieldName);
      field.setAccessible(true);
      return (String) field.get(null);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("Ratis no longer declares " + RATIS_SERVER_IMPL + "." + fieldName
          + ": RaftNetworkPartition cannot intercept client requests on this Ratis version", e);
    }
  }

  /** Reconnects the isolated peer's server-to-server traffic. Client requests keep their current treatment. */
  void healServerTraffic() {
    BlockRequestHandlingInjection.getInstance().unblockAll();
    LogManager.instance().log(this, Level.INFO, "TEST: healed the server traffic of Raft peer %s", isolatedPeerId);
  }

  /** Lets every parked and every later intercepted client request through: a request delayed in flight arrives. */
  void releaseClientRequests() {
    clientTraffic = ClientTraffic.PASS;
  }

  /** How many intercepted client requests were parked so far. */
  int heldClientRequests() {
    return held.get();
  }

  /** How many intercepted client requests were refused so far. */
  int droppedClientRequests() {
    return dropped.get();
  }

  /** Clears every block and the client-request interception, whatever partition installed them. */
  static void resetAll() {
    BlockRequestHandlingInjection.getInstance().unblockAll();
    CodeInjectionForTesting.remove(APPEND_TRANSACTION);
  }

  @Override
  public void close() {
    // Parked requests are refused, not let through: nothing the test held may commit behind its back
    clientTraffic = ClientTraffic.DROP;
    healServerTraffic();
    CodeInjectionForTesting.remove(APPEND_TRANSACTION);
  }

  /**
   * {@code execute(localId, clientId, request, context, cacheEntry)}, run by the peer before it appends a client request.
   * Ratis ignores the return value at this point (it only reports whether injected code ran), so returning lets the
   * request take its normal path; throwing is the only way to refuse it.
   */
  private boolean onAppendTransaction(final Object localId, final Object clientId, final Object... args) {
    if (isolatedPeerId.equals(String.valueOf(localId)) || args.length == 0
        || !(args[0] instanceof RaftClientRequest request) || !request.is(TypeCase.WRITE)
        || !intercepted.test(request.getMessage().getContent()))
      return false;

    if (clientTraffic == ClientTraffic.HOLD) {
      held.incrementAndGet();
      LogManager.instance().log(this, Level.INFO, "TEST: %s holds client request %s from %s", localId, request.getCallId(),
          clientId);
      try {
        while (clientTraffic == ClientTraffic.HOLD)
          Thread.sleep(50);
      } catch (final InterruptedException e) {
        // Only a server stopping interrupts this thread, i.e. teardown: refuse every parked request, not only this one,
        // so none commits on a cluster going down
        Thread.currentThread().interrupt();
        clientTraffic = ClientTraffic.DROP;
      }
    }

    if (clientTraffic == ClientTraffic.DROP) {
      dropped.incrementAndGet();
      LogManager.instance().log(this, Level.INFO, "TEST: %s drops client request %s from %s", localId, request.getCallId(),
          clientId);
      throw new IllegalStateException("TEST: client request dropped by the network partition (issue #9934)");
    }
    LogManager.instance().log(this, Level.INFO, "TEST: %s lets client request %s from %s through", localId,
        request.getCallId(), clientId);
    return true;
  }
}
