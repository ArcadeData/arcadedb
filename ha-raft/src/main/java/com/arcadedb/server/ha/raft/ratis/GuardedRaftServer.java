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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.log.LogManager;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.logging.Level;

/**
 * The {@link RaftServer} the Raft gRPC services are handed instead of the real one: it behaves exactly like it, except
 * for two checks on the inbound server RPCs that Apache Ratis does not make on its own.
 * <ul>
 * <li><b>A request addressed to another peer is refused</b> (issue #10016). Ratis routes an inbound server RPC by its
 * group alone and answers it under its own id, whoever the request was meant for. So when the address a node holds for
 * a peer leads somewhere else - a stopped container's host name that the resolver now answers with the asking node
 * itself, or with another member - the wrong node answers in the right peer's place. Against itself that is fatal: a
 * leader that receives its own {@code AppendEntries} recognises itself as the leader of the current term and steps
 * down, and a candidate that receives its own {@code RequestVote} grants it and counts itself among the OTHER peers'
 * votes, which trips the {@code !others.contains(selfId)} precondition of {@code RaftConfigurationImpl.hasMajority},
 * kills the election, and leaves the cluster with no leader for as long as the name keeps resolving that way. A node
 * that answers only for itself turns all of that into a failed RPC, which every caller already handles.</li>
 * <li><b>A {@code StartLeaderElection} first waits for the commit it was sent to campaign on</b> (issue #8533, see
 * {@link TransfereeCatchUp}).</li>
 * </ul>
 * Both hooks share one {@link Proxy} so the hot {@code appendEntriesAsync} path pays for one reflective dispatch, not
 * one per hook. A request with no target id - nothing Ratis itself sends - is let through, since there is nothing to
 * compare.
 * <p>
 * Wired in by {@link FixedGrpcFactory#newRaftServerRpc(RaftServer)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GuardedRaftServer {

  /**
   * Key under which {@code RaftHAServer} puts its {@link MisdirectedRequestListener} into the Ratis
   * {@link Parameters}, where {@link FixedGrpcFactory} finds it.
   */
  public static final String MISDIRECTED_REQUEST_LISTENER_KEY = "arcadedb.raft.misdirectedRequestListener";

  /**
   * Told about every refused request, after the refusal is decided and before it is sent back. It runs on the gRPC
   * handler thread of the request, so it must not block: anything slow - resolving a name, closing a channel - belongs
   * on an executor of its own. Whatever it throws is logged and does not change the refusal.
   */
  @FunctionalInterface
  public interface MisdirectedRequestListener {
    /**
     * @param rpc         the Ratis method the request was refused on, e.g. {@code requestVote}
     * @param addressedTo the peer the request was meant for
     * @param requestor   the peer that sent it; this node itself when the sender's address for {@code addressedTo}
     *                    leads back to the sender
     */
    void onMisdirectedRequest(String rpc, RaftPeerId addressedTo, RaftPeerId requestor);
  }

  private GuardedRaftServer() {
  }

  /**
   * Returns a {@link RaftServer} that delegates every call to {@code server}, refusing inbound server RPCs addressed to
   * another peer and holding a {@code StartLeaderElection} back as {@link TransfereeCatchUp} describes.
   *
   * @param listener told about each refusal; {@code null} for none
   */
  public static RaftServer wrap(final RaftServer server, final MisdirectedRequestListener listener) {
    final InvocationHandler handler = (proxy, method, args) -> {
      // Every server RPC takes its request as the one argument, and nothing else a RaftServer exposes takes a request
      // proto, so the argument's type is what identifies the call - no method-name lookup on the hot path.
      if (args != null && args.length == 1) {
        final RaftRpcRequestProto header = serverRequestOf(args[0]);
        if (header != null) {
          refuseIfMisdirected(server, method.getName(), header, listener);
          if (args[0] instanceof StartLeaderElectionRequestProto request)
            TransfereeCatchUp.beforeStartLeaderElection(server, request);
        }
      }
      try {
        return method.invoke(server, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    };
    return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[] { RaftServer.class },
        handler);
  }

  /**
   * Throws {@link MisdirectedRaftRequestException} when {@code header} names a target peer other than
   * {@code server}. Every Ratis server RPC declares an {@link IOException}, so throwing it straight out of the
   * proxy reaches the gRPC service as itself, which sends it back to the sender - on the {@code appendEntries} stream
   * too, where a failed future would leave an out-of-order heartbeat with no reply at all.
   */
  static void refuseIfMisdirected(final RaftServer server, final String rpc, final RaftRpcRequestProto header,
      final MisdirectedRequestListener listener) throws MisdirectedRaftRequestException {
    final ByteString addressedTo = header.getReplyId();
    if (addressedTo.isEmpty())
      return;
    final RaftPeerId self = server.getId();
    if (addressedTo.equals(self.toByteString()))
      return;

    final RaftPeerId target = RaftPeerId.valueOf(addressedTo);
    final RaftPeerId requestor = header.getRequestorId().isEmpty() ? null : RaftPeerId.valueOf(header.getRequestorId());
    if (listener != null)
      try {
        listener.onMisdirectedRequest(rpc, target, requestor);
      } catch (final RuntimeException e) {
        LogManager.instance().log(GuardedRaftServer.class, Level.WARNING,
            "Error notifying a misdirected Raft %s addressed to %s: %s", rpc, target, e.toString());
      }
    throw new MisdirectedRaftRequestException(rpc, target, requestor, self);
  }

  /**
   * The header of a Raft server-protocol request, or {@code null} for anything else - a client or admin request,
   * which is routed to the leader by a different rule, or a method that takes no request at all.
   */
  static RaftRpcRequestProto serverRequestOf(final Object argument) {
    if (argument instanceof AppendEntriesRequestProto request)
      return request.getServerRequest();
    if (argument instanceof RequestVoteRequestProto request)
      return request.getServerRequest();
    if (argument instanceof InstallSnapshotRequestProto request)
      return request.getServerRequest();
    if (argument instanceof StartLeaderElectionRequestProto request)
      return request.getServerRequest();
    if (argument instanceof ReadIndexRequestProto request)
      return request.getServerRequest();
    return null;
  }
}
