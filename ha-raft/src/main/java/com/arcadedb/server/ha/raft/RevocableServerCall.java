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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.Status;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * A {@link ServerCall} that a third party - {@link PeerTransportSession#closeLiveCalls()} - may terminate while the
 * handler owning it is still running, used to cut the in-flight Raft RPCs of a peer whose transport has just been
 * revoked (issue #7250).
 * <p>
 * The wrapper exists for exactly one reason: {@code ServerCall.close} is single-shot ({@code ServerCallImpl} throws
 * {@code IllegalStateException} on a second call) and the revoking thread is not the thread running the handler, so
 * without a shared latch the two race to close the same call. Every close the handler performs goes through
 * {@link #close(Status, Metadata)} because the handler is handed <i>this</i> object rather than the delegate, so the
 * {@link AtomicBoolean} below arbitrates both directions with one CAS.
 * <p>
 * {@code ServerCall} is documented as not thread-safe, and this class does not make it so: it makes exactly one
 * additional caller safe against the handler, which is what a revocation needs. A close the delegate performs on its
 * own - the gRPC runtime aborting the stream, say - bypasses the latch, so {@link #closeAsRevoked()} still guards its
 * delegate call with a try/catch rather than trusting the CAS to be the whole story.
 */
final class RevocableServerCall<Q, R> extends ForwardingServerCall.SimpleForwardingServerCall<Q, R> {

  private final AtomicBoolean closed = new AtomicBoolean();

  RevocableServerCall(final ServerCall<Q, R> delegate) {
    super(delegate);
  }

  @Override
  public void close(final Status status, final Metadata trailers) {
    if (closed.compareAndSet(false, true))
      super.close(status, trailers);
  }

  /**
   * Closes this call on behalf of a revocation, unless it is already closed.
   *
   * @return true when this invocation is the one that closed the call
   */
  boolean closeAsRevoked() {
    if (!closed.compareAndSet(false, true))
      return false;
    try {
      super.close(PeerTransportSession.REVOKED, new Metadata());
      return true;
    } catch (final RuntimeException e) {
      // The delegate was already finished by the gRPC runtime without passing through close(Status, Metadata)
      // above. Nothing to revoke, and nothing an operator can act on: the RPC is over either way.
      LogManager.instance().log(this, Level.FINE,
          "Raft gRPC call was already terminated when its transport was revoked: %s", e.toString());
      return false;
    }
  }

  /** Whether this call has been closed through this wrapper. Exposed for testing. */
  boolean isClosed() {
    return closed.get();
  }
}
