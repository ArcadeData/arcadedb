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
package com.arcadedb.server.grpc;

import io.grpc.Metadata;

/**
 * Wire constants of the externally-managed transaction that both ends have to agree on. They live in the module
 * that owns the protocol definition rather than on either side of it, so the server writing a trailer and the
 * client reading it cannot drift apart - the same arrangement {@link GraphBatchProtocol} makes for
 * {@code GraphBatchLoad} (issue #6070).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TransactionProtocol {

  /**
   * Says that the client-managed transaction this call ran in published a COMMIT while the call was executing,
   * so part of the caller's block is already durable (issue #8134, the gRPC half of issue #8062).
   * <p>
   * A statement with an explicit batch boundary - {@code UPDATE/DELETE/MOVE VERTEX ... BATCH n} - calls
   * {@code commit(); begin();} in the MIDDLE of the transaction and re-begins straight afterwards, so from the
   * client's side the transaction looks exactly as it did going in. The retry loop in
   * {@code RemoteDatabase.transaction()}, which {@code RemoteGrpcDatabase} inherits, cannot see it from anything
   * it holds: its entire view of the transaction is the transaction id, and that does not change. Without this
   * signal it re-runs a block whose earlier half is already on disk and then reports clean success.
   * <p>
   * A TRAILER rather than a field on the response message, because the guard is consulted exactly when something
   * went wrong and a gRPC error ends the call with a status and no message at all - the same reason
   * {@link GraphBatchProtocol#RESULT_TRAILER} carries the batch counters. Trailers are sent on every close, so
   * one mechanism covers the failing call and the successful one that preceded it alike.
   * <p>
   * Only ever sent with the value {@code "true"}; its ABSENCE is the negative. It is additive: a client that
   * ignores it behaves as it did before, and an older server that never sends it leaves the client's guard off,
   * which is the behaviour issue #8134 describes.
   * <p>
   * Deliberately the same name as the HTTP response header
   * {@code RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT} / {@code DatabaseAbstractHandler.SESSION_PARTIAL_COMMIT}:
   * the three spell one contract on three wires, and a reader who has met one has met all of them.
   */
  public static final Metadata.Key<String> SESSION_PARTIAL_COMMIT_TRAILER = Metadata.Key.of(
      "arcadedb-session-partial-commit", Metadata.ASCII_STRING_MARSHALLER);

  /** The only value {@link #SESSION_PARTIAL_COMMIT_TRAILER} is ever sent with. */
  public static final String SESSION_PARTIAL_COMMIT_VALUE = "true";

  private TransactionProtocol() {
  }
}
