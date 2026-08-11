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
import io.grpc.protobuf.ProtoUtils;

/**
 * Wire constants of the {@code GraphBatchLoad} RPC that both ends have to agree on. They live in the module that
 * owns the protocol definition rather than on either side of it, so the server writing a trailer and the client
 * reading it cannot drift apart (issue #6070).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GraphBatchProtocol {

  /**
   * Carries the partial-commit counters of a failed {@code GraphBatchLoad} on the trailers of the failed call.
   * A gRPC error ends the stream with a status and no message, but the batch commits incrementally, so a load
   * that failed part-way is not rolled back: without this the caller would be told that the load failed and
   * nothing about how much of it is durable, leaving re-sending everything as the only safe option.
   */
  public static final Metadata.Key<GraphBatchResult> RESULT_TRAILER = Metadata.Key.of(
      "arcadedb-graph-batch-result-bin", ProtoUtils.metadataMarshaller(GraphBatchResult.getDefaultInstance()));

  private GraphBatchProtocol() {
  }
}
