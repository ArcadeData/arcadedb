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

import java.util.logging.Filter;
import java.util.logging.LogRecord;

/**
 * Suppresses the WARNING, complete with stack trace, that the Apache Ratis gRPC server service logs for every request
 * {@link GuardedRaftServer} refuses as addressed to another peer (issue #10016):
 *
 * <pre>
 * arcade1_2434: Failed APPEND_ENTRIES request arcade1_2434-&gt;arcade2_2434#4296102-t2,...,HEARTBEAT
 * com.arcadedb.server.ha.raft.ratis.MisdirectedRaftRequestException: Raft appendEntriesAsync addressed to ...
 * </pre>
 *
 * <p>While a peer's name resolves to the wrong node, the sender retries every few seconds and each attempt leaves one
 * such record behind. {@code RaftHAServer} already reports the condition once per peer and period, with the address
 * the name resolves to and what was done about it, which is the line an operator can act on; the Ratis records only
 * repeat it with a stack trace that says nothing about the cause.
 *
 * <p>Only a record whose attached throwable is a {@link MisdirectedRaftRequestException} is dropped; every other
 * failure of that logger stays visible. See {@link RatisLogRecordFilter} for what the muting relies on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RatisMisdirectedRequestFilter extends RatisLogRecordFilter {
  /** The Ratis logger that emits the record. Matches the class name Ratis logs under. */
  public static final String RATIS_GRPC_SERVER_PROTOCOL_LOGGER = "org.apache.ratis.grpc.server.GrpcServerProtocolService";

  /** How deep a cause chain is walked; deeper than any real chain, and a guard against a cyclic one. */
  private static final int MAX_CAUSE_DEPTH = 8;

  RatisMisdirectedRequestFilter(final Filter delegate) {
    super(delegate);
  }

  /** Installs the filter on the Ratis logger, chaining to whatever filter it already carried. Idempotent. */
  public static void install() {
    install(RATIS_GRPC_SERVER_PROTOCOL_LOGGER, RatisMisdirectedRequestFilter.class, RatisMisdirectedRequestFilter::new);
  }

  @Override
  protected boolean drops(final LogRecord record) {
    // Ratis logs the refusal as thrown, but a future version that wraps it (a CompletionException on the async path)
    // must not bring the noise back. Bounded, so a cyclic chain cannot hang the logging thread.
    Throwable t = record.getThrown();
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause())
      if (t instanceof MisdirectedRaftRequestException)
        return true;
    return false;
  }
}
