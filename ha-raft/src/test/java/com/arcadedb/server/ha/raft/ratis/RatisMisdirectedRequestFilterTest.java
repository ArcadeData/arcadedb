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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Ratis gRPC service's per-request WARNING for a request refused as addressed to another peer (issue #10016) is
 * dropped; every other failure of the same logger stays visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RatisMisdirectedRequestFilterTest {

  @Test
  void dropsOnlyTheMisdirectedRequestRecord() {
    final RatisMisdirectedRequestFilter filter = new RatisMisdirectedRequestFilter(null);

    final LogRecord refused = new LogRecord(Level.WARNING, "n1: Failed APPEND_ENTRIES request n1->n2#7-t2,HEARTBEAT");
    refused.setThrown(new MisdirectedRaftRequestException("refused"));
    assertThat(filter.isLoggable(refused)).as("the refusal is reported by RaftHAServer, once per period").isFalse();

    final LogRecord wrapped = new LogRecord(Level.WARNING, "n1: Failed APPEND_ENTRIES request");
    wrapped.setThrown(new CompletionException(new MisdirectedRaftRequestException("refused")));
    assertThat(filter.isLoggable(wrapped)).as("a wrapped refusal is the same refusal").isFalse();

    final LogRecord transportFailure = new LogRecord(Level.WARNING, "n1: Failed APPEND_ENTRIES request");
    transportFailure.setThrown(new IOException("connection reset"));
    assertThat(filter.isLoggable(transportFailure)).isTrue();

    assertThat(filter.isLoggable(new LogRecord(Level.WARNING, "n1: appendEntries onError"))).as("no throwable at all")
        .isTrue();
  }

  /** A cyclic cause chain must not hang the logging thread. */
  @Test
  void aCyclicCauseChainIsWalkedToTheBound() {
    final RuntimeException a = new RuntimeException("a");
    final RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    final LogRecord record = new LogRecord(Level.WARNING, "cyclic");
    record.setThrown(a);
    assertThat(new RatisMisdirectedRequestFilter(null).isLoggable(record)).isTrue();
  }

  @Test
  void chainsToTheFilterTheLoggerAlreadyCarried() {
    final Filter rejectAll = record -> false;
    final RatisMisdirectedRequestFilter filter = new RatisMisdirectedRequestFilter(rejectAll);
    assertThat(filter.isLoggable(new LogRecord(Level.WARNING, "anything"))).isFalse();
    assertThat(filter.getDelegate()).isSameAs(rejectAll);
  }

  /** The filter is keyed to a Ratis-internal class name: a Ratis bump that renames it must turn this red. */
  @Test
  void theLoggerNameIsAClassOfTheShippedRatis() throws ClassNotFoundException {
    assertThat(Class.forName(RatisMisdirectedRequestFilter.RATIS_GRPC_SERVER_PROTOCOL_LOGGER)).isNotNull();
  }

  @Test
  void installIsIdempotent() {
    RatisMisdirectedRequestFilter.install();
    final Logger logger = Logger.getLogger(RatisMisdirectedRequestFilter.RATIS_GRPC_SERVER_PROTOCOL_LOGGER);
    final Filter installed = logger.getFilter();
    assertThat(installed).isInstanceOf(RatisMisdirectedRequestFilter.class);

    RatisMisdirectedRequestFilter.install();
    assertThat(logger.getFilter()).as("a second install must not stack filters").isSameAs(installed);
  }
}
