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

import com.arcadedb.exception.ConcurrentModificationException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Ratis client's SEVERE for an entry the leader refused before appending it (issue #6965) is dropped; every other
 * failure of the same logger stays visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RatisRefusedEntryErrorFilterTest {

  @Test
  void dropsOnlyTheRefusedEntryRecord() {
    final RatisRefusedEntryErrorFilter filter = new RatisRefusedEntryErrorFilter(null);

    final LogRecord refused = new LogRecord(Level.SEVERE, "Failed to send request, message=Message:0100(size=97)");
    refused.setThrown(new CompletionException(new StateMachineException("conflict",
        new ConcurrentModificationException("Concurrent modification on page 3/0"), false)));
    assertThat(filter.isLoggable(refused)).as("a pre-append refusal is an ordinary retryable conflict").isFalse();

    final LogRecord otherStateMachineFailure = new LogRecord(Level.SEVERE, "Failed to send request");
    otherStateMachineFailure.setThrown(new CompletionException(new StateMachineException("boom", new IOException("disk"), true)));
    assertThat(filter.isLoggable(otherStateMachineFailure)).as("a genuine state machine failure stays visible").isTrue();

    final LogRecord transportFailure = new LogRecord(Level.SEVERE, "Failed to send request");
    transportFailure.setThrown(new CompletionException(new IOException("connection reset")));
    assertThat(filter.isLoggable(transportFailure)).isTrue();

    assertThat(filter.isLoggable(new LogRecord(Level.SEVERE, "Failed to send request"))).as("no throwable at all").isTrue();
  }

  @Test
  void chainsToTheFilterTheLoggerAlreadyCarried() {
    final Filter rejectAll = record -> false;
    final RatisRefusedEntryErrorFilter filter = new RatisRefusedEntryErrorFilter(rejectAll);
    assertThat(filter.isLoggable(new LogRecord(Level.SEVERE, "anything"))).isFalse();
    assertThat(filter.getDelegate()).isSameAs(rejectAll);
  }

  /** The filter is keyed to a Ratis-internal class name: a Ratis bump that renames it must turn this red. */
  @Test
  void theLoggerNameIsAClassOfTheShippedRatis() throws ClassNotFoundException {
    assertThat(Class.forName(RatisRefusedEntryErrorFilter.RATIS_ORDERED_ASYNC_LOGGER)).isNotNull();
  }

  @Test
  void installIsIdempotent() {
    RatisRefusedEntryErrorFilter.install();
    final Logger logger = Logger.getLogger(RatisRefusedEntryErrorFilter.RATIS_ORDERED_ASYNC_LOGGER);
    final Filter installed = logger.getFilter();
    assertThat(installed).isInstanceOf(RatisRefusedEntryErrorFilter.class);

    RatisRefusedEntryErrorFilter.install();
    assertThat(logger.getFilter()).as("a second install must not stack filters").isSameAs(installed);
  }
}
