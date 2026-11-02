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

import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11059, follow-up: the chunk that opens a load's running-query entry enters before there is an entry to bind, so its
 * rows and the commit they reach ran unbound, and a termination arriving meanwhile was seen only by the next chunk. The entry is
 * now bound on the opening chunk's thread from the moment it is opened, and unbound with that chunk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11059StreamingLoadFirstChunkBindingTest {

  @Test
  void theChunkThatOpensTheEntryRunsBound() {
    final RunningQueryRegistry registry = new RunningQueryRegistry();
    final StreamingLoadEntry entry = new StreamingLoadEntry(registry, "TimeSeriesWriteStream");

    final StreamingLoadEntry.Bound first = entry.enter();
    try {
      assertThat(RunningQuery.current()).as("nothing to bind before the entry is opened").isNull();
      entry.open("db", "root", null, () -> "TimeSeriesWriteStream", () -> {
      });
      assertThat(RunningQuery.current()).as("the rest of the opening chunk runs bound").isNotNull();
      assertThat(RunningQuery.current().getDatabase()).isEqualTo("db");
    } finally {
      first.close();
    }
    assertThat(RunningQuery.current()).as("unbound with the chunk").isNull();

    // A later chunk binds it as before
    final StreamingLoadEntry.Bound second = entry.enter();
    try {
      assertThat(RunningQuery.current()).isNotNull();
    } finally {
      second.close();
    }
    assertThat(RunningQuery.current()).isNull();
    entry.close();
  }

  @Test
  void anEntryOpenedOutsideAChunkIsNotBoundOnTheOpeningThread() {
    // The bidirectional insert opens its entry from a message that took no working lock: nothing would close such a binding
    final StreamingLoadEntry entry = new StreamingLoadEntry(new RunningQueryRegistry(), "InsertBidirectional");
    entry.open("db", "root", null, () -> "InsertBidirectional", () -> {
    });
    assertThat(RunningQuery.current()).isNull();
    entry.close();
  }
}
