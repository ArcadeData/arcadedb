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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins, without a cluster, that the read-your-writes bookmark comes from {@link HAReplicatedDatabase#getCommitIndexBookmark()}
 * rather than the bare applied index, that an existing header is kept, and that a failing database never throws (issue #11740).
 */
class CommitIndexBookmarkEmitTest {
  private static final String HEADER = "X-ArcadeDB-Commit-Index";

  private static class FakeHaDatabase implements HAReplicatedDatabase {
    private final long applied;
    private final long bookmark;
    private final boolean fail;

    FakeHaDatabase(final long applied, final long bookmark, final boolean fail) {
      this.applied = applied;
      this.bookmark = bookmark;
      this.fail = fail;
    }

    @Override
    public void createInReplicas() {
    }

    @Override
    public void dropInReplicas() {
    }

    @Override
    public HAServerPlugin.QUORUM getQuorum() {
      return null;
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public String getLeaderHttpAddress() {
      return null;
    }

    @Override
    public long getLastAppliedIndex() {
      return applied;
    }

    @Override
    public long getCommitIndexBookmark() {
      if (fail)
        throw new IllegalStateException("boom");
      return bookmark;
    }
  }

  @Test
  void headerCarriesTheBookmarkNotTheAppliedIndex() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    AbstractServerHttpHandler.emitCommitIndexBookmarkIfAbsent(exchange, new FakeHaDatabase(5, 9, false));
    assertThat(exchange.getResponseHeaders().getFirst(HEADER)).isEqualTo("9");
  }

  @Test
  void existingHeaderIsKept() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getResponseHeaders().put(new HttpString(HEADER), "3");
    AbstractServerHttpHandler.emitCommitIndexBookmarkIfAbsent(exchange, new FakeHaDatabase(5, 9, false));
    assertThat(exchange.getResponseHeaders().getFirst(HEADER)).isEqualTo("3");
  }

  @Test
  void failingDatabaseNeverThrowsAndEmitsNothing() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    AbstractServerHttpHandler.emitCommitIndexBookmarkIfAbsent(exchange, new FakeHaDatabase(5, 9, true));
    assertThat(exchange.getResponseHeaders().contains(HEADER)).isFalse();
  }

  @Test
  void negativeBookmarkEmitsNothing() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    AbstractServerHttpHandler.emitCommitIndexBookmarkIfAbsent(exchange, new FakeHaDatabase(-1, -1, false));
    assertThat(exchange.getResponseHeaders().contains(HEADER)).isFalse();
  }
}
