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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8323, the auto-commit retry: {@code DatabaseAbstractHandler.executeInTransaction} runs {@code execute()}
 * again after a conflict, and every forward to the leader that attempt takes is a retry of a forward the previous
 * attempt took. It must reuse the id that forward had, not draw the next ordinal - otherwise the retried write gets
 * a fresh key on the leader and is never deduplicated against the first attempt.
 */
class Issue8323AutoCommitRetryRequestIdTest {
  private static final String DATABASE_PATH = "./target/databases/Issue8323AutoCommitRetryRequestIdTest";

  private DatabaseInternal database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = (DatabaseInternal) factory.create();
  }

  @AfterEach
  void dropDatabase() {
    ForwardedRequestIdContext.clear();
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aRetriedAttemptForwardsUnderTheSameIdsAsTheFirst() {
    ForwardedRequestIdContext.set("client-8323");
    final List<List<String>> idsPerAttempt = new ArrayList<>();
    final AtomicInteger attempts = new AtomicInteger();

    final DatabaseAbstractHandler handler = new DatabaseAbstractHandler(null) {
      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
          final Database database, final JSONObject payload) {
        // Two forwards per attempt, standing in for RaftReplicatedDatabase.forwardCommandToLeaderViaRaft.
        idsPerAttempt.add(List.of(ForwardedRequestIdContext.nextForwardRequestId(),
            ForwardedRequestIdContext.nextForwardRequestId()));
        if (attempts.incrementAndGet() == 1)
          throw new ConcurrentModificationException("Record #1:1 modified by another transaction");
        return new ExecutionResponse(200, "{}");
      }
    };

    handler.executeInTransaction(null, null, database, null, new AtomicReference<>(), 3);

    assertThat(attempts.get()).isEqualTo(2);
    assertThat(idsPerAttempt).containsExactly(
        List.of("client-8323", "client-8323#2"),
        List.of("client-8323", "client-8323#2"));
  }
}
