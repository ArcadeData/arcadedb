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
package com.arcadedb.mongo;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9845: a follower's refusal of leader-only work reached a MongoDB client as 112 WriteConflict, which a driver
 * retries in place - on the one node that will refuse again - and the leader's address was dropped. A refusal naming
 * the leader is 10107 NotWritablePrimary, which a driver answers by rescanning the topology, and names the leader.
 */
class Issue9845MongoLeaderRefusalTest {

  private static final String LEADER = "arcadedb-0:2480";

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", leader);
  }

  @Test
  void aRefusalNamingTheLeaderIsNotWritablePrimary() {
    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("Error on executing MongoDB 'insert' command",
        refusal(LEADER));

    assertThat(wrapped).isInstanceOf(MongoServerError.class);
    final MongoServerError error = (MongoServerError) wrapped;
    assertThat(error.getCode()).isEqualTo(10107);
    assertThat(error.getCodeName()).isEqualTo("NotWritablePrimary");
    assertThat(error.getMessage()).contains("not primary").contains("The leader is server '" + LEADER + "'");
    assertThat(error.getCause()).isInstanceOf(ServerIsNotTheLeaderException.class);
  }

  @Test
  void aWrappedRefusalNamingTheLeaderIsNotWritablePrimary() {
    final MongoServerException wrapped = MongoDBDatabaseWrapper.wireException("failed",
        new TransactionException("Error on commit", refusal(LEADER)));

    assertThat(((MongoServerError) wrapped).getCode()).isEqualTo(10107);
  }

  @Test
  void aRefusalNamingNoLeaderStaysAWriteConflict() {
    for (final String noLeader : new String[] { null, "", " " }) {
      final MongoServerError error = (MongoServerError) MongoDBDatabaseWrapper.wireException("failed", refusal(noLeader));
      assertThat(error.getCode()).isEqualTo(112);
      assertThat(error.getCodeName()).isEqualTo("WriteConflict");
    }
  }

  @Test
  void aTerminationOutranksTheRefusal() {
    final QueryTerminatedException terminated = new QueryTerminatedException("terminated");
    terminated.initCause(refusal(LEADER));
    assertThat(((MongoServerError) MongoDBDatabaseWrapper.wireException("failed", terminated)).getCode()).isEqualTo(11601);
  }

  @Test
  void anOrdinaryConflictIsStillAWriteConflict() {
    final MongoServerError error = (MongoServerError) MongoDBDatabaseWrapper.wireException("failed",
        new ConcurrentModificationException("retry"));
    assertThat(error.getCode()).isEqualTo(112);
  }
}
