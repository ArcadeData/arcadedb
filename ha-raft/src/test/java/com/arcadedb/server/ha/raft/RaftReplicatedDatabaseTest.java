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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RaftReplicatedDatabaseTest {

  @Test
  void implementsDatabaseInternal() {
    assertThat(DatabaseInternal.class.isAssignableFrom(RaftReplicatedDatabase.class)).isTrue();
  }

  @Test
  void parseResultSetFromJsonWithRecords() {
    final String json = "{\"result\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((String) results.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((int) results.get(0).getProperty("age")).isEqualTo(30);
    assertThat((String) results.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void parseResultSetFromJsonEmptyResult() {
    final String json = "{\"result\":[]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonNoResultKey() {
    final String json = "{\"user\":\"root\"}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonWithScalarValues() {
    final String json = "{\"result\":[42,\"hello\"]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((int) results.get(0).getProperty("value")).isEqualTo(42);
    assertThat((String) results.get(1).getProperty("value")).isEqualTo("hello");
  }

  // reconstructLeaderException tests

  @Test
  void reconstructLeaderExceptionDuplicatedKey() {
    final String body = "{\"error\":\"Found duplicate key in index\","
        + "\"detail\":\"Duplicated key [42] found on index 'Account_PK' already assigned to record #7:1\","
        + "\"exception\":\"com.arcadedb.exception.DuplicatedKeyException\","
        + "\"exceptionArgs\":\"Account_PK|[42]|#7:1\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(DuplicatedKeyException.class);
    final DuplicatedKeyException dup = (DuplicatedKeyException) result;
    assertThat(dup.getIndexName()).isEqualTo("Account_PK");
    assertThat(dup.getKeys()).isEqualTo("[42]");
    assertThat(dup.getCurrentIndexedRID()).isEqualTo(new RID(7, 1L));
  }

  @Test
  void reconstructLeaderExceptionNeedRetry() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Record has been modified by another transaction\","
        + "\"exception\":\"com.arcadedb.exception.NeedRetryException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Record has been modified by another transaction");
  }

  @Test
  void reconstructLeaderExceptionConcurrentModification() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Concurrent modification detected\","
        + "\"exception\":\"com.arcadedb.exception.ConcurrentModificationException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    // Reconstructed as the exact type, not collapsed to its NeedRetryException supertype, so a
    // caller catching ConcurrentModificationException specifically still matches.
    assertThat(result).isInstanceOf(ConcurrentModificationException.class);
    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Concurrent modification detected");
  }

  @Test
  void reconstructLeaderExceptionLockTimeoutIsRetryable() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Timeout on acquiring lock\","
        + "\"exception\":\"com.arcadedb.exception.LockTimeoutException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    // LockTimeoutException extends NeedRetryException: it must stay retryable on the Follower.
    assertThat(result).isInstanceOf(LockTimeoutException.class);
    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Timeout on acquiring lock");
  }

  @Test
  void reconstructLeaderExceptionTimeoutIsNotRetryable() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Query timeout\","
        + "\"exception\":\"com.arcadedb.exception.TimeoutException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    // A plain (query-deadline) TimeoutException is NOT retryable and must not become a
    // NeedRetryException, otherwise callers would wrongly retry it.
    assertThat(result).isInstanceOf(TimeoutException.class);
    assertThat(result).isNotInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Query timeout");
  }

  @Test
  void reconstructLeaderExceptionSqlParsingKeepsSubtype() {
    final String body = "{\"error\":\"Error on parsing query\","
        + "\"detail\":\"Syntax error near 'SELCT'\","
        + "\"exception\":\"com.arcadedb.exception.CommandSQLParsingException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    // Reconstructed as the exact subtype, so a caller catching CommandParsingException matches too.
    assertThat(result).isInstanceOf(CommandSQLParsingException.class);
    assertThat(result).isInstanceOf(CommandParsingException.class);
    assertThat(result.getMessage()).isEqualTo("Syntax error near 'SELCT'");
  }

  @Test
  void reconstructLeaderExceptionCommittedRemotelyKeepsDoNotRetryContract() {
    // #5064: a follower forwarding a write must hand the caller the SAME do-not-retry contract the
    // leader produced - collapsing it to a generic TransactionException (or worse, a retryable type)
    // would let the client re-drive a transaction the cluster already committed, inserting duplicates.
    final String body = "{\"error\":\"Transaction committed cluster-wide but the local apply failed - do not retry\","
        + "\"detail\":\"Transaction TX(1) is committed cluster-wide but the local apply failed. Do NOT retry: reload the records and continue\","
        + "\"exception\":\"com.arcadedb.exception.TransactionCommittedRemotelyException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(TransactionCommittedRemotelyException.class);
    assertThat(result).isNotInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).contains("committed cluster-wide").contains("Do NOT retry");
  }

  @Test
  void reconstructLeaderExceptionQueryNotIdempotent() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Query is not idempotent\","
        + "\"exception\":\"com.arcadedb.exception.QueryNotIdempotentException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(QueryNotIdempotentException.class);
    assertThat(result.getMessage()).isEqualTo("Query is not idempotent");
  }

  @Test
  void reconstructLeaderExceptionTransactionException() {
    final String body = "{\"error\":\"Error on transaction commit\","
        + "\"detail\":\"Commit failed\","
        + "\"exception\":\"com.arcadedb.exception.TransactionException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).isEqualTo("Commit failed");
  }

  @Test
  void reconstructLeaderExceptionUnknownClass() {
    final String body = "{\"error\":\"Some error\","
        + "\"detail\":\"Some detail\","
        + "\"exception\":\"com.example.SomeUnknownException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionNoExceptionField() {
    final String body = "{\"error\":\"Internal error\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionMalformedJson() {
    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, "not-json");

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionNullBody() {
    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, null);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionDuplicatedKeyMalformedArgs() {
    // exceptionArgs with wrong number of segments - should fall back to TransactionException
    final String body = "{\"error\":\"Found duplicate key in index\","
        + "\"exception\":\"com.arcadedb.exception.DuplicatedKeyException\","
        + "\"exceptionArgs\":\"only-one-segment\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(TransactionException.class);
  }
}
