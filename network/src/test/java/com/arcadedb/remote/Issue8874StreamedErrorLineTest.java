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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #8874: the in-band {@code error} line of a streamed query carries {@code status}, {@code exception} and
 * {@code exceptionArgs} since #8235, but {@link RemoteStreamingResultSet} read only {@code message} and threw a plain
 * {@link RemoteException}, so a caller of the streamed encoding could not catch a retryable conflict as a
 * {@link NeedRetryException} the way the buffered encoding lets it.
 * <p>
 * Drives the result set the driver actually builds ({@link RemoteDatabase#newStreamingResultSet}) over a scripted
 * body: one row, then the error line. No server is contacted; the end-to-end path against a real server is covered by
 * {@code Issue8874RemoteStreamedTypedErrorIT} in the server module.
 */
class Issue8874StreamedErrorLineTest {
  private static final String ROW = "{\"record\":{\"n\":0}}\n";

  private RemoteDatabase database;

  @BeforeEach
  void openDatabase() {
    database = new OfflineDatabase();
  }

  @AfterEach
  void closeDatabase() {
    database.close();
  }

  @Test
  void aConcurrentModificationIsRebuiltAsARetryableException() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "conflict while streaming").put("status", 503)
        .put("exception", ConcurrentModificationException.class.getName())));

    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(thrown.getMessage()).contains("conflict while streaming");
  }

  @Test
  void aDuplicatedKeyIsRebuiltFromItsExceptionArgs() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "duplicated").put("status", 409)
        .put("exception", DuplicatedKeyException.class.getName()).put("exceptionArgs", "Person[name]|[Jay]|#3:0")));

    assertThat(thrown).isInstanceOf(DuplicatedKeyException.class);
    final DuplicatedKeyException duplicated = (DuplicatedKeyException) thrown;
    assertThat(duplicated.getIndexName()).isEqualTo("Person[name]");
    assertThat(duplicated.getKeys()).isEqualTo("[Jay]");
    assertThat(duplicated.getCurrentIndexedRID()).isEqualTo(new RID(3, 0));
  }

  @Test
  void aRecordNotFoundIsRebuiltWithItsRid() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "Record #12:7 not found").put("status", 404)
        .put("exception", RecordNotFoundException.class.getName())));

    assertThat(thrown).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(new RID(12, 7));
  }

  @Test
  void aSecurityRefusalIsRebuiltAsSecurityException() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "not allowed").put("status", 403)
        .put("exception", SecurityException.class.getName())));

    assertThat(thrown).isExactlyInstanceOf(SecurityException.class).hasMessage("not allowed");
  }

  /** A 503 with no exception class is retryable by the status-code contract, exactly as on the buffered encoding. */
  @Test
  void aStatusOnly503IsRebuiltAsRetryable() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "catching up").put("status", 503)));

    assertThat(thrown).isExactlyInstanceOf(NeedRetryException.class).hasMessageContaining("catching up");
  }

  /** A server fault the mapping has no type for keeps the message the driver always reported. */
  @Test
  void anUnmappedServerFaultKeepsTheGenericRemoteException() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "engine fault").put("status", 500)
        .put("exception", IllegalStateException.class.getName())));

    assertThat(thrown).isExactlyInstanceOf(RemoteException.class)
        .hasMessage("The server failed while streaming the result: engine fault");
  }

  /** A server that predates #8235 sends only the message: the driver answers exactly as it did before. */
  @Test
  void anErrorLineFromAServerThatPredates8235KeepsTheGenericRemoteException() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "conflict while streaming")));

    assertThat(thrown).isExactlyInstanceOf(RemoteException.class)
        .hasMessage("The server failed while streaming the result: conflict while streaming");
  }

  /** A malformed exceptionArgs must not turn into an ArrayIndexOutOfBoundsException out of hasNext(). */
  @Test
  void aMalformedDuplicatedKeyFallsBackToTheGenericRemoteException() {
    final Throwable thrown = failureOf(errorLine(new JSONObject().put("message", "duplicated").put("status", 409)
        .put("exception", DuplicatedKeyException.class.getName()).put("exceptionArgs", "only-one-part")));

    assertThat(thrown).isExactlyInstanceOf(RemoteException.class)
        .hasMessage("The server failed while streaming the result: duplicated");
  }

  private static String errorLine(final JSONObject error) {
    return ROW + new JSONObject().put("error", error) + "\n";
  }

  /** Reads the first row, then returns what the error line after it raised. */
  private Throwable failureOf(final String body) {
    try (final ResultSet rs = database.newStreamingResultSet(new BufferedReader(new StringReader(body)), true,
        "select from V")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("n")).intValue()).isZero();
      return catchThrowable(rs::hasNext);
    }
  }

  /**
   * A driver that never reaches the network. The port is only an address the constructor requires: the cluster probe
   * is overridden away and the test feeds the response body itself, so nothing ever binds or dials it.
   */
  private static final class OfflineDatabase extends RemoteDatabase {
    OfflineDatabase() {
      super("127.0.0.1", 9, "db", "root", "test", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No cluster and no server: the test feeds the response body itself
    }
  }
}
