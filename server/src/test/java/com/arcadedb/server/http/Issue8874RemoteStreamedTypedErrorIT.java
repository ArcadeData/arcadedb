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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #8874: since #8235 the in-band {@code error} line of a streamed query carries the {@code status},
 * {@code exception} and {@code exceptionArgs} the buffered encoding answers the same failure with, but the Java
 * driver read only {@code message} and raised a plain {@link RemoteException}. A caller of
 * {@link RemoteDatabase#queryStream} or {@link RemoteDatabase#commandStream} could therefore not catch a retryable
 * conflict as a {@link NeedRetryException}.
 * <p>
 * Every test fails the query on its SECOND row, so the first row has already gone out under a 200 and the failure
 * can only travel in band - which is the path under test, end to end against a real server.
 */
public class Issue8874RemoteStreamedTypedErrorIT extends BaseGraphServerTest {
  private static final String TYPE_NAME = "Stream8874";
  private static final String FUNCTION  = "fail8874";

  // Shared by every test of the class and reset in registerFunction(): sound only because the methods of one class run
  // sequentially against one server.
  private static volatile Supplier<RuntimeException> failure;
  private static final    AtomicInteger              calls = new AtomicInteger();

  /** Lets the first row through, then throws whatever the test installed for every later one. */
  private static final class FailOnSecondRow extends SQLFunctionAbstract {
    FailOnSecondRow() {
      super(FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
        final Object[] params, final CommandContext context) {
      final int call = calls.incrementAndGet();
      if (call > 1)
        throw failure.get();
      return call;
    }

    @Override
    public String getSyntax() {
      return FUNCTION + "()";
    }
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      for (int i = 0; i < 5; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
  }

  @BeforeEach
  void registerFunction() {
    calls.set(0);
    sqlEngine().getFunctionFactory().register(new FailOnSecondRow());
  }

  @AfterEach
  void unregisterFunction() {
    sqlEngine().getFunctionFactory().unregister(FUNCTION);
    failure = null;
  }

  private SQLQueryEngine sqlEngine() {
    return (SQLQueryEngine) getServerDatabase(0, getDatabaseName()).getQueryEngine("sql");
  }

  @Test
  void aConflictOnQueryStreamIsCaughtAsNeedRetryException() {
    failure = () -> new ConcurrentModificationException("conflict while streaming");

    final Throwable thrown = failureOf(db -> db.queryStream("sql", failingQuery(), Map.of()));

    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(thrown.getMessage()).contains("conflict while streaming");
  }

  @Test
  void aDuplicatedKeyOnCommandStreamIsRebuiltWithItsIndexAndRid() {
    failure = () -> new DuplicatedKeyException("Stream8874[idx]", "[1]", new RID(3, 0));

    final Throwable thrown = failureOf(db -> db.commandStream("sql", failingQuery(), Map.of()));

    assertThat(thrown).isInstanceOf(DuplicatedKeyException.class);
    final DuplicatedKeyException duplicated = (DuplicatedKeyException) thrown;
    assertThat(duplicated.getIndexName()).isEqualTo("Stream8874[idx]");
    assertThat(duplicated.getCurrentIndexedRID()).isEqualTo(new RID(3, 0));
  }

  @Test
  void aSecurityRefusalIsRebuiltAsSecurityException() {
    failure = () -> new SecurityException("not allowed to read this row");

    final Throwable thrown = failureOf(db -> db.queryStream("sql", failingQuery(), Map.of()));

    assertThat(thrown).isExactlyInstanceOf(SecurityException.class);
    assertThat(thrown.getMessage()).contains("not allowed to read this row");
  }

  /** A fault the driver has no type for keeps the message it always reported. */
  @Test
  void anUnmappedServerFaultKeepsTheGenericRemoteException() {
    failure = () -> new IllegalStateException("engine fault");

    final Throwable thrown = failureOf(db -> db.queryStream("sql", failingQuery(), Map.of()));

    assertThat(thrown).isExactlyInstanceOf(RemoteException.class)
        .hasMessage("The server failed while streaming the result: engine fault");
  }

  private String failingQuery() {
    return "SELECT idx, " + FUNCTION + "() AS f FROM " + TYPE_NAME;
  }

  /** Opens the stream, reads the first row, and returns what the in-band failure after it raised. */
  private Throwable failureOf(final Function<RemoteDatabase, ResultSet> open) {
    try (final RemoteDatabase db = new RemoteDatabase("localhost", getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ResultSet rs = open.apply(db)) {
        assertThat(rs.hasNext()).as("the failure must come after a row was already sent").isTrue();
        rs.next();
        return catchThrowable(() -> {
          while (rs.hasNext())
            rs.next();
        });
      }
    }
  }
}
