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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7874 covering the two arms of {@code ensureDatabase()} that need a server in a
 * state a running test server cannot be put into: one with no database at all, and one whose handle is closed.
 * <p>
 * The wire-level IT beside it covers the reported case (a name that does not exist) end to end. These two are
 * the arms that separate PERMANENT from TRANSIENT, which is the whole distinction the issue is about: a driver
 * retries {@code Neo.TransientError.Database.DatabaseUnavailable} and must not retry
 * {@code Neo.ClientError.Database.DatabaseNotFound}. Untested, the two could quietly collapse onto one code and
 * every symptom the issue describes would come back for the transient half (PR #7939 review).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7874BoltDatabaseSelectionHandlerTest {

  /**
   * A server whose databases are not open yet - the state of one still starting - with no default configured.
   * The caller named nothing wrong, so this is not DatabaseNotFound; the same request succeeds once a database
   * is there, so it must be the transient code a driver may retry rather than a generic server fault.
   */
  @Test
  void aServerWithNoDatabaseAtAllAnswersTransientlyUnavailable() throws Exception {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getDatabaseNames()).thenReturn(Set.of());

    final Map<String, Object> failure = BoltHandlerProbe.databaseSelectionFailureOf(server, null);

    assertThat(failure).isNotNull();
    assertThat(failure.get("code")).isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
    assertThat(failure.get("code")).isEqualTo("Neo.TransientError.Database.DatabaseUnavailable");
    assertThat(failure.get("message").toString()).contains("No database available");
  }

  /**
   * The database exists - the server handed back a handle for it - but it is closed: dropped, or taken offline
   * under a resolved handle. The name is right, so the advice is the opposite of the one DatabaseNotFound gives.
   */
  @Test
  void aClosedDatabaseIsUnavailableRatherThanNotFound() throws Exception {
    final ServerDatabase closed = mock(ServerDatabase.class);
    when(closed.isOpen()).thenReturn(false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getDatabase(anyString())).thenReturn(closed);

    final Map<String, Object> failure = BoltHandlerProbe.databaseSelectionFailureOf(server, "sales");

    assertThat(failure).isNotNull();
    assertThat(failure.get("code")).isEqualTo(BoltErrorCodes.DATABASE_UNAVAILABLE_ERROR);
    assertThat(failure.get("code"))
        .as("a database that is THERE but closed must not be reported as a name that does not exist")
        .isNotEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    assertThat(failure.get("message").toString()).contains("sales");
  }

  /**
   * The reported case, through the handler rather than through the classifier: the server throws for a name it
   * does not have, and the catch has to classify it as the permanent client error.
   */
  @Test
  void aNameTheServerThrowsForIsThePermanentClientError() throws Exception {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    when(server.getDatabase(anyString()))
        .thenThrow(new DatabaseNotFoundException("Database '/data/nosuchdb' does not exist"));

    final Map<String, Object> failure = BoltHandlerProbe.databaseSelectionFailureOf(server, "nosuchdb");

    assertThat(failure).isNotNull();
    assertThat(failure.get("code")).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
  }

  /**
   * The virtual-name path: Neo4j's "system" and "neo4j" map to whatever BOLT_DEFAULT_DATABASE names, and a
   * default that is configured but absent is still the caller naming nothing wrong - the refusal has to come
   * from the resolved name, not from the virtual one the client sent.
   */
  @Test
  void aVirtualNameResolvesToTheConfiguredDefaultBeforeItIsRefused() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.BOLT_DEFAULT_DATABASE, "configured");

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getDatabase(anyString()))
        .thenThrow(new DatabaseNotFoundException("Database '/data/configured' does not exist"));

    final Map<String, Object> failure = BoltHandlerProbe.databaseSelectionFailureOf(server, "neo4j");

    assertThat(failure).isNotNull();
    assertThat(failure.get("code")).isEqualTo(BoltErrorCodes.DATABASE_NOT_FOUND_ERROR);
    assertThat(failure.get("message").toString())
        .as("the name the operator has to fix is the configured default, not the virtual name Neo4j sent")
        .contains("configured");
  }
}
