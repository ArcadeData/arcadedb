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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AbstractServerHttpHandler#filterAuthorizedDatabases}, the primitive the routes that
 * enumerate the whole database registry (database listing, server status, cluster status) reduce their
 * per-database output with. Every one of those routes is one missing call away from a cross-database
 * disclosure, so the primitive itself is pinned down here rather than only through the routes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractServerHttpHandlerDatabaseScopingTest {

  /** Minimal concrete handler: the method under test needs no server, exchange or request state. */
  private static class TestHandler extends AbstractServerHttpHandler {
    TestHandler() {
      super(null);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      throw new UnsupportedOperationException("not exercised");
    }
  }

  private final TestHandler handler = new TestHandler();

  private static final Set<String> INSTALLED = new LinkedHashSet<>(List.of("alpha", "beta", "gamma"));

  @Test
  void keepsOnlyTheDatabasesTheUserIsGranted() {
    assertThat(handler.filterAuthorizedDatabases(user("beta"), INSTALLED)).containsExactly("beta");
  }

  @Test
  void keepsEverythingForAWildcardGrant() {
    assertThat(handler.filterAuthorizedDatabases(user("*"), INSTALLED)).containsExactly("alpha", "beta", "gamma");
  }

  @Test
  void returnsNothingForAUserGrantedNoDatabase() {
    assertThat(handler.filterAuthorizedDatabases(user(), INSTALLED)).isEmpty();
  }

  /**
   * A grant naming a database that is not installed must not conjure it into the result: the output is a
   * subset of what the server actually holds, never of what the user configuration mentions.
   */
  @Test
  void neverReportsADatabaseTheServerDoesNotHold() {
    assertThat(handler.filterAuthorizedDatabases(user("beta", "delta"), INSTALLED)).containsExactly("beta");
  }

  @Test
  void preservesTheIterationOrderOfTheInstalledSet() {
    assertThat(handler.filterAuthorizedDatabases(user("*"), INSTALLED)).containsExactlyElementsOf(INSTALLED);
  }

  /** A null user means the route runs unauthenticated, matching {@code checkAuthorizationOnDatabase}. */
  @Test
  void returnsEverythingWhenThereIsNoAuthenticatedUser() {
    assertThat(handler.filterAuthorizedDatabases(null, INSTALLED)).containsExactlyElementsOf(INSTALLED);
  }

  @Test
  void doesNotAliasTheCallerSet() {
    final Set<String> result = handler.filterAuthorizedDatabases(user("*"), INSTALLED);
    result.clear();
    assertThat(INSTALLED).as("the installed set must survive a caller mutating the returned one").hasSize(3);
  }

  private static ServerSecurityUser user(final String... databases) {
    final JSONObject granted = new JSONObject();
    for (final String database : databases)
      granted.put(database, new JSONArray(new String[] { "admin" }));
    return new ServerSecurityUser(null,
        new JSONObject().put("name", "tenant").put("databases", granted));
  }
}
