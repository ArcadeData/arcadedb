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
package com.arcadedb.server.gremlin;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The traversal source a Gremlin request runs against is checked against the databases of its user whether or not the request
 * names one: a script that carries no {@code aliases} runs on {@code g}, which the server resolves to the default database.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ArcadeGremlinAuthorizerDefaultSourceTest extends AbstractGremlinServerIT {
  private static final String OTHER_DATABASE = "gremlinOtherDatabase";
  private static final String OTHER_USER     = "gremlinOtherDatabaseUser";
  private static final String DEFAULT_USER   = "gremlinDefaultDatabaseUser";
  private static final String PASSWORD       = "gremlinTraversalSource1";

  @BeforeEach
  void createFixture() {
    getServer(0).getOrCreateDatabase(OTHER_DATABASE);

    final ServerSecurity security = getServer(0).getSecurity();
    createUser(security, OTHER_USER, OTHER_DATABASE);
    createUser(security, DEFAULT_USER, getDatabaseName());
  }

  @AfterEach
  void dropFixture() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security != null) {
      if (security.getUser(OTHER_USER) != null)
        security.dropUser(OTHER_USER);
      if (security.getUser(DEFAULT_USER) != null)
        security.dropUser(DEFAULT_USER);
    }
    if (getServer(0).existsDatabase(OTHER_DATABASE)) {
      ((DatabaseInternal) getServer(0).getDatabaseForOperator(OTHER_DATABASE)).getEmbedded().drop();
      getServer(0).removeDatabase(OTHER_DATABASE);
    }
    GremlinAuthContext.setClient(null);
    GremlinAuthContext.clear();
  }

  @Test
  void aScriptWithNoAliasesNeedsAccessToTheDefaultDatabase() {
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(null, false))).isInstanceOf(AuthorizationException.class)
        .hasMessageContaining("not authorized");

    assertThatCode(() -> authorizer().authorize(user(DEFAULT_USER), script(null, false))).doesNotThrowAnyException();
    assertThatCode(() -> authorizer().authorize(user("root"), script(null, false))).doesNotThrowAnyException();
  }

  @Test
  void aScriptWithEmptyAliasesNeedsAccessToTheDefaultDatabase() {
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of(), true))).isInstanceOf(AuthorizationException.class);

    assertThatCode(() -> authorizer().authorize(user(DEFAULT_USER), script(Map.of(), true))).doesNotThrowAnyException();
  }

  @Test
  void aScriptWhoseAliasesDoNotRebindTheDefaultSourceNeedsAccessToTheDefaultDatabase() {
    // The alias is granted, but the script reads 'g', which is not rebound by it
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("x", OTHER_DATABASE), true))).isInstanceOf(
        AuthorizationException.class);

    assertThatCode(() -> authorizer().authorize(user(DEFAULT_USER), script(Map.of("x", getDatabaseName()), true))).doesNotThrowAnyException();
  }

  @Test
  void aScriptThatRebindsTheDefaultSourceNeedsAccessOnlyToTheTarget() {
    assertThatCode(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("g", OTHER_DATABASE), true))).doesNotThrowAnyException();

    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("g", getDatabaseName()), true))).isInstanceOf(
        AuthorizationException.class);
    assertThatThrownBy(() -> authorizer().authorize(user(DEFAULT_USER), script(Map.of("g", OTHER_DATABASE), true))).isInstanceOf(
        AuthorizationException.class);
  }

  @Test
  void anAliasPointingAtTheDefaultSourceNeedsAccessToTheDefaultDatabase() {
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("g", "g"), true))).isInstanceOf(
        AuthorizationException.class);
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("x", "g"), true))).isInstanceOf(
        AuthorizationException.class);

    assertThatCode(() -> authorizer().authorize(user(DEFAULT_USER), script(Map.of("g", "g"), true))).doesNotThrowAnyException();
  }

  @Test
  void aScriptWithMalformedAliasesIsRefused() {
    assertThatThrownBy(() -> authorizer().authorize(user(DEFAULT_USER), script(getDatabaseName(), true))).isInstanceOf(
        AuthorizationException.class);
    assertThatThrownBy(() -> authorizer().authorize(user("root"), script(getDatabaseName(), true))).isInstanceOf(
        AuthorizationException.class);

    final Map<String, Object> noTarget = new HashMap<>();
    noTarget.put("g", null);
    assertThatThrownBy(() -> authorizer().authorize(user(DEFAULT_USER), script(noTarget, true))).isInstanceOf(AuthorizationException.class);
  }

  @Test
  void theRefusalNamesTheTraversalSourceAsSent() {
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(null, false))).isInstanceOf(AuthorizationException.class)
        .hasMessageNotContaining(getDatabaseName());
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), script(Map.of("g", "g"), true))).isInstanceOf(
        AuthorizationException.class).hasMessageNotContaining(getDatabaseName());
  }

  @Test
  void aBytecodeRequestWithNoAliasesNeedsAccessToTheDefaultDatabase() {
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), new Bytecode(), null)).isInstanceOf(AuthorizationException.class);
    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), new Bytecode(), Map.of())).isInstanceOf(AuthorizationException.class);

    assertThatCode(() -> authorizer().authorize(user(DEFAULT_USER), new Bytecode(), null)).doesNotThrowAnyException();
    assertThatCode(() -> authorizer().authorize(user("root"), new Bytecode(), null)).doesNotThrowAnyException();
  }

  @Test
  void aBytecodeRequestWithAnAliasNeedsAccessToTheTarget() {
    assertThatCode(() -> authorizer().authorize(user(OTHER_USER), new Bytecode(), Map.of("g", OTHER_DATABASE))).doesNotThrowAnyException();

    assertThatThrownBy(() -> authorizer().authorize(user(OTHER_USER), new Bytecode(), Map.of("g", getDatabaseName()))).isInstanceOf(
        AuthorizationException.class);
  }

  private ArcadeGremlinAuthorizer authorizer() {
    final ArcadeGremlinAuthorizer authorizer = new ArcadeGremlinAuthorizer();
    authorizer.setup(Map.of("server", getServer(0)));
    return authorizer;
  }

  private static AuthenticatedUser user(final String name) {
    return new AuthenticatedUser(name);
  }

  private static RequestMessage script(final Object aliases, final boolean withAliases) {
    final RequestMessage.Builder request = RequestMessage.build(Tokens.OPS_EVAL).addArg(Tokens.ARGS_GREMLIN, "g.V().count()")
        .addArg(Tokens.ARGS_LANGUAGE, ArcadeGremlinAuthorizer.GREMLIN_LANG);
    if (withAliases)
      request.addArg(Tokens.ARGS_ALIASES, aliases);
    return request.create();
  }

  private static void createUser(final ServerSecurity security, final String name, final String database) {
    if (security.getUser(name) == null)
      security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put(database, new JSONArray())));
  }
}
