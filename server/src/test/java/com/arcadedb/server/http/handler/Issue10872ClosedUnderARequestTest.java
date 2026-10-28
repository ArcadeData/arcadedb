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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10872, the in-flight half: a {@code close database} that closes the handle a request had already resolved answers that
 * request what the resolver answers the next one, 503, while the database is still there; and 404 only once nothing of it is
 * left. Before, the in-flight request was told 404 and the next one 503 for the same closed database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10872ClosedUnderARequestTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10872ClosedUnder";

  @AfterEach
  void dropDatabase() {
    final ArcadeDBServer server = getServer(0);
    server.unmarkClosedByCommand(NAME);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  @Test
  void aHandleClosedUnderARequestIsUnavailableWhileTheDatabaseIsThere() {
    final ArcadeDBServer server = getServer(0);
    server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    final ClassifyingHandler handler = new ClassifyingHandler(server.getHttpServer());

    // closed by command: the database is there, and the next request is told 503 by the resolver
    server.getDatabaseForOperator(NAME).getEmbedded().close();
    server.removeDatabase(NAME);
    server.markClosedByCommand(NAME);
    final AbstractServerHttpHandler.ErrorClassification closed = handler.classifyError(DatabaseIsClosedException.of(NAME));
    assertThat(closed.status()).isEqualTo(503);
    assertThat(closed.message()).isEqualTo("Database is closed");

    // what the engine raises carries the name
    assertThat(DatabaseIsClosedException.of(NAME).getDatabaseName()).isEqualTo(NAME);
    assertThat(new DatabaseIsReopeningException(NAME).getDatabaseName()).isEqualTo(NAME);

    // dropped: nothing of it is left, 404
    server.unmarkClosedByCommand(NAME);
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    assertThat(handler.classifyError(DatabaseIsClosedException.of(NAME + "Gone")).status()).isEqualTo(404);
    // a thrower that did not say which database keeps the answer it always had
    assertThat(handler.classifyError(new DatabaseIsClosedException("a closed handle")).status()).isEqualTo(404);
  }

  private static final class ClassifyingHandler extends AbstractServerHttpHandler {
    private ClassifyingHandler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
      throw new UnsupportedOperationException();
    }
  }
}
