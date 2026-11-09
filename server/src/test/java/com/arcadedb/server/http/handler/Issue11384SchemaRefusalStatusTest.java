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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.UnknownProcedureException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11384 over HTTP: a statement naming an index, a trigger, a view, a bucket, a property or a type the schema does not define
 * answered 500 "Internal error" with a stack trace in the server log, whether it raised a {@link CommandExecutionException} (the
 * statements that checked the name themselves) or a {@link SchemaException} (the ones that went through the schema). Both raise a
 * {@link SchemaException} now, which HTTP answers 400 as every other protocol answers the caller's mistake; a
 * {@link SchemaException} carrying a failure under it is a server fault and keeps the 500.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11384SchemaRefusalStatusTest extends BaseGraphServerTest {

  @Test
  void aMissingSchemaObjectIs400AndASchemaFailureWithACauseKeepsThe500() {
    final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());
    final Database database = getServerDatabase(0, getDatabaseName());

    for (final String statement : List.of("DROP INDEX nosuchindex", "DROP TRIGGER nosuchtrg", "TRUNCATE BUCKET nosuchbucket",
        "SELECT FROM bucket:nosuch", "DROP BUCKET nosuchbucket", "REFRESH MATERIALIZED VIEW nosuchmv")) {
      final Throwable error = catchThrowable(() -> {
        try (final ResultSet rs = database.command("sql", statement)) {
          rs.stream().count();
        }
      });
      final AbstractServerHttpHandler.ErrorClassification classified = handler.classifyError(error);
      assertThat(classified.status()).as(statement).isEqualTo(400);
      assertThat(classified.message()).as(statement).isEqualTo("Schema error");
      assertThat(classified.reported()).as(statement).isInstanceOf(SchemaException.class);
    }

    // wrapped by the engine, the same answer
    assertThat(handler.classifyError(new CommandExecutionException("Error", new SchemaException("Type 'X' not found"))).status())
        .isEqualTo(400);
    // a procedure nothing answers is the caller's too
    assertThat(handler.classifyError(new UnknownProcedureException("db.nosuch", "Unknown procedure")).status()).isEqualTo(400);
    // the neighbouring arm keeps its 409
    assertThat(handler.classifyError(new AlreadyExistsException(AlreadyExistsException.Kind.TYPE, "Type 'X' already exists")).status())
        .isEqualTo(409);
    // a schema failure that carries what broke under it is the server's
    assertThat(handler.classifyError(new SchemaException("Error on adding new item to the dictionary", new IOException("disk full")))
        .status()).isEqualTo(500);
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
