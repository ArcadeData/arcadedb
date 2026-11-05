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

import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314: the HTTP arms added for a refused value and an unknown function, next to the arms around them. A
 * {@link ValidationException} answered 500 "Internal error" because no arm named it; it is a 400 now, with the kind it carries in
 * {@code exceptionArgs}. Its subtype {@link RecordVetoedException}, a listener's veto, is the same refusal of the request and
 * gets the same 400 with no kind, and the {@link DuplicatedKeyException} arm, which does not extend it, keeps its 409.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314ValidationStatusTest extends BaseGraphServerTest {

  @Test
  void aRefusedValueIs400WithItsKindAndTheNeighbouringArmsKeepTheirStatus() {
    final ClassifyingHandler handler = new ClassifyingHandler(getServer(0).getHttpServer());

    final AbstractServerHttpHandler.ErrorClassification notNull = handler.classifyError(
        new CommandExecutionException("Error on executing", new ValidationException(ValidationException.Kind.NOT_NULL, "cannot be null")));
    assertThat(notNull.status()).isEqualTo(400);
    assertThat(notNull.message()).isEqualTo("Constraint violated (NOT_NULL)");
    assertThat(notNull.exceptionArgs()).isEqualTo("NOT_NULL");
    assertThat(notNull.reported()).isInstanceOf(ValidationException.class);

    final AbstractServerHttpHandler.ErrorClassification vetoed = handler.classifyError(RecordVetoedException.ofCreate("V"));
    assertThat(vetoed.status()).isEqualTo(400);
    assertThat(vetoed.message()).isEqualTo("Validation error");
    assertThat(vetoed.exceptionArgs()).isNull();
    assertThat(vetoed.reported()).isInstanceOf(RecordVetoedException.class);

    final AbstractServerHttpHandler.ErrorClassification unknown = handler.classifyError(
        new UnknownFunctionException("foo", "Unknown function name 'foo'"));
    assertThat(unknown.status()).isEqualTo(400);
    assertThat(unknown.reported()).isInstanceOf(UnknownFunctionException.class);

    assertThat(handler.classifyError(new DuplicatedKeyException("idx", "[1]", new RID(1, 1))).status()).isEqualTo(409);
    // a failure nothing identifies as the caller's keeps the 500
    assertThat(handler.classifyError(new CommandExecutionException("Backup failed")).status()).isEqualTo(500);
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
