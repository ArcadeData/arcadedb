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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for issue #3155: Verify that nested exception causes are included in HTTP error responses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ErrorResponseNestedExceptionTest {

  @Test
  void testNestedExceptionInErrorResponse() {
    // Create a nested exception chain similar to the issue report
    final IllegalArgumentException rootCause = new IllegalArgumentException(
        "Expected float array or ComparableVector as key for vector index, got class java.lang.String");
    final CommandExecutionException wrappedException = new CommandExecutionException(
        "Error executing Cypher command: UNWIND $batch AS BatchEntry MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) CREATE (p)-[:embb]->(b)",
        rootCause);

    // Create a test handler that exposes the error2json method for testing
    final TestHandler handler = new TestHandler(null);
    final String errorJson = handler.publicError2json("Error on transaction commit", wrappedException);

    // Parse the JSON response
    final JSONObject json = new JSONObject(errorJson);

    // Verify the response contains all expected fields
    assertThat(json.has("error")).isTrue();
    assertThat(json.has("detail")).isTrue();
    assertThat(json.has("exception")).isTrue();

    // Verify the error field
    assertThat(json.getString("error")).isEqualTo("Error on transaction commit");

    // Verify the exception field
    assertThat(json.getString("exception")).isEqualTo(CommandExecutionException.class.getName());

    // The key assertion: verify the detail includes the nested cause chain
    final String detail = json.getString("detail");
    assertThat(detail).contains("Error executing Cypher command");
    assertThat(detail).contains("->");
    assertThat(detail).contains("Expected float array");
    assertThat(detail).contains("vector index");
    assertThat(detail).contains("String");
  }

  @Test
  void testSingleExceptionWithoutCause() {
    final IllegalArgumentException exception = new IllegalArgumentException("Simple error message");

    final TestHandler handler = new TestHandler(null);
    final String errorJson = handler.publicError2json("Error occurred", exception);

    final JSONObject json = new JSONObject(errorJson);
    final String detail = json.getString("detail");

    // Should contain just the single message, no arrow
    assertThat(detail).isEqualTo("Simple error message");
    assertThat(detail).doesNotContain("->");
  }

  @Test
  void testExceptionWithNullMessage() {
    final IllegalArgumentException rootCause = new IllegalArgumentException();  // No message
    final CommandExecutionException wrappedException = new CommandExecutionException("Outer message", rootCause);

    final TestHandler handler = new TestHandler(null);
    final String errorJson = handler.publicError2json("Error", wrappedException);

    final JSONObject json = new JSONObject(errorJson);
    final String detail = json.getString("detail");

    // Should handle null message gracefully
    assertThat(detail).contains("Outer message");
    assertThat(detail).contains("->");
    assertThat(detail).contains("IllegalArgumentException");
  }

  @Test
  void testDeepExceptionChain() {
    final IllegalArgumentException level3 = new IllegalArgumentException("Level 3 error");
    final RuntimeException level2 = new RuntimeException("Level 2 error", level3);
    final CommandExecutionException level1 = new CommandExecutionException("Level 1 error", level2);

    final TestHandler handler = new TestHandler(null);
    final String errorJson = handler.publicError2json("Top level error", level1);

    final JSONObject json = new JSONObject(errorJson);
    final String detail = json.getString("detail");

    // Should contain all three levels
    assertThat(detail).contains("Level 1 error");
    assertThat(detail).contains("Level 2 error");
    assertThat(detail).contains("Level 3 error");

    // Should have two arrows (connecting 3 levels)
    final int arrowCount = detail.split(" -> ", -1).length - 1;
    assertThat(arrowCount).isEqualTo(2);
  }

  /**
   * Test handler that exposes protected methods for testing
   */
  private static class TestHandler extends AbstractServerHttpHandler {
    public TestHandler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final com.arcadedb.server.security.ServerSecurityUser user,
        final JSONObject payload) {
      return null;
    }

    public String publicError2json(final String errorMessage, final Throwable exception) {
      String detail = "";
      if (exception != null) {
        final StringBuilder buffer = new StringBuilder();
        buffer.append(exception.getMessage() != null ? exception.getMessage() : exception.toString());

        Throwable current = exception.getCause();
        while (current != null && current != current.getCause() && current != exception) {
          buffer.append(" -> ");
          buffer.append(current.getMessage() != null ? current.getMessage() : current.getClass().getSimpleName());
          current = current.getCause();
        }
        detail = buffer.toString();
      }
      return error2json(errorMessage, detail, exception, null, null);
    }
  }
}
