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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CoreApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void batchDeclaresStreamingBodyMediaTypes() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getOperationId()).isEqualTo("executeBatch");
    assertThat(post.getTags()).containsExactly("Batch");
    assertThat(post.getRequestBody().getContent().keySet())
        .as("the body is streamed JSONL or CSV and is never parsed as JSON")
        .containsExactlyInAnyOrder("application/x-ndjson", "application/jsonl", "text/csv");
  }

  @Test
  void batchDeclaresTheDocumentedQueryParameters() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getParameters().stream().map(Parameter::getName).toList())
        .contains("database", "batchSize", "refMode", "commitEvery", "vertexBatchSize",
            "expectedRecords", "ordinalBase", "idMapping", "lightEdges", "wal", "parallelFlush",
            "preAllocateEdgeChunks", "edgeListInitialSize", "bidirectional", "expectedEdgeCount",
            "commitRetries", "commitRetryDelayMs", "expectedVertexCount");
  }

  @Test
  void batchDeclaresTruncationTimeout() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getResponses().keySet())
        .as("a body that ends early is answered 408 with the partial-commit counts, never 200")
        .contains("200", "400", "408");
  }

  @Test
  void batchResponseCarriesTheCountsAndIdMapping() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "verticesCreated", "edgesCreated", "elapsedMs",
        "bytesRead", "linesRead", "linesSkipped", "verticesWithoutId",
        "idMapping", "idMappingOmitted", "idMappingSize");
  }

  @Test
  void batchFailuresCarryThePartialCommitCountsNotTheGenericError() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "exception", "requestId", "verticesCreated", "edgesCreated", "partialCommit",
        "bytesRead", "linesRead", "linesSkipped", "verticesWithoutId");

    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    for (final String code : List.of("400", "408")) {
      assertThat(post.getResponses().get(code).getContent().get("application/json")
          .getSchema().get$ref())
          .as("a client cannot reconcile after a failed load without these counts, so %s "
              + "must not point at the generic error body", code)
          .isEqualTo("#/components/schemas/BatchError");
    }
  }

  @Test
  void progressResponseMatchesOperationProgressJson() {
    final Operation get = openAPI.getPaths().get("/api/v1/progress/{database}").getGet();
    assertThat(get.getOperationId()).isEqualTo("getOperationProgress");

    final Schema<?> entry = openAPI.getComponents().getSchemas().get("ProgressResponse");
    assertThat(entry.getProperties()).containsKey("result");
    final Schema<?> item = entry.getProperties().get("result").getItems();
    assertThat(item.getProperties().keySet()).containsExactlyInAnyOrder(
        "id", "database", "operation", "stepName", "stepIndex", "totalSteps",
        "done", "total", "percentage", "startedOn", "elapsedMs");
  }

  @Test
  void existingOperationIdsAreUnchanged() {
    assertThat(openAPI.getPaths().get("/api/v1/query/{database}").getPost().getOperationId())
        .isEqualTo("executeQueryPost");
    assertThat(openAPI.getPaths().get("/api/v1/server").getGet().getOperationId())
        .isEqualTo("getServerInfo");
  }

  @Test
  void checkDatabaseExistsDeclaresOnlyTheStatusCodesTheHandlerCanReturn() {
    final Operation get = openAPI.getPaths().get("/api/v1/exists/{database}").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkDatabaseExists");

    assertThat(get.getResponses().keySet())
        .as("the handler returns 200 always, and 400 only when the database parameter is missing; "
            + "it never returns 404")
        .containsExactlyInAnyOrder("200", "400", "401", "500");

    final Schema<?> okSchema = get.getResponses().get("200").getContent().get("application/json").getSchema();
    assertThat(okSchema.get$ref())
        .as("a client needs a typed boolean to read the result, not just the status code")
        .isEqualTo("#/components/schemas/DatabaseExists");

    final Schema<?> databaseExists = openAPI.getComponents().getSchemas().get("DatabaseExists");
    assertThat(databaseExists.getProperties().keySet()).containsExactly("result");
  }

  @Test
  void checkHealthNeverDeclaresA200() {
    final Operation get = openAPI.getPaths().get("/api/v1/health").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkHealth");
    assertThat(get.getResponses().keySet())
        .as("GetHealthHandler.execute only ever returns 204")
        .containsExactly("204");
  }

  @Test
  void checkReadyNeverDeclaresA200() {
    final Operation get = openAPI.getPaths().get("/api/v1/ready").getGet();
    assertThat(get.getOperationId()).isEqualTo("checkReady");
    assertThat(get.getResponses().keySet())
        .as("GetReadyHandler.execute only ever returns 204 or 503")
        .containsExactlyInAnyOrder("204", "503");
  }

  @Test
  void beginDeclaresTheSessionIdResponseHeader() {
    final Operation post = openAPI.getPaths().get("/api/v1/begin/{database}").getPost();

    assertThat(post.getResponses().get("204").getHeaders())
        .as("a client that cannot read the session id cannot use the transaction it just opened")
        .containsKey("arcadedb-session-id");
  }

  @Test
  void beginDeclaresTheSessionIdRequestHeaderAsOptional() {
    final Operation post = openAPI.getPaths().get("/api/v1/begin/{database}").getPost();
    final Parameter header = post.getParameters().stream()
        .filter(p -> "arcadedb-session-id".equals(p.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no session header declared on /api/v1/begin/{database}"));

    assertThat(header.getIn())
        .as("PostBeginHandler reads the session id from a request header, not a query parameter")
        .isEqualTo("header");
    assertThat(header.getRequired())
        .as("a client cannot otherwise know what triggers the 409: supplying an id that still "
            + "resolves is what triggers it, not omitting the header")
        .isFalse();
  }

  @Test
  void commitAndRollbackDeclareTheSessionIdRequestHeader() {
    for (final String path : List.of("/api/v1/commit/{database}", "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      final Parameter header = post.getParameters().stream()
          .filter(p -> "arcadedb-session-id".equals(p.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no session header declared on " + path));

      assertThat(header.getIn())
          .as("the session id travels as a header on " + path)
          .isEqualTo("header");
    }
  }

  @Test
  void queryAndCommandAcceptAnOptionalSessionIdHeader() {
    for (final String path : List.of("/api/v1/query/{database}", "/api/v1/command/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      final Parameter header = post.getParameters().stream()
          .filter(p -> "arcadedb-session-id".equals(p.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no session header declared on " + path));

      assertThat(header.getRequired())
          .as("running outside a transaction must remain legal on " + path)
          .isFalse();
    }
  }

  @Test
  void transactionOperationsDeclare204AndNot200() {
    for (final String path : List.of("/api/v1/begin/{database}", "/api/v1/commit/{database}",
        "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();

      assertThat(post.getResponses())
          .as("PostBeginHandler, PostCommitHandler and PostRollbackHandler all return "
              + "'new ExecutionResponse(204, \"\")', so " + path + " must declare 204")
          .containsKey("204");
      assertThat(post.getResponses())
          .as(path + " must not declare 200: the handler never returns it, and a generated client "
              + "would deserialize a body that is never sent")
          .doesNotContainKey("200");
    }
  }

  @Test
  void beginDeclaresA409ForAnAlreadyOpenSession() {
    final Operation post = openAPI.getPaths().get("/api/v1/begin/{database}").getPost();

    assertThat(post.getResponses())
        .as("PostBeginHandler returns 409 when the request carries a session id that still resolves")
        .containsKey("409");
  }

  @Test
  void commitAndRollbackDoNotAdvertiseTheSessionIdResponseHeader() {
    for (final String path : List.of("/api/v1/commit/{database}", "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();

      assertThat(post.getResponses().get("204").getHeaders())
          .as("PostCommitHandler/PostRollbackHandler strip the session header before responding on "
              + path + "; advertising it here would be a lie only 'begin' is entitled to make")
          .isNullOrEmpty();
    }
  }

  @Test
  void commitAndRollbackDoNotDeclareA409() {
    for (final String path : List.of("/api/v1/commit/{database}", "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();

      assertThat(post.getResponses())
          .as("only 'begin' can answer 409 for an already-open session; " + path
              + " must not share that response with it via a memoized createTransactionResponses()")
          .doesNotContainKey("409");
    }
  }

  @Test
  void commitAndRollbackWarnThatALostOrStaleSessionIdIsASilentNoOp() {
    for (final String path : List.of("/api/v1/commit/{database}", "/api/v1/rollback/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();
      final Parameter header = post.getParameters().stream()
          .filter(p -> "arcadedb-session-id".equals(p.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("no session header declared on " + path));

      assertThat(header.getDescription())
          .as(path + " must warn that an omitted or stale session id still answers 204 while "
              + "committing or rolling back nothing, or a generated client will report success on data loss")
          .contains("no-op");
    }
  }

  @Test
  void queryAndCommand404CoversAStaleSessionIdNotOnlyAMissingDatabase() {
    for (final String path : List.of("/api/v1/query/{database}", "/api/v1/command/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();

      assertThat(post.getResponses().get("404").getDescription())
          .as(path + " also answers 404 for a stale session id, not only a missing database")
          .contains("Remote transaction session not found or expired");
    }
  }

  /**
   * Issue #7569: both operations declare 'application/x-ndjson' under their 200, but
   * PostCommandHandler.requireStreamableStatement() refuses to stream a statement that is not
   * provably read-only with a 400 before it ever runs - and nothing in the contract said so. A
   * client author reading the contract alone had no way to know the restriction existed short of
   * sending a mutating statement at a live server and reading the 400.
   * <p>
   * Checked on both /command and /query: PostQueryHandler extends PostCommandHandler and overrides
   * only executeCommand(), so the very same requireStreamableStatement() call at execute() time
   * guards both operations identically (the same sharing this file already pins down for 'limit'
   * in commandRequestDeclaresLimitMatchingQueryRequest).
   */
  @Test
  void commandAndQueryDescribeTheReadOnlyStreamingRestriction() {
    // The literal text CoreApiSpec appends to both operations' description. Duplicated here rather
    // than referencing the (private) production constant, matching how this file already pins other
    // shared text verbatim (e.g. STALE_SESSION_404_DESCRIPTION's message in
    // queryAndCommand404CoversAStaleSessionIdNotOnlyAMissingDatabase).
    final String restriction = "only a statement provably read-only may stream";

    final Operation command = openAPI.getPaths().get("/api/v1/command/{database}").getPost();
    final Operation query = openAPI.getPaths().get("/api/v1/query/{database}").getPost();

    for (final Operation operation : List.of(command, query)) {
      assertThat(operation.getDescription())
          .as(operation.getOperationId() + " must warn that the ndjson encoding is refused before "
              + "it runs for a statement that is not provably read-only")
          .contains(restriction)
          .contains("400");
    }

    final String commandSuffix = command.getDescription().substring(command.getDescription().indexOf(restriction));
    final String querySuffix = query.getDescription().substring(query.getDescription().indexOf(restriction));
    assertThat(commandSuffix)
        .as("both operations run through the exact same requireStreamableStatement() check, so the "
            + "restriction text must not diverge between them and confuse a reader of the generated docs")
        .isEqualTo(querySuffix);
  }

  @Test
  void commandRequestDeclaresLanguageAsRequired() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("CommandRequest");

    assertThat(schema.getProperties())
        .as("PostCommandHandler.execute rejects a request with no language (400 \"Language is null\"), "
            + "so a client generated strictly from the contract must be told to send it")
        .containsKey("language");
    assertThat(schema.getRequired())
        .as("PostCommandHandler.execute treats a missing or empty language exactly like a missing "
            + "command: both fail requireStringField / the explicit null-or-empty check with a 400")
        .contains("language");
  }

  @Test
  void commandRequestDeclaresLimitMatchingQueryRequest() {
    // Issue #6584: PostQueryHandler extends PostCommandHandler and overrides only executeCommand(),
    // so both /api/v1/query/{database} and /api/v1/command/{database} run through the very same
    // execute() and honor an optional 'limit' field identically. QueryRequest documents it; CommandRequest
    // did not, leaving a client generated strictly from the contract with no typed way to cap a command's
    // result set even though the server supports it.
    final Schema<?> commandRequest = openAPI.getComponents().getSchemas().get("CommandRequest");
    final Schema<?> queryRequest = openAPI.getComponents().getSchemas().get("QueryRequest");

    assertThat(commandRequest.getProperties())
        .as("PostCommandHandler.execute() reads an optional 'limit' field for both the command and "
            + "query endpoints, so CommandRequest must declare it exactly like QueryRequest does")
        .containsKey("limit");

    final Schema<?> commandLimit = (Schema<?>) commandRequest.getProperties().get("limit");
    final Schema<?> queryLimit = (Schema<?>) queryRequest.getProperties().get("limit");

    assertThat(commandLimit.getType())
        .as("CommandRequest.limit must be typed the same as QueryRequest.limit: both feed the same "
            + "optionalIntField(requestMap, \"limit\") call")
        .isEqualTo(queryLimit.getType());
    assertThat(commandLimit.getDescription())
        .as("the two endpoints share the exact same limit/truncation semantics, so the contract text "
            + "must not diverge and confuse a reader of the generated docs")
        .isEqualTo(queryLimit.getDescription());
  }

  @Test
  void getQuery404DoesNotClaimTheStaleSessionCasePostAndCommandDo() {
    final Operation get = openAPI.getPaths().get("/api/v1/query/{database}/{language}/{command}").getGet();

    assertThat(get.getResponses().get("404").getDescription())
        .as("GetQueryHandler.requiresTransaction() returns false, so a stale session id degrades "
            + "session-less and answers 200, never 404: the GET query 404 must not claim otherwise")
        .doesNotContain("Remote transaction session not found or expired");

    for (final String path : List.of("/api/v1/query/{database}", "/api/v1/command/{database}")) {
      final Operation post = openAPI.getPaths().get(path).getPost();

      assertThat(post.getResponses().get("404").getDescription())
          .as(path + " has no requiresTransaction() override, so its 404 must still cover the "
              + "stale-session case")
          .contains("Remote transaction session not found or expired");
    }
  }

  /**
   * Issue #7351: the read-your-writes bookmark is emitted on the streamed encoding as well as the buffered one,
   * so it belongs on the response rather than on either media type - and a generated client has no way to know
   * it exists unless the document says so.
   * <p>
   * Asserted in both directions, because the document can be wrong either way. The response-commit listener is
   * registered partway through {@code DatabaseAbstractHandler.execute}, once the request has been authenticated
   * and its database resolved, so a 401 or a 404 for a database that does not exist is decided before the
   * bookmark exists and can never carry it. Declaring it there would be the same doc/runtime mismatch this test
   * exists to prevent, only pointing the other way.
   */
  @Test
  void theQueryOperationsDeclareTheBookmarkExactlyWhereItCanBeSent() {
    final List<Operation> operations = List.of(
        openAPI.getPaths().get("/api/v1/query/{database}/{language}/{command}").getGet(),
        openAPI.getPaths().get("/api/v1/query/{database}").getPost(),
        openAPI.getPaths().get("/api/v1/command/{database}").getPost());

    for (final Operation operation : operations) {
      assertThat(operation.getResponses().keySet())
          .as("the statuses below are the ones this assertion is written against")
          .contains("200", "400", "401", "404", "500");

      for (final String status : operation.getResponses().keySet()) {
        final Header bookmark = operation.getResponses().get(status).getHeaders() == null
            ? null
            : operation.getResponses().get(status).getHeaders().get("X-ArcadeDB-Commit-Index");

        if ("401".equals(status) || "404".equals(status)) {
          assertThat(bookmark)
              .as("%s answers %s before the request reaches the database, so it never carries the bookmark and "
                  + "must not promise it", operation.getOperationId(), status)
              .isNull();
          continue;
        }

        assertThat(bookmark)
            .as("%s must declare the read-your-writes bookmark on its %s: the listener emits it whatever the "
                + "outcome once the request is inside the database, and a document that named only the 200 "
                + "would hide that", operation.getOperationId(), status)
            .isNotNull();
        assertThat(bookmark.getDescription())
            .as("the description has to name the request-side header the value is fed back as, or a client "
                + "cannot act on it")
            .contains("X-ArcadeDB-Read-After");
      }
    }
  }
}
