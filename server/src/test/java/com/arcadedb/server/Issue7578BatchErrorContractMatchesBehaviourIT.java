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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7578, the half the sweeps cannot do: a {@code required} list is a claim about what a HANDLER sends, so
 * the only way to know it is true is to make the handler send one.
 * <p>
 * {@code BatchError} is the schema where getting it wrong was easiest, and where this PR did get it wrong first
 * time round (code review on PR #7749). It is bound to two statuses, not one: {@code POST /batch} answers 400
 * for a line it refuses and 408 for a body that did not arrive whole, and the two are built by different methods.
 * {@code exception} is unconditional on the 400 and absent on two of the three 408 paths - a body that simply
 * ended before its announced length, and a malformed record that turned out to be a cut upload - because
 * {@code partialPayloadResponse} writes it only when there is a class to name.
 * <p>
 * Both statuses are driven here and every name the schema requires is checked against what actually arrived. The
 * expectation is read out of {@link CoreApiSpec}'s own document, so the assertion cannot drift from the thing it
 * is checking: adding a name to the required list and not to the response fails this test.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7578">issue #7578</a>
 */
class Issue7578BatchErrorContractMatchesBehaviourIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  /** The names {@code BatchError} promises, read from the document rather than written out here. */
  private static List<String> requiredByTheDocument() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);

    final Schema<?> batchError = openAPI.getComponents().getSchemas().get("BatchError");
    assertThat(batchError).as("BatchError must be registered").isNotNull();
    assertThat(batchError.getRequired()).as("and it must declare what it always sends").isNotEmpty();
    return batchError.getRequired();
  }

  private static void assertCarriesEveryRequiredName(final JSONObject body) {
    for (final String required : requiredByTheDocument())
      assertThat(body.has(required))
          .as("BatchError promises '%s' on every failure; the response was %s", required, body)
          .isTrue();
  }

  /** The 400: a line the parser refuses. Here `exception` IS present, which is why it reads as unconditional. */
  @Test
  void aRefusedLineCarriesEveryNameTheSchemaRequires() throws Exception {
    createType("Batch7578A");

    final JSONObject body = postWholeBody(400,
        "{\"@type\":\"vertex\",\"@class\":\"Batch7578A\",\"@id\":\"p1\",\"@rid\":\"#1:0\"}\n");

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("a refused line is an IllegalArgumentException, so the 400 does name a class")
        .isTrue();
  }

  /**
   * The 408 that makes {@code exception} conditional, and the reason this test exists at all.
   * <p>
   * A line the parser cannot read is normally a 400 naming it - but when the body ALSO ended before its
   * announced length, {@code PostBatchHandler} reports the load as truncated instead, because a cut upload hands
   * the parser stale bytes that look like a malformed line the client never sent (issue #5470). That branch
   * calls {@code truncatedBody} with a null exception class: nothing threw, the parser simply refused a line, so
   * there is no class to name and {@code partialPayloadResponse} omits the member.
   * <p>
   * Reached by announcing more bytes than are sent and then half-closing. The parser refuses line one on its own
   * content - it never reaches a read that could raise - and only THEN does {@code bodyEndedEarly} probe and find
   * the peer gone. That ordering is what separates this path from the one below, where the line is valid, the
   * parser loops for more input, and Undertow raises the I/O failure itself.
   */
  @Test
  void aRefusedLineOnACutUploadCarriesNoExceptionWhichIsWhyItIsNotRequired() throws Exception {
    createType("Batch7578B");

    // A JSON array, which the parser refuses on sight as a MalformedBatchRecordException.
    final String sent = "[{\"@type\":\"vertex\",\"@class\":\"Batch7578B\"}]\n";
    final JSONObject body = post(sent, sent.length() + 512, 408, true);

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("nothing threw - the parser refused a line and the body was short, so there is no class to name. "
            + "This is why 'exception' must not be in BatchError's required list. The response was %s", body)
        .isFalse();
    assertThat(body.getString("error")).contains("truncated");
  }

  /**
   * The other 408 path, kept as the contrast: when the line is VALID, the parser loops for the bytes that never
   * arrive, Undertow raises an {@link java.io.IOException} itself, and the same response DOES name it.
   * <p>
   * So {@code exception} is not "absent on the 408" either - it is present on one of that status's three paths
   * and absent on the other two, which is exactly the shape a {@code required} list cannot express and must
   * therefore leave out.
   */
  @Test
  void aPeerThatGoesAwayMidBodyDoesNameItsException() throws Exception {
    createType("Batch7578C");

    final String sent = "{\"@type\":\"vertex\",\"@class\":\"Batch7578C\",\"@id\":\"p1\",\"name\":\"Alice\"}\n";
    final JSONObject body = post(sent, sent.length() + 512, 408, true);

    assertCarriesEveryRequiredName(body);
    assertThat(body.has("exception"))
        .as("the peer closing mid-body IS an IOException, so this path names a class. The response was %s", body)
        .isTrue();
    assertThat(body.getString("error")).contains("truncated");
  }

  private void createType(final String vertexType) {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType(vertexType))
      db.getSchema().createVertexType(vertexType);
  }

  private String host() {
    return "127.0.0.1";
  }

  private int port() {
    return getServer(0).getHttpServer().getPort();
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private JSONObject postWholeBody(final int expectedStatus, final String payload) throws Exception {
    return post(payload, payload.getBytes(StandardCharsets.UTF_8).length, expectedStatus, false);
  }

  /**
   * @param halfClose true to shut the write side down after the body, which makes Undertow report the peer as
   *                  gone and raise an IOException; false to leave it open, so a parser that refuses a line does
   *                  so without anything throwing
   */
  private JSONObject post(final String payload, final int contentLength, final int expectedStatus,
      final boolean halfClose) throws Exception {
    final byte[] sent = payload.getBytes(StandardCharsets.UTF_8);

    try (final Socket socket = new Socket(host(), port())) {
      socket.setSoTimeout(30_000);
      final OutputStream out = socket.getOutputStream();
      out.write(("POST /api/v1/batch/" + getDatabaseName() + " HTTP/1.1\r\n"
          + "Host: " + host() + ":" + port() + "\r\n"
          + "Authorization: " + authorization() + "\r\n"
          + "Content-Type: application/x-ndjson\r\n"
          + "Content-Length: " + contentLength + "\r\n"
          + "Connection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
      out.write(sent);
      out.flush();
      if (halfClose)
        // The server sees the peer go away mid-body, which Undertow reports as an IOException.
        socket.shutdownOutput();

      return readResponse(socket, expectedStatus);
    }
  }

  private static JSONObject readResponse(final Socket socket, final int expectedStatus) throws Exception {
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

    final String statusLine = reader.readLine();
    assertThat(statusLine).as("the server must answer at all").isNotNull();

    String line;
    boolean chunked = false;
    while ((line = reader.readLine()) != null && !line.isEmpty())
      if (line.toLowerCase().startsWith("transfer-encoding:") && line.toLowerCase().contains("chunked"))
        chunked = true;

    final StringBuilder body = new StringBuilder();
    while ((line = reader.readLine()) != null)
      body.append(line);

    final String raw = body.toString();
    // The JSON object is the only thing on the body, whether or not chunk sizes are interleaved around it.
    final int start = raw.indexOf('{');
    final int end = raw.lastIndexOf('}');
    assertThat(start).as("response carried no JSON body: status '%s', raw '%s'", statusLine, raw).isNotNegative();

    assertThat(statusLine)
        .as("expected %d, body was %s", expectedStatus, raw)
        .contains(" " + expectedStatus + " ");
    assertThat(chunked || !raw.isEmpty()).isTrue();

    return new JSONObject(raw.substring(start, end + 1));
  }
}
