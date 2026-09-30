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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupportPortalClientTest {
  private static final String INSTANCE = "adb-123e4567-e89b-12d3-a456-426614174000";

  @TempDir
  Path dir;

  private MockPortal portal;

  @BeforeEach
  void start() throws IOException {
    portal = new MockPortal();
  }

  @AfterEach
  void stop() {
    portal.close();
  }

  private SupportPortalClient client() {
    return new SupportPortalClient(portal.registration(), INSTANCE, 1L);
  }

  private void failWith(final int status, final String body, final Map<String, String> headers) {
    portal.handler = r -> new MockPortal.Response(status, body, headers);
  }

  private SupportPortalException catchPortal(final Runnable call) {
    final Throwable[] caught = new Throwable[1];
    try {
      call.run();
    } catch (final Throwable t) {
      caught[0] = t;
    }
    assertThat(caught[0]).isInstanceOf(SupportPortalException.class);
    return (SupportPortalException) caught[0];
  }

  @Test
  void whoamiSendsTheContractHeaders() {
    final JSONObject who = new JSONObject(client().whoami());
    assertThat(who.getJSONObject("workspace").getString("name")).isEqualTo("Acme Corp");
    assertThat(who.getJSONObject("plan").getBoolean("entitled")).isTrue();

    final MockPortal.Recorded r = portal.last();
    assertThat(r.method()).isEqualTo("GET");
    assertThat(r.path()).isEqualTo("/api/v1/support/whoami");
    assertThat(r.header("Authorization")).isEqualTo("Bearer " + MockPortal.KEY);
    assertThat(r.header("X-Client-Id")).isEqualTo(MockPortal.CLIENT_ID);
    assertThat(r.header("X-Instance-Id")).isEqualTo(INSTANCE);
    assertThat(r.header("User-Agent")).matches("ArcadeDB/\\S+ support-client");
  }

  @Test
  void instanceHeaderIsOptional() {
    new SupportPortalClient(portal.registration(), null, 1L).whoami();
    assertThat(portal.last().header("X-Instance-Id")).isNull();
  }

  @Test
  void listGetCommentAndCloseReopen() {
    final SupportPortalClient client = client();

    assertThat(client.listIssues("closed")).contains("\"Slow\"");
    assertThat(portal.last().path()).isEqualTo("/api/v1/support/issues");
    assertThat(portal.last().query()).isEqualTo("status=closed");
    client.listIssues(null);
    assertThat(portal.last().query()).isEqualTo("status=open");
    assertThatThrownBy(() -> client.listIssues("../x")).isInstanceOf(IllegalArgumentException.class);

    assertThat(new JSONObject(client.getIssue(42)).getInt("number")).isEqualTo(42);
    assertThat(portal.last().path()).isEqualTo("/api/v1/support/issues/42");

    assertThat(new JSONObject(client.addComment(42, "thanks \"quoted\"")).getString("side")).isEqualTo("client");
    assertThat(portal.last().method()).isEqualTo("POST");
    assertThat(portal.last().path()).isEqualTo("/api/v1/support/issues/42/comments");
    assertThat(portal.last().header("Content-Type")).isEqualTo("application/json");
    assertThat(new JSONObject(portal.last().bodyText()).getString("body")).isEqualTo("thanks \"quoted\"");

    client.setOpen(42, false);
    assertThat(portal.last().method()).isEqualTo("PUT");
    assertThat(new JSONObject(portal.last().bodyText()).getBoolean("open")).isFalse();
    client.setOpen(42, true);
    assertThat(new JSONObject(portal.last().bodyText()).getBoolean("open")).isTrue();
  }

  @Test
  void createIssueSendsAMultipartWithTheFiles() throws Exception {
    final Path logs = Files.write(dir.resolve("logs.zip"), "ZIPDATA".getBytes(StandardCharsets.UTF_8));
    final Path diagnostics = Files.writeString(dir.resolve("d.json"), "{\"schema\":1}");
    final Path summary = Files.writeString(dir.resolve("s.json"), "{\"schema\":1,\"lines\":3}");
    final Path threads = Files.writeString(dir.resolve("t.txt"), "\"main\" #1 RUNNABLE");

    final JSONObject metadata = new JSONObject().put("title", "Slow queries").put("body", "details").put("severity", "S2")
        .put("source", "studio");
    final JSONObject answer = new JSONObject(client().createIssue(metadata, logs, diagnostics, summary, threads));
    assertThat(answer.getInt("number")).isEqualTo(42);
    assertThat(answer.getString("url")).endsWith("/#/issues/42");

    final MockPortal.Recorded r = portal.last();
    assertThat(r.method()).isEqualTo("POST");
    assertThat(r.path()).isEqualTo("/api/v1/support/issues");
    assertThat(r.header("Content-Type")).startsWith("multipart/form-data; boundary=");
    assertThat(r.header("Content-Length")).isEqualTo(String.valueOf(r.bodyLength()));
    assertThat(r.header("Transfer-Encoding")).isNull();

    final String body = r.bodyText();
    assertThat(body).contains("name=\"metadata\"").contains("Content-Type: application/json").contains("\"title\":\"Slow queries\"");
    assertThat(body).contains("name=\"logs\"; filename=\"logs.zip\"").contains("Content-Type: application/zip").contains("ZIPDATA");
    assertThat(body).contains("name=\"diagnostics\"; filename=\"diagnostics.json\"").contains("{\"schema\":1}");
    assertThat(body).contains("name=\"summary\"; filename=\"summary.json\"");
    assertThat(body).contains("name=\"threads\"; filename=\"threads.txt\"").contains("Content-Type: text/plain");
    final String boundary = r.header("Content-Type").substring(r.header("Content-Type").indexOf("boundary=") + 9);
    assertThat(body).endsWith("--" + boundary + "--\r\n");
  }

  @Test
  void createIssueWithoutFilesSendsOnlyTheMetadata() throws Exception {
    client().createIssue(new JSONObject().put("title", "t").put("body", "").put("severity", "S4"), null, null, null, null);
    assertThat(portal.last().bodyText()).contains("name=\"metadata\"").doesNotContain("name=\"logs\"");
  }

  @Test
  void attachmentsGoToTheIssue() throws Exception {
    final Path logs = Files.write(dir.resolve("logs.zip"), new byte[] { 1, 2, 3 });
    assertThat(client().addAttachments(42, logs, null, null, null)).contains("logs.zip");
    assertThat(portal.last().path()).isEqualTo("/api/v1/support/issues/42/attachments");
    assertThatThrownBy(() -> client().addAttachments(42, null, null, null, null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aLargeFileIsStreamedFromDiskWithAKnownLength() throws Exception {
    // 40 MB, written in blocks: neither the test nor the client holds it in memory
    final Path big = dir.resolve("logs.zip");
    final byte[] block = new byte[1024 * 1024];
    java.util.Arrays.fill(block, (byte) 'x');
    try (final OutputStream out = Files.newOutputStream(big)) {
      for (int i = 0; i < 40; i++)
        out.write(block);
    }

    client().createIssue(new JSONObject().put("title", "big").put("body", "").put("severity", "S3"), big, null, null, null);

    final MockPortal.Recorded r = portal.last();
    assertThat(r.bodyLength()).isGreaterThan(40L * 1024 * 1024);
    assertThat(r.header("Content-Length")).isEqualTo(String.valueOf(r.bodyLength()));
    assertThat(r.header("Transfer-Encoding")).isNull();
  }

  @Test
  void multipartBodyIsReadInPiecesAndCanBeReadAgain() throws Exception {
    final Path file = Files.writeString(dir.resolve("a.json"), "0123456789".repeat(1000));
    final SupportPortalClient.Multipart body = new SupportPortalClient.Multipart("B", List.of(
        SupportPortalClient.Part.json("metadata", "{}"), SupportPortalClient.Part.file("summary", "summary.json", "application/json", file)));
    for (int round = 0; round < 2; round++) {
      long total = 0;
      try (final InputStream in = body.open()) {
        final byte[] buffer = new byte[100];
        int n;
        while ((n = in.read(buffer)) >= 0) {
          assertThat(n).isLessThanOrEqualTo(100);
          total += n;
        }
      }
      assertThat(total).isEqualTo(body.length());
    }
  }

  @Test
  void errorCodesAreMappedToClearMessages() {
    final SupportPortalClient client = client();

    failWith(401, MockPortal.error("invalid_key", "nope"), Map.of());
    SupportPortalException e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("invalid_key");
    assertThat(e.getMessage()).contains("rejected the Client key");
    assertThat(e.getPortalStatus()).isEqualTo(401);
    // Studio must never receive a 401/403
    assertThat(e.getStudioStatus()).isEqualTo(502);

    failWith(403, MockPortal.error("client_mismatch", "x"), Map.of());
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("client_mismatch");
    assertThat(e.getMessage()).contains("Client ID");
    assertThat(e.getStudioStatus()).isEqualTo(502);

    failWith(403, MockPortal.error("scope_denied", "x"), Map.of());
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("scope_denied");
    assertThat(e.getMessage()).contains("support:create");

    failWith(402, MockPortal.error("support_not_active", "plan lapsed"), Map.of());
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("support_not_active");
    assertThat(e.getStudioStatus()).isEqualTo(402);
    assertThat(e.getMessage()).contains("not active").contains("arcadedb.com/pricing.html");

    failWith(413, MockPortal.error("too_large", "x"), Map.of());
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("too_large");
    assertThat(e.getStudioStatus()).isEqualTo(413);
    assertThat(e.getMessage()).contains("Narrow the log window");

    failWith(429, MockPortal.error("rate_limited", "x"), Map.of("Retry-After", "120"));
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("rate_limited");
    assertThat(e.getRetryAfterSeconds()).isEqualTo(120);
    assertThat(e.getMessage()).contains("120 seconds");
    assertThat(e.getStudioStatus()).isEqualTo(429);

    failWith(404, MockPortal.error("not_found", "x"), Map.of());
    e = catchPortal(() -> client.getIssue(7));
    assertThat(e.getCode()).isEqualTo("not_found");
    assertThat(e.getStudioStatus()).isEqualTo(404);

    failWith(400, MockPortal.error("bad_request", "title is too long"), Map.of());
    e = catchPortal(client::whoami);
    assertThat(e.getCode()).isEqualTo("bad_request");
    assertThat(e.getMessage()).contains("title is too long");
    assertThat(e.getStudioStatus()).isEqualTo(400);
  }

  @Test
  void statusWithoutABodyIsMappedFromTheStatusCode() {
    failWith(402, "", Map.of());
    assertThat(catchPortal(client()::whoami).getCode()).isEqualTo("support_not_active");
    failWith(401, "<html>Unauthorized</html>", Map.of());
    assertThat(catchPortal(client()::whoami).getCode()).isEqualTo("invalid_key");
    failWith(500, "boom", Map.of());
    final SupportPortalException e = catchPortal(client()::whoami);
    assertThat(e.getCode()).isEqualTo("portal_error");
    assertThat(e.getMessage()).contains("HTTP 500");
  }

  @Test
  void anUnknownErrorCodeBecomesPortalError() {
    failWith(400, MockPortal.error("something_new", "x"), Map.of());
    assertThat(catchPortal(client()::whoami).getCode()).isEqualTo("portal_error");
  }

  @Test
  void theKeyIsScrubbedEvenWhenThePortalEchoesIt() {
    failWith(400, MockPortal.error("bad_request", "invalid token " + MockPortal.KEY + " for " + MockPortal.KEY), Map.of());
    final SupportPortalException e = catchPortal(client()::whoami);
    assertThat(e.getMessage()).doesNotContain(MockPortal.KEY).doesNotContain("wsk_");

    portal.handler = r -> new MockPortal.Response(200, "{\"echo\":\"" + MockPortal.KEY + "\"}");
    assertThat(client().listIssues("open")).doesNotContain(MockPortal.KEY).contains("***");
  }

  @Test
  void retriesOnceOnATransientError() {
    final AtomicInteger calls = new AtomicInteger();
    portal.handler = r -> calls.incrementAndGet() == 1 ? new MockPortal.Response(503, "") : new MockPortal.Response(200, "{\"ok\":true}");
    assertThat(client().whoami()).contains("ok");
    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  void givesUpAfterTheRetry() {
    final AtomicInteger calls = new AtomicInteger();
    portal.handler = r -> {
      calls.incrementAndGet();
      return new MockPortal.Response(503, MockPortal.error("portal_down", "later"));
    };
    final SupportPortalException e = catchPortal(client()::whoami);
    assertThat(calls.get()).isEqualTo(2);
    assertThat(e.getCode()).isEqualTo("portal_error");
  }

  @Test
  void aPostIsNotRetriedOnAServerErrorThatMayHaveBeenProcessed() throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    portal.handler = r -> {
      calls.incrementAndGet();
      return new MockPortal.Response(502, "");
    };
    assertThatThrownBy(() -> client().createIssue(new JSONObject().put("title", "t").put("severity", "S4"), null, null, null, null))
        .isInstanceOf(SupportPortalException.class);
    assertThat(calls.get()).isEqualTo(1);

    calls.set(0);
    catchPortal(() -> client().addComment(1, "x"));
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void aGetIsRetriedOnABadGateway() {
    final AtomicInteger calls = new AtomicInteger();
    portal.handler = r -> calls.incrementAndGet() == 1 ? new MockPortal.Response(502, "") : new MockPortal.Response(200, "[]");
    assertThat(client().listIssues("all")).isEqualTo("[]");
  }

  @Test
  void anUnreachablePortalIsReportedClearly() {
    final SupportConfiguration.Registration registration = portal.registration();
    portal.close();
    final SupportPortalException e = catchPortal(new SupportPortalClient(registration, INSTANCE, 1L)::whoami);
    assertThat(e.getCode()).isEqualTo("portal_unreachable");
    assertThat(e.getStudioStatus()).isEqualTo(503);
    assertThat(e.getMessage()).contains("Cannot reach the support portal").doesNotContain(MockPortal.KEY);
    // stop() closes again: harmless
  }

  @Test
  void redirectsAreNeverFollowedSoTheKeyCannotGoElsewhere() throws Exception {
    try (final MockPortal other = new MockPortal()) {
      portal.handler = r -> new MockPortal.Response(302, "", Map.of("Location", other.url() + "/api/v1/support/whoami"));
      final SupportPortalException e = catchPortal(client()::whoami);
      assertThat(e.getCode()).isEqualTo("portal_error");
      assertThat(other.requests).isEmpty();
    }
  }

  @Test
  void plainHttpToARemoteHostIsRefused() {
    final SupportConfiguration.Registration registration = new SupportConfiguration.Registration("http://portal.example.com", "ws", MockPortal.KEY,
        "", false);
    assertThatThrownBy(() -> new SupportPortalClient(registration, INSTANCE)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("HTTPS");
  }
}
