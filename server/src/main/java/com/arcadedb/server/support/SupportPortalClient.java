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

import com.arcadedb.Constants;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.BoundedHttpExchange;
import com.arcadedb.serializer.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * Client of the support API of the ArcadeData customer portal (contract SUPPORT-API.md, section 2). Uses
 * {@code java.net.http} with timeouts and TLS verification, sends the Client key as {@code Authorization: Bearer}, the
 * Client ID as {@code X-Client-Id}, the instance id as {@code X-Instance-Id}, retries once on a transient error, and
 * uploads files as a streamed multipart body from disk (never the whole content in the heap).
 * <p>
 * Every message and every body handed back has the Client key scrubbed out. Redirects are never followed, so the key
 * cannot be sent to another host.
 */
public class SupportPortalClient {
  // Static so all clients in the JVM share one client: each instance spawns a SelectorManager NIO thread
  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
      .followRedirects(HttpClient.Redirect.NEVER).build();

  private static final java.util.regex.Pattern PROCESS_REFUSAL = java.util.regex.Pattern.compile(
      "failed: (?:Error: )?([a-z_]+(?:\\.[a-z_]+)?): (.*)$", java.util.regex.Pattern.DOTALL);

  static final String API = "/api/v1/support";
  /** A key runs a tenant process through the platform's ordinary process endpoint; it may run only one that declares it. */
  static final String PROCESS_PATH = "/api/v1/process-execute";
  /** The portal process that creates or completes the Installation of a server (access: {key: 'support:create'}). */
  static final String REGISTER_PROCESS = "studio-register-instance";

  public static final long CALL_TIMEOUT_MS   = 30_000L;
  public static final long UPLOAD_TIMEOUT_MS = 20 * 60_000L;
  /** The most of a portal response that is read: the portal is trusted, this is cheap insurance against an unbounded body. */
  public static final long MAX_RESPONSE_BYTES = 16L * 1024 * 1024;

  private static final SecureRandom RANDOM = new SecureRandom();
  /** A workspace key as the portal issues it: {@code wsk_} and 43 URL-safe base64 characters. */
  private static final Pattern PORTAL_KEY = Pattern.compile("wsk_[A-Za-z0-9_-]{20,}");

  private final SupportConfiguration.Registration registration;
  private final String                            instanceId;
  private final String                            baseUrl;
  private final long                              retryDelayMs;
  private       long                              maxResponseBytes = MAX_RESPONSE_BYTES;

  /** One part of a multipart body: bytes held in memory (small) or a file streamed from disk. */
  record Part(String name, String filename, String contentType, byte[] bytes, Path file) {
    static Part json(final String name, final String json) {
      return new Part(name, null, "application/json", json.getBytes(StandardCharsets.UTF_8), null);
    }

    static Part file(final String name, final String filename, final String contentType, final Path file) {
      return new Part(name, filename, contentType, null, file);
    }
  }

  public SupportPortalClient(final SupportConfiguration.Registration registration, final String instanceId) {
    this(registration, instanceId, 500L);
  }

  SupportPortalClient(final SupportConfiguration.Registration registration, final String instanceId, final long retryDelayMs) {
    SupportConfiguration.validatePortalUrl(registration.getPortalUrl());
    this.registration = registration;
    this.instanceId = instanceId;
    this.baseUrl = registration.getPortalUrl();
    this.retryDelayMs = retryDelayMs;
  }

  /** For tests: the most of a response that is read. */
  void setMaxResponseBytes(final long maxResponseBytes) {
    this.maxResponseBytes = maxResponseBytes;
  }

  public String whoami() {
    return call("GET", "/whoami", null);
  }

  /**
   * Registers this server as an installation of the workspace: runs the portal process {@link #REGISTER_PROCESS} with
   * {@code {diagnostics}}, and returns what the process answered ({@code output.data}).
   */
  public String registerInstallation(final String jsonBody) {
    final String answer;
    try {
      answer = execute("POST", PROCESS_PATH, REGISTER_PROCESS, "application/json", () -> HttpRequest.BodyPublishers.ofString(jsonBody),
          CALL_TIMEOUT_MS);
    } catch (final IOException e) {
      throw new SupportPortalException("portal_error", 0, scrub(e.getMessage()), 0);
    }
    try {
      final JSONObject output = new JSONObject(answer).getJSONObject("output");
      return (output.has("data") ? output.getJSONObject("data") : output).toString();
    } catch (final RuntimeException e) {
      throw new SupportPortalException("portal_error", 0, "The portal answered in a form this server does not understand", 0);
    }
  }

  public String listIssues(final String status) {
    final String s = status == null || status.isBlank() ? "open" : status;
    if (!s.equals("open") && !s.equals("closed") && !s.equals("all"))
      throw new IllegalArgumentException("status must be open, closed or all");
    return call("GET", "/issues?status=" + s, null);
  }

  public String getIssue(final long number) {
    return call("GET", "/issues/" + number, null);
  }

  public String addComment(final long number, final String body) {
    return call("POST", "/issues/" + number + "/comments", new JSONObject().put("body", body).toString());
  }

  public void setOpen(final long number, final boolean open) {
    call("PUT", "/issues/" + number, new JSONObject().put("open", open).toString());
  }

  /**
   * Opens an issue. {@code logs}, {@code diagnostics}, {@code summary} and {@code threads} are files on disk (any may be
   * {@code null}) and are streamed.
   */
  public String createIssue(final JSONObject metadata, final Path logs, final Path diagnostics, final Path summary,
      final Path threads) throws IOException {
    final List<Part> parts = new ArrayList<>();
    parts.add(Part.json("metadata", metadata.toString()));
    addFiles(parts, logs, diagnostics, summary, threads);
    return callMultipart("/issues", parts);
  }

  /** Adds files to an existing issue. */
  public String addAttachments(final long number, final Path logs, final Path diagnostics, final Path summary,
      final Path threads) throws IOException {
    final List<Part> parts = new ArrayList<>();
    addFiles(parts, logs, diagnostics, summary, threads);
    if (parts.isEmpty())
      throw new IllegalArgumentException("Nothing to send");
    return callMultipart("/issues/" + number + "/attachments", parts);
  }

  private static void addFiles(final List<Part> parts, final Path logs, final Path diagnostics, final Path summary,
      final Path threads) {
    if (logs != null)
      parts.add(Part.file("logs", "logs.zip", "application/zip", logs));
    if (diagnostics != null)
      parts.add(Part.file("diagnostics", "diagnostics.json", "application/json", diagnostics));
    if (summary != null)
      parts.add(Part.file("summary", "summary.json", "application/json", summary));
    if (threads != null)
      parts.add(Part.file("threads", "threads.txt", "text/plain", threads));
  }

  private String callMultipart(final String path, final List<Part> parts) throws IOException {
    final String boundary = "arcadedb-" + HexFormat.of().formatHex(randomBytes());
    final Multipart body = new Multipart(boundary, parts);
    return execute("POST", API + path, null, "multipart/form-data; boundary=" + boundary, () -> HttpRequest.BodyPublishers.fromPublisher(
        HttpRequest.BodyPublishers.ofInputStream(body::open), body.length()), UPLOAD_TIMEOUT_MS);
  }

  private String call(final String method, final String path, final String jsonBody) {
    try {
      return execute(method, API + path, null, jsonBody != null ? "application/json" : null,
          () -> jsonBody != null ? HttpRequest.BodyPublishers.ofString(jsonBody) : HttpRequest.BodyPublishers.noBody(),
          CALL_TIMEOUT_MS);
    } catch (final IOException e) {
      // execute() wraps the transport failures; this is a failure to read a local file, not reachable for JSON calls
      throw new SupportPortalException("portal_error", 0, scrub(e.getMessage()), 0);
    }
  }

  /** @param process the tenant process to run ({@code x-api-process}), or null for a support route */
  private String execute(final String method, final String path, final String process, final String contentType,
      final Supplier<HttpRequest.BodyPublisher> publisher, final long timeoutMs) throws IOException {
    final boolean idempotent = !method.equals("POST");
    SupportPortalException last = null;

    for (int attempt = 0; attempt < 2; attempt++) {
      if (attempt > 0)
        try {
          Thread.sleep(retryDelayMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }

      final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(baseUrl + path))
          .timeout(Duration.ofMillis(timeoutMs)).header("Authorization", "Bearer " + registration.getKey())
          .header("X-Client-Id", registration.getClientId()).header("User-Agent", userAgent()).header("Accept", "application/json");
      if (instanceId != null && !instanceId.isBlank())
        builder.header("X-Instance-Id", instanceId);
      if (process != null)
        builder.header("x-api-process", process);
      if (contentType != null)
        builder.header("Content-Type", contentType);
      builder.method(method, publisher.get());

      final HttpResponse<String> response;
      try {
        response = BoundedHttpExchange.send(HTTP_CLIENT, builder.build(), info -> new LimitedStringBody(maxResponseBytes), timeoutMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SupportPortalException("portal_unreachable", 0, "The request to the portal was interrupted", 0);
      } catch (final ResponseTooLargeException e) {
        // Not transient and not a connection problem: the portal sent more than this server reads
        throw new SupportPortalException("portal_error", 0, e.getMessage(), 0);
      } catch (final ConnectException | HttpConnectTimeoutException e) {
        // A request that never connected was not processed: it is safe to retry it whatever the method
        last = unreachable(e);
        continue;
      } catch (final IOException e) {
        last = unreachable(e);
        if (idempotent)
          continue;
        throw last;
      }

      final int status = response.statusCode();
      if (status >= 200 && status < 300)
        return scrubKey(response.body() == null ? "" : response.body());

      last = toException(status, response);
      // Only a call that can be repeated is retried on a gateway or availability error: a proxy may answer 502/503/504
      // after the portal has already processed a POST, and repeating it would file a second issue or comment.
      if (idempotent && (status == 502 || status == 503 || status == 504))
        continue;
      throw last;
    }
    if (last != null)
      throw last;
    throw new SupportPortalException("portal_error", 0, "The support portal did not answer", 0);
  }

  private SupportPortalException unreachable(final IOException e) {
    return new SupportPortalException("portal_unreachable", 0,
        "Cannot reach the support portal at " + registration.getPortalUrl() + " (" + describe(e) + "). Check the network "
            + "connection and the proxy settings of this server", 0);
  }

  SupportPortalException toException(final int status, final HttpResponse<String> response) {
    String code = null;
    String message = null;
    try {
      final JSONObject json = new JSONObject(response.body());
      code = json.getString("error", null);
      message = json.getString("message", null);
    } catch (final RuntimeException ignored) {
      // not JSON
    }
    if ("process_failed".equals(code)) {
      // The portal's process refused with "<code>: <sentence>"; anything else is its own bug, shown without its text
      final java.util.regex.Matcher m = PROCESS_REFUSAL.matcher(message == null ? "" : message);
      final boolean refused = m.find();
      code = refused ? m.group(1) : "portal_error";
      message = refused ? m.group(2).strip() : null;
    } else if ("unauthorized".equals(code) || "no_such_process".equals(code))
      // The platform does not run this process for a key: the portal has not got it (a valid key would not be refused)
      code = status == 401 || status == 404 ? "not_supported" : code;
    if (code == null || code.isBlank())
      code = switch (status) {
        case 400 -> "bad_request";
        case 401 -> "invalid_key";
        case 402 -> "support_not_active";
        case 404 -> "not_found";
        case 413 -> "too_large";
        case 429 -> "rate_limited";
        default -> "portal_error";
      };
    // A code we do not know of is kept as "portal_error": Studio switches on the known ones
    if (!List.of("invalid_key", "client_mismatch", "scope_denied", "support_not_active", "not_found", "too_large", "rate_limited",
        "bad_request", "not_supported", "no_workspace", "forbidden", "key_has_no_owner", "instance_id.taken", "invalid_instance_id",
        "invalid_version").contains(code))
      code = "portal_error";

    long retryAfter = 0L;
    if (code.equals("rate_limited"))
      try {
        retryAfter = Long.parseLong(response.headers().firstValue("Retry-After").orElse("0").trim());
      } catch (final NumberFormatException e) {
        retryAfter = 0L;
      }

    return new SupportPortalException(code, status, userMessage(code, status, message, retryAfter), retryAfter);
  }

  private String userMessage(final String code, final int status, final String portalMessage, final long retryAfter) {
    final String detail = portalMessage == null || portalMessage.isBlank() ? "" : " (" + truncate(scrub(portalMessage), 300) + ")";
    return switch (code) {
      case "invalid_key" -> "The portal rejected the Client key. Check it, or create a new key in the customer portal.";
      case "client_mismatch" -> "The Client ID does not belong to the workspace of this key. Copy both from the customer portal.";
      case "scope_denied" -> "This key is not allowed to do that: create a key with both the support:create and support:read "
          + "scopes in the customer portal.";
      case "support_not_active" -> "Your ArcadeDB support plan is not active (it expired or there is none). Renew or buy a plan at "
          + "https://arcadedb.com/pricing.html; you can still report a public GitHub issue.";
      case "not_found" -> "The issue was not found in your workspace.";
      case "too_large" -> "The upload is larger than the portal accepts. Narrow the log window, or do not send the logs.";
      case "rate_limited" -> "Too many requests to the portal" + (retryAfter > 0 ? ": retry in " + retryAfter + " seconds" : ": retry later")
          + ".";
      case "bad_request" -> "The portal refused the request" + detail + ".";
      case "not_supported" -> "This portal cannot register servers from Studio yet.";
      case "no_workspace", "forbidden", "key_has_no_owner" -> "The person who created this key can no longer register servers in the "
          + "workspace (they left it, or are only a viewer). Create a new key in the customer portal with an owner or admin account.";
      case "instance_id.taken" -> "The instance id of this server is already registered in the portal. If it is yours, contact "
          + "ArcadeData support.";
      case "invalid_instance_id", "invalid_version" -> "The portal refused the identity this server reported" + detail + ".";
      default -> "The support portal answered with an error (HTTP " + status + ")" + detail + ".";
    };
  }

  private String userAgent() {
    return "ArcadeDB/" + Constants.getRawVersion() + " support-client";
  }

  /**
   * Removes the Client key (and anything shaped like a portal key) from a SUCCESSFUL portal response, and nothing else.
   * The redactor is built for log lines: on compact JSON its unquoted-value rule would swallow the rest of the
   * document after a key such as {@code "tokenId":5}, and it would mask text the user wrote in an issue or a comment.
   */
  String scrubKey(final String body) {
    if (body == null)
      return null;
    String result = body;
    final String key = registration.getKey();
    if (key != null && !key.isEmpty())
      result = result.replace(key, "***");
    return PORTAL_KEY.matcher(result).replaceAll("***");
  }

  /** Removes the Client key and anything the log redactor would mask, from a text shown to a person (an error message). */
  String scrub(final String text) {
    if (text == null)
      return null;
    String result = text;
    final String key = registration.getKey();
    if (key != null && !key.isEmpty())
      result = result.replace(key, "***");
    return SupportRedactor.redact(result);
  }

  private static String describe(final Exception e) {
    final String message = e.getMessage();
    return e.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + truncate(message, 200));
  }

  private static String truncate(final String s, final int max) {
    return s.length() <= max ? s : s.substring(0, max) + "...";
  }

  private static byte[] randomBytes() {
    final byte[] bytes = new byte[12];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /** The portal answered with more bytes than {@link #MAX_RESPONSE_BYTES}. */
  static final class ResponseTooLargeException extends IOException {
    ResponseTooLargeException(final long max) {
      super("The support portal answered with more than " + (max >> 10) + " KB, which this server does not read");
    }
  }

  /** The body as a UTF-8 string, cancelled as soon as it exceeds the limit. */
  static final class LimitedStringBody implements HttpResponse.BodySubscriber<String> {
    private final long                                                max;
    private final ByteArrayOutputStream                       out    = new ByteArrayOutputStream();
    private final CompletableFuture<String>      result = new CompletableFuture<>();
    private       Flow.Subscription              subscription;
    private       long                                                size;

    LimitedStringBody(final long max) {
      this.max = max;
    }

    @Override
    public CompletionStage<String> getBody() {
      return result;
    }

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
      this.subscription = subscription;
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(final List<ByteBuffer> buffers) {
      if (result.isDone())
        return;
      for (final ByteBuffer buffer : buffers) {
        size += buffer.remaining();
        if (size > max) {
          subscription.cancel();
          result.completeExceptionally(new ResponseTooLargeException(max));
          return;
        }
        final byte[] chunk = new byte[buffer.remaining()];
        buffer.get(chunk);
        out.write(chunk, 0, chunk.length);
      }
    }

    @Override
    public void onError(final Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(out.toString(StandardCharsets.UTF_8));
    }
  }

  /** A multipart/form-data body whose files are read from disk while the request is sent. */
  static final class Multipart {
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    private final List<Object> chunks = new ArrayList<>();
    private       long         length;

    Multipart(final String boundary, final List<Part> parts) throws IOException {
      for (final Part part : parts) {
        final StringBuilder header = new StringBuilder("--").append(boundary).append("\r\nContent-Disposition: form-data; name=\"")
            .append(part.name()).append('"');
        if (part.filename() != null)
          header.append("; filename=\"").append(part.filename()).append('"');
        header.append("\r\nContent-Type: ").append(part.contentType()).append("\r\n\r\n");
        add(header.toString().getBytes(StandardCharsets.UTF_8));
        if (part.bytes() != null)
          add(part.bytes());
        else {
          chunks.add(part.file());
          // A preview file that has vanished is a failure on this server, reported as one (internal_error), not a request error
          length += Files.size(part.file());
        }
        add(CRLF);
      }
      add(("--" + boundary + "--\r\n").getBytes(StandardCharsets.US_ASCII));
    }

    private void add(final byte[] bytes) {
      chunks.add(bytes);
      length += bytes.length;
    }

    long length() {
      return length;
    }

    /** A new stream over the whole body, so a retry sends it again. Files are opened one at a time. */
    InputStream open() {
      return new InputStream() {
        private int         index;
        private InputStream current;

        @Override
        public int read() throws IOException {
          final byte[] one = new byte[1];
          final int n = read(one, 0, 1);
          return n < 0 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
          while (true) {
            if (current == null) {
              if (index >= chunks.size())
                return -1;
              final Object chunk = chunks.get(index++);
              current = chunk instanceof byte[] bytes ? new ByteArrayInputStream(bytes) : Files.newInputStream((Path) chunk);
            }
            final int n = current.read(b, off, len);
            if (n >= 0)
              return n;
            current.close();
            current = null;
          }
        }

        @Override
        public void close() throws IOException {
          if (current != null) {
            current.close();
            current = null;
          }
        }
      };
    }
  }

  static void logFailure(final Object source, final String what, final SupportPortalException e) {
    LogManager.instance().log(source, Level.WARNING, "Support portal: %s failed (%s, HTTP %d)", what, e.getCode(), e.getPortalStatus());
  }
}
