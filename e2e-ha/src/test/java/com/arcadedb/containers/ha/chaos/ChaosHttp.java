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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.test.support.ContainersTestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Minimal HTTP client that tells "never sent" apart from "sent, answer unknown": only a failure of
 * {@link HttpURLConnection#connect()} is reported as {@link NotSentException}. A failure on a reused keep-alive
 * connection surfaces later, on write or read, and is therefore classified UNKNOWN, which is the conservative side.
 * (A {@code Connection: close} request header would not help: HttpURLConnection silently drops it as restricted.)
 */
public final class ChaosHttp {
  static {
    // each op must be sent exactly once: HttpURLConnection would otherwise silently resend a POST after a read failure
    System.setProperty("sun.net.http.retryPost", "false");
  }

  private static final String AUTHORIZATION = "Basic " + Base64.getEncoder()
      .encodeToString(("root:" + ContainersTestTemplate.PASSWORD).getBytes(StandardCharsets.UTF_8));

  public record Response(int status, String body) {
  }

  /**
   * The TCP connection could not be established, so the request was never sent.
   */
  public static final class NotSentException extends IOException {
    public NotSentException(final IOException cause) {
      super(cause.getMessage(), cause);
    }
  }

  private ChaosHttp() {
  }

  public static Response post(final String host, final int port, final String path, final String json,
      final int connectTimeoutMs, final int readTimeoutMs) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) URI.create("http://" + host + ":" + port + path).toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", AUTHORIZATION);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setConnectTimeout(connectTimeoutMs);
    connection.setReadTimeout(readTimeoutMs);
    connection.setDoOutput(true);
    try {
      try {
        connection.connect();
      } catch (final IOException e) {
        throw new NotSentException(e);
      }
      try (final OutputStream out = connection.getOutputStream()) {
        out.write(json.getBytes(StandardCharsets.UTF_8));
      }
      final int status = connection.getResponseCode();
      final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }
}
