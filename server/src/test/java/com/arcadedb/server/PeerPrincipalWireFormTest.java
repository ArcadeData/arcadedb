/*
 * Copyright \u00a9 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import io.undertow.Undertow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The principal names {@link PeerRequest} accepts are checked against a real HTTP listener: every name over a small alphabet that it
 * accepts is sent in the principal header, and the value the listener reads must be the name that was sent. The alphabet holds the
 * space and the tab, the characters at the edge of ISO-8859-1 and the ones that only look like whitespace.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PeerPrincipalWireFormTest {

  private static final char[] ALPHABET   = { 'a', ' ', '\t', '\u00e9', '\u00a0', '\u00ff' };
  private static final int    MAX_LENGTH = 4;

  private Undertow  listener;
  private String    listenerUrl;
  private HttpClient client;

  @BeforeEach
  void startListener() {
    // The receiving side answers with the value its parser read for the principal header, and nothing else
    listener = Undertow.builder().addHttpListener(0, "127.0.0.1").setHandler(exchange -> {
      final String read = exchange.getRequestHeaders().getFirst(PeerRequest.FORWARDED_USER_HEADER);
      exchange.getResponseSender().send(read == null ? "<none>" : "[" + read + "]", StandardCharsets.ISO_8859_1);
    }).build();
    listener.start();
    listenerUrl = "http://127.0.0.1:" + ((InetSocketAddress) listener.getListenerInfo().get(0).getAddress()).getPort() + "/";
    client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  }

  @AfterEach
  void stopListener() {
    listener.stop();
  }

  @Test
  void everyPrincipalNameAcceptedIsReadByTheListenerAsSent() throws Exception {
    int accepted = 0;
    int readDifferently = 0;

    for (final String name : namesOver(ALPHABET, MAX_LENGTH)) {
      final HttpRequest.Builder builder;
      try {
        builder = PeerRequest.newRequest(listenerUrl, false, PeerPrincipalWireFormTest.class, "testing", "s3cr3t", name);
      } catch (final IllegalArgumentException refused) {
        // Refused: the scan has to reach names the listener would read differently
        final String read = readByTheListener(name);
        if (read != null && !read.equals("[" + name + "]"))
          readDifferently++;
        continue;
      }
      accepted++;
      final HttpResponse<String> answer = client.send(builder.GET().build(),
          HttpResponse.BodyHandlers.ofString(StandardCharsets.ISO_8859_1));
      assertThat(answer.body()).as("the principal '%s' as the listener read it", printable(name)).isEqualTo("[" + name + "]");
    }

    assertThat(accepted).as("the scan reached names PeerRequest accepts").isGreaterThan(ALPHABET.length);
    assertThat(readDifferently).as("the scan reached names that are refused and that the listener reads differently")
        .isGreaterThan(0);
  }

  /** What a header with the name sent as is would come back as, or {@code null} when the JDK client itself cannot send it. */
  private String readByTheListener(final String name) throws Exception {
    final HttpRequest request;
    try {
      request = HttpRequest.newBuilder().uri(URI.create(listenerUrl)).header(PeerRequest.FORWARDED_USER_HEADER, name).GET().build();
    } catch (final IllegalArgumentException cannotBeSent) {
      return null;
    }
    return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.ISO_8859_1)).body();
  }

  private static List<String> namesOver(final char[] alphabet, final int maxLength) {
    final List<String> names = new ArrayList<>();
    List<String> previous = List.of("");
    for (int length = 1; length <= maxLength; length++) {
      final List<String> current = new ArrayList<>(previous.size() * alphabet.length);
      for (final String prefix : previous)
        for (final char c : alphabet)
          current.add(prefix + c);
      names.addAll(current);
      previous = current;
    }
    return names;
  }

  private static String printable(final String name) {
    final StringBuilder printable = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      final char c = name.charAt(i);
      printable.append(c == ' ' ? '_' : c < 0x20 || c > 0x7e ? String.format("\\u%04x", (int) c) : String.valueOf(c));
    }
    return printable.toString();
  }
}
