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
import com.arcadedb.server.security.ApiTokenTrustedProxies;
import com.arcadedb.utility.IPAddressBlocklist;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7822: a proxy listed in {@code arcadedb.server.apiTokenTrustedProxies} that reports the scheme only through
 * the RFC 7239 {@code Forwarded} header read as unprotected, so the mint was refused for a deployment that was in fact
 * protected. The header is now read with the same rule as {@code X-Forwarded-Proto}: a listed peer is required, and
 * every hop must report https.
 * <p>
 * RFC 7239 carries one element per hop, several elements per header line and possibly several lines, and each element
 * is a {@code ;}-separated list of {@code name=value} pairs whose values may be quoted strings. The parsing is where a
 * forgery would hide, so most of this class is about it.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7822ApiTokenForwardedHeaderTest {

  private static final String PROXY_IP = "203.0.113.9";

  /**
   * The case the issue reports: the operator listed the proxy, the proxy emits only {@code Forwarded}, and the
   * client's leg was encrypted.
   */
  @Test
  void aListedProxyEmittingOnlyForwardedVouchesForTheTransport() {
    assertThat(safe(null, List.of("for=198.51.100.7;proto=https"))).isTrue();
    assertThat(safe(null, List.of("proto=https"))).isTrue();
    assertThat(safe(null, List.of("for=198.51.100.7;proto=HTTPS;by=203.0.113.9"))).isTrue();
    assertThat(safe(null, List.of("Proto=https")))
        .as("parameter names are case-insensitive (RFC 7239 section 4)").isTrue();
    assertThat(safe(null, List.of("for=\"[2001:db8:cafe::17]:4711\";proto=\"https\"")))
        .as("a quoted value is unquoted before it is compared").isTrue();
  }

  /**
   * The checks the issue asks to carry over unchanged: the header weighs nothing from a peer that is not on the
   * list, and nothing at all while the list is empty.
   */
  @Test
  void onlyAListedPeerIsBelieved() {
    final InetSocketAddress remote = new InetSocketAddress("198.51.100.7", 51234);
    final String reported = ApiTokenTrustedProxies.reportedForwardedProto(null, List.of("proto=https"));

    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote, reported, trusting(PROXY_IP))).isFalse();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), reported, trusting(""))).isFalse();
    assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), reported, null)).isFalse();
  }

  @Test
  void aCleartextOrUnrecognisedProtoIsRefused() {
    assertThat(safe(null, List.of("proto=http"))).isFalse();
    assertThat(safe(null, List.of("proto=wss"))).isFalse();
    assertThat(safe(null, List.of("proto="))).isFalse();
    assertThat(safe(null, List.of("proto=\"\""))).isFalse();
  }

  /**
   * Several elements in one line and several lines both describe a chain of hops, and every hop must have
   * encrypted its leg - the parsing counterpart of the flattening {@code joinForwardedProto} does.
   */
  @Test
  void everyHopAcrossElementsAndLinesMustBeHttps() {
    assertThat(safe(null, List.of("for=a;proto=https, for=b;proto=https"))).isTrue();
    assertThat(safe(null, List.of("for=a;proto=https", "for=b;proto=https"))).isTrue();

    assertThat(safe(null, List.of("for=a;proto=https, for=b;proto=http"))).as("second element cleartext").isFalse();
    assertThat(safe(null, List.of("for=a;proto=http, for=b;proto=https"))).as("client's own leg cleartext").isFalse();
    assertThat(safe(null, List.of("proto=https", "proto=http"))).as("second line cleartext").isFalse();
  }

  /**
   * The forgery an appending proxy would otherwise let through. The client sends {@code Forwarded: proto=https};
   * a proxy that appends an element recording only {@code for=} reports no scheme for its own hop. If an element
   * without {@code proto} were skipped, the client's injected element would be the only one left and would be
   * believed. An element that does not report a scheme is therefore a hop that reported nothing, and nothing is
   * not https.
   */
  @Test
  void anElementWithoutProtoIsAHopThatReportedNothing() {
    assertThat(safe(null, List.of("proto=https, for=198.51.100.7"))).isFalse();
    assertThat(safe(null, List.of("proto=https", "for=198.51.100.7"))).isFalse();
    assertThat(safe(null, List.of("for=198.51.100.7"))).isFalse();
  }

  /**
   * Empty elements - leading, interior, trailing, or a whole blank line - are hops that reported nothing, exactly as
   * {@code "https,"} is for {@code X-Forwarded-Proto} (the trailing-empty-hop bypass caught in PR #7824).
   */
  @Test
  void emptyElementsAreRejectedWhereverTheySit() {
    assertThat(safe(null, List.of("proto=https,"))).isFalse();
    assertThat(safe(null, List.of("proto=https, "))).isFalse();
    assertThat(safe(null, List.of(",proto=https"))).isFalse();
    assertThat(safe(null, List.of("proto=https,,proto=https"))).isFalse();
    assertThat(safe(null, List.of(""))).isFalse();
    assertThat(safe(null, List.of("   "))).isFalse();
    assertThat(safe(null, List.of("proto=https", ""))).isFalse();
  }

  /**
   * A comma or a semicolon inside a quoted string is part of the value, not a separator. Splitting naively would turn
   * {@code proto="https,https"} into two https hops; read correctly it is one hop whose scheme is not https.
   */
  @Test
  void separatorsInsideQuotesDoNotSplit() {
    assertThat(safe(null, List.of("proto=\"https,https\""))).isFalse();
    assertThat(safe(null, List.of("proto=\"https;x=y\""))).isFalse();
    assertThat(safe(null, List.of("for=\"a,b\";proto=https"))).as("a quoted comma in another pair").isTrue();
    assertThat(safe(null, List.of("for=\"a;b\";proto=https"))).as("a quoted semicolon in another pair").isTrue();
    assertThat(safe(null, List.of("for=\"a\\\"b\";proto=https"))).as("an escaped quote stays inside").isTrue();
  }

  /**
   * Anything the grammar does not allow makes its element a hop that reported nothing - never a hop that reported
   * https.
   */
  @Test
  void aMalformedElementFailsClosed() {
    assertThat(safe(null, List.of("proto=\"https"))).as("unterminated quoted string").isFalse();
    assertThat(safe(null, List.of("proto=https;proto=https"))).as("a parameter twice in one element").isFalse();
    assertThat(safe(null, List.of("proto"))).as("a name with no value").isFalse();
    assertThat(safe(null, List.of("https"))).as("an X-Forwarded-Proto value in the wrong header").isFalse();
    assertThat(safe(null, List.of("proto=https=https"))).isFalse();
    assertThat(safe(null, List.of("\"proto\"=https"))).as("a quoted parameter name").isFalse();
    assertThat(safe(null, List.of("proto=\"ht\"tps"))).as("a token glued to a quoted string").isFalse();
    assertThat(safe(null, List.of("proto=ht\"tps\""))).as("a quoted string glued to a token").isFalse();
    assertThat(safe(null, List.of("proto=\"ht\"\"tps\""))).as("two quoted strings in one value").isFalse();
    assertThat(safe(null, List.of("proto=\" https\""))).as("a quoted value is taken verbatim").isFalse();
    assertThat(safe(null, List.of("proto=\"https\" ;for=a"))).as("whitespace after the closing quote").isTrue();
  }

  /**
   * One malformed element must not take a well-formed neighbour's verdict with it, nor borrow it: each element
   * stands for its own hop.
   */
  @Test
  void aMalformedElementDoesNotLeakIntoItsNeighbours() {
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(null, List.of("proto=\"https, proto=https")))
        .as("the unterminated quote swallows the rest of the line into one malformed element").isEqualTo("");
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(null, List.of("proto, proto=https"))).isEqualTo(",https");
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(null, List.of("proto=https, for=x"))).isEqualTo("https,");
  }

  /**
   * Both headers on one request: whichever header the client could have injected must not outvote the one the proxy
   * wrote honestly, so every scheme reported in either header has to be https.
   */
  @Test
  void whenBothHeadersArePresentBothMustReportHttps() {
    assertThat(safe(List.of("https"), List.of("proto=https"))).isTrue();
    assertThat(safe(List.of("http"), List.of("proto=https")))
        .as("the proxy wrote X-Forwarded-Proto honestly; the client injected Forwarded").isFalse();
    assertThat(safe(List.of("https"), List.of("proto=http")))
        .as("the proxy wrote Forwarded honestly; the client injected X-Forwarded-Proto").isFalse();
    assertThat(safe(List.of("https"), List.of("for=198.51.100.7")))
        .as("a Forwarded element without a scheme still counts as a hop that reported nothing").isFalse();
  }

  /**
   * The {@code X-Forwarded-Proto}-only path of #7804 keeps its exact behaviour, and with neither header the listed
   * proxy has said nothing.
   */
  @Test
  void theXForwardedProtoPathIsUnchanged() {
    assertThat(safe(List.of("https"), null)).isTrue();
    assertThat(safe(List.of("https", "http"), null)).isFalse();
    assertThat(safe(null, null)).isFalse();
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(null, null)).isNull();
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(List.of(), List.of())).isNull();
    assertThat(ApiTokenTrustedProxies.reportedForwardedProto(List.of("https", "http"), null))
        .isEqualTo(PostApiTokenHandler.joinForwardedProto(List.of("https", "http")));
  }

  /**
   * The wiring the handler actually runs: the request's headers as Undertow hands them over, looked up by name. A
   * check on the parser alone would stay green if the handler read the wrong header or never read this one.
   */
  @Test
  void theHandlerReadsBothHeadersOffTheRequest() {
    final HeaderMap headers = new HeaderMap();
    headers.add(new HttpString("forwarded"), "for=198.51.100.7;proto=https");
    assertThat(PostApiTokenHandler.forwardedProtoOf(headers)).isEqualTo("https");

    headers.add(new HttpString("Forwarded"), "for=203.0.113.9;proto=http");
    assertThat(PostApiTokenHandler.forwardedProtoOf(headers)).isEqualTo("https,http");

    headers.add(new HttpString("x-forwarded-proto"), "https");
    assertThat(PostApiTokenHandler.forwardedProtoOf(headers)).isEqualTo("https,https,http");

    assertThat(PostApiTokenHandler.forwardedProtoOf(new HeaderMap())).isNull();
  }

  /**
   * With the refusal on, a listed proxy that speaks only RFC 7239 is let through - the operator's symptom in the
   * issue, "I listed my proxy and it still refuses", is gone.
   */
  @Test
  void withTheRefusalOnAListedRfc7239ProxyProceeds() {
    final HeaderMap headers = new HeaderMap();
    headers.add(new HttpString("Forwarded"), "for=198.51.100.7;proto=https");

    assertThat(PostApiTokenHandler.checkTransport("http", proxy(), PostApiTokenHandler.forwardedProtoOf(headers),
        trusting(PROXY_IP), true)).isNull();
  }

  @Test
  void theSettingDescriptionNamesTheStandardHeader() {
    assertThat(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getDescription()).contains("Forwarded");
    assertThat(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getDescription()).contains("RFC 7239");
  }

  private static boolean safe(final List<String> xForwardedProto, final List<String> forwarded) {
    return PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(),
        ApiTokenTrustedProxies.reportedForwardedProto(xForwardedProto, forwarded), trusting(PROXY_IP));
  }

  private static IPAddressBlocklist trusting(final String csv) {
    return IPAddressBlocklist.parse(csv);
  }

  private static InetSocketAddress proxy() {
    return new InetSocketAddress(PROXY_IP, 51234);
  }
}
