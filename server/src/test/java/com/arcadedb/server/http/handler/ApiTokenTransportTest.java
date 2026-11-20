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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.security.ApiTokenTrustedProxies;
import com.arcadedb.utility.IPAddressBlocklist;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transport precondition of {@code POST /server/api-tokens}: TLS, trusted proxies and their headers, and the forwarded hop.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class ApiTokenTransportTest {

  /**
   * Issue #7372: the transport precondition {@code POST /server/api-tokens} applies to the one response on
   * the HTTP API that carries material the server can never reissue.
   * <p>
   * The decision is asserted here rather than end to end because the case that matters - a cleartext request
   * from a peer that is <em>not</em> on this machine - cannot be produced from a test that talks to
   * 127.0.0.1, and binding a real routable address in CI is not something a test can rely on.
   * {@code RemoteServerSecurityControlPlaneIT} covers the loopback half against a live server.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class TransportGate {

    @Test
    void httpsIsFitWhateverThePeer() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("https", remote())).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("HTTPS", remote())).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("https", loopback())).isTrue();
    }

    @Test
    void cleartextFromLoopbackIsFit() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", loopback())).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", new InetSocketAddress("127.9.9.9", 51234)))
          .isTrue();
    }

    /**
     * The case the gate exists for: a token written back in the clear to something that is not on this
     * machine.
     */
    @Test
    void cleartextToARemotePeerIsNotFit() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote())).isFalse();
    }

    /**
     * Fails closed on an address it cannot read. An unresolved address is not a peer known to be local, and
     * a missing one says nothing at all - answering "fit" for either is the wrong direction to guess in.
     */
    @Test
    void anUnreadableAddressIsNotFit() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", null)).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http",
          InetSocketAddress.createUnresolved("127.0.0.1", 51234))).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets(null, remote())).isFalse();
    }

    /**
     * The setting that decides whether an unfit transport is refused or merely logged. It ships off, so an
     * upgrade does not start refusing the mints Studio's own token UI makes over plain HTTP; the assertion
     * is here so flipping the default is a deliberate edit with a test to update, not a silent one.
     */
    @Test
    void theRefusalIsOffByDefault() {
      assertThat(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getDefValue()).isEqualTo(Boolean.FALSE);
      assertThat(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey())
          .isEqualTo("arcadedb.server.apiTokenRequireSecureTransport");
    }

    /**
     * With the setting on, an unprotected transport is refused - and with a status that says the connection
     * is the problem rather than the caller, since the identical request over TLS succeeds.
     */
    @Test
    void withTheSettingOnAnUnprotectedMintIsRefusedWith412() {
      final ExecutionResponse refusal = PostApiTokenHandler.checkTransport("http", remote(), true);

      assertThat(refusal).isNotNull();
      assertThat(refusal.getCode()).isEqualTo(412);
      assertThat(new JSONObject(refusal.getResponse()).getString("error"))
          .contains("HTTPS")
          .contains(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
    }

    /**
     * With the setting on, a fit transport is still let through: the gate refuses connections, not callers.
     */
    @Test
    void withTheSettingOnAFitTransportProceeds() {
      assertThat(PostApiTokenHandler.checkTransport("http", loopback(), true)).isNull();
      assertThat(PostApiTokenHandler.checkTransport("https", remote(), true)).isNull();
    }

    /**
     * With the setting off - the default - the same unprotected mint proceeds. It is logged at WARNING, which
     * is deliberately not asserted here: what this pins is that the default does not refuse, because that is
     * the compatibility promise the default exists to keep.
     */
    @Test
    void withTheSettingOffAnUnprotectedMintProceeds() {
      assertThat(PostApiTokenHandler.checkTransport("http", remote(), false)).isNull();
      assertThat(PostApiTokenHandler.checkTransport("http", null, false)).isNull();
    }

    private static InetSocketAddress loopback() {
      return new InetSocketAddress("127.0.0.1", 51234);
    }

    private static InetSocketAddress remote() {
      return new InetSocketAddress("198.51.100.7", 51234);
    }
  }

  /**
   * Issue #7804: the third of the three questions #7372 left open - whether a TLS-terminating reverse proxy
   * in front of a cleartext listener may vouch for the transport, and on what terms.
   * <p>
   * The answer implemented here is "only a proxy the operator named". {@code X-Forwarded-Proto} stays
   * untrusted by itself, exactly as #7372 argued, because the caller asking for a token is the caller who
   * could set it; what changes is that a peer address listed in
   * {@code arcadedb.server.apiTokenTrustedProxies} is no longer just another remote cleartext client, so the
   * header it forwards is worth reading. With the list empty - the default - every assertion in
   * {@code ApiTokenTransportTest.TransportGate} still describes the behaviour unchanged.
   * <p>
   * The decision is asserted at this level rather than end to end for the reason #7372 gave: the case that
   * matters is a cleartext request from a peer that is not on this machine, and a test that talks to
   * 127.0.0.1 cannot produce one.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class TrustedProxy {

    private static final String PROXY_IP = "203.0.113.9";

    /**
     * The case the setting exists for: TLS is terminated at a proxy the operator listed, the hop from that
     * proxy to the server is cleartext on a network the operator controls, and the client's own leg was
     * encrypted. The token is not readable by anything the operator has not already trusted.
     */
    @Test
    void aListedProxyForwardingHttpsVouchesForTheTransport() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https", trusting(PROXY_IP))).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "HTTPS", trusting(PROXY_IP))).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), " https ", trusting(PROXY_IP))).isTrue();
    }

    /**
     * The attack the whole design turns on. Any client can send {@code X-Forwarded-Proto: https}; a client
     * asking for a token is exactly the one with a reason to. Unless the connection came from a listed
     * proxy the header carries no weight at all, so an ordinary remote peer cannot talk its way past the gate.
     */
    @Test
    void anUnlistedPeerCannotVouchForItself() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote(), "https", trusting(PROXY_IP))).isFalse();
    }

    /**
     * A listed proxy that reports the client reached it in the clear is reporting an exposed leg, and says so
     * honestly. Trusting the proxy means believing that answer too, not only the convenient one.
     */
    @Test
    void aListedProxyForwardingCleartextIsRefused() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "http", trusting(PROXY_IP))).isFalse();
    }

    /**
     * A listed proxy that forwards no scheme at all has told us nothing, and nothing is not https. Fails closed
     * on a blank, absent or unrecognised value rather than reading the omission as consent.
     */
    @Test
    void aListedProxyThatForwardsNoSchemeIsRefused() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), null, trusting(PROXY_IP))).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "", trusting(PROXY_IP))).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "   ", trusting(PROXY_IP))).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "wss", trusting(PROXY_IP))).isFalse();
    }

    /**
     * Through a chain of proxies the header accumulates one scheme per hop. Every hop that reported a scheme
     * has to have encrypted its leg: one cleartext hop anywhere in the chain put the token on a wire in the
     * clear, and which hop it was does not make it less readable.
     */
    @Test
    void everyHopInTheChainMustHaveBeenEncrypted() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https, https", trusting(PROXY_IP)))
          .as("both legs encrypted").isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https, http", trusting(PROXY_IP)))
          .as("the second hop was cleartext").isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "http, https", trusting(PROXY_IP)))
          .as("the client's own leg was cleartext").isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https,,https", trusting(PROXY_IP)))
          .as("an empty element is not a scheme that reported https").isFalse();
    }

    /**
     * A <em>trailing</em> empty hop has to be rejected for the same reason an interior one is, and it very nearly
     * was not: {@code "https,".split(",")} yields just {@code ["https"]}, because String.split drops trailing
     * empty strings unless it is given a negative limit. So {@code X-Forwarded-Proto: https,} - a value a client
     * can send and a pass-through proxy will forward unchanged - read as a single fully-encrypted hop and let the
     * mint through, while the interior form {@code "https,,https"} was correctly refused. Caught in review of
     * PR #7824.
     */
    @Test
    void aTrailingEmptyHopIsRejectedJustLikeAnInteriorOne() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https,", trusting(PROXY_IP)))
          .as("'https,' is a hop that reported https and a hop that reported nothing").isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https,,", trusting(PROXY_IP)))
          .isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https, ", trusting(PROXY_IP)))
          .as("a blank trailing hop is no better than an empty one").isFalse();

      // The leading form always failed, since split keeps leading empties; pinned so the fix cannot regress it.
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), ",https", trusting(PROXY_IP)))
          .isFalse();

      assertThat(PostApiTokenHandler.forwardedProtoIsFullyEncrypted("https,")).isFalse();
      assertThat(PostApiTokenHandler.forwardedProtoIsFullyEncrypted("https"))
          .as("the honest single-hop case must still pass").isTrue();
    }

    /**
     * A proxy configured to append rather than overwrite leaves a client-supplied {@code X-Forwarded-Proto} in
     * place and adds its own after it, so the request arrives carrying two separate headers with the client's
     * first. Reading only the first would hand the forgery straight back the win the trusted-proxy list is
     * there to deny, so every value is flattened into the one list the check runs over.
     */
    @Test
    void aClientInjectedHeaderCannotHideBehindAnAppendingProxy() {
      // What Undertow hands over when the client sent 'https' and the proxy honestly appended its cleartext leg.
      final String asReceived = PostApiTokenHandler.joinForwardedProto(List.of("https", "http"));

      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), asReceived, trusting(PROXY_IP)))
          .as("the proxy's own 'http' is still in the list, so the injected 'https' buys nothing").isFalse();

      assertThat(PostApiTokenHandler.joinForwardedProto(List.of("https"))).isEqualTo("https");
      assertThat(PostApiTokenHandler.joinForwardedProto(List.of("https", "https"))).isEqualTo("https,https");
    }

    @Test
    void anAbsentHeaderFlattensToNothingRatherThanAnEmptyString() {
      assertThat(PostApiTokenHandler.joinForwardedProto(null)).isNull();
      assertThat(PostApiTokenHandler.joinForwardedProto(List.of())).isNull();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(),
          PostApiTokenHandler.joinForwardedProto(null), trusting(PROXY_IP))).isFalse();
    }

    /**
     * The compatibility promise this setting has to keep: with no proxy listed - the shipped default - the
     * header is not consulted at all and the gate behaves exactly as #7372 left it.
     */
    @Test
    void withNoProxyListedTheHeaderIsIgnoredEntirely() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote(), "https", trusting(""))).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote(), "https", null)).isFalse();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https", trusting(""))).isFalse();
    }

    /**
     * A CIDR entry lists a whole proxy fleet without naming each member, which is how these deployments are
     * actually configured. A peer outside the range is still an ordinary remote client.
     */
    @Test
    void aCidrRangeListsAFleetOfProxies() {
      final IPAddressBlocklist fleet = trusting("203.0.113.0/24");
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https", fleet)).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", remote(), "https", fleet)).isFalse();
    }

    /**
     * Loopback and real TLS keep working whatever the list says: the new rule only ever adds a third way to
     * be fit, it never takes one away.
     */
    @Test
    void theExistingWaysToBeFitAreUnaffected() {
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("https", remote(), null, trusting(""))).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", loopback(), null, trusting(""))).isTrue();
      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", loopback(), "http", trusting(PROXY_IP))).isTrue();
    }

    /**
     * A list the operator mistyped must not silently widen into "trust everyone". An unparseable entry makes
     * the whole list empty, which denies - the same direction #7372 chose for an address it cannot read.
     */
    @Test
    void anUnparseableListFailsClosed() {
      assertThat(PostApiTokenHandler.parseTrustedProxies("203.0.113.9, not-an-address").isEmpty()).isTrue();
      assertThat(PostApiTokenHandler.parseTrustedProxies("203.0.113.0/99").isEmpty()).isTrue();
      assertThat(PostApiTokenHandler.parseTrustedProxies("proxy.internal").isEmpty())
          .as("a hostname would need a DNS lookup an attacker may influence").isTrue();

      assertThat(PostApiTokenHandler.isTransportSafeForSecrets("http", proxy(), "https",
          PostApiTokenHandler.parseTrustedProxies("203.0.113.9, not-an-address"))).isFalse();
    }

    @Test
    void aBlankListParsesToNoProxies() {
      assertThat(PostApiTokenHandler.parseTrustedProxies(null).isEmpty()).isTrue();
      assertThat(PostApiTokenHandler.parseTrustedProxies("").isEmpty()).isTrue();
      assertThat(PostApiTokenHandler.parseTrustedProxies("   ").isEmpty()).isTrue();
      assertThat(PostApiTokenHandler.parseTrustedProxies("203.0.113.9").isEmpty()).isFalse();
    }

    /**
     * The setting ships listing nobody, so an upgrade changes no deployment's behaviour: naming a proxy is
     * the operator asserting where their trust boundary is, and only they can say that.
     */
    @Test
    void noProxyIsTrustedByDefault() {
      assertThat(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getDefValue()).isEqualTo("");
      assertThat(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey())
          .isEqualTo("arcadedb.server.apiTokenTrustedProxies");
    }

    /**
     * With the refusal on, an unlisted peer still gets the 412 - and the refusal now names the setting that
     * would let a proxy through, because an operator who has one is reading this message to find out why
     * their deployment stopped working.
     */
    @Test
    void theRefusalPointsAtTheTrustedProxySetting() {
      final ExecutionResponse refusal = PostApiTokenHandler.checkTransport("http", remote(), "https",
          trusting(PROXY_IP), true);

      assertThat(refusal).isNotNull();
      assertThat(refusal.getCode()).isEqualTo(412);
      assertThat(new JSONObject(refusal.getResponse()).getString("error"))
          .contains(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey());
    }

    /**
     * And with the refusal on, a listed proxy is let through - the point of the whole exercise.
     */
    @Test
    void withTheRefusalOnAListedProxyProceeds() {
      assertThat(PostApiTokenHandler.checkTransport("http", proxy(), "https", trusting(PROXY_IP), true)).isNull();
    }

    /**
     * Issue #7804's first question, recorded where it cannot be lost: the default flips in 27.1.1, and the
     * setting's own description says so, since that is the text an operator reads. Until then the default
     * stays false - {@code ApiTokenTransportTest.TransportGate#theRefusalIsOffByDefault} pins that half.
     */
    @Test
    void theReleaseThatFlipsTheDefaultIsWrittenDown() {
      assertThat(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getDescription())
          .as("the operator has to be able to read the deprecation window off the setting itself")
          .contains("27.1.1");
    }

    private static IPAddressBlocklist trusting(final String csv) {
      return IPAddressBlocklist.parse(csv);
    }

    private static InetSocketAddress proxy() {
      return new InetSocketAddress(PROXY_IP, 51234);
    }

    private static InetSocketAddress remote() {
      return new InetSocketAddress("198.51.100.7", 51234);
    }

    private static InetSocketAddress loopback() {
      return new InetSocketAddress("127.0.0.1", 51234);
    }
  }

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
  @Nested
  class ForwardedHeader {

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
      assertThat(safe(null, List.of("proto=\"https\\"))).as("a trailing lone backslash inside quotes").isFalse();
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

  /**
   * Issue #8109: {@code POST /server/api-tokens} is forwarded to the leader on an HA cluster, so the plaintext token
   * travels back over two legs - leader to follower, follower to client. The follower checks the client's leg before
   * forwarding; the leader checks the hop it received the forward on. When the hop is what fails the check, the
   * refusal has to name the hop: the client did connect over TLS, and "connect over TLS" would send it after the wrong
   * connection.
   * <p>
   * Asserted on the decision rather than end to end for the reason {@link ApiTokenTransportTest.TransportGate} gives:
   * every node of an in-JVM cluster dials 127.0.0.1, which is always a safe leg.
   */
  @Nested
  class ForwardedHopTransport {

    @AfterEach
    void clearForwardContext() {
      LeaderForwardContext.clear();
    }

    @Test
    void aClientLegRefusalIsAnsweredAsIs() {
      final ExecutionResponse refusal = cleartextRemoteRefusal();

      assertThat(PostApiTokenHandler.refusalForThisLeg(refusal)).isSameAs(refusal);
    }

    @Test
    void aForwardedHopRefusalNamesTheHopBetweenTheNodes() {
      final ExecutionResponse clientRefusal = cleartextRemoteRefusal();
      LeaderForwardContext.markAlreadyForwarded();

      final ExecutionResponse refusal = PostApiTokenHandler.refusalForThisLeg(clientRefusal);

      assertThat(refusal.getCode()).isEqualTo(412);
      final String error = new JSONObject(refusal.getResponse()).getString("error");
      assertThat(error).contains("from another node");
      assertThat(error).contains("HTTPS between the cluster nodes");
      assertThat(error).contains(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
    }

    /** The refusal {@code checkTransport} builds for a cleartext mint from a peer that is not on this machine. */
    private static ExecutionResponse cleartextRemoteRefusal() {
      final ExecutionResponse refusal = PostApiTokenHandler.checkTransport("http",
          new InetSocketAddress("203.0.113.7", 51234), true);
      assertThat(refusal).as("a cleartext mint from a remote peer must be refused with the setting on").isNotNull();
      return refusal;
    }
  }
}
