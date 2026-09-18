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
import com.arcadedb.utility.IPAddressBlocklist;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7804: the third of the three questions #7372 left open - whether a TLS-terminating reverse proxy
 * in front of a cleartext listener may vouch for the transport, and on what terms.
 * <p>
 * The answer implemented here is "only a proxy the operator named". {@code X-Forwarded-Proto} stays
 * untrusted by itself, exactly as #7372 argued, because the caller asking for a token is the caller who
 * could set it; what changes is that a peer address listed in
 * {@code arcadedb.server.apiTokenTrustedProxies} is no longer just another remote cleartext client, so the
 * header it forwards is worth reading. With the list empty - the default - every assertion in
 * {@code Issue7372ApiTokenTransportGateTest} still describes the behaviour unchanged.
 * <p>
 * The decision is asserted at this level rather than end to end for the reason #7372 gave: the case that
 * matters is a cleartext request from a peer that is not on this machine, and a test that talks to
 * 127.0.0.1 cannot produce one.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7804ApiTokenTrustedProxyTest {

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
   * stays false - {@code Issue7372ApiTokenTransportGateTest#theRefusalIsOffByDefault} pins that half.
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
