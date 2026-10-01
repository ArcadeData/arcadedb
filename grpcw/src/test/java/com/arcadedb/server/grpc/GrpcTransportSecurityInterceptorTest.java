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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7309: what {@link GrpcTransportSecurityInterceptor} decides, per connection.
 * <p>
 * The decision is the whole of the server-side guard on {@code CreateApiToken}, and it is made from
 * two transport attributes that no integration test can vary - every in-process test connects over
 * loopback. So it is driven here directly, one case per combination that matters, including the two
 * that must fail closed: an address shape the code does not recognise, and an unresolved address.
 */
class GrpcTransportSecurityInterceptorTest {

  @Test
  void aLoopbackPeerOverCleartextIsSafe() {
    assertThat(decisionFor(new InetSocketAddress("127.0.0.1", 51000), false)).isTrue();
  }

  @Test
  void anIpv6LoopbackPeerIsSafe() {
    assertThat(decisionFor(new InetSocketAddress("::1", 51000), false)).isTrue();
  }

  /**
   * The case the guard exists for: a remote peer on a cleartext connection. Minting a token towards
   * this one would put the token on the wire in the clear.
   */
  @Test
  void aRemotePeerOverCleartextIsNotSafe() {
    assertThat(decisionFor(new InetSocketAddress("203.0.113.7", 51000), false)).isFalse();
  }

  /**
   * TLS makes a remote peer safe - that is the configuration the refusal is telling the operator to
   * move to, so it has to actually be accepted, or the message would be advice that does not work.
   */
  @Test
  void aRemotePeerOverTlsIsSafe() {
    assertThat(decisionFor(new InetSocketAddress("203.0.113.7", 51000), true)).isTrue();
  }

  /**
   * Fail closed on an address the code cannot read as an IP socket - an in-process or unix-domain
   * transport reports one of these. "Unknown" must not resolve to "local".
   */
  @Test
  void anUnrecognisedAddressShapeIsNotSafe() {
    assertThat(decisionFor(new SocketAddress() {
    }, false)).isFalse();
  }

  /**
   * Fail closed on an unresolved address: {@code InetSocketAddress.getAddress()} is null there, and a
   * peer whose address was never resolved is not a peer known to be local.
   */
  @Test
  void anUnresolvedAddressIsNotSafe() {
    assertThat(decisionFor(InetSocketAddress.createUnresolved("localhost", 51000), false)).isFalse();
  }

  @Test
  void noAddressAtAllIsNotSafe() {
    assertThat(decisionFor(null, false)).isFalse();
  }

  // ------------------------------------------------------------------------------------------------
  // Issue #7821: a TLS-terminating proxy listed in arcadedb.server.apiTokenTrustedProxies vouches
  // for the leg it terminated through the x-forwarded-proto metadata key, exactly as it does for HTTP.
  // ------------------------------------------------------------------------------------------------

  private static final String PROXY_IP = "10.0.0.5";

  private static InetSocketAddress proxy() {
    return new InetSocketAddress(PROXY_IP, 51000);
  }

  private static InetSocketAddress remote() {
    return new InetSocketAddress("203.0.113.7", 51000);
  }

  private static Metadata forwardedProto(final String... values) {
    final Metadata metadata = new Metadata();
    for (final String value : values)
      metadata.put(GrpcTransportSecurityInterceptor.X_FORWARDED_PROTO_KEY, value);
    return metadata;
  }

  @Test
  void aListedProxyReportingHttpsVouchesForTheCall() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> PROXY_IP), proxy(), false,
        forwardedProto("https"))).isTrue();
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> PROXY_IP), proxy(), false,
        forwardedProto("HTTPS"))).isTrue();
  }

  @Test
  void aListedCidrRangeVouchesForAProxyInsideIt() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> "10.0.0.0/24"), proxy(), false,
        forwardedProto("https"))).isTrue();
  }

  /**
   * The forgery the list exists to stop: a cleartext caller that is not the operator's proxy sends the
   * metadata key itself. It must gain nothing.
   */
  @Test
  void anUnlistedPeerCannotVouchForItself() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> PROXY_IP), remote(), false,
        forwardedProto("https"))).isFalse();
  }

  /**
   * The shipped default: an empty list leaves the metadata unread, so the decision is exactly the
   * TLS-or-loopback test #7309 shipped, header or not.
   */
  @Test
  void anEmptyListTrustsNoProxy() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> ""), proxy(), false,
        forwardedProto("https"))).isFalse();
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> null), proxy(), false,
        forwardedProto("https"))).isFalse();
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(), proxy(), false, forwardedProto("https")))
        .isFalse();
  }

  @Test
  void aListedProxyReportingCleartextOrNothingDoesNotVouch() {
    final GrpcTransportSecurityInterceptor interceptor = new GrpcTransportSecurityInterceptor(() -> PROXY_IP);
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("http"))).isFalse();
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto())).isFalse();
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto(""))).isFalse();
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https,"))).isFalse();
  }

  /**
   * A proxy configured to append rather than overwrite leaves a client-supplied value in place and adds
   * its own: every value has to report https, so an injected one cannot outvote the proxy's honest
   * {@code http}.
   */
  @Test
  void everyForwardedValueMustBeHttps() {
    final GrpcTransportSecurityInterceptor interceptor = new GrpcTransportSecurityInterceptor(() -> PROXY_IP);
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https", "http"))).isFalse();
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https", "https"))).isTrue();
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https, http"))).isFalse();
  }

  /** A typo in an allow-list has to deny, never widen. */
  @Test
  void anUnparseableListTrustsNoProxy() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> "proxy.example.com"), proxy(), false,
        forwardedProto("https"))).isFalse();
  }

  /**
   * The list is re-read on every call, so SET SERVER SETTING takes effect on the next mint, as it does
   * for HTTP; a cached parse must not outlive a change of the setting.
   */
  @Test
  void aChangedListTakesEffectOnTheNextCall() {
    final AtomicReference<String> setting = new AtomicReference<>("");
    final GrpcTransportSecurityInterceptor interceptor = new GrpcTransportSecurityInterceptor(setting::get);

    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isFalse();
    setting.set(PROXY_IP);
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isTrue();
    setting.set("10.9.9.9");
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isFalse();
  }

  /** TLS and loopback stay safe whatever the list and the metadata say. */
  @Test
  void theTrustedProxyListNeverNarrowsTlsOrLoopback() {
    final GrpcTransportSecurityInterceptor interceptor = new GrpcTransportSecurityInterceptor(() -> PROXY_IP);
    assertThat(decisionFor(interceptor, remote(), true, forwardedProto("http"))).isTrue();
    assertThat(decisionFor(interceptor, new InetSocketAddress("127.0.0.1", 51000), false, forwardedProto("http")))
        .isTrue();
  }

  /**
   * The metadata key is read only from a listed peer; a supplier that throws must still not let anything
   * through, and must not fail the call.
   */
  @Test
  void aFailingSettingSupplierTrustsNoProxy() {
    assertThat(decisionFor(new GrpcTransportSecurityInterceptor(() -> {
      throw new IllegalStateException("configuration unavailable");
    }), proxy(), false, forwardedProto("https"))).isFalse();
  }

  /**
   * Reachability: the interceptor the gRPC server registers is built by {@code forConfiguration} from the
   * server's own configuration, so the listed proxy has to be honored through that factory - and a change
   * of the setting on that configuration object, which is what SET SERVER SETTING makes, has to be seen.
   */
  @Test
  void theServerFactoryReadsTheTrustedProxySettingLive() {
    final ContextConfiguration configuration = new ContextConfiguration();
    final GrpcTransportSecurityInterceptor interceptor = GrpcTransportSecurityInterceptor.forConfiguration(configuration);

    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isFalse();

    configuration.setValue(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES, PROXY_IP);
    assertThat(decisionFor(interceptor, proxy(), false, forwardedProto("https"))).isTrue();
    assertThat(decisionFor(interceptor, remote(), false, forwardedProto("https"))).isFalse();
  }

  /**
   * The refusal names the setting that would let a proxy through: an operator behind one is reading this
   * message to find out why the mint is refused, and it previously offered only TLS and loopback.
   */
  @Test
  void theRefusalPointsAtTheTrustedProxySetting() throws Exception {
    final StatusException refusal = Context.current()
        .withValue(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY, false)
        .call(() -> {
          try {
            ArcadeDbGrpcAdminService.requireTransportSafeForSecrets();
            return null;
          } catch (final StatusException e) {
            return e;
          }
        });

    assertThat(refusal).isNotNull();
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(refusal.getStatus().getDescription())
        .contains(GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey())
        .contains("x-forwarded-proto");
  }

  private static boolean decisionFor(final SocketAddress remoteAddress, final boolean tls) {
    return decisionFor(new GrpcTransportSecurityInterceptor(), remoteAddress, tls, new Metadata());
  }

  /**
   * Runs one call through the interceptor and reports the value it published, which is the only thing
   * the interceptor produces.
   */
  private static boolean decisionFor(final GrpcTransportSecurityInterceptor interceptor,
      final SocketAddress remoteAddress, final boolean tls, final Metadata headers) {
    final Attributes.Builder attributes = Attributes.newBuilder();
    if (remoteAddress != null)
      attributes.set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remoteAddress);
    if (tls)
      attributes.set(Grpc.TRANSPORT_ATTR_SSL_SESSION, unhandshakenSslSession());

    final AtomicReference<Boolean> published = new AtomicReference<>();

    interceptor.interceptCall(
        new AttributesOnlyServerCall(attributes.build()),
        headers,
        (call, received) -> {
          published.set(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY.get());
          return new ServerCall.Listener<>() {
          };
        });

    assertThat(published.get()).as("the interceptor must publish a decision on every call").isNotNull();
    // Outside the call, the key is unset again - the guard must not leak a permissive value into
    // whatever runs next on this thread.
    assertThat(GrpcTransportSecurityInterceptor.SECRET_SAFE_TRANSPORT_KEY.get(Context.current())).isNull();
    return published.get();
  }

  /**
   * A real {@link SSLSession}, taken from a fresh {@code SSLEngine} that has not handshaken. The
   * interceptor tests this attribute for PRESENCE only - gRPC sets it exactly when the transport is
   * TLS - so an un-handshaken session is the honest stand-in and needs no stubbing at all.
   */
  private static SSLSession unhandshakenSslSession() {
    try {
      return SSLContext.getDefault().createSSLEngine().getSession();
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("the platform must provide a default SSLContext", e);
    }
  }

  /**
   * A {@link ServerCall} that answers only what the interceptor asks of it. Hand written rather than
   * mocked so the test states exactly which parts of the call the interceptor is allowed to depend on:
   * if it ever starts reading something else, this fails loudly instead of returning a mock default.
   */
  private static final class AttributesOnlyServerCall extends ServerCall<Object, Object> {
    private final Attributes attributes;

    private AttributesOnlyServerCall(final Attributes attributes) {
      this.attributes = attributes;
    }

    @Override
    public Attributes getAttributes() {
      return attributes;
    }

    @Override
    public void request(final int numMessages) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendHeaders(final Metadata headers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendMessage(final Object message) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      throw new UnsupportedOperationException();
    }
  }

}
