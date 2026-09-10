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

import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
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

  /**
   * Runs one call through the interceptor and reports the value it published, which is the only thing
   * the interceptor produces.
   */
  private static boolean decisionFor(final SocketAddress remoteAddress, final boolean tls) {
    final Attributes.Builder attributes = Attributes.newBuilder();
    if (remoteAddress != null)
      attributes.set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remoteAddress);
    if (tls)
      attributes.set(Grpc.TRANSPORT_ATTR_SSL_SESSION, unhandshakenSslSession());

    final AtomicReference<Boolean> published = new AtomicReference<>();

    new GrpcTransportSecurityInterceptor().interceptCall(
        new AttributesOnlyServerCall(attributes.build()),
        new Metadata(),
        (call, headers) -> {
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
