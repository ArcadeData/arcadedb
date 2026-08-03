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
package com.arcadedb.server.security;

import com.arcadedb.utility.PinnedDnsResolution;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that the server's {@link PinnedInetAddressResolverProvider} is actually installed and that a pin genuinely
 * constrains name resolution — the mechanism that closes the DNS-rebinding window left open by GHSA-4w2m-77c8-83mw.
 * <p>
 * The rebinding attack is a hostname that resolves to a permitted address when {@code SafeHttpFetcher} validates it and
 * to an internal one moments later when the connection resolves it again. Rather than assert on timing, these tests
 * assert the property that makes the attack impossible: while a pin is bound, resolution of that hostname returns the
 * pinned addresses and nothing else, so the second lookup cannot differ from the validated one.
 * <p>
 * Note this runs in the {@code arcadedb-server} module because that is where the {@code META-INF/services}
 * registration lives. {@code arcadedb-engine} deliberately does not register a provider — see
 * {@link PinnedInetAddressResolverProvider} for why — and the engine-side fallback is asserted by
 * {@code SafeHttpFetcherTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PinnedDnsResolutionIT {

  // Names that must not resolve through any real DNS, so a successful lookup can only come from the pin. Each test
  // uses its OWN name: InetAddress caches negative lookups (networkaddress.cache.negative.ttl, 10s by default) ABOVE
  // the resolver SPI, so a name that has already failed to resolve would keep failing from cache and never reach the
  // provider - which would make these tests pass or fail for the wrong reason.
  private static final String PINNED_HOST     = "pinned-host-a.invalid";
  private static final String CLEARED_HOST    = "pinned-host-b.invalid";
  private static final String OTHER_THREAD_HOST = "pinned-host-c.invalid";
  private static final String CONTROL_HOST    = "control-host.invalid";

  @BeforeEach
  void ensureResolverInstalled() {
    // The system-wide resolver is created from the first ServiceLoader-discovered provider at the first lookup that
    // actually needs resolving. A literal IP address is short-circuited by InetAddress and never reaches a provider, so
    // bootstrapping with one would leave the provider uninstantiated and make these tests order-dependent. Force it
    // with a NAME lookup; the name need not resolve, it only has to reach the resolver.
    try {
      InetAddress.getAllByName("resolver-bootstrap.invalid");
    } catch (final UnknownHostException expected) {
      // Expected: we only need the lookup to have gone through the resolver.
    }
  }

  @AfterEach
  void clearPin() {
    PinnedDnsResolution.clear();
  }

  @Test
  void providerIsInstalledInTheServerJvm() {
    assertThat(PinnedDnsResolution.isPinningAvailable())
        .as("the server module must register an InetAddressResolverProvider so pins take effect").isTrue();
  }

  @Test
  void pinnedHostResolvesOnlyToTheValidatedAddresses() throws UnknownHostException {
    // Control on a SEPARATE name in the same .invalid TLD: proves such names are genuinely unresolvable here, so the
    // success below cannot be coming from real DNS. It must not be the pinned name itself, or the negative cache would
    // shadow the pin.
    assertThatThrownBy(() -> InetAddress.getAllByName(CONTROL_HOST)).isInstanceOf(UnknownHostException.class);

    final InetAddress pinned = InetAddress.getByAddress(new byte[] { 93, (byte) 184, (byte) 216, 34 });
    PinnedDnsResolution.bind(PINNED_HOST, new InetAddress[] { pinned });

    final InetAddress[] resolved = InetAddress.getAllByName(PINNED_HOST);
    assertThat(resolved).as("a pinned name resolves to exactly the validated addresses").hasSize(1);
    assertThat(resolved[0].getHostAddress()).isEqualTo("93.184.216.34");
  }

  @Test
  void unpinnedNamesStillResolveNormally() throws UnknownHostException {
    // The provider must be inert outside a validated fetch: every other name goes to the built-in resolver unchanged.
    PinnedDnsResolution.bind(PINNED_HOST, new InetAddress[] { InetAddress.getLoopbackAddress() });

    assertThat(InetAddress.getByName("127.0.0.1").getHostAddress()).isEqualTo("127.0.0.1");
    assertThat(InetAddress.getByName("localhost").isLoopbackAddress()).isTrue();
  }

  @Test
  void clearingRemovesThePin() {
    PinnedDnsResolution.bind(CLEARED_HOST, new InetAddress[] { InetAddress.getLoopbackAddress() });
    assertThat(PinnedDnsResolution.lookup(CLEARED_HOST)).as("the pin is recorded").isNotNull();

    PinnedDnsResolution.clear();
    assertThat(PinnedDnsResolution.lookup(CLEARED_HOST)).as("the pin is gone from the registry").isNull();

    // End-to-end: resolution now falls through to the built-in resolver, which cannot resolve this name. A leaked pin
    // would constrain this thread's resolution of the name for every later, unrelated request it serves, so the
    // fetcher clearing in a finally block is itself part of the security property.
    //
    // The name is deliberately NOT resolved while pinned: InetAddress caches SUCCESSFUL lookups too
    // (networkaddress.cache.ttl, 30s by default), so doing that would serve the pinned answer from cache afterwards
    // and the assertion would say nothing about whether the pin had really been released.
    assertThatThrownBy(() -> InetAddress.getAllByName(CLEARED_HOST)).isInstanceOf(UnknownHostException.class);
  }

  @Test
  void pinIsConfinedToTheBindingThread() throws Exception {
    PinnedDnsResolution.bind(OTHER_THREAD_HOST, new InetAddress[] { InetAddress.getLoopbackAddress() });

    // A pin bound while one request is being served must never affect a concurrent request on another thread.
    final Throwable[] otherThreadResult = new Throwable[1];
    final Thread other = new Thread(() -> {
      try {
        InetAddress.getAllByName(OTHER_THREAD_HOST);
      } catch (final Throwable t) {
        otherThreadResult[0] = t;
      }
    });
    other.start();
    other.join();

    assertThat(otherThreadResult[0])
        .as("another thread must not see this thread's pin").isInstanceOf(UnknownHostException.class);
  }
}
