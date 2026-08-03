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

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.PinnedDnsResolution;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.stream.Stream;

/**
 * Name-resolution provider (JEP 418, Java 18+) that lets the server pin a hostname to addresses it has already
 * validated, for the duration of a single outbound fetch.
 * <p>
 * This closes the DNS-rebinding window in {@code SafeHttpFetcher}. Validating a hostname and then opening a connection
 * by that hostname leaves the connection free to resolve the name a second time and reach a different address; pinning
 * makes the second resolution return only the addresses the first one approved. The alternative - connecting to a
 * literal IP - would require overriding the {@code Host} header (silently dropped by the JDK) and hand-rolling SNI and
 * certificate-hostname verification for HTTPS. Substituting the resolver instead leaves the request addressed by
 * hostname, so TLS behaves exactly as it would without pinning and no verification logic is reimplemented.
 * <p>
 * Every lookup that is not currently pinned is delegated unchanged to the JDK's built-in resolver, so this is inert
 * outside the window of a validated fetch.
 * <p>
 * <b>Why this ships with the server and not with the engine.</b> A JVM maintains a <i>single</i> system-wide resolver:
 * it is created from the first provider {@link java.util.ServiceLoader} finds - in an order the JDK explicitly leaves
 * implementation specific - at the first lookup, and cannot be replaced afterwards. Registering one therefore takes a
 * JVM-global decision away from whoever owns the process, and would silently race any other provider on the classpath.
 * That is acceptable for the server distribution, where ArcadeDB owns the JVM, and is not acceptable for
 * {@code arcadedb-engine}, which is embedded inside other applications. Consequently the {@code META-INF/services}
 * registration exists only in {@code arcadedb-server}; embedded users get {@code SafeHttpFetcher}'s documented
 * fallback instead. An application that embeds the server and needs its own resolver provider can exclude this one by
 * removing the service registration from its packaging.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PinnedInetAddressResolverProvider extends InetAddressResolverProvider {

  @Override
  public InetAddressResolver get(final Configuration configuration) {
    final InetAddressResolver builtin = configuration.builtinResolver();

    // This method must NEVER throw. A JVM has a single system-wide resolver, and the SPI contract is explicit that if
    // instantiating a provider's resolver fails, the system-wide resolver is not set and the error propagates to
    // whoever triggered the lookup - i.e. a failure here does not merely disable pinning, it breaks ALL name
    // resolution in the process. The realistic trigger is a split deployment where arcadedb-server is newer than
    // arcadedb-engine and PinnedDnsResolution is absent, which surfaces as NoClassDefFoundError.
    //
    // Touching the class here forces that failure to happen inside this guard rather than later, on the first lookup,
    // from inside the returned resolver. On any failure we hand back the built-in resolver unchanged: pinning is
    // silently unavailable (SafeHttpFetcher then relies on its documented DNS-cache fallback) and networking keeps
    // working, which is the correct trade in both directions.
    try {
      PinnedDnsResolution.markProviderInstalled();
    } catch (final Throwable t) {
      try {
        LogManager.instance().log(this, Level.WARNING,
            "Cannot install the ArcadeDB pinned DNS resolver (%s: %s); outbound URL fetches will fall back to "
                + "validate-before-connect without address pinning. This usually means arcadedb-server and "
                + "arcadedb-engine are different versions.", t.getClass().getName(), t.getMessage());
      } catch (final Throwable ignore) {
        // Logging must never turn a degraded resolver into a broken one.
      }
      return builtin;
    }

    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(final String host, final LookupPolicy lookupPolicy) throws UnknownHostException {
        final InetAddress[] pinned = PinnedDnsResolution.lookup(host);
        if (pinned != null)
          return Arrays.stream(pinned);
        return builtin.lookupByName(host, lookupPolicy);
      }

      @Override
      public String lookupByAddress(final byte[] addr) throws UnknownHostException {
        // Reverse lookups are never pinned: nothing in the fetch path depends on them.
        return builtin.lookupByAddress(addr);
      }
    };
  }

  @Override
  public String name() {
    return "arcadedb-pinned";
  }
}
