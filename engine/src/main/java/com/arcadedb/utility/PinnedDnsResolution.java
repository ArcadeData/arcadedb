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
package com.arcadedb.utility;

import java.net.InetAddress;
import java.util.Locale;
import java.util.Map;

/**
 * Per-thread registry of hostname to already-validated IP addresses, used to close the DNS-rebinding (TOCTOU) window
 * when the server fetches a caller-supplied URL.
 * <p>
 * {@link SafeHttpFetcher} validates a hostname's addresses and then opens a connection <i>by hostname</i>, because
 * connecting to a literal IP would break TLS: the JDK silently drops a caller-set {@code Host} header, and HTTPS would
 * additionally need SNI and certificate-hostname verification reproduced by hand. The connection therefore performs its
 * own, independent resolution, which can return a different answer than the one that was validated.
 * <p>
 * The fix is to constrain resolution itself rather than the connection. A {@code java.net.spi.InetAddressResolverProvider}
 * (JEP 418, Java 18+) installed by the server consults this registry first: while a fetch is in progress, the hostname
 * being fetched resolves only to the addresses that were already checked, and every other name resolves normally
 * through the built-in resolver. Because the substitution happens below the socket layer, the connection is still made
 * by hostname and TLS behaves exactly as it does without pinning.
 * <p>
 * <b>The binding is per-thread and must always be cleared in a finally block.</b> {@link SafeHttpFetcher} owns that
 * lifecycle; nothing else should bind.
 * <p>
 * When no provider is installed - which is the case for every embedded use of {@code arcadedb-engine}, since the
 * provider ships only with the server - nothing consults the binding, and {@link SafeHttpFetcher} falls back to its
 * documented behaviour of validating immediately before connecting and relying on the JVM positive DNS cache.
 * Installing a resolver provider is a JVM-global, single-slot decision, so it is deliberately not taken on an
 * embedder's behalf.
 * <p>
 * A {@link ThreadLocal} is used rather than a {@code ScopedValue} because the latter is still a preview API on Java 21.
 * The whole fetch (validation, connect, response) runs synchronously on the calling thread, so a thread-local binding
 * covers exactly the resolution performed by that connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PinnedDnsResolution {

  private static final ThreadLocal<Map<String, InetAddress[]>> PINNED = new ThreadLocal<>();

  /**
   * Set once by the resolver provider when the JVM installs it. Never unset: the system-wide resolver is created at the
   * first lookup and cannot be replaced afterwards.
   */
  private static volatile boolean providerInstalled = false;

  private PinnedDnsResolution() {
  }

  /**
   * Called by the resolver provider as it is instantiated, so callers can tell whether pinning is actually in effect.
   */
  public static void markProviderInstalled() {
    providerInstalled = true;
  }

  /**
   * Returns true when a resolver provider backed by this registry has been instantiated in the current JVM, i.e. when
   * a binding will genuinely constrain resolution. Informational only - {@link #bind} does not depend on it. Note the
   * provider is created lazily at the first lookup needing resolution, so this can read false before any name has been
   * resolved even in a JVM where the provider is registered.
   */
  public static boolean isPinningAvailable() {
    return providerInstalled;
  }

  /**
   * Restricts {@code host} to {@code addresses} for the current thread until {@link #clear()} is called.
   * <p>
   * The binding is recorded unconditionally, not only when {@link #isPinningAvailable()} is true. The system-wide
   * resolver is created lazily at the first lookup that actually needs resolving - a literal IP address is
   * short-circuited by {@link InetAddress} and never reaches a provider - so {@code providerInstalled} would otherwise
   * depend on what the JVM happened to look up first. Recording the binding regardless is harmless when no provider is
   * installed (nothing consults it) and removes that ordering dependency entirely.
   */
  public static void bind(final String host, final InetAddress[] addresses) {
    if (host == null || addresses == null || addresses.length == 0)
      return;
    PINNED.set(Map.of(host.toLowerCase(Locale.ROOT), addresses));
  }

  /**
   * Removes any binding for the current thread. Must be called in a finally block: a leaked binding would pin a pooled
   * worker thread's resolution of that hostname for every later, unrelated request it serves.
   */
  public static void clear() {
    PINNED.remove();
  }

  /**
   * Returns the pinned addresses for {@code host} on the current thread, or null when the name is not pinned - in which
   * case the caller must fall through to the built-in resolver. Called by the resolver provider on every lookup, so it
   * stays allocation-free and returns fast for the overwhelmingly common unpinned case.
   */
  public static InetAddress[] lookup(final String host) {
    final Map<String, InetAddress[]> pinned = PINNED.get();
    if (pinned == null || host == null)
      return null;
    return pinned.get(host.toLowerCase(Locale.ROOT));
  }
}
