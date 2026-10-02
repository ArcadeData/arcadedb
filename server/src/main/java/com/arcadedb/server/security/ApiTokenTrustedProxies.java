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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.IPAddressBlocklist;

import java.util.logging.Level;

/**
 * The trusted-proxy rules an API token mint applies to a cleartext connection, shared by the HTTP route
 * ({@code PostApiTokenHandler}) and the gRPC {@code CreateApiToken} RPC ({@code GrpcTransportSecurityInterceptor})
 * so the two control planes keep one trust boundary rather than two copies of it (issues #7804, #7821).
 */
public final class ApiTokenTrustedProxies {
  private ApiTokenTrustedProxies() {
  }

  /**
   * Parses {@link GlobalConfiguration#SERVER_API_TOKEN_TRUSTED_PROXIES} into the matcher
   * {@code PostApiTokenHandler.isTransportSafeForSecrets} and
   * {@code GrpcTransportSecurityInterceptor} consult. A list that does not parse yields an empty matcher, which
   * trusts nobody: a typo in an allow-list has to deny, never widen. The operator is told at SEVERE, because
   * the symptom on its own - mints refused from behind the proxy they did configure - does not point here.
   * <p>
   * {@link IPAddressBlocklist} is reused rather than copied: it is the codebase's one CIDR parser, it already
   * rejects hostnames instead of resolving them, and duplicating a second matcher is how GHSA-67m7-7w7g-mpmh
   * got its bypass. Read {@code isBlocked} here as "this address is in the set" - the class is a set matcher;
   * only its callers decide whether membership allows or denies.
   * <p>
   * One consequence of that reuse is worth stating, since the class was written for a block-list and this is
   * the one allow-list using it: it also matches an IPv6 address that merely <em>encodes</em> a listed IPv4
   * one (IPv4-mapped, 6to4, Teredo, NAT64), so an entry of {@code 10.0.0.5} matches a peer arriving as
   * {@code ::ffff:10.0.0.5}. That is the intended reading - it is the same host - and it does not widen the
   * trust boundary, because a peer still has to complete a TCP handshake from the address it claims; an
   * attacker able to do that from the operator's proxy address is already on the path and can read the
   * cleartext leg without forging anything.
   */
  public static IPAddressBlocklist parse(final String csv) {
    try {
      return IPAddressBlocklist.parse(csv);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(ApiTokenTrustedProxies.class, Level.SEVERE,
          "Ignoring the whole of %s ('%s'): %s. No reverse proxy is trusted to vouch for the transport of an API "
              + "token mint until the list parses", null, GlobalConfiguration.SERVER_API_TOKEN_TRUSTED_PROXIES.getKey(),
          csv, e.getMessage());
      return IPAddressBlocklist.parse(null);
    }
  }

  /**
   * Whether every hop that reported a scheme in {@code X-Forwarded-Proto} encrypted its leg. The header carries
   * one entry per proxy, oldest (the client's own leg) first; one cleartext hop anywhere in the chain put the
   * response on a wire in the clear, and it does not matter which one. A blank or absent header reports nothing,
   * and nothing is not https.
   * <p>
   * The {@code -1} limit is load-bearing, not tidiness. {@code String.split} discards trailing empty strings by
   * default, so {@code "https,"} would come back as a single {@code ["https"]} and read as one fully-encrypted
   * hop - while the interior form {@code "https,,https"} was refused. A client can send that trailing comma and
   * a pass-through proxy forwards it unchanged, so the default limit turned an empty hop into a way past the
   * check depending only on where in the string it sat (found reviewing PR #7824).
   */
  public static boolean forwardedProtoIsFullyEncrypted(final String forwardedProto) {
    if (forwardedProto == null || forwardedProto.isBlank())
      return false;

    for (final String hop : forwardedProto.split(",", -1))
      if (!"https".equalsIgnoreCase(hop.trim()))
        return false;

    return true;
  }
}
