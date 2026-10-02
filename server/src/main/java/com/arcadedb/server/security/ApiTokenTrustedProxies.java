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
 * <p>
 * A listed proxy reports the client's scheme through {@code X-Forwarded-Proto}, through the standard RFC 7239
 * {@code Forwarded} header, or both (issue #7822). {@link #reportedForwardedProto} flattens whatever arrived into one
 * comma-separated list of schemes, one per hop, and {@link #forwardedProtoIsFullyEncrypted} requires every one of them
 * to be https.
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
   * Every scheme the request's proxies reported, from {@code X-Forwarded-Proto} and from the RFC 7239 {@code Forwarded}
   * header alike, as the one comma-separated list {@link #forwardedProtoIsFullyEncrypted} checks; {@code null} when
   * neither header is present. The two are concatenated rather than either one preferred, so every scheme reported in
   * either header has to be https (issue #7822): a proxy writes one of them honestly and a client can inject the other,
   * and preferring a header would let the injected one outvote the honest one whenever the proxy happened to write the
   * other.
   *
   * @param xForwardedProto every {@code X-Forwarded-Proto} value in the order received, or null when absent
   * @param forwarded       every {@code Forwarded} header line in the order received, or null when absent
   */
  public static String reportedForwardedProto(final Iterable<String> xForwardedProto, final Iterable<String> forwarded) {
    final String fromXForwardedProto = joinForwardedProto(xForwardedProto);
    final String fromForwarded = isEmpty(forwarded) ? null : protosOfForwarded(forwarded);
    if (fromForwarded == null)
      return fromXForwardedProto;
    if (fromXForwardedProto == null)
      return fromForwarded;
    return fromXForwardedProto + "," + fromForwarded;
  }

  /**
   * Flattens every {@code X-Forwarded-Proto} value into one comma-separated list, so that all of them are read and not
   * just the first, or {@code null} when the header is absent.
   * <p>
   * This is the difference between trusting the proxy and trusting whoever reached it. A proxy configured to
   * <em>append</em> rather than overwrite leaves a client-supplied header in place and adds its own after it, so the
   * request arrives carrying two values and the client wrote the first one. Requiring every value makes an injected
   * one useless: an injected {@code https} still leaves the proxy's own honest {@code http} in the list.
   */
  public static String joinForwardedProto(final Iterable<String> values) {
    return isEmpty(values) ? null : String.join(",", values);
  }

  /**
   * Parses every RFC 7239 {@code Forwarded} header line into one {@code proto} per element, comma-separated in the
   * order received - the parsing counterpart of {@link #joinForwardedProto}. RFC 7239 allows several lines and several
   * comma-separated elements per line, each element being one hop: a {@code ;}-separated list of {@code name=value}
   * pairs whose value may be a quoted string, so a comma or semicolon inside quotes is part of the value.
   * <p>
   * Each element contributes exactly one entry, and the entry is empty - a hop that reported nothing, which
   * {@link #forwardedProtoIsFullyEncrypted} refuses - whenever the element carries no {@code proto}, is malformed, or
   * reports something that is not a URI scheme. The missing-{@code proto} case is the one that matters: a proxy that
   * appends an element recording only {@code for=} would otherwise leave a client-injected {@code proto=https} as the
   * only scheme in the header, and have it believed.
   */
  static String protosOfForwarded(final Iterable<String> lines) {
    final StringBuilder protos = new StringBuilder(16);
    final StringBuilder token = new StringBuilder(32);
    boolean first = true;
    for (final String line : lines)
      first = parseForwardedLine(line == null ? "" : line, protos, token, first);
    return protos.toString();
  }

  /**
   * Appends one entry to {@code protos} per element of {@code line}.
   *
   * @return whether {@code protos} is still empty of entries, i.e. the next entry needs no leading comma
   */
  private static boolean parseForwardedLine(final String line, final StringBuilder protos, final StringBuilder token,
      final boolean firstEntry) {
    token.setLength(0);
    boolean first = firstEntry;
    String name = null;
    String proto = null;
    boolean malformed = false;
    boolean inQuotes = false;
    // A value that was a quoted string: it is taken verbatim, and nothing but whitespace may follow its closing quote.
    boolean quoted = false;

    final int length = line.length();
    // One position past the end acts as a closing ',' so the last element is emitted like every other.
    for (int i = 0; i <= length; i++) {
      final char c = i < length ? line.charAt(i) : ',';

      if (inQuotes && i < length) {
        if (c == '\\' && i + 1 < length)
          token.append(line.charAt(++i));
        else if (c == '"')
          inQuotes = false;
        else
          token.append(c);
        continue;
      }

      switch (c) {
      case '"' -> {
        // A quoted string is only legal as a whole value: not as a name, not after a token or another quoted string.
        if (name == null || quoted || !token.toString().isBlank())
          malformed = true;
        else {
          token.setLength(0);
          inQuotes = true;
          quoted = true;
        }
      }
      case '=' -> {
        if (name != null)
          malformed = true;
        else {
          name = token.toString().trim();
          token.setLength(0);
        }
      }
      case ';', ',' -> {
        if (inQuotes) {
          // Only reachable at the end of the line: the quoted string was never closed.
          malformed = true;
          inQuotes = false;
        }

        if (name == null) {
          // A bare token is not a pair; an empty pair (a stray ';') carries nothing either way.
          if (!token.toString().isBlank())
            malformed = true;
        } else if (name.isEmpty())
          malformed = true;
        else if ("proto".equalsIgnoreCase(name)) {
          if (proto != null)
            malformed = true; // RFC 7239 section 4: a parameter MUST NOT occur more than once per element
          else
            proto = quoted ? token.toString() : token.toString().trim();
        }
        name = null;
        quoted = false;
        token.setLength(0);

        if (c == ',') {
          if (!first)
            protos.append(',');
          first = false;
          if (!malformed && proto != null && isUriScheme(proto))
            protos.append(proto);
          proto = null;
          malformed = false;
        }
      }
      default -> {
        if (!quoted)
          token.append(c);
        else if (!Character.isWhitespace(c))
          malformed = true;
      }
      }
    }
    return first;
  }

  /**
   * RFC 3986 {@code scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )}. Anything else - a comma or blank above all,
   * which a quoted value could otherwise smuggle into the joined list as extra hops - is not a scheme.
   */
  private static boolean isUriScheme(final String value) {
    if (value.isEmpty())
      return false;
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      final boolean alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
      if (i == 0 ? !alpha : !(alpha || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'))
        return false;
    }
    return true;
  }

  private static boolean isEmpty(final Iterable<String> values) {
    return values == null || !values.iterator().hasNext();
  }

  /**
   * Whether every hop that reported a scheme encrypted its leg, given the list {@link #reportedForwardedProto}
   * builds. The list carries one entry per proxy, oldest (the client's own leg) first; one cleartext hop anywhere
   * in the chain put the response on a wire in the clear, and it does not matter which one. A blank or absent list
   * reports nothing, and nothing is not https.
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
