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
package com.arcadedb.server.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks secrets in the text that leaves the server through the support bundle (log lines, JVM arguments, settings,
 * thread dumps). It is applied BEFORE anything is written to the bundle and counts what it masked, so the preview can
 * show it.
 * <p>
 * The rule is conservative: it is better to mask a harmless value than to leak a password. What is masked:
 * <ul>
 * <li>a value following {@code =} or {@code :} (also in JSON, {@code "name": "value"}) whose name contains
 * {@code password, passwd, passphrase, pwd, secret, token, api key, credential(s), private key, client key, access key,
 * session id, cookie}; unless the name ends with a word that says it is not the secret itself (length, size, count, max,
 * timeout, path, file, ...), so {@code token.length=32} and {@code rootPasswordPath=/run/secrets/x} are kept;</li>
 * <li>the value of {@code Authorization}, {@code Proxy-Authorization} and {@code Cookie} headers (the rest of the line);
 * {@code Bearer <token>} anywhere; the arguments after {@code --password} style options; {@code IDENTIFIED BY x};</li>
 * <li>the password of URLs with credentials ({@code scheme://user:pass@host});</li>
 * <li>PEM blocks ({@code -----BEGIN ...-----} to {@code -----END ...-----}), also across lines;</li>
 * <li>well known token shapes: ArcadeDB API tokens ({@code at-<hex>}), portal keys ({@code wsk_...}), JWTs, AWS access key
 * ids, GitHub tokens.</li>
 * </ul>
 * What it cannot catch: a secret that is not introduced by one of those names or shapes (a password in free prose, a
 * secret in query text, a connection string in a custom format), personal data, host names, IP addresses, database, user
 * and type names. Nothing is uploaded automatically: the user reviews the bundle.
 * <p>
 * The class is stateless and thread safe; the state of one file (the PEM block in progress, the count) lives in a
 * {@link Session}.
 */
public final class SupportRedactor {
  public static final String MASK = "***";
  public static final String PEM  = "[REDACTED PEM BLOCK]";

  private static final String KEYWORDS =
      "pass(?:word|wd|phrase)|pwd|secret|token|api[-_. ]?key|credentials?|private[-_]?key|client[-_]?key|access[-_]?key|session[-_]?id|cookie";

  // Cheap pre-filter: a line that matches none of these cannot be changed by any rule below.
  private static final Pattern PRE_FILTER = Pattern.compile(
      "(?i)" + KEYWORDS + "|authorization|bearer|identified\\s+by|-----|://[^/\\s]*@|wsk_|eyJ|AKIA|gh[pousr]_|\\bat-[0-9a-f]{20}");

  // Possessive: a long quoted value must neither backtrack nor recurse once per character (a 64K-character string overflows the
  // stack with a plain (?:a|b)* and is polynomial without the possessive quantifiers)
  private static final String QUOTED_VALUE = "\"(?:[^\"\\\\]++|\\\\.)*+\"?|'(?:[^'\\\\]++|\\\\.)*+'?|\\\\\"(?:[^\\\\]++|\\\\(?!\"))*+\\\\\"";
  // (keyword)(rest of the name)(separator)(value)
  private static final Pattern KEY_VALUE = Pattern.compile(
      "(?i)(" + KEYWORDS + ")([\\w.\\-]{0,64})((?:\\\\?[\"'])?\\s*[=:]\\s*)(" + QUOTED_VALUE + "|\\S+)");

  // A name is not the secret itself when its last word says what it is about
  private static final Pattern BENIGN_SUFFIX = Pattern.compile(
      "(?i)(?:length|len|size|count|max|min|timeout|ttl|expiry|expiration|expires|enabled|disabled|path|file|dir|directory|policy|"
          + "type|algorithm|mode|interval|retries|attempts|url|uri|ms|secs|seconds|minutes|header|name|field|class|provider)$");

  private static final Pattern HEADER_VALUE = Pattern.compile(
      "(?i)\\b(authorization|proxy-authorization|set-cookie|cookie|x-api-key|x-auth-token)(\\\\?[\"']?\\s*[:=]\\s*)(.*)");
  private static final Pattern BEARER       = Pattern.compile("(?i)\\b(bearer)(\\s+)[A-Za-z0-9._~+/=\\-]{8,}");
  private static final Pattern CLI_OPTION   = Pattern.compile(
      "(?i)(--?[\\w.\\-]{0,64}(?:" + KEYWORDS + ")[\\w.\\-]{0,64})(\\s+)(?!-)(\\S+)");
  private static final Pattern IDENTIFIED   = Pattern.compile("(?i)(identified\\s+by\\s+)('[^']*'|\"[^\"]*\"|\\S+)");
  private static final Pattern URL_CREDS    = Pattern.compile("(?i)\\b([a-z][a-z0-9+.\\-]{0,31}://)([^/\\s:@]{1,256}):([^@\\s/]{0,256})@");
  private static final Pattern PEM_INLINE   = Pattern.compile("-----BEGIN [A-Z0-9 ]+-----.*?-----END [A-Z0-9 ]+-----");
  private static final Pattern PEM_BEGIN    = Pattern.compile("-----BEGIN [A-Z0-9 ]+-----");
  private static final Pattern PEM_END      = Pattern.compile("-----END [A-Z0-9 ]+-----");
  private static final Pattern SHAPES       = Pattern.compile(
      "\\bat-[0-9a-f]{32,}\\b|\\bwsk_[A-Za-z0-9_\\-]{20,}|\\beyJ[A-Za-z0-9_\\-]{8,4096}\\.[A-Za-z0-9_\\-]{8,4096}\\.[A-Za-z0-9_\\-]*|"
          + "\\bAKIA[0-9A-Z]{16}\\b|\\bgh[pousr]_[A-Za-z0-9]{20,}");

  private static final Pattern PASSWORD_OPTION_ONLY = Pattern.compile("(?i)^--?[\\w.\\-]{0,64}(?:" + KEYWORDS + ")[\\w.\\-]{0,64}$");

  private SupportRedactor() {
  }

  /**
   * Redaction of one file (or one document): keeps the count and the PEM block in progress across lines.
   */
  public static final class Session {
    private int     count;
    private boolean inPem;

    public int getCount() {
      return count;
    }

    /**
     * Redacts one line.
     *
     * @return the redacted line; {@code null} when the line is swallowed by a PEM block already replaced by one marker
     */
    public String redactLine(final String line) {
      if (line == null)
        return null;

      if (inPem) {
        final Matcher end = PEM_END.matcher(line);
        if (!end.find())
          return null;
        inPem = false;
        // What follows the END marker on the same line is kept, after the usual rules
        final String remainder = redactPlain(line.substring(end.end()));
        return remainder.isBlank() ? null : remainder;
      }

      if (line.indexOf("-----") >= 0) {
        final Matcher inline = PEM_INLINE.matcher(line);
        String current = line;
        if (inline.find()) {
          final StringBuilder out = new StringBuilder(line.length());
          inline.reset();
          while (inline.find()) {
            inline.appendReplacement(out, Matcher.quoteReplacement(PEM));
            count++;
          }
          inline.appendTail(out);
          current = out.toString();
        }
        final Matcher begin = PEM_BEGIN.matcher(current);
        if (begin.find()) {
          inPem = true;
          count++;
          return redactPlain(current.substring(0, begin.start())) + PEM;
        }
        return redactPlain(current);
      }

      return redactPlain(line);
    }

    /** Ends the block in progress: a new log entry starts, an unterminated PEM block must not swallow the whole file. */
    public void resetBlock() {
      inPem = false;
    }

    /** Redacts a single value (a setting, a JVM argument, a message) that has no lines of its own. */
    public String redact(final String text) {
      if (text == null)
        return null;
      if (text.indexOf('\n') < 0 && text.indexOf('\r') < 0) {
        final boolean saved = inPem;
        inPem = false;
        final String result = redactLine(text);
        inPem = saved;
        return result == null ? PEM : result;
      }
      final String[] lines = text.split("\r?\n", -1);
      final StringBuilder out = new StringBuilder(text.length());
      final boolean saved = inPem;
      inPem = false;
      for (final String l : lines) {
        final String r = redactLine(l);
        if (r != null) {
          if (out.length() > 0)
            out.append('\n');
          out.append(r);
        }
      }
      inPem = saved;
      return out.toString();
    }

    /** Redacts the JVM input arguments: {@code -Dx.password=abc} and {@code --password abc} (the next argument). */
    public List<String> redactArguments(final List<String> arguments) {
      final List<String> result = new ArrayList<>(arguments.size());
      boolean maskNext = false;
      for (final String arg : arguments) {
        if (maskNext) {
          maskNext = false;
          if (!arg.startsWith("-")) {
            result.add(MASK);
            count++;
            continue;
          }
        }
        result.add(redact(arg));
        if (arg.indexOf('=') < 0 && PASSWORD_OPTION_ONLY.matcher(arg).matches() && !BENIGN_SUFFIX.matcher(arg).find())
          maskNext = true;
      }
      return result;
    }

    private String redactPlain(final String line) {
      if (line.isEmpty() || !PRE_FILTER.matcher(line).find())
        return line;

      String current = line;

      // Header values: the whole rest of the line
      Matcher m = HEADER_VALUE.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          final String value = m.group(3);
          if (isAlreadyMasked(value) || value.isBlank()) {
            m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
            continue;
          }
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + m.group(2) + MASK));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = URL_CREDS.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          if (MASK.equals(m.group(3))) {
            m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
            continue;
          }
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + m.group(2) + ":" + MASK + "@"));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = KEY_VALUE.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          final String rest = m.group(2);
          String value = m.group(4);
          if (!rest.isEmpty() && BENIGN_SUFFIX.matcher(rest).find() || isAlreadyMasked(value) || isNeutral(value)) {
            m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
            continue;
          }
          String tail = "";
          final char first = value.charAt(0);
          if (first != '"' && first != '\'' && first != '\\') {
            // In a query string the value ends at the next parameter
            final int start = m.start();
            final boolean inQueryString = start > 0 && (current.charAt(start - 1) == '?' || current.charAt(start - 1) == '&'
                || queryStringBefore(current, start));
            if (inQueryString) {
              final int amp = value.indexOf('&');
              if (amp >= 0) {
                tail = value.substring(amp);
                value = value.substring(0, amp);
              }
            }
          }
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + rest + m.group(3) + quoteLike(value) + tail));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = CLI_OPTION.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          if (isAlreadyMasked(m.group(3)) || BENIGN_SUFFIX.matcher(m.group(1)).find()) {
            m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
            continue;
          }
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + m.group(2) + MASK));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = IDENTIFIED.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          if (isAlreadyMasked(m.group(2))) {
            m.appendReplacement(out, Matcher.quoteReplacement(m.group()));
            continue;
          }
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + MASK));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = BEARER.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          m.appendReplacement(out, Matcher.quoteReplacement(m.group(1) + m.group(2) + MASK));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      m = SHAPES.matcher(current);
      if (m.find()) {
        final StringBuilder out = new StringBuilder(current.length());
        m.reset();
        while (m.find()) {
          m.appendReplacement(out, Matcher.quoteReplacement(MASK));
          count++;
        }
        m.appendTail(out);
        current = out.toString();
      }

      return current;
    }
  }

  /** Redacts one text with a throw-away session: for callers that do not need the count. */
  public static String redact(final String text) {
    return new Session().redact(text);
  }

  private static boolean queryStringBefore(final String text, final int start) {
    // "...?a=b&password=x": an '&' or '?' introduced this parameter, possibly after its own name prefix
    int i = start - 1;
    while (i >= 0 && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '.' || text.charAt(i) == '-' || text.charAt(i) == '_'))
      i--;
    return i >= 0 && (text.charAt(i) == '?' || text.charAt(i) == '&');
  }

  private static boolean isAlreadyMasked(final String value) {
    String v = value.trim();
    if (v.length() >= 2 && (v.charAt(0) == '"' || v.charAt(0) == '\'') && v.charAt(v.length() - 1) == v.charAt(0))
      v = v.substring(1, v.length() - 1);
    return v.equals(MASK) || v.startsWith("*****") || v.equals("<hidden>") || v.equals("[hidden]") || v.startsWith(MASK + "@");
  }

  /** Values that cannot be a secret: empty, booleans, null. */
  private static boolean isNeutral(final String value) {
    String v = value;
    if (v.length() >= 2 && (v.charAt(0) == '"' || v.charAt(0) == '\'') && v.charAt(v.length() - 1) == v.charAt(0))
      v = v.substring(1, v.length() - 1);
    if (v.isEmpty())
      return true;
    final String lower = v.toLowerCase(Locale.ROOT);
    return lower.equals("true") || lower.equals("false") || lower.equals("null") || lower.equals("none");
  }

  /** The mask keeps the quotes of the value, so JSON stays parseable. */
  private static String quoteLike(final String value) {
    if (value.startsWith("\\\"") && value.endsWith("\\\"") && value.length() >= 4)
      return "\\\"" + MASK + "\\\"";
    final char first = value.charAt(0);
    if (first == '"' || first == '\'')
      return first + MASK + first;
    return MASK;
  }
}
