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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SupportRedactorTest {

  private static String redact(final String text) {
    return SupportRedactor.redact(text);
  }

  @Test
  void passwordInSystemPropertyArgument() {
    assertThat(redact("-Darcadedb.server.rootPassword=Sup3rS3cret!")).isEqualTo("-Darcadedb.server.rootPassword=***");
    assertThat(redact("-Darcadedb.ssl.keyStorePassword=abc,def;ghi")).isEqualTo("-Darcadedb.ssl.keyStorePassword=***");
    assertThat(redact("-Darcadedb.support.clientKey=wsk_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG"))
        .isEqualTo("-Darcadedb.support.clientKey=***");
  }

  @Test
  void jvmArgumentsWithSeparatedValue() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    final List<String> result = session.redactArguments(
        List.of("-Xmx6g", "--password", "hunter2", "-Dfoo=bar", "--api-key", "abc123", "--password-file", "/run/secrets/pw", "--secret",
            "--verbose"));
    assertThat(result).containsExactly("-Xmx6g", "--password", "***", "-Dfoo=bar", "--api-key", "***", "--password-file",
        "/run/secrets/pw", "--secret", "--verbose");
    assertThat(session.getCount()).isEqualTo(2);
  }

  @Test
  void authorizationHeaders() {
    assertThat(redact("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc.def")).isEqualTo("Authorization: ***");
    assertThat(redact("authorization=Basic cm9vdDpwYXNzd29yZA==")).isEqualTo("authorization=***");
    assertThat(redact("Sent header Proxy-Authorization: Basic dXNlcjpwYXNz")).doesNotContain("dXNlcjpwYXNz");
    assertThat(redact("Cookie: a=1; arcadedb-session=xyz")).isEqualTo("Cookie: ***");
    assertThat(redact("the token is Bearer abcdefghijklmnop123")).isEqualTo("the token is Bearer ***");
    assertThat(redact("\"Authorization\":\"Bearer abcdefgh\"")).doesNotContain("abcdefgh");
  }

  @Test
  void urlsWithCredentials() {
    assertThat(redact("Connecting to https://admin:p%40ss@host.example.com:8443/path?x=1"))
        .isEqualTo("Connecting to https://admin:***@host.example.com:8443/path?x=1");
    assertThat(redact("jdbc:postgresql://user:secret@localhost:5432/db")).isEqualTo("jdbc:postgresql://user:***@localhost:5432/db");
    assertThat(redact("ftp://u:p@h/f and http://a:b@c")).isEqualTo("ftp://u:***@h/f and http://a:***@c");
    // no credentials: unchanged
    assertThat(redact("https://arcadedb.com/pricing.html")).isEqualTo("https://arcadedb.com/pricing.html");
    assertThat(redact("ssh://git@github.com/x/y")).isEqualTo("ssh://git@github.com/x/y");
  }

  @Test
  void queryStringParametersStopAtTheNextParameter() {
    assertThat(redact("GET /api?user=root&password=abc&db=orders")).isEqualTo("GET /api?user=root&password=***&db=orders");
    assertThat(redact("GET /api?a=1&access_token=xyz")).isEqualTo("GET /api?a=1&access_token=***");
  }

  @Test
  void jsonFields() {
    assertThat(redact("{\"user\":\"root\",\"password\":\"abc def\",\"x\":1}")).isEqualTo("{\"user\":\"root\",\"password\":\"***\",\"x\":1}");
    assertThat(redact("{\"rootPassword\": \"a\\\"b\", \"n\": 2}")).isEqualTo("{\"rootPassword\": \"***\", \"n\": 2}");
    assertThat(redact("{'secret': 'abc'}")).isEqualTo("{'secret': '***'}");
    assertThat(redact("payload \\\"password\\\":\\\"abc\\\" end")).doesNotContain("abc");
    // the JSON stays parseable
    final String redacted = redact("{\"user\":\"root\",\"password\":\"abc def\",\"token\":\"t\"}");
    assertThat(new JSONObject(redacted).getString("password")).isEqualTo("***");
    assertThat(new JSONObject(redacted).getString("token")).isEqualTo("***");
  }

  @Test
  void variousKeyNames() {
    for (final String key : List.of("password", "PASSWORD", "passwd", "passphrase", "secret", "clientSecret", "AWS_SECRET_ACCESS_KEY",
        "api_key", "api-key", "apiKey", "X-Api-Key", "access_token", "token", "credentials", "credential", "privateKey", "private_key",
        "clientKey", "accessKey", "sessionId", "dbPassword", "my.secret.value"))
      assertThat(redact(key + "=v4lu3")).as(key).doesNotContain("v4lu3");
    assertThat(redact("password: hunter2")).isEqualTo("password: ***");
    assertThat(redact("password : hunter2")).isEqualTo("password : ***");
    assertThat(redact("DB_PASSWORD=hunter2 DB_USER=root")).isEqualTo("DB_PASSWORD=*** DB_USER=root");
  }

  @Test
  void numericPasswordsAreMasked() {
    assertThat(redact("password=123456")).isEqualTo("password=***");
  }

  @Test
  void falsePositiveFriendlyCases() {
    assertThat(redact("token.length=32")).isEqualTo("token.length=32");
    assertThat(redact("arcadedb.server.rootPasswordPath=/run/secrets/root-pw")).isEqualTo("arcadedb.server.rootPasswordPath=/run/secrets/root-pw");
    assertThat(redact("arcadedb.ha.clusterTokenPath=/etc/token")).isEqualTo("arcadedb.ha.clusterTokenPath=/etc/token");
    assertThat(redact("password.maxAttempts=5 tokenTimeout=3600 secretFile=/x")).isEqualTo("password.maxAttempts=5 tokenTimeout=3600 secretFile=/x");
    assertThat(redact("passwordRequired=true")).isEqualTo("passwordRequired=true");
    assertThat(redact("password=")).isEqualTo("password=");
    assertThat(redact("password=null")).isEqualTo("password=null");
    assertThat(redact("Opening database /data/databases/orders with 4 buckets")).isEqualTo("Opening database /data/databases/orders with 4 buckets");
    assertThat(redact("2026-09-30 12:00:00.000 INFO  [HttpServer] Listening on 0.0.0.0:2480"))
        .isEqualTo("2026-09-30 12:00:00.000 INFO  [HttpServer] Listening on 0.0.0.0:2480");
    assertThat(redact("Login failed for user 'root'")).isEqualTo("Login failed for user 'root'");
    assertThat(redact("-Xmx6g -XX:+UseG1GC -Djava.io.tmpdir=/tmp")).isEqualTo("-Xmx6g -XX:+UseG1GC -Djava.io.tmpdir=/tmp");
  }

  @Test
  void alreadyMaskedValuesAreNotCounted() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    assertThat(session.redactLine("password=*****")).isEqualTo("password=*****");
    assertThat(session.redactLine("password=***")).isEqualTo("password=***");
    assertThat(session.redactLine("\"password\":\"<hidden>\"")).isEqualTo("\"password\":\"<hidden>\"");
    assertThat(session.getCount()).isZero();
  }

  @Test
  void pemBlockInsideOneLine() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    final String result = session.redactLine("key=-----BEGIN PRIVATE KEY-----MIIEvQIBADANBg-----END PRIVATE KEY----- done");
    assertThat(result).doesNotContain("MIIEvQ").contains(SupportRedactor.PEM).endsWith("done");
    assertThat(session.getCount()).isEqualTo(1);
  }

  @Test
  void pemBlockAcrossLines() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    assertThat(session.redactLine("before")).isEqualTo("before");
    assertThat(session.redactLine("-----BEGIN RSA PRIVATE KEY-----")).isEqualTo(SupportRedactor.PEM);
    assertThat(session.redactLine("MIIEowIBAAKCAQEA7")).isNull();
    assertThat(session.redactLine("abcdef==")).isNull();
    assertThat(session.redactLine("-----END RSA PRIVATE KEY-----")).isNull();
    assertThat(session.redactLine("after")).isEqualTo("after");
    assertThat(session.getCount()).isEqualTo(1);
  }

  @Test
  void unterminatedPemBlockEndsWithTheEntry() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    assertThat(session.redactLine("-----BEGIN CERTIFICATE-----")).isEqualTo(SupportRedactor.PEM);
    assertThat(session.redactLine("AAAA")).isNull();
    session.resetBlock();
    assertThat(session.redactLine("2026-09-30 12:00:00.000 INFO next entry")).isEqualTo("2026-09-30 12:00:00.000 INFO next entry");
  }

  @Test
  void multiLineTextWithPem() {
    final String result = redact("a\n-----BEGIN PRIVATE KEY-----\nAAA\n-----END PRIVATE KEY-----\nb");
    assertThat(result).isEqualTo("a\n" + SupportRedactor.PEM + "\nb");
  }

  @Test
  void knownTokenShapes() {
    assertThat(redact("created at-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef ok")).isEqualTo("created *** ok");
    assertThat(redact("key wsk_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG used")).isEqualTo("key *** used");
    assertThat(redact("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U end"))
        .isEqualTo("jwt *** end");
    assertThat(redact("AKIAIOSFODNN7EXAMPLE in env")).isEqualTo("*** in env");
    assertThat(redact("ghp_abcdefghijklmnopqrstuvwxyz0123456789 pushed")).isEqualTo("*** pushed");
  }

  @Test
  void sqlIdentifiedBy() {
    assertThat(redact("CREATE USER bob IDENTIFIED BY 'pw d'")).isEqualTo("CREATE USER bob IDENTIFIED BY ***");
    assertThat(redact("create user bob identified by pw")).isEqualTo("create user bob identified by ***");
  }

  @Test
  void countsEachRedaction() {
    final SupportRedactor.Session session = new SupportRedactor.Session();
    session.redactLine("password=a token=b");
    session.redactLine("nothing here");
    session.redactLine("https://u:p@h Authorization: Bearer abcdefgh1234");
    assertThat(session.getCount()).isEqualTo(4);
  }

  @Test
  void lineWithoutSecretsIsReturnedAsTheSameInstance() {
    final String line = "2026-09-30 12:00:00.000 INFO  [X] all good";
    assertThat(new SupportRedactor.Session().redactLine(line)).isSameAs(line);
  }

  // A hang detector against catastrophic backtracking, not a latency bound
  @Test
  @Timeout(120)
  void hugeLineWithoutKeywordDoesNotBacktrackBadly() {
    final String line = "a".repeat(2_000_000);
    assertThat(redact(line)).isEqualTo(line);
  }

  @Test
  @Timeout(120)
  void hugeLineWithKeywordsIsBounded() {
    final String line = ("password" + "x".repeat(500) + " ").repeat(200);
    assertThat(redact(line)).isNotNull();
  }

  /**
   * A log line may be up to {@link SupportLogCollector#MAX_LINE_CHARS} characters. The patterns that scan for a name around a
   * keyword and for a URL scheme used to be unbounded, which made one such line cost minutes (quadratic backtracking) and a
   * long quoted value overflow the stack. Loosening the bound deletes the test.
   */
  @Test
  void aSixtyFourKilobyteLineIsRedactedInBoundedTime() {
    final int n = SupportLogCollector.MAX_LINE_CHARS;
    final String[] lines = { "-".repeat(n - 10) + " token x", "-a".repeat(n / 2) + " token", "a.".repeat(n / 2) + "token",
        "-token".repeat(n / 6), "token".repeat(n / 5) + " = x", "password=\"" + "a".repeat(n - 20), "password=\\\"" + "a".repeat(n - 20),
        "eyJ".repeat(n / 3), "a://".repeat(n / 4), "a://b:" + "c".repeat(n - 20), "x".repeat(n - 20) + " --token" };
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    for (final String line : lines)
      new SupportRedactor.Session().redactLine(line);
    watch.assertStayedUnder(10_000L, "redacting a 64K-character line is linear-ish, not quadratic in the line length");
  }

  @Test
  void aLongQuotedValueIsMaskedWithoutOverflowingTheStack() {
    final String secret = "a".repeat(SupportLogCollector.MAX_LINE_CHARS - 40);
    final String redacted = new SupportRedactor.Session().redactLine("x password=\"" + secret + "\" y");
    assertThat(redacted).contains(SupportRedactor.MASK).doesNotContain("aaaaaaaaaaaaaaaa");
  }

  @Test
  void anOptionNameLongerThanTheBoundIsStillRedactedAroundItsKeyword() {
    final String option = "--" + "x".repeat(80) + "-password";
    assertThat(redact(option + " hunter2")).doesNotContain("hunter2");
  }

  @Test
  void anApiKeyNameWithADotIsRecognisedLikeTheOtherSeparators() {
    for (final String name : new String[] { "apikey", "api-key", "api_key", "api.key", "api key" })
      assertThat(redact("service." + name + "=abc123def456")).as(name).doesNotContain("abc123def456");
  }

  @Test
  void aPasswordThatContainsAnAtSignIsMaskedWholeInAUrl() {
    assertThat(redact("connect scheme://user:p@ss@host/db")).isEqualTo("connect scheme://user:***@host/db");
    assertThat(redact("jdbc://admin:a@b@c@host:5432 done")).doesNotContain("a@b").doesNotContain("c@host").contains("***@host:5432 done");
    // a plain address in the text is not credentials
    assertThat(redact("contact me@example.com or see https://example.com/a@b")).isEqualTo("contact me@example.com or see https://example.com/a@b");
  }

  @Test
  void aShortBearerTokenIsMaskedToo() {
    assertThat(redact("sent bearer abc123 to the portal")).doesNotContain("abc123");
  }
}
