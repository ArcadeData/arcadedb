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
package com.arcadedb.server.http;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.GetApiDocsHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9681: the HTTP server served Studio without a Content-Security-Policy, so an HTML-injection bug anywhere in
 * the page ran script with the logged-in user's bearer token. Studio's HTML pages and the Swagger UI page now carry a
 * policy that refuses inline script; static assets do not need one.
 */
class ContentSecurityPolicyIT extends BaseGraphServerTest {
  private static final String CSP             = "Content-Security-Policy";
  private static final String CSP_REPORT_ONLY = "Content-Security-Policy-Report-Only";

  private final HttpClient client = HttpClient.newHttpClient();

  @AfterEach
  void restoreDefaults() {
    final var config = getServer(0).getConfiguration();
    config.setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY,
        GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY.getDefValue());
    config.setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY_REPORT_ONLY, false);
  }

  @Test
  void studioPageCarriesAPolicyThatRefusesInlineScript() throws Exception {
    // Studio is not on the server module's test classpath, so "/" answers the self-contained "Studio not bundled" page,
    // which goes through the same HTML path of GetDynamicContentHandler as Studio's own index.html.
    final HttpResponse<String> response = get("/", false);

    assertThat(response.statusCode()).isEqualTo(200);
    final String policy = response.headers().firstValue(CSP).orElse(null);
    assertThat(policy).isNotNull();
    assertThat(directive(policy, "script-src")).isEqualTo("'self'");
    assertThat(policy).doesNotContain("'unsafe-eval'");
    assertThat(directive(policy, "object-src")).isEqualTo("'none'");
    assertThat(directive(policy, "base-uri")).isEqualTo("'self'");
    assertThat(directive(policy, "frame-ancestors")).isEqualTo("'self'");
    assertThat(response.headers().firstValue(CSP_REPORT_ONLY)).isEmpty();
  }

  @Test
  void theDefaultPolicyOnlyAllowsInlineStylesNotInlineScript() {
    final String policy = GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY.getDefValue().toString();
    assertThat(directive(policy, "default-src")).isEqualTo("'self'");
    assertThat(directive(policy, "script-src")).doesNotContain("unsafe");
    assertThat(directive(policy, "style-src")).contains("'unsafe-inline'");
  }

  @Test
  void staticAssetsCarryNoPolicy() throws Exception {
    // A policy only governs documents; repeating it on every script, stylesheet and font is bytes for nothing. The
    // server test classpath has no Studio, so ask for a missing asset: the 404 still goes through the non-HTML branch.
    final HttpResponse<String> response = get("/js/does-not-exist.js", false);
    assertThat(response.statusCode()).isEqualTo(404);
    assertThat(response.headers().firstValue(CSP)).isEmpty();
  }

  @Test
  void anEmptySettingTurnsThePolicyOff() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY, "");

    final HttpResponse<String> response = get("/", false);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue(CSP)).isEmpty();
    assertThat(response.headers().firstValue(CSP_REPORT_ONLY)).isEmpty();

    final HttpResponse<String> docs = get("/api/v1/docs", true);
    assertThat(docs.statusCode()).isEqualTo(200);
    assertThat(docs.headers().firstValue(CSP)).isEmpty();
  }

  @Test
  void reportOnlyServesThePolicyUnderTheReportOnlyHeader() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY_REPORT_ONLY, true);

    final HttpResponse<String> response = get("/", false);
    assertThat(response.headers().firstValue(CSP)).isEmpty();
    assertThat(response.headers().firstValue(CSP_REPORT_ONLY)).isPresent();
    assertThat(directive(response.headers().firstValue(CSP_REPORT_ONLY).get(), "script-src")).isEqualTo("'self'");
  }

  @Test
  void aCustomPolicyIsServedVerbatim() throws Exception {
    final String custom = "default-src 'self'; script-src 'self'; img-src *";
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY, custom);

    assertThat(get("/", false).headers().firstValue(CSP)).hasValue(custom);
  }

  @Test
  void swaggerUiPageAllowsExactlyItsOwnInlineBootstrapScript() throws Exception {
    final HttpResponse<String> response = get("/api/v1/docs", true);
    assertThat(response.statusCode()).isEqualTo(200);

    final String policy = response.headers().firstValue(CSP).orElse(null);
    assertThat(policy).isNotNull();

    // The page keeps one inline <script> that boots Swagger UI. The policy names its hash, so that script runs and an
    // injected one does not; 'unsafe-inline' would let both run.
    final String scriptSrc = directive(policy, "script-src");
    assertThat(scriptSrc).contains("'self'").doesNotContain("unsafe-inline");
    assertThat(scriptSrc).contains("'sha256-" + sha256(inlineScript(response.body())) + "'");
    assertThat(scriptSrc).contains(GetApiDocsHandler.inlineScriptHashSource());
  }

  @Test
  void swaggerUiHashIsAddedToACustomPolicyWithoutScriptSrc() throws Exception {
    // An operator policy with only default-src still governs scripts through default-src. The docs page must then add a
    // script-src made of the default-src sources plus its hash, or its own bootstrap script is refused.
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY, "default-src 'self' https://example.com");

    final String policy = get("/api/v1/docs", true).headers().firstValue(CSP).orElseThrow();
    assertThat(directive(policy, "default-src")).isEqualTo("'self' https://example.com");
    assertThat(directive(policy, "script-src")).isEqualTo("'self' https://example.com " + GetApiDocsHandler.inlineScriptHashSource());
  }

  @Test
  void swaggerUiLeavesAPolicyThatDoesNotGovernScriptsAlone() throws Exception {
    // With neither script-src nor default-src the policy places no limit on scripts; adding a script-src would
    // introduce one the operator did not ask for.
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY, "frame-ancestors 'none'");

    assertThat(get("/api/v1/docs", true).headers().firstValue(CSP)).hasValue("frame-ancestors 'none'");
  }

  private HttpResponse<String> get(final String path, final boolean authenticated) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(new URI(getServerHttpUrl(path))).GET();
    if (authenticated)
      builder.setHeader("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    return client.send(builder.build(), BodyHandlers.ofString());
  }

  private static String directive(final String policy, final String name) {
    for (final String d : policy.split(";")) {
      final String trimmed = d.trim();
      if (trimmed.equals(name))
        return "";
      if (trimmed.startsWith(name + " "))
        return trimmed.substring(name.length() + 1).trim();
    }
    return null;
  }

  private static String inlineScript(final String html) {
    final Matcher m = Pattern.compile("<script>(.*?)</script>", Pattern.DOTALL).matcher(html);
    assertThat(m.find()).as("the docs page has an inline bootstrap script").isTrue();
    final String script = m.group(1);
    assertThat(m.find()).as("the docs page has exactly one inline script").isFalse();
    return script;
  }

  private static String sha256(final String text) throws Exception {
    return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
  }
}
