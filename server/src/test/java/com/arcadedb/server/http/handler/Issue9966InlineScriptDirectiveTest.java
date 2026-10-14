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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9966: the helper that adds the Swagger UI bootstrap's hash to a custom Content-Security-Policy knew only
 * {@code script-src}. The source has to go on the directive the browser consults for an inline {@code <script>}
 * element, the first one the policy declares of {@code script-src-elem}, {@code script-src}, {@code default-src}; and a
 * directive that refuses every script is a decision of the operator, never widened by appending to it.
 * <p>
 * One row per directive of the precedence chain, rather than one per shipped policy, so the rule is what is pinned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9966InlineScriptDirectiveTest {
  private static final String HASH = "'sha256-XYZ'";

  @ParameterizedTest(name = "{0}")
  @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
      # script-src-elem overrides script-src for a <script> element: the hash goes there, and only there
      default-src 'self'; script-src 'self'; script-src-elem 'self'  | default-src 'self'; script-src 'self'; script-src-elem 'self' 'sha256-XYZ'
      script-src-elem 'self'; default-src 'self'                     | script-src-elem 'self' 'sha256-XYZ'; default-src 'self'
      SCRIPT-SRC-ELEM 'self'                                         | SCRIPT-SRC-ELEM 'self' 'sha256-XYZ'
      # script-src, when no script-src-elem is declared
      default-src 'self'; script-src 'self'                          | default-src 'self'; script-src 'self' 'sha256-XYZ'
      # default-src alone: a script-src is invented with its sources, since a script-src replaces it for scripts
      default-src 'self'                                             | default-src 'self'; script-src 'self' 'sha256-XYZ'
      # script-src-attr governs inline event handlers, not elements
      default-src 'self'; script-src-attr 'none'                     | default-src 'self'; script-src-attr 'none'; script-src 'self' 'sha256-XYZ'
      # nothing governs scripts: nothing to add
      frame-ancestors 'self'                                         | frame-ancestors 'self'
      """)
  void theHashGoesOnTheDirectiveTheBrowserConsults(final String policy, final String expected) {
    assertThat(AbstractServerHttpHandler.allowInlineScript(policy, HASH)).isEqualTo(expected);
  }

  /**
   * A source list that is {@code 'none'}, or empty, refuses every script. Appending a source to it is not a refusal any
   * more: browsers ignore {@code 'none'} next to another source, so the inline script the operator forbade would run.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
      script-src 'none'
      script-src
      script-src 'NONE'
      default-src 'none'
      default-src 'none'; img-src 'self'
      script-src-elem 'none'; script-src 'self'
      default-src 'self'; script-src-elem
      """)
  void aDirectiveThatRefusesEveryScriptIsLeftAlone(final String policy) {
    assertThat(AbstractServerHttpHandler.allowInlineScript(policy, HASH)).isEqualTo(policy);
  }

  /**
   * {@code 'unsafe-inline'} already lets the inline script run, and a hash next to it would REVOKE it: a browser that
   * honours hashes ignores {@code 'unsafe-inline'} in a list that carries one, refusing every other inline script the
   * operator allowed. Once the list carries a hash or a nonce, {@code 'unsafe-inline'} is ignored already and the hash is
   * what lets the page's script run.
   */
  @Test
  void unsafeInlineIsNotRevokedByAHash() {
    assertThat(AbstractServerHttpHandler.allowInlineScript("script-src 'self' 'unsafe-inline'", HASH)).isEqualTo(
        "script-src 'self' 'unsafe-inline'");
    assertThat(AbstractServerHttpHandler.allowInlineScript("script-src 'self' 'unsafe-inline' 'nonce-abc'", HASH)).isEqualTo(
        "script-src 'self' 'unsafe-inline' 'nonce-abc' 'sha256-XYZ'");
  }

  @Test
  void aHashAlreadyInThePolicyIsNotRepeated() {
    assertThat(AbstractServerHttpHandler.allowInlineScript("script-src 'self' 'sha256-XYZ'", HASH)).isEqualTo(
        "script-src 'self' 'sha256-XYZ'");
  }

  /** The shipped default keeps its shape: the hash on script-src, nothing else moved. */
  @Test
  void theShippedDefaultPolicyGetsTheHashOnScriptSrc() {
    final String policy = GlobalConfiguration.SERVER_HTTP_CONTENT_SECURITY_POLICY.getDefValue().toString();
    final String result = AbstractServerHttpHandler.allowInlineScript(policy, HASH);
    assertThat(result).contains("script-src 'self' " + HASH);
    assertThat(result.replace(" " + HASH, "")).isEqualTo(String.join("; ", policy.split("\\s*;\\s*")));
  }
}
