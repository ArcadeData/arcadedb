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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandHandlerDecodeTest extends BaseGraphServerTest {

  @Test
  void javaScriptFunctionWithLogicalAndOperatorViaHTTP() throws Exception {
    executeCommand(0, "sql",
        "DEFINE FUNCTION Test.getMatchRating \"return 1==1 && 0==0\" LANGUAGE js");

    final var response = executeCommand(0, "sql",
        "SELECT `Test.getMatchRating`() as result");

    assertThat(response.toString()).contains("\"result\":true");
  }

  @Test
  void javaScriptFunctionWithLogicalAndOperatorDirectCommand() {
    getServer(0).getDatabase(getDatabaseName()).command("sql",
        "DEFINE FUNCTION Test.getMatchRating2 \"return 1==1 && 0==0\" LANGUAGE js");

    final var result = getServer(0).getDatabase(getDatabaseName()).query("sql",
        "SELECT `Test.getMatchRating2`() as result");

    assertThat(result.next().<Boolean>getProperty("result")).isTrue();
  }

  @Test
  void decodeMethodCorrectlyHandlesHtmlEntities() {
    final var handler = new PostCommandHandler(getServer(0).getHttpServer()) {
      public String testDecode(final String command) {
        return decode(command);
      }
    };

    // Test the decode method directly with HTML-encoded entities
    String encoded = "return 1==1 &amp;&amp; 0==0";
    String decoded = handler.testDecode(encoded);

    // Should decode &amp; to & not to space
    assertThat(decoded).isEqualTo("return 1==1 && 0==0");

    // Test other HTML entities too
    assertThat(handler.testDecode("&lt;script&gt;")).isEqualTo("<script>");
    assertThat(handler.testDecode("&quot;quoted&quot;")).isEqualTo("\"quoted\"");
    assertThat(handler.testDecode("&#039;apostrophe&#039;")).isEqualTo("'apostrophe'");
  }
}
