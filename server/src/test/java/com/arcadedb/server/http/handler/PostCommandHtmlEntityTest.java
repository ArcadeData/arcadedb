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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the data-corruption caused by the server-side HTML-entity decoding of the
 * command text. A command can legitimately carry HTML entities ({@code &quot;}, {@code &amp;},
 * {@code &lt;}, {@code &gt;}) inside its data (e.g. HTML pasted from Google Sheets). The previous
 * {@code decode()} pass rewrote those entities to real characters, breaking the embedded JSON of an
 * {@code INSERT ... CONTENT { ... }} command (reported on Discord:
 * "extraneous input '1' expecting '}', ','").
 */
class PostCommandHtmlEntityTest extends BaseGraphServerTest {

  @Test
  void insertContentWithHtmlEntitiesIsNotCorrupted() throws Exception {
    executeCommand(0, "sql", "create document type Investor");

    // The `descr` value mirrors the Discord payload: real double quotes around the HTML attribute are
    // JSON-escaped (\"), while the inner quotes are literal &quot; entities that must survive verbatim.
    final String descr = """
        <p><span data-sheets-value=\\"{&quot;1&quot;:2,&quot;2&quot;:&quot;M&uuml;nchen&quot;}\\">\
        M&uuml;nchen &amp; Co &lt;b&gt;</span></p>""";

    final String command = "insert into Investor content {\"name\":\"munich\",\"descr\":\"" + descr + "\"}";

    final JSONObject insertResponse = executeCommand(0, "sql", command);
    assertThat(insertResponse).isNotNull();

    final JSONObject selectResponse = executeCommand(0, "sql", "select descr from Investor where name = 'munich'");
    assertThat(selectResponse).isNotNull();

    final String stored = selectResponse.getJSONObject("result").getJSONArray("records").getJSONObject(0).getString("descr");

    // The literal entities must round-trip untouched; only the JSON \" escapes are resolved to ".
    final String expected = """
        <p><span data-sheets-value="{&quot;1&quot;:2,&quot;2&quot;:&quot;M&uuml;nchen&quot;}">\
        M&uuml;nchen &amp; Co &lt;b&gt;</span></p>""";
    assertThat(stored).isEqualTo(expected);
  }
}
