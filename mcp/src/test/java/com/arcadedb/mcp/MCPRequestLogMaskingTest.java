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
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The line the dispatcher writes for every {@code tools/call}, before the tool runs, describes the call by its arguments. The text
 * of a statement and the values of a record are caller data that can carry a credential, and the running-query listing already
 * masks the value after the keywords that introduce one ({@code IDENTIFIED BY}, {@code PASSWORD}, {@code TOKEN} ...). These tests
 * pin that the request log masks it the same way, with the same rule, before the text is cut to its logged length.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MCPRequestLogMaskingTest {

  private static final String VALUE = "Zq7-masked-value-4471";

  @Test
  void aCypherStatementIsLoggedWithoutTheValueOfItsPasswordClause() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("database", "db")
        .put("language", "cypher")
        .put("command", "CREATE USER bob SET PASSWORD '" + VALUE + "' CHANGE NOT REQUIRED"));

    assertThat(logged).doesNotContain(VALUE);
    assertThat(logged).contains("CREATE USER bob SET PASSWORD ***");
    assertThat(logged).contains("database=\"db\"");
  }

  @Test
  void aSqlStatementIsLoggedWithoutTheValueAfterIdentifiedBy() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("database", "db")
        .put("language", "sql")
        .put("command", "CREATE USER bob IDENTIFIED BY " + VALUE + " ROLE admin"));

    assertThat(logged).doesNotContain(VALUE);
    assertThat(logged).contains("CREATE USER bob IDENTIFIED BY *** ROLE admin");
  }

  @Test
  void theQueryToolIsMaskedLikeTheCommandTool() {
    final String logged = MCPDispatcher.formatArgs("query", new JSONObject()
        .put("database", "db")
        .put("query", "ALTER USER bob SET PASSWORD \"" + VALUE + "\""));

    assertThat(logged).doesNotContain(VALUE);
    assertThat(logged).contains("ALTER USER bob SET PASSWORD ***");
  }

  @Test
  void aQuotedValueWithBlanksIsMaskedWhole() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("command", "ALTER USER bob SET PASSWORD 'first second-" + VALUE + " third' CHANGE NOT REQUIRED"));

    assertThat(logged).doesNotContain("first").doesNotContain("second").doesNotContain(VALUE).doesNotContain("third");
    assertThat(logged).contains("SET PASSWORD *** CHANGE NOT REQUIRED");
  }

  @Test
  void aQuotedValueWithAnEscapedQuoteIsMaskedWhole() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("command", "ALTER USER bob SET PASSWORD 'ab\\'cd-" + VALUE + "' CHANGE NOT REQUIRED"));

    assertThat(logged).doesNotContain("cd-").doesNotContain(VALUE);
    assertThat(logged).contains("SET PASSWORD *** CHANGE NOT REQUIRED");
  }

  /** A value the cut would have split is masked before it, so the half that survives the cut is not the value's start. */
  @Test
  void aValueAcrossTheLoggedLengthIsMaskedBeforeTheCut() {
    final String head = "x".repeat(60) + " WHERE password = '";
    assertThat(head.length()).isLessThan(100);
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("command", head + VALUE + VALUE + VALUE + "'"));

    assertThat(logged).doesNotContain("Zq7");
    assertThat(logged).contains("WHERE password = ***");
  }

  /** A quoted value that does not close inside the part of the text that is looked at is masked up to where the look ends. */
  @Test
  void aQuotedValueThatOutlastsTheLookedAtTextIsMasked() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("command", "ALTER USER bob SET PASSWORD 'one two " + ("three-" + VALUE + " ").repeat(500) + "'"));

    assertThat(logged).doesNotContain("one").doesNotContain("two").doesNotContain("three").doesNotContain(VALUE);
    assertThat(logged).contains("SET PASSWORD ***");
  }

  @Test
  void theValuesOfARecordArgumentAreMaskedAndTheOthersKept() {
    final JSONObject properties = new JSONObject().put("name", "bob").put("password", VALUE).put("token", "t-" + VALUE);
    final String logged = MCPDispatcher.formatArgs("upsert_entity", new JSONObject()
        .put("database", "db")
        .put("typeName", "Account")
        .put("matchKeys", new JSONObject().put("id", 7))
        .put("setProperties", properties));

    assertThat(logged).doesNotContain(VALUE);
    assertThat(logged).contains("\"name\":\"bob\"").contains("\"password\":***").contains("matchKeys={\"id\":7}");
  }

  @Test
  void aTextWithoutAKeywordIsLoggedAsBefore() {
    assertThat(MCPDispatcher.formatArgs("query", new JSONObject().put("database", "db").put("query", "MATCH (n:Person) RETURN n.name LIMIT 5")))
        .isEqualTo("{database=\"db\", query=\"MATCH (n:Person) RETURN n.name LIMIT 5\"}");

    final String longText = "MATCH (n:Person) WHERE n.name = 'x' " + "AND n.age > 1 ".repeat(30) + "RETURN n";
    final String logged = MCPDispatcher.formatArgs("query", new JSONObject().put("query", longText));
    assertThat(logged).isEqualTo("{query=\"" + longText.substring(0, 100) + "...\"}");
  }

  @Test
  void lineBreaksAreStillEscapedAndAValueAfterOneIsStillMasked() {
    final String logged = MCPDispatcher.formatArgs("execute_command", new JSONObject()
        .put("command", "CREATE USER bob\nSET PASSWORD\n'" + VALUE + "'\r\nCHANGE NOT REQUIRED"));

    assertThat(logged).doesNotContain(VALUE).doesNotContain("\n").doesNotContain("\r");
    assertThat(logged).contains("SET PASSWORD\\n***\\r\\nCHANGE NOT REQUIRED");
  }

  @Test
  void aSettingValueAndAnArrayAreStillRenderedAsBefore() {
    assertThat(MCPDispatcher.formatArgs("set_server_setting", new JSONObject().put("key", "arcadedb.ha.clusterToken").put("value", VALUE)))
        .contains("value=\"*****\"").doesNotContain(VALUE);
    assertThat(MCPDispatcher.formatArgs("sample_records", new JSONObject().put("types", new JSONArray().put("A").put("B"))))
        .isEqualTo("{types=[2 item(s)]}");
    assertThat(MCPDispatcher.formatArgs("query", new JSONObject().put("limit", 5))).isEqualTo("{limit=5}");
  }

  /** The timeout is a hang detector, not a latency bound: a pattern that backtracks takes minutes on this input. */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aLargeRecordArgumentIsMaskedInOnePass() {
    final JSONObject properties = new JSONObject();
    for (int i = 0; i < 20_000; i++)
      properties.put("field" + i, "value " + i + " password " + " ".repeat(8));
    final String blanks = "SELECT password" + " ".repeat(200_000) + ",";

    for (int i = 0; i < 5; i++) {
      assertThat(MCPDispatcher.formatArgs("upsert_entity", new JSONObject().put("setProperties", properties))).isNotEmpty();
      assertThat(MCPDispatcher.formatArgs("query", new JSONObject().put("query", blanks))).isNotEmpty();
    }
  }
}
