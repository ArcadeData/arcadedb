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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.QueryEngineManager;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresQueryLanguagesTest {
  private static final PostgresQueryLanguages DEFAULTS = new PostgresQueryLanguages(
      GlobalConfiguration.POSTGRES_LANGUAGES.getDefValue().toString());

  @Test
  void aStatementWithoutALeadingMarkerIsSqlAsItIs() {
    for (final String statement : new String[] { //
        "SELECT FROM Person WHERE name = '{cypher}MATCH (n) DETACH DELETE n //'", //
        "SELECT FROM Person WHERE name = '{gremlin}g.V().drop()'", //
        "INSERT INTO Person CONTENT {\"name\": \"{java}x\"}", //
        "-- {cypher}\nSELECT 1", //
        "/* {cypher} */ SELECT 1", //
        "SELECT 1 /* {cypher} */", //
        "UPDATE Person SET tag = '{redis}' RETURN AFTER" }) {
      final PostgresQueryLanguages.Selection selection = DEFAULTS.select(statement);
      assertThat(selection.language()).as(statement).isEqualTo("sql");
      assertThat(selection.text()).as(statement).isEqualTo(statement);
    }
  }

  @Test
  void aBraceThatIsNotAMarkerLeavesTheStatementToSql() {
    for (final String statement : new String[] { "{", "{}", "{cypher", "{cy pher}MATCH", "{ \"a\": 1 }", "/* unterminated {cypher}" })
      assertThat(DEFAULTS.select(statement)).as(statement).isEqualTo(new PostgresQueryLanguages.Selection("sql", statement));
  }

  @Test
  void aLeadingMarkerSelectsThePermittedLanguage() {
    assertThat(DEFAULTS.select("{cypher}MATCH (n) RETURN n")).isEqualTo(new PostgresQueryLanguages.Selection("cypher", "MATCH (n) RETURN n"));
    assertThat(DEFAULTS.select("  {SqlScript}  BEGIN; COMMIT;")).isEqualTo(new PostgresQueryLanguages.Selection("sqlscript", "BEGIN; COMMIT;"));
    assertThat(DEFAULTS.select("{sql}SELECT 1")).isEqualTo(new PostgresQueryLanguages.Selection("sql", "SELECT 1"));
    // Comments in front of the marker, nested ones included, are not the first token
    assertThat(DEFAULTS.select("-- tool:: user 1\r\n/* a /* nested */ comment */\t{cypher} MATCH (n) RETURN n"))//
        .isEqualTo(new PostgresQueryLanguages.Selection("cypher", "MATCH (n) RETURN n"));
    // Only the first marker is one: the second is text of the selected engine
    assertThat(DEFAULTS.select("{cypher}RETURN '{gremlin}' AS x")).isEqualTo(new PostgresQueryLanguages.Selection("cypher", "RETURN '{gremlin}' AS x"));
  }

  @Test
  void aMarkerNamingALanguageOutsideTheListIsRefused() {
    for (final String statement : new String[] { "{java}com.arcadedb.Constants::getVersion", "{redis}PING", "{js}1", "{nosuchlanguage}x" })
      assertThatThrownBy(() -> DEFAULTS.select(statement))//
          .isInstanceOf(PostgresQueryLanguages.Refusal.class)//
          .hasMessageContaining("not permitted on the PostgreSQL protocol")//
          .hasMessageContaining(GlobalConfiguration.POSTGRES_LANGUAGES.getKey())//
          .satisfies(e -> assertThat(((PostgresQueryLanguages.Refusal) e).sqlState).isEqualTo("0A000"));
  }

  @Test
  void anEmptyListAcceptsSqlOnly() {
    final PostgresQueryLanguages sqlOnly = new PostgresQueryLanguages(" , ");
    assertThat(sqlOnly.permitted()).containsExactly("sql");
    assertThat(sqlOnly.select("{SQL}SELECT 1").language()).isEqualTo("sql");
    assertThatThrownBy(() -> sqlOnly.select("{cypher}MATCH (n) RETURN n")).isInstanceOf(PostgresQueryLanguages.Refusal.class);
  }

  @Test
  void aPermittedLanguageNoEngineAnswersIsRefused() {
    final PostgresQueryLanguages languages = new PostgresQueryLanguages("cypher, nosuchengine");
    assertThat(languages.permitted()).containsExactly("sql", "cypher", "nosuchengine");
    assertThatThrownBy(() -> languages.select("{NoSuchEngine}x"))//
        .isInstanceOf(PostgresQueryLanguages.Refusal.class)//
        .hasMessageContaining("no query engine of this server answers it");
  }

  /**
   * Census over the engine registry, the authority a marker resolves against: every language an engine of this JVM
   * answers is selected by a leading marker if and only if the configured list names it, and refused otherwise.
   */
  @Test
  void everyRegisteredLanguageIsSelectedOnlyWhenPermitted() {
    final List<String> registered = QueryEngineManager.getInstance().getAvailableLanguages();
    assertThat(registered).as("the scan of the query engine registry reaches the SQL and Java engines")//
        .contains("sql", "sqlscript", "java");

    int selected = 0;
    int refused = 0;
    for (final String language : registered) {
      final String statement = "{" + language + "}x";
      if (DEFAULTS.permitted().contains(language)) {
        assertThat(DEFAULTS.select(statement).language()).as(language).isEqualTo(language);
        ++selected;
      } else {
        assertThatThrownBy(() -> DEFAULTS.select(statement)).as(language).isInstanceOf(PostgresQueryLanguages.Refusal.class);
        ++refused;
      }
    }
    assertThat(selected).as("registered languages the default list permits").isGreaterThanOrEqualTo(2);
    assertThat(refused).as("registered languages the default list refuses (java at least)").isGreaterThanOrEqualTo(1);
  }
}
