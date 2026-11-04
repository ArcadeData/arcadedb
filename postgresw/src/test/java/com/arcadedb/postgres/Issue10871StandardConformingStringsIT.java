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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10871: every connection is told {@code standard_conforming_strings = on}, which is a client's licence to put a
 * backslash in a plain literal unescaped. The literal was then escape-processed by the engine lexer: {@code 'a\b'} was
 * stored as {@code ab}, {@code 'C:\'} was a syntax error, {@code E'...'} did not parse, and a {@code SET} of the
 * parameter to {@code off} was accepted and changed nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10871StandardConformingStringsIT extends PostgresWireProtocolTestBase {

  private static final String[] VALUES = { "a\\b", "C:\\", "C:\\tmp\\x", "it's", "x\\ty", "line\nbreak", "\\'" };

  @Override
  protected String getDatabaseName() {
    return "issue10871";
  }

  private Connection connect(final String queryMode) throws SQLException {
    return connect(queryMode, null);
  }

  private Connection connect(final String queryMode, final String options) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    if (queryMode != null)
      props.setProperty("preferQueryMode", queryMode);
    if (options != null)
      props.setProperty("options", options);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  @Test
  void theConnectionCanStartWithTheParameterOff() throws Exception {
    try (final Connection conn = connect(null, "-c standard_conforming_strings=off"); final Statement st = conn.createStatement()) {
      assertThat(show(st)).isEqualTo("off");
      assertThat(selectOne(st, "SELECT 'x\\\\ty' AS v")).isEqualTo("x\\ty");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\ty");
    }
  }

  @Test
  void aPlainLiteralKeepsItsBackslashes() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      assertThat(show(st)).isEqualTo("on");
      assertThat(selectOne(st, "SELECT 'a\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\\ty");
      assertThat(selectOne(st, "SELECT 'C:\\' AS v")).isEqualTo("C:\\");
      assertThat(selectOne(st, "SELECT 'C:\\tmp\\x' AS v")).isEqualTo("C:\\tmp\\x");
      assertThat(selectOne(st, "SELECT 'it''s' AS v")).isEqualTo("it's");
      // the literal after one that ends in a backslash is a literal of its own
      try (final ResultSet rs = st.executeQuery("SELECT 'a\\' AS v, 'b' AS w")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("v")).isEqualTo("a\\");
        assertThat(rs.getString("w")).isEqualTo("b");
      }
    }
  }

  @Test
  void anEscapeStringProcessesPostgresEscapes() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      assertThat(selectOne(st, "SELECT E'x\\ty' AS v")).isEqualTo("x\ty");
      assertThat(selectOne(st, "SELECT E'a\\\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT E'it\\'s' AS v")).isEqualTo("it's");
      assertThat(selectOne(st, "SELECT E'a\\bc' AS v")).isEqualTo("a\bc");
    }
  }

  @Test
  void aSetOfTheParameterIsHonoured() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      st.execute("SET standard_conforming_strings = off");
      assertThat(show(st)).isEqualTo("off");
      assertThat(selectOne(st, "SELECT 'a\\\\b' AS v")).isEqualTo("a\\b");
      assertThat(selectOne(st, "SELECT 'x\\ty' AS v")).isEqualTo("x\ty");

      st.execute("SET standard_conforming_strings = on");
      assertThat(show(st)).isEqualTo("on");
      assertThat(selectOne(st, "SELECT 'a\\b' AS v")).isEqualTo("a\\b");
    }
  }

  @Test
  void everyQueryModeStoresTheBoundValueAsItIs() throws Exception {
    try (final Connection conn = connect(null); final Statement st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE Bs");
    }

    // simple: the driver composes the literal itself, following the reported setting; extended: the value is bound
    for (final String mode : new String[] { "simple", "extended" })
      for (final boolean standardConformingStrings : new boolean[] { true, false })
        try (final Connection conn = connect(mode)) {
          if (!standardConformingStrings)
            try (final Statement st = conn.createStatement()) {
              st.execute("SET standard_conforming_strings = off");
            }
          try (final Statement st = conn.createStatement()) {
            st.execute("DELETE FROM Bs");
          }
          for (final String value : VALUES)
            try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO Bs (v) VALUES (?)")) {
              ps.setString(1, value);
              ps.execute();
            }
          final List<String> stored = new ArrayList<>();
          try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SELECT v FROM Bs")) {
            while (rs.next())
              stored.add(rs.getString("v"));
          }
          assertThat(stored).as("preferQueryMode=%s, standard_conforming_strings=%s", mode, standardConformingStrings)
              .containsExactlyInAnyOrder(VALUES);

          for (final String value : VALUES)
            try (final PreparedStatement ps = conn.prepareStatement("SELECT v FROM Bs WHERE v = ?")) {
              ps.setString(1, value);
              try (final ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("lookup of %s in mode %s", value, mode).isTrue();
                assertThat(rs.getString("v")).isEqualTo(value);
              }
            }
        }
  }

  private static String show(final Statement st) throws SQLException {
    try (final ResultSet rs = st.executeQuery("SHOW standard_conforming_strings")) {
      assertThat(rs.next()).isTrue();
      return rs.getString(1);
    }
  }

  private static String selectOne(final Statement st, final String sql) throws SQLException {
    try (final ResultSet rs = st.executeQuery(sql)) {
      assertThat(rs.next()).as(sql).isTrue();
      return rs.getString("v");
    }
  }
}
