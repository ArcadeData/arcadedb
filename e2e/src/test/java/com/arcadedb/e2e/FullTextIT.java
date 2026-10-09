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
package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Full-text indexes with every Lucene analyzer ArcadeDB bundles. The analyzer is created from the class name in the
 * index metadata and Lucene creates its token attributes reflectively, so in the native image each of them needs
 * reachability metadata: without it no FULL_TEXT index could be created there (#9495).
 */
class FullTextIT extends ArcadeContainerTemplate {
  private static final String   DATABASE  = "fulltext";
  private static final String   THAI      = "org.apache.lucene.analysis.th.ThaiAnalyzer";
  /** Every analyzer of lucene-core and lucene-analysis-common with a public no-arg constructor. */
  private static final String[] ANALYZERS = {
      "org.apache.lucene.analysis.ar.ArabicAnalyzer",
      "org.apache.lucene.analysis.bg.BulgarianAnalyzer",
      "org.apache.lucene.analysis.bn.BengaliAnalyzer",
      "org.apache.lucene.analysis.br.BrazilianAnalyzer",
      "org.apache.lucene.analysis.ca.CatalanAnalyzer",
      "org.apache.lucene.analysis.cjk.CJKAnalyzer",
      "org.apache.lucene.analysis.ckb.SoraniAnalyzer",
      "org.apache.lucene.analysis.classic.ClassicAnalyzer",
      "org.apache.lucene.analysis.core.KeywordAnalyzer",
      "org.apache.lucene.analysis.core.SimpleAnalyzer",
      "org.apache.lucene.analysis.core.UnicodeWhitespaceAnalyzer",
      "org.apache.lucene.analysis.core.WhitespaceAnalyzer",
      "org.apache.lucene.analysis.cz.CzechAnalyzer",
      "org.apache.lucene.analysis.da.DanishAnalyzer",
      "org.apache.lucene.analysis.de.GermanAnalyzer",
      "org.apache.lucene.analysis.el.GreekAnalyzer",
      "org.apache.lucene.analysis.en.EnglishAnalyzer",
      "org.apache.lucene.analysis.es.SpanishAnalyzer",
      "org.apache.lucene.analysis.et.EstonianAnalyzer",
      "org.apache.lucene.analysis.eu.BasqueAnalyzer",
      "org.apache.lucene.analysis.fa.PersianAnalyzer",
      "org.apache.lucene.analysis.fi.FinnishAnalyzer",
      "org.apache.lucene.analysis.fr.FrenchAnalyzer",
      "org.apache.lucene.analysis.ga.IrishAnalyzer",
      "org.apache.lucene.analysis.gl.GalicianAnalyzer",
      "org.apache.lucene.analysis.hi.HindiAnalyzer",
      "org.apache.lucene.analysis.hu.HungarianAnalyzer",
      "org.apache.lucene.analysis.hy.ArmenianAnalyzer",
      "org.apache.lucene.analysis.id.IndonesianAnalyzer",
      "org.apache.lucene.analysis.it.ItalianAnalyzer",
      "org.apache.lucene.analysis.lt.LithuanianAnalyzer",
      "org.apache.lucene.analysis.lv.LatvianAnalyzer",
      "org.apache.lucene.analysis.ne.NepaliAnalyzer",
      "org.apache.lucene.analysis.nl.DutchAnalyzer",
      "org.apache.lucene.analysis.no.NorwegianAnalyzer",
      "org.apache.lucene.analysis.pt.PortugueseAnalyzer",
      "org.apache.lucene.analysis.ro.RomanianAnalyzer",
      "org.apache.lucene.analysis.ru.RussianAnalyzer",
      "org.apache.lucene.analysis.sr.SerbianAnalyzer",
      "org.apache.lucene.analysis.sv.SwedishAnalyzer",
      "org.apache.lucene.analysis.ta.TamilAnalyzer",
      "org.apache.lucene.analysis.te.TeluguAnalyzer",
      "org.apache.lucene.analysis.th.ThaiAnalyzer",
      "org.apache.lucene.analysis.tr.TurkishAnalyzer",
      "org.apache.lucene.analysis.standard.StandardAnalyzer"
  };

  private static RemoteDatabase database;
  private static int            typeCounter;

  @BeforeAll
  static void createDatabase() {
    final RemoteServer server = new RemoteServer(ARCADE.getHost(), ARCADE.getMappedPort(2480), "root", "playwithdata");
    if (server.exists(DATABASE))
      server.drop(DATABASE);
    server.create(DATABASE);
    database = new RemoteDatabase(ARCADE.getHost(), ARCADE.getMappedPort(2480), DATABASE, "root", "playwithdata");
    database.setTimeout(60_000);
  }

  @AfterAll
  static void closeDatabase() {
    if (database != null)
      database.close();
  }

  @Test
  void defaultAnalyzer() {
    final String type = newType();
    database.command("sql", "CREATE INDEX ON " + type + " (txt) FULL_TEXT");
    database.command("sql", "INSERT INTO " + type + " SET txt = 'another quick document'");

    assertThat(search(type, "quick")).isEqualTo(2);
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE txt CONTAINSTEXT 'foxes'")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void englishAnalyzerStems() {
    final String type = newType();
    database.command("sql",
        "CREATE INDEX ON " + type + " (txt) FULL_TEXT METADATA {\"analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\"}");
    // "jumped" is indexed as "jump"
    assertThat(search(type, "jumping")).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("analyzers")
  void everyBundledAnalyzerIndexesAndSearches(final String analyzer) {
    // the JDK's dictionary-based Thai BreakIterator is not in the native image (docs/native-image.md)
    assumeFalse(NATIVE && THAI.equals(analyzer), "ThaiAnalyzer is not supported by the native image");

    final String type = newType();
    database.command("sql", "CREATE INDEX ON " + type + " (txt) FULL_TEXT METADATA {\"analyzer\": \"" + analyzer + "\"}");
    database.command("sql", "INSERT INTO " + type + " SET txt = 'another quick document'");
    // KeywordAnalyzer indexes the whole value as one token
    assertThat(search(type, analyzer.endsWith(".KeywordAnalyzer") ? "another quick document" : "quick")).isGreaterThanOrEqualTo(1);
  }

  static List<String> analyzers() {
    return List.of(ANALYZERS);
  }

  private static synchronized String newType() {
    final String type = "Doc" + (++typeCounter);
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".txt STRING");
    database.command("sql", "INSERT INTO " + type + " SET txt = 'The quick brown foxes jumped over the lazy dogs'");
    return type;
  }

  private static long search(final String type, final String text) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE SEARCH_INDEX('" + type + "[txt]', ?) = true", text)) {
      final List<Object> rows = new ArrayList<>();
      rs.stream().forEach(rows::add);
      return rows.size();
    }
  }
}
