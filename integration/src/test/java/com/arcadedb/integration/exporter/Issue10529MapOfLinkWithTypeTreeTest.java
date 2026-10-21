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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10529: a {@code MAP OF LINK} holding the key {@code t} is exported WITH a type tree, because the reader would otherwise take
 * the key for the type name of an embedded document. The tree decodes the link strings into {@code RID} objects before the link
 * remapper runs, and the remapper mapped only a {@code String}: the links kept the SOURCE database's RIDs, which in the target name an
 * unrelated record or none, without a trace in {@code danglingLinks}. #10483 gave {@code remapLinkValue} the {@code RID} arm; this pins
 * the exact shapes of the report - a real round trip, and hand-written files whose source RIDs cannot coincide with the target's, with
 * the targets before the holder, after it, and missing from the file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10529MapOfLinkWithTypeTreeTest {
  private static final String SOURCE_PATH = "target/databases/issue10529-source";
  private static final String TARGET_PATH = "target/databases/issue10529-target";
  private static final String FILE        = "issue10529.jsonl.tgz";

  private static final String SCHEMA = "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\","
      + "\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{"
      + "\"Target\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Target_0\"],\"properties\":{\"name\":{\"type\":\"STRING\"}},\"indexes\":{},\"custom\":{}},"
      + "\"D\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"D_0\"],\"properties\":{\"v\":{\"type\":\"MAP\",\"of\":\"LINK\"}},\"indexes\":{},\"custom\":{}}}}}\n";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  /** The export of the report: the tree is written for the key {@code t} and not for {@code a}. Each key must name its own record. */
  @ParameterizedTest(name = "keys {0} and {1}")
  @ValueSource(strings = { "a,b", "t,b", "b,t", "t,u" })
  void everyLinkOfTheMapNamesTheRecordItWasExportedWith(final String keys) {
    final String[] pair = keys.split(",");
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.name STRING");
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.v MAP OF LINK");
      source.transaction(() -> {
        final MutableDocument one = source.newDocument("Target").set("name", "one");
        one.save();
        final MutableDocument two = source.newDocument("Target").set("name", "two");
        two.save();
        final Map<String, Object> links = new LinkedHashMap<>();
        links.put(pair[0], one.getIdentity());
        links.put(pair[1], two.getIdentity());
        source.newDocument("D").set("v", links).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());
      final Map<?, ?> map = (Map<?, ?>) firstOf(target, "D").get("v");
      assertThat(nameOf(target, map.get(pair[0]))).isEqualTo("one");
      assertThat(nameOf(target, map.get(pair[1]))).isEqualTo("two");
    }
  }

  /** The tree is on the record and the targets come first in the file: the links resolve at once, to the new RIDs. */
  @Test
  void aTreeLinkToARecordImportedEarlierIsRemapped() throws Exception {
    // the source RIDs (#5:7, #5:8) are positions the target database never allocates, so a link left on one reads as missing
    final Map<String, String> names = importedFile(target("#5:7", "one") + target("#5:8", "two") + holder("#5:7", "#5:8"));
    assertThat(names).containsEntry("t", "one").containsEntry("b", "two");
  }

  /** The holder comes first: the links are forward references, kept aside and rewritten by the reconciliation pass. */
  @Test
  void aTreeLinkToARecordImportedLaterIsRemapped() throws Exception {
    final Map<String, String> names = importedFile(holder("#5:7", "#5:8") + target("#5:7", "one") + target("#5:8", "two"));
    assertThat(names).containsEntry("t", "one").containsEntry("b", "two");
  }

  /**
   * The targets are not in the file at all: the source RIDs name nothing in the target database (or another record), and must not be
   * kept. A MAP holds no null, so the entries are removed and counted as dangling.
   */
  @Test
  void aTreeLinkToARecordThatIsNotInTheFileIsDroppedNotKeptOnItsSourceRid() throws Exception {
    final Map<String, String> names = importedFile(target("#1:0", "unrelated") + holder("#7:3", "#7:4"));
    assertThat(names).doesNotContainKeys("t", "b");
  }

  /** Imports {@code content} behind the schema of the report into a fresh database and returns the {@code name} each link of {@code D.v} resolves to. */
  private static Map<String, String> importedFile(final String content) throws Exception {
    final File file = new File(SOURCE_PATH + ".jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), SCHEMA + content);
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      final Object value = firstOf(target, "D").get("v");
      final Map<String, String> names = new LinkedHashMap<>();
      if (value instanceof Map<?, ?> map)
        for (final Map.Entry<?, ?> entry : map.entrySet())
          names.put(String.valueOf(entry.getKey()), nameOf(target, entry.getValue()));
      return names;
    } finally {
      file.delete();
    }
  }

  private static String target(final String rid, final String name) {
    return "{\"t\":\"d\",\"c\":{\"p\":{\"name\":\"" + name + "\"},\"r\":\"" + rid + "\",\"t\":\"Target\"}}\n";
  }

  /** A D record whose MAP OF LINK carries the type tree {@code y}, as the exporter writes it for a key spelled {@code t}. */
  private static String holder(final String linkT, final String linkB) {
    return "{\"t\":\"d\",\"c\":{\"p\":{\"v\":{\"t\":\"" + linkT + "\",\"b\":\"" + linkB + "\"}},\"r\":\"#2:0\",\"t\":\"D\","
        + "\"y\":{\"v\":{\"t\":\"LINK\",\"b\":\"LINK\"}}}}\n";
  }

  private static Document firstOf(final Database database, final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
      return rs.next().getElement().get();
    }
  }

  private static String nameOf(final Database database, final Object link) {
    if (link == null)
      return "<null>";
    final Document linked = database.lookupByRID(((Identifiable) link).getIdentity(), false).asDocument();
    return String.valueOf(linked.get("name"));
  }
}
