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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.gremlin.integration.importer.format.GraphMLImporterFormat;
import com.arcadedb.gremlin.integration.importer.format.GraphSONImporterFormat;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.integration.importer.format.FormatImporter;
import com.arcadedb.integration.importer.format.GloVeImporterFormat;
import com.arcadedb.integration.importer.format.JSONImporterFormat;
import com.arcadedb.integration.importer.format.JsonlImporterFormat;
import com.arcadedb.integration.importer.format.Neo4jImporterFormat;
import com.arcadedb.integration.importer.format.OrientDBImporterFormat;
import com.arcadedb.integration.importer.format.RDFImporterFormat;
import com.arcadedb.integration.importer.format.Word2VecImporterFormat;
import com.arcadedb.integration.importer.format.Word2VecImporterFormatLSM;
import com.arcadedb.integration.importer.format.XMLImporterFormat;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11311: the outcome of an import that read nothing depended on the format. Every {@code FormatImporter} on the class path
 * is driven here through {@code Importer.load()} with a source of no bytes, a source that is not what it claims, a source with no
 * records and a control: the first is refused, the next two are refused or answered with an explicit zero, and the control creates
 * records and reports them, which is what makes the zero mean something. The formats are enumerated by reflection, so one added
 * later fails {@link #everyFormatOnTheClassPathHasARow()} until it has a row. Here because this is the one module with all of them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11311EveryFormatOutcomeTest {
  private static final String DATABASE_PATH = "target/databases/test-import-11311-every-format";
  private static final Map<String, String> VECTORS = Map.of("distanceFunction", "cosine", "m", "16", "beamWidth", "100", "vertexType",
      "Word", "vectorProperty", "vector", "idProperty", "name");

  private static final String ORIENTDB_HEAD = """
      {"info":{"name":"x","default-cluster-id":3,"exporter-version":12,"engine-version":"3.1.13","engine-build":"test",\
      "storage-config-version":23,"schema-version":4,"schemaRecordId":"#0:1","indexMgrRecordId":"#0:2"},\
      "clusters":[{"name":"internal","id":0},{"name":"index","id":1},{"name":"manindex","id":2},{"name":"default","id":3}],\
      "schema":{"version":1,"blob-clusters":[],"classes":[{"name":"Account","default-cluster-id":3,"cluster-ids":[3],\
      "cluster-selection":"round-robin","properties":[]}]},\
      "records":[{"@type":"d","@rid":"#0:1","@version":1},{"@type":"d","@rid":"#0:2","@version":1,"indexes":[]}""";

  /**
   * @param fileName    what routes the source to the format: its extension, or a name that leaves it to the content
   * @param setupSql    what the source needs from the database, or null
   * @param settings    the settings of the import
   * @param control     a source the format imports
   * @param notTheFormat a source that opens like the format and is not
   * @param noRecords   a source of the format with no records in it
   */
  private record Row(String fileName, String setupSql, Map<String, String> settings, String control, String notTheFormat,
                     String noRecords) {
  }

  private static Map<Class<?>, Row> rows() {
    final Map<Class<?>, Row> rows = new LinkedHashMap<>();
    rows.put(CSVImporterFormat.class, new Row("data.csv", null, Map.of(), "id,name\n1,Ann\n2,Bob\n", "hello", "id,name\n"));
    // THE ANALYSIS OF AN RDF SOURCE REGISTERS ONLY THE EDGE TYPE: THE VERTEX TYPE IS THE CALLER'S TO CREATE
    rows.put(RDFImporterFormat.class, new Row("data.nt",
        "CREATE VERTEX TYPE Node; CREATE PROPERTY Node.id STRING; CREATE INDEX ON Node (id) UNIQUE;", Map.of(),
        "<http://e/a> <http://e/knows> <http://e/b> .\n",
        "<http://e/a> <http://e/knows> broken\n", "# no statement\n"));
    rows.put(JSONImporterFormat.class, new Row("data.json", null, Map.of(), "[{\"a\":1},{\"a\":2}]", "[\"a\",\"b\"]", "[]"));
    rows.put(JsonlImporterFormat.class, new Row("data.jsonl", "CREATE DOCUMENT TYPE Document", Map.of(), """
        {"t":"d","c":{"r":"#1:0","t":"Document","p":{"name":"a"}}}
        {"t":"d","c":{"r":"#1:1","t":"Document","p":{"name":"b"}}}
        """, "not json\n", "\n"));
    rows.put(XMLImporterFormat.class, new Row("data.xml", null, Map.of(),
        "<people>\n<person><name>Ann</name></person>\n<person><name>Bob</name></person>\n</people>\n", "<people><person>", "<people/>\n"));
    rows.put(GraphMLImporterFormat.class, new Row("data.graphml", null, Map.of(), """
        <?xml version="1.0" encoding="UTF-8"?>
        <graphml xmlns="http://graphml.graphdrawing.org/xmlns">
        <graph id="G" edgedefault="directed">
        <node id="1"/><node id="2"/><edge id="3" source="1" target="2"/>
        </graph>
        </graphml>
        """, "hello", """
        <?xml version="1.0" encoding="UTF-8"?>
        <graphml xmlns="http://graphml.graphdrawing.org/xmlns"><graph id="G" edgedefault="directed"></graph></graphml>
        """));
    rows.put(GraphSONImporterFormat.class, new Row("data.graphson", null, Map.of(), """
        {"id":"http://ex/n1","label":"Person","properties":{"name":[{"id":1,"value":"Jay"}]},\
        "outE":{"knows":[{"id":10,"inV":"http://ex/n2"}]}}
        {"id":"http://ex/n2","label":"Person","properties":{"name":[{"id":2,"value":"Kim"}]}}
        """, "hello", "\n"));
    rows.put(GloVeImporterFormat.class, new Row("data.txt", null, VECTORS, "aa 0.1 0.2 0.3\nbb 0.4 0.5 0.6\n",
        "aa 0.1 0.2 0.3\nbb x y z\n", "\n"));
    rows.put(Word2VecImporterFormat.class, new Row("data.txt", null, VECTORS, "2 3\naa 0.1 0.2 0.3\nbb 0.4 0.5 0.6\n",
        "2 3\naa x y z\nbb x y z\n", "0 3\n"));
    rows.put(Neo4jImporterFormat.class, new Row("data.json", null, Map.of(), """
        {"type":"node","id":"0","labels":["User"],"properties":{"name":"Ann"}}
        {"type":"node","id":"1","labels":["User"],"properties":{"name":"Bob"}}
        """, "{\"type\":\"node\",\"id\":\"0\",", "\n"));
    rows.put(OrientDBImporterFormat.class, new Row("data.json", null, Map.of(),
        ORIENTDB_HEAD + ",{\"@type\":\"d\",\"@rid\":\"#3:0\",\"@class\":\"Account\",\"@version\":1,\"code\":42}]}",
        "{\"info\":{\"name\":\"x\"}}", ORIENTDB_HEAD + "]}"));
    return rows;
  }

  /** Formats on the class path no import can reach, each with why. */
  private static final Map<Class<?>, String> UNREACHABLE = Map.of(Word2VecImporterFormatLSM.class,
      "nothing constructs it: SourceDiscovery answers Word2VecImporterFormat for that content");

  @TempDir
  Path directory;

  @Test
  void everyFormatOnTheClassPathHasARow() throws IOException {
    final Set<Class<?>> formats = concreteFormatsOnTheClassPath();

    assertThat(formats).as("the scan must reach the formats of the integration module, here CSVImporterFormat")
        .contains(CSVImporterFormat.class);
    assertThat(formats).as("the scan must reach the formats of the gremlin module, here GraphMLImporterFormat")
        .contains(GraphMLImporterFormat.class);

    final Set<Class<?>> covered = new HashSet<>(rows().keySet());
    covered.addAll(UNREACHABLE.keySet());
    assertThat(formats).as("a format with no row can answer an import that read nothing in a way of its own").isEqualTo(covered);
  }

  @Test
  void theControlOfEveryFormatCreatesRecordsAndReportsThem() throws IOException {
    for (final Map.Entry<Class<?>, Row> entry : rows().entrySet()) {
      final Map<String, Object> report = run(entry.getValue(), entry.getValue().control());

      assertThat((Long) report.get("createdRecords")).as("%s control", entry.getKey().getSimpleName()).isPositive();
      assertThat(report).as("%s control", entry.getKey().getSimpleName()).doesNotContainKey("warningMessages");
    }
  }

  @Test
  void aSourceOfNoBytesIsRefusedByEveryFormat() {
    for (final Map.Entry<Class<?>, Row> entry : rows().entrySet())
      assertThatThrownBy(() -> run(entry.getValue(), "")).as(entry.getKey().getSimpleName()).isInstanceOf(ImportException.class)
          .hasMessageContaining("is empty");
  }

  @Test
  void aSourceThatIsNotTheFormatIsRefusedOrAnsweredWithAnExplicitZero() throws IOException {
    for (final Map.Entry<Class<?>, Row> entry : rows().entrySet())
      assertRefusedOrExplicitZero(entry.getKey(), entry.getValue(), entry.getValue().notTheFormat());
  }

  @Test
  void aSourceWithNoRecordsIsRefusedOrAnsweredWithAnExplicitZero() throws IOException {
    for (final Map.Entry<Class<?>, Row> entry : rows().entrySet())
      assertRefusedOrExplicitZero(entry.getKey(), entry.getValue(), entry.getValue().noRecords());
  }

  private void assertRefusedOrExplicitZero(final Class<?> format, final Row row, final String content) throws IOException {
    final Map<String, Object> report;
    try {
      report = run(row, content);
    } catch (final ImportException e) {
      assertThat(e.getMessage()).as("%s: the refusal names the problem, not a state of the importer", format.getSimpleName())
          .doesNotContain("Transaction not begun");
      return;
    }
    assertThat(report).as(format.getSimpleName()).containsEntry("createdRecords", 0L);
    assertThat(report).as(format.getSimpleName()).containsKey("parsedRecords").containsKey("warningMessages");
  }

  private Map<String, Object> run(final Row row, final String content) throws IOException {
    final Path source = directory.resolve(row.fileName()).toAbsolutePath();
    Files.writeString(source, content, StandardCharsets.UTF_8);

    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      if (row.setupSql() != null)
        database.command("sqlscript", row.setupSql());

      final Importer importer = new Importer(database, "file://" + source);
      importer.setSettings(row.settings());
      return importer.load();
    } finally {
      database.drop();
    }
  }

  /** Every concrete {@code FormatImporter} on the class path, whatever its package or module. */
  private static Set<Class<?>> concreteFormatsOnTheClassPath() throws IOException {
    final Set<Class<?>> formats = new HashSet<>();
    final String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    for (final String entry : classPath.split(File.pathSeparator)) {
      final File root = new File(entry);
      if (root.isDirectory()) {
        final Path base = root.toPath();
        try (final Stream<Path> walk = Files.walk(base)) {
          walk.forEach(file -> addIfAFormat(base.relativize(file).toString().replace(File.separatorChar, '/'), formats));
        }
      } else if (root.isFile() && entry.endsWith(".jar"))
        try (final JarFile jar = new JarFile(root)) {
          for (final Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements(); )
            addIfAFormat(entries.nextElement().getName(), formats);
        }
    }
    return formats;
  }

  private static void addIfAFormat(final String classFile, final Set<Class<?>> formats) {
    if (!classFile.startsWith("com/arcadedb/") || !classFile.endsWith(".class") || classFile.contains("$"))
      return;

    final Class<?> candidate;
    try {
      candidate = Class.forName(classFile.substring(0, classFile.length() - ".class".length()).replace('/', '.'), false,
          Issue11311EveryFormatOutcomeTest.class.getClassLoader());
    } catch (final ClassNotFoundException | LinkageError e) {
      // A CLASS WHOSE OPTIONAL DEPENDENCY IS NOT ON THIS CLASS PATH: NOT A FORMAT THIS MODULE COULD RUN EITHER
      return;
    }
    if (FormatImporter.class.isAssignableFrom(candidate) && !candidate.isInterface() && !Modifier.isAbstract(candidate.getModifiers()))
      formats.add(candidate);
  }
}
