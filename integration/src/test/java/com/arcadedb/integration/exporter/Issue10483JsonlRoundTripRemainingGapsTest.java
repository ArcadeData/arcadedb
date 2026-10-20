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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10483: after #10391 a JSONL EXPORT DATABASE followed by IMPORT DATABASE still failed, or silently changed the data, in
 * four cases:
 * <ol>
 *   <li>a {@code LINK OF T}, {@code LIST OF T} or {@code MAP OF T} (T a record type) holding a record exported AFTER its owner
 *   aborted the import: the link was validated on the source RID, which in the target database names another type's bucket;</li>
 *   <li>a {@code MAP OF LINK} with the key {@code t} was imported with the link still on the source RID;</li>
 *   <li>an edge type with a MANDATORY or READONLY property aborted the import, because the edge was created empty and the
 *   properties set on a second save;</li>
 *   <li>a {@code LIST OF DATE} or {@code MAP OF DATE} holding a date past +-292 million years made the export skip the record.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10483JsonlRoundTripRemainingGapsTest {
  private static final String SOURCE_PATH = "target/databases/issue10483-source";
  private static final String TARGET_PATH = "target/databases/issue10483-target";
  private static final String FILE        = "issue10483.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  // ---- (1) a typed link to a record the export writes later ----

  /**
   * The vertex type is created AFTER the document type it links to, so its buckets get the higher ids, and the exporter writes every
   * vertex before every document: on import the link is a forward reference whose source RID names, in the target database, a
   * bucket of the holder's own type.
   */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "LINK", "LINK OF Target", "LIST OF LINK", "LIST OF Target", "MAP OF LINK", "MAP OF Target" })
  void aLinkToADocumentExportedLaterIsRestored(final String declaration) {
    final Function<RID, Object> wrap = wrapperOf(declaration);
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p " + declaration);
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("Target").set("id", 7L);
        target.save();
        source.newVertex("Holder").set("p", wrap.apply(target.getIdentity())).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      assertThat(typeAndIdOfLinkIn(target, "Holder")).isEqualTo("Target id=7");
    }
  }

  /**
   * The holder is also the end of an edge: loading the edge updates the vertex record (its edge list), while the link it holds still
   * waits for the document the file reaches before the edges but after the vertices.
   */
  @ParameterizedTest(name = "{0}, holder is the {1} of the edge")
  @MethodSource("typedLinksOnVerticesWithEdges")
  void aHolderWithAnEdgeKeepsItsLinkToADocumentExportedLater(final String declaration, final String end) {
    final Function<RID, Object> wrap = wrapperOf(declaration);
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p " + declaration);
      source.command("sql", "CREATE EDGE TYPE E");
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("Target").set("id", 7L);
        target.save();
        final MutableVertex holder = source.newVertex("Holder").set("p", wrap.apply(target.getIdentity()));
        holder.save();
        final MutableVertex other = source.newVertex("Holder");
        other.save();
        if (end.equals("source"))
          holder.newEdge("E", other);
        else
          other.newEdge("E", holder);
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      try (final ResultSet rs = target.query("sql", "SELECT FROM Holder WHERE p IS NOT NULL")) {
        final Object value = rs.next().getElement().get().get("p");
        final Identifiable link = value instanceof List<?> list ?
            (Identifiable) list.get(0) :
            value instanceof Map<?, ?> map ? (Identifiable) map.get("k") : (Identifiable) value;
        final Document linked = target.lookupByRID(link.getIdentity(), true).asDocument();
        assertThat(linked.getTypeName() + " id=" + linked.get("id")).isEqualTo("Target id=7");
      }
      assertThat(target.countType("E", false)).isEqualTo(1L);
    }
  }

  /** The same, for a vertex linking to a vertex of a type the export lists after its own. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "LINK OF Later", "LIST OF Later", "MAP OF Later" })
  void aLinkToAVertexOfALaterTypeIsRestored(final String declaration) {
    final Function<RID, Object> wrap = wrapperOf(declaration);
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE Earlier");
      source.command("sql", "CREATE VERTEX TYPE Later");
      source.command("sql", "CREATE PROPERTY Later.id LONG");
      source.command("sql", "CREATE PROPERTY Earlier.p " + declaration);
      source.transaction(() -> {
        final MutableVertex later = source.newVertex("Later").set("id", 7L);
        later.save();
        source.newVertex("Earlier").set("p", wrap.apply(later.getIdentity())).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      assertThat(typeAndIdOfLinkIn(target, "Earlier")).isEqualTo("Later id=7");
    }
  }

  /** MANDATORY and NOTNULL are still enforced on the owner while its link waits to be resolved: they are not what is deferred. */
  @Test
  void aMandatoryNotNullLinkToADocumentExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p LINK OF Target");
      source.command("sql", "ALTER PROPERTY Holder.p MANDATORY true");
      source.command("sql", "ALTER PROPERTY Holder.p NOTNULL true");
      source.transaction(() -> {
        final MutableDocument target = source.newDocument("Target").set("id", 7L);
        target.save();
        source.newVertex("Holder").set("p", target.getIdentity()).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      assertThat(typeAndIdOfLinkIn(target, "Holder")).isEqualTo("Target id=7");
    }
  }

  /** A link to an instance of a subtype of the declared type is a link of the declared type, forward or not. */
  @Test
  void aLinkToASubtypeExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Base");
      source.command("sql", "CREATE DOCUMENT TYPE Sub EXTENDS Base");
      source.command("sql", "CREATE PROPERTY Sub.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p LINK OF Base");
      source.transaction(() -> {
        final MutableDocument sub = source.newDocument("Sub").set("id", 7L);
        sub.save();
        source.newVertex("Holder").set("p", sub.getIdentity()).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      assertThat(typeAndIdOfLinkIn(target, "Holder")).isEqualTo("Sub id=7");
    }
  }

  /**
   * The deferral is of the target's type only: the link is checked against the declared type once it is resolved, so a link to a
   * record of the wrong type still fails the import instead of being stored, whatever the declaration is and whether the holder is
   * also the end of an edge (the edge updates it before the reconciliation).
   */
  @ParameterizedTest(name = "{0}, with an edge: {1}")
  @MethodSource("declarationsWithAndWithoutAnEdge")
  void aForwardLinkToTheWrongTypeStillFailsTheImport(final String declaration, final boolean withEdge) throws Exception {
    final String value = declaration.startsWith("LIST") ? "[\"#5:0\"]" : declaration.startsWith("MAP") ? "{\"k\":\"#5:0\"}" : "\"#5:0\"";
    final String type = declaration.startsWith("LIST") ? "LIST" : declaration.startsWith("MAP") ? "MAP" : "LINK";
    // the type tree the export writes for the links held by a list or a map: without it a file names no link to remap
    final String tree = declaration.startsWith("LIST") ? ",\"y\":{\"p\":[\"LINK\"]}" : declaration.startsWith("MAP") ? ",\"y\":{\"p\":{\"k\":\"LINK\"}}" : "";
    final File file = new File(SOURCE_PATH + "-wrong.jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":2}}\n"
        + "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{"
        + "\"Wanted\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Wanted_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"Other\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Other_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"E\":{\"type\":\"e\",\"parents\":[],\"buckets\":[\"E_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"Holder\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Holder_0\"],\"properties\":{\"p\":{\"type\":\"" + type
        + "\",\"of\":\"Wanted\"}},\"indexes\":{},\"custom\":{}}}}}\n"
        + "{\"t\":\"v\",\"c\":{\"p\":{\"p\":" + value + "}" + tree + ",\"r\":\"#3:0\",\"t\":\"Holder\"}}\n"
        + "{\"t\":\"v\",\"c\":{\"p\":{},\"r\":\"#3:1\",\"t\":\"Holder\"}}\n"
        + "{\"t\":\"d\",\"c\":{\"p\":{},\"r\":\"#5:0\",\"t\":\"Other\"}}\n"
        + (withEdge ? "{\"t\":\"e\",\"c\":{\"p\":{},\"r\":\"#4:0\",\"t\":\"E\",\"o\":\"#3:0\",\"i\":\"#3:1\"}}\n" : ""));

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      assertThatThrownBy(() -> target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath()))
          .hasStackTraceContaining("declared as").hasStackTraceContaining("'Wanted'");

      // The import that failed left no deferral open on this thread: the same wrong-type link is refused again
      final Function<RID, Object> wrap = wrapperOf(declaration);
      target.transaction(() -> {
        final MutableDocument other = target.newDocument("Other");
        other.save();
        assertThatThrownBy(() -> target.newVertex("Holder").set("p", wrap.apply(other.getIdentity())).save()).isInstanceOf(
            ValidationException.class);
      });
    } finally {
      file.delete();
    }
  }

  /**
   * An edge is created with the check of its links' target types suspended when one of its vertices still waits for a link, so it has
   * to be validated again like the vertex, or its own link to a record of the wrong type, resolved at once, would be stored.
   */
  @Test
  void anEdgeLinkToTheWrongTypeStillFailsTheImportWhenAVertexWaitsForALink() throws Exception {
    final File file = new File(SOURCE_PATH + "-edge-wrong.jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":2}}\n"
        + "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{"
        + "\"Wanted\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Wanted_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"Other\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Other_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"E\":{\"type\":\"e\",\"parents\":[],\"buckets\":[\"E_0\"],\"properties\":{\"via\":{\"type\":\"LINK\",\"of\":\"Wanted\"}},\"indexes\":{},\"custom\":{}},"
        + "\"Holder\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Holder_0\"],\"properties\":{\"p\":{\"type\":\"LINK\",\"of\":\"Wanted\"}},\"indexes\":{},\"custom\":{}}}}}\n"
        // the vertex waits for the Wanted document the file reaches after it
        + "{\"t\":\"v\",\"c\":{\"p\":{\"p\":\"#5:0\"},\"r\":\"#3:0\",\"t\":\"Holder\"}}\n"
        + "{\"t\":\"v\",\"c\":{\"p\":{},\"r\":\"#3:1\",\"t\":\"Holder\"}}\n"
        + "{\"t\":\"d\",\"c\":{\"p\":{},\"r\":\"#5:0\",\"t\":\"Wanted\"}}\n"
        + "{\"t\":\"d\",\"c\":{\"p\":{},\"r\":\"#6:0\",\"t\":\"Other\"}}\n"
        // its own link names the Other document, known by now: wrong type
        + "{\"t\":\"e\",\"c\":{\"p\":{\"via\":\"#6:0\"},\"r\":\"#4:0\",\"t\":\"E\",\"o\":\"#3:0\",\"i\":\"#3:1\"}}\n");

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      assertThatThrownBy(() -> target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath())).hasStackTraceContaining(
          "declared as LINK of 'Wanted'");
    } finally {
      file.delete();
    }
  }

  /**
   * A record saved with its link unresolved is validated again by the reconciliation pass whatever happens to that link, so the
   * suspension of the check never lets a record through: a link whose target is not in the file is cleared (it is never remapped), and a
   * record that cannot hold the null fails the import.
   */
  @Test
  void aForwardLinkToARecordThatIsNotInTheFileIsClearedAndTheRecordStillValidated() throws Exception {
    final File file = new File(SOURCE_PATH + "-dangling.jsonl");
    file.getParentFile().mkdirs();
    Files.writeString(file.toPath(), danglingFile(""));
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath());
      final Document holder = firstOf(target, "Holder");
      assertThat(holder.get("p")).isNull();
    } finally {
      file.delete();
    }

    FileUtils.deleteRecursively(new File(TARGET_PATH));
    Files.writeString(file.toPath(), danglingFile(",\"mandatory\":true,\"notNull\":true"));
    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      assertThatThrownBy(() -> target.command("sql", "IMPORT DATABASE file://" + file.getAbsolutePath())).hasStackTraceContaining("Holder.p");
    } finally {
      file.delete();
    }
  }

  private static String danglingFile(final String propertyConstraints) {
    return ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":2}}\n"
        + "{\"t\":\"schema\",\"c\":{\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{"
        + "\"Wanted\":{\"type\":\"d\",\"parents\":[],\"buckets\":[\"Wanted_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}},"
        + "\"Holder\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Holder_0\"],\"properties\":{\"p\":{\"type\":\"LINK\",\"of\":\"Wanted\""
        + propertyConstraints + "}},\"indexes\":{},\"custom\":{}}}}}\n"
        + "{\"t\":\"v\",\"c\":{\"p\":{\"p\":\"#9:9\"},\"r\":\"#3:0\",\"t\":\"Holder\"}}\n";
  }

  // ---- (2) a MAP OF LINK with the key "t" ----

  @ParameterizedTest(name = "{0} key {1}")
  @MethodSource("mapDeclarationsAndKeys")
  void aMapLinkIsRemappedWhateverItsKey(final String declaration, final String key) {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE DOCUMENT TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p " + declaration);
      source.transaction(() -> {
        // two other records, so that a link left on its source RID names another record instead of none
        source.newVertex("Target").set("id", 5L).save();
        source.newVertex("Target").set("id", 6L).save();
        final MutableVertex wanted = source.newVertex("Target").set("id", 7L);
        wanted.save();
        source.newDocument("Holder").set("p", new LinkedHashMap<>(Map.of(key, wanted.getIdentity()))).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      final Document holder = firstOf(target, "Holder");
      final Map<?, ?> map = (Map<?, ?>) holder.get("p");
      final Identifiable link = (Identifiable) map.get(key);
      assertThat(target.lookupByRID(link.getIdentity(), true).asDocument().get("id")).isEqualTo(7L);
    }
  }

  // ---- (3) an edge type with a MANDATORY or READONLY property ----

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "none", "MANDATORY", "MANDATORY NOTNULL", "READONLY", "MANDATORY READONLY", "DEFAULT" })
  void anEdgePropertyWithAConstraintIsRestored(final String constraint) {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE EDGE TYPE E");
      source.command("sql", "CREATE PROPERTY E.w STRING");
      if (constraint.contains("MANDATORY"))
        source.command("sql", "ALTER PROPERTY E.w MANDATORY true");
      if (constraint.contains("NOTNULL"))
        source.command("sql", "ALTER PROPERTY E.w NOTNULL true");
      if (constraint.contains("READONLY"))
        source.command("sql", "ALTER PROPERTY E.w READONLY true");
      if (constraint.equals("DEFAULT"))
        source.command("sql", "ALTER PROPERTY E.w DEFAULT 'd'");
      source.transaction(() -> {
        final MutableVertex a = source.newVertex("V").save();
        final MutableVertex b = source.newVertex("V").save();
        a.newEdge("E", b, "w", "x");
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      assertThat(firstOf(target, "E").get("w")).isEqualTo("x");
      assertThat(target.countType("E", false)).isEqualTo(1L);
    }
  }

  /** The same edge property, as the constraint of the type the edge goes to: a MANDATORY LINK of an edge to a vertex. */
  @Test
  void anEdgeMandatoryLinkPropertyIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE PROPERTY V.id LONG");
      source.command("sql", "CREATE EDGE TYPE E");
      source.command("sql", "CREATE PROPERTY E.via LINK OF V");
      source.command("sql", "ALTER PROPERTY E.via MANDATORY true");
      source.transaction(() -> {
        final MutableVertex a = source.newVertex("V").set("id", 1L).save();
        final MutableVertex b = source.newVertex("V").set("id", 2L).save();
        a.newEdge("E", b, "via", b.getIdentity());
      });
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      final Identifiable via = (Identifiable) firstOf(target, "E").get("via");
      assertThat(target.lookupByRID(via.getIdentity(), true).asDocument().get("id")).isEqualTo(2L);
    }
  }

  // ---- (4) a LIST OF DATE / MAP OF DATE at the edges of the range ----

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("extremeDates")
  void aDateInAContainerRoundTripsAtTheEdgesOfTheRange(final String shape, final LocalDate day) {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE D");
      source.command("sql", "CREATE PROPERTY D.p " + shape + " DATE");
      final Object value = shape.startsWith("LIST") ?
          new ArrayList<>(List.of(day)) :
          new LinkedHashMap<>(Map.of("k", day));
      source.transaction(() -> source.newDocument("D").set("p", value).save());
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = importedInto()) {
      final Object after = firstOf(target, "D").get("p");
      final Object date = after instanceof List<?> list ? list.get(0) : ((Map<?, ?>) after).get("k");
      assertThat(date).isEqualTo(day);
    }
  }

  // ---- helpers ----

  private static Database importedInto() {
    final Database target = new DatabaseFactory(TARGET_PATH).create();
    target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath());
    return target;
  }

  private static Function<RID, Object> wrapperOf(final String declaration) {
    if (declaration.startsWith("LIST"))
      return rid -> new ArrayList<>(List.of(rid));
    if (declaration.startsWith("MAP"))
      return rid -> new LinkedHashMap<>(Map.of("k", rid));
    return rid -> rid;
  }

  private static Document firstOf(final Database database, final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
      return rs.next().getElement().get();
    }
  }

  /** "Type id=N" of the record the first {@code holderType} record links to, whether it holds the link, a list of it or a map of it. */
  private static String typeAndIdOfLinkIn(final Database database, final String holderType) {
    final Object value = firstOf(database, holderType).get("p");
    final Identifiable link = value instanceof List<?> list ?
        (Identifiable) list.get(0) :
        value instanceof Map<?, ?> map ? (Identifiable) map.get("k") : (Identifiable) value;
    final Document target = database.lookupByRID(link.getIdentity(), true).asDocument();
    return target.getTypeName() + " id=" + target.get("id");
  }

  static Stream<Arguments> declarationsWithAndWithoutAnEdge() {
    final List<Arguments> cases = new ArrayList<>();
    for (final String declaration : new String[] { "LINK OF Wanted", "LIST OF Wanted", "MAP OF Wanted" })
      for (final boolean withEdge : new boolean[] { false, true })
        cases.add(Arguments.of(declaration, withEdge));
    return cases.stream();
  }

  static Stream<Arguments> typedLinksOnVerticesWithEdges() {
    final List<Arguments> cases = new ArrayList<>();
    for (final String declaration : new String[] { "LINK OF Target", "LIST OF Target", "MAP OF Target" })
      for (final String end : new String[] { "source", "destination" })
        cases.add(Arguments.of(declaration, end));
    return cases.stream();
  }

  static Stream<Arguments> mapDeclarationsAndKeys() {
    final List<Arguments> cases = new ArrayList<>();
    for (final String declaration : new String[] { "MAP OF LINK", "MAP OF Target", "MAP" })
      for (final String key : new String[] { "k", "t" })
        cases.add(Arguments.of(declaration, key));
    return cases.stream();
  }

  static Stream<Arguments> extremeDates() {
    final List<Arguments> cases = new ArrayList<>();
    final LocalDate[] days = { LocalDate.of(2020, 1, 2), LocalDate.of(292278994, 8, 17), LocalDate.of(292278994, 8, 18),
        LocalDate.of(-292275055, 5, 17), LocalDate.of(-292275055, 5, 16), LocalDate.MAX, LocalDate.MIN };
    for (final String shape : new String[] { "LIST OF", "MAP OF" })
      for (final LocalDate day : days)
        cases.add(Arguments.of(shape, day));
    return cases.stream();
  }
}
