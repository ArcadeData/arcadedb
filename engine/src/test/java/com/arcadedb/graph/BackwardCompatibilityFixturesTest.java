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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backward compatibility (#9265, part of the #9269 roadmap): the current build must open, read, check and keep
 * writing a database written by an OLDER release.
 * <p>
 * Each fixture under {@code src/test/resources/compat/db-<version>.zip} was written by the released engine of that
 * version with {@code src/test/compat/BackwardCompatFixtureGenerator.java} (see the README next to it for how to
 * regenerate one, or add the next release). Both hubs in the fixture are promoted supernodes (their edge list head is
 * a type-7 {@link StripeDirectory}) mixing several edge types, and the persons are ordinary low-degree vertices. The
 * constants below mirror the generator: change both together, then regenerate every fixture.
 * <p>
 * Every later change to the edge-list format (#8868) must keep this test green.
 */
class BackwardCompatibilityFixturesTest {
  // Mirrors BackwardCompatFixtureGenerator.
  private static final int   PERSONS            = 500;
  private static final int   HUB_IN_KNOWS       = 400;
  private static final int   HUB_IN_LIKES       = 250;
  private static final int   HUB_IN_TAGS        = 120;
  private static final int   HUB_IN_PARENT_FROM = 7;
  private static final int   HUB_OUT_KNOWS      = 300;
  private static final int[] HUB_OUT_PARENT_TO  = { 3, 5 };
  private static final int   CHAIN              = 99;

  /** Releases with a fixture under {@code src/test/resources/compat/}. Add the next one here. */
  static List<String> versions() {
    return List.of("26.8.1", "26.9.1", "26.10.1");
  }

  @TempDir
  Path tempDir;

  private Database database;

  @AfterEach
  void closeDatabase() {
    if (database != null && database.isOpen())
      database.close();
    TestHelper.checkActiveDatabases();
  }

  /** A fixture added without its version in {@link #versions()} would silently never be tested. */
  @Test
  void everyFixtureIsTested() throws IOException, URISyntaxException {
    final URL dir = BackwardCompatibilityFixturesTest.class.getResource("/compat");
    assertThat(dir).isNotNull();
    final Set<String> onDisk = new HashSet<>();
    try (final Stream<Path> files = Files.list(Path.of(dir.toURI()))) {
      files.map(f -> f.getFileName().toString()).filter(n -> n.startsWith("db-") && n.endsWith(".zip"))
          .forEach(n -> onDisk.add(n.substring("db-".length(), n.length() - ".zip".length())));
    }
    assertThat(onDisk).containsExactlyInAnyOrderElementsOf(versions());
  }

  @ParameterizedTest
  @MethodSource("versions")
  void hubsArePromotedSupernodes(final String version) throws IOException {
    open(version);
    database.transaction(() -> {
      // Guards the fixture itself: without promotion the rest of this class would not exercise the type-7 layout.
      assertThat(database.lookupByRID(inHead(hubIn()), true)).isInstanceOf(StripeDirectory.class);
      assertThat(database.lookupByRID(outHead(hubOut()), true)).isInstanceOf(StripeDirectory.class);
      // Low-degree vertices keep the plain single-list layout.
      assertThat(database.lookupByRID(outHead(person(0)), true)).isInstanceOf(EdgeSegment.class);
    });
  }

  @ParameterizedTest
  @MethodSource("versions")
  void edgeCounts(final String version) throws IOException {
    open(version);
    database.transaction(() -> {
      assertThat(database.countType("Person", false)).isEqualTo(PERSONS);
      assertThat(database.countType("Hub", false)).isEqualTo(2);
      assertThat(database.countType("Knows", false)).isEqualTo(HUB_IN_KNOWS + HUB_OUT_KNOWS + CHAIN);
      assertThat(database.countType("Likes", false)).isEqualTo(HUB_IN_LIKES + 1);
      assertThat(database.countType("Parent", false)).isEqualTo(1 + HUB_OUT_PARENT_TO.length);

      final Vertex hubIn = hubIn();
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Knows")).isEqualTo(HUB_IN_KNOWS);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Likes")).isEqualTo(HUB_IN_LIKES);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Tags")).isEqualTo(HUB_IN_TAGS);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Parent")).isEqualTo(1);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Parent", "Likes")).isEqualTo(1 + HUB_IN_LIKES);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN)).isEqualTo(HUB_IN_KNOWS + HUB_IN_LIKES + HUB_IN_TAGS + 1);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(1);

      final Vertex hubOut = hubOut();
      assertThat(hubOut.countEdges(Vertex.DIRECTION.OUT, "Knows")).isEqualTo(HUB_OUT_KNOWS);
      assertThat(hubOut.countEdges(Vertex.DIRECTION.OUT, "Parent")).isEqualTo(HUB_OUT_PARENT_TO.length);
      assertThat(hubOut.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(HUB_OUT_KNOWS + HUB_OUT_PARENT_TO.length);
      assertThat(hubOut.countEdges(Vertex.DIRECTION.IN, "Likes")).isEqualTo(1);
    });
  }

  @ParameterizedTest
  @MethodSource("versions")
  void filteredAndUnfilteredWalks(final String version) throws IOException {
    open(version);
    database.transaction(() -> {
      final Vertex hubIn = hubIn();

      // Filtered walk on the promoted IN list: the single Parent edge among ~770 entries.
      assertThat(ids(hubIn.getVertices(Vertex.DIRECTION.IN, "Parent"))).containsExactly(HUB_IN_PARENT_FROM);

      // Filtered walk carrying edge properties: every Knows edge, each once, with its property intact.
      long sumW = 0;
      final Set<Integer> knowsFrom = new HashSet<>();
      for (final Edge e : hubIn.getEdges(Vertex.DIRECTION.IN, "Knows")) {
        sumW += e.getInteger("w");
        assertThat(knowsFrom.add(e.getOutVertex().getInteger("id"))).isTrue();
      }
      assertThat(knowsFrom).hasSize(HUB_IN_KNOWS);
      assertThat(sumW).isEqualTo((long) HUB_IN_KNOWS * (HUB_IN_KNOWS - 1) / 2);

      assertThat(count(hubIn.getVertices(Vertex.DIRECTION.IN, "Tags"))).isEqualTo(HUB_IN_TAGS);
      assertThat(count(hubIn.getVertices(Vertex.DIRECTION.IN, "Likes", "Parent"))).isEqualTo(HUB_IN_LIKES + 1);

      // Unfiltered walk: every entry of every type, nothing lost or duplicated across stripes.
      final Set<RID> unfiltered = new HashSet<>();
      int total = 0;
      for (final Edge e : hubIn.getEdges(Vertex.DIRECTION.IN)) {
        ++total;
        if (!(e instanceof ImmutableLightEdge))
          assertThat(unfiltered.add(e.getIdentity())).isTrue();
      }
      assertThat(total).isEqualTo(HUB_IN_KNOWS + HUB_IN_LIKES + HUB_IN_TAGS + 1);
      assertThat(unfiltered).hasSize(HUB_IN_KNOWS + HUB_IN_LIKES + 1);

      final Vertex hubOut = hubOut();
      assertThat(ids(hubOut.getVertices(Vertex.DIRECTION.OUT, "Parent"))).containsExactlyInAnyOrder(HUB_OUT_PARENT_TO[0],
          HUB_OUT_PARENT_TO[1]);
      final List<Integer> outKnows = ids(hubOut.getVertices(Vertex.DIRECTION.OUT, "Knows"));
      assertThat(outKnows).hasSize(HUB_OUT_KNOWS).doesNotHaveDuplicates().allMatch(id -> id >= 0 && id < HUB_OUT_KNOWS);
      assertThat(count(hubOut.getVertices(Vertex.DIRECTION.OUT))).isEqualTo(HUB_OUT_KNOWS + HUB_OUT_PARENT_TO.length);

      // The same walk through SQL.
      try (final ResultSet rs = database.query("sql", "SELECT expand(in('Parent')) FROM Hub WHERE name = 'hubIn'")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("id")).isEqualTo(HUB_IN_PARENT_FROM);
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT out('Knows').size() AS c FROM Hub WHERE name = 'hubOut'")) {
        assertThat(rs.next().<Integer>getProperty("c")).isEqualTo(HUB_OUT_KNOWS);
      }
    });
  }

  @ParameterizedTest
  @MethodSource("versions")
  void isConnectedTo(final String version) throws IOException {
    open(version);
    database.transaction(() -> {
      final Vertex hubIn = hubIn();
      final Vertex hubOut = hubOut();
      final Vertex parent = person(HUB_IN_PARENT_FROM);
      final Vertex other = person(HUB_IN_PARENT_FROM + 1);

      // Probed from the promoted side.
      assertThat(hubIn.isConnectedTo(parent, Vertex.DIRECTION.IN, "Parent")).isTrue();
      assertThat(hubIn.isConnectedTo(other, Vertex.DIRECTION.IN, "Parent")).isFalse();
      assertThat(hubIn.isConnectedTo(other, Vertex.DIRECTION.IN, "Knows")).isTrue();
      assertThat(hubIn.isConnectedTo(person(HUB_IN_KNOWS), Vertex.DIRECTION.IN)).isFalse();
      assertThat(hubIn.isConnectedTo(parent, Vertex.DIRECTION.OUT)).isFalse();
      assertThat(hubIn.isConnectedTo(hubOut, Vertex.DIRECTION.OUT, "Likes")).isTrue();
      assertThat(hubOut.isConnectedTo(person(HUB_OUT_PARENT_TO[1]), Vertex.DIRECTION.OUT, "Parent")).isTrue();
      assertThat(hubOut.isConnectedTo(person(HUB_OUT_PARENT_TO[1] + 1), Vertex.DIRECTION.OUT, "Parent")).isFalse();

      // Probed from the low-degree side.
      assertThat(parent.isConnectedTo(hubIn, Vertex.DIRECTION.OUT, "Parent")).isTrue();
      assertThat(other.isConnectedTo(hubIn, Vertex.DIRECTION.OUT, "Parent")).isFalse();
      assertThat(person(0).isConnectedTo(hubOut, Vertex.DIRECTION.IN, "Knows")).isTrue();
    });
  }

  @ParameterizedTest
  @MethodSource("versions")
  void lowDegreeVertices(final String version) throws IOException {
    open(version);
    database.transaction(() -> {
      // Walk the chain Person[0] -> Person[CHAIN] through the plain edge lists.
      Vertex current = person(0);
      for (int i = 1; i <= CHAIN; i++) {
        final List<Vertex> next = new ArrayList<>();
        for (final Edge e : current.getEdges(Vertex.DIRECTION.OUT, "Knows"))
          if (e.getInteger("w") == -1)
            next.add(e.getInVertex());
        assertThat(next).hasSize(1);
        current = next.getFirst();
        assertThat(current.getInteger("id")).isEqualTo(i);
      }

      final Vertex p7 = person(HUB_IN_PARENT_FROM);
      // Knows to hubIn + chain Knows, Likes, Tags, Parent.
      assertThat(p7.countEdges(Vertex.DIRECTION.OUT)).isEqualTo(5);
      assertThat(ids(p7.getVertices(Vertex.DIRECTION.IN, "Knows"))).containsExactlyInAnyOrder(HUB_IN_PARENT_FROM - 1, -1);

      final Vertex last = person(PERSONS - 1);
      assertThat(last.countEdges(Vertex.DIRECTION.OUT)).isZero();
      assertThat(last.countEdges(Vertex.DIRECTION.IN)).isZero();
    });
  }

  @ParameterizedTest
  @MethodSource("versions")
  void checkDatabaseIsClean(final String version) throws IOException {
    open(version);
    assertCheckDatabaseClean();
  }

  @ParameterizedTest
  @MethodSource("versions")
  void writesOnAnOldDatabaseSurviveReopen(final String version) throws IOException {
    final Path dbDir = open(version);

    // Append to both promoted lists and to a low-degree vertex with the current build.
    database.transaction(() -> {
      final MutableVertex newcomer = database.newVertex("Person").set("id", PERSONS).save();
      newcomer.newEdge("Parent", hubIn());
      newcomer.newEdge("Knows", hubIn(), "w", 0);
      hubOut().modify().newEdge("Parent", newcomer);
      person(PERSONS - 1).modify().newEdge("Likes", newcomer);
    });
    database.close();

    database = new DatabaseFactory(dbDir.toString()).open();
    database.transaction(() -> {
      final Vertex hubIn = hubIn();
      assertThat(ids(hubIn.getVertices(Vertex.DIRECTION.IN, "Parent"))).containsExactlyInAnyOrder(HUB_IN_PARENT_FROM, PERSONS);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Knows")).isEqualTo(HUB_IN_KNOWS + 1);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN)).isEqualTo(HUB_IN_KNOWS + HUB_IN_LIKES + HUB_IN_TAGS + 1 + 2);
      assertThat(database.lookupByRID(inHead(hubIn), true)).isInstanceOf(StripeDirectory.class);

      final Vertex hubOut = hubOut();
      assertThat(hubOut.countEdges(Vertex.DIRECTION.OUT, "Parent")).isEqualTo(HUB_OUT_PARENT_TO.length + 1);
      assertThat(hubOut.isConnectedTo(person(PERSONS), Vertex.DIRECTION.OUT, "Parent")).isTrue();

      assertThat(person(PERSONS - 1).isConnectedTo(person(PERSONS), Vertex.DIRECTION.OUT, "Likes")).isTrue();
      assertThat(database.countType("Parent", false)).isEqualTo(1 + HUB_OUT_PARENT_TO.length + 2);
    });
    assertCheckDatabaseClean();
  }

  @ParameterizedTest
  @MethodSource("versions")
  void deletesOnAnOldDatabaseSurviveReopen(final String version) throws IOException {
    final Path dbDir = open(version);
    final int deletedPerson = HUB_IN_PARENT_FROM + 1; // Knows + Likes + Tags into hubIn, Knows from hubOut, chain both ways

    database.transaction(() -> {
      // An edge removed from a promoted list directly...
      for (final Edge e : hubIn().getEdges(Vertex.DIRECTION.IN, "Parent"))
        e.delete();
      for (final Edge e : hubOut().getEdges(Vertex.DIRECTION.OUT, "Parent"))
        if (e.getInVertex().getInteger("id") == HUB_OUT_PARENT_TO[0])
          e.delete();
      // ...and a vertex whose edges sit in both promoted lists and in low-degree lists.
      person(deletedPerson).delete();
    });
    database.close();

    database = new DatabaseFactory(dbDir.toString()).open();
    database.transaction(() -> {
      final Vertex hubIn = hubIn();
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Parent")).isZero();
      assertThat(hubIn.isConnectedTo(person(HUB_IN_PARENT_FROM), Vertex.DIRECTION.IN, "Parent")).isFalse();
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Knows")).isEqualTo(HUB_IN_KNOWS - 1);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Likes")).isEqualTo(HUB_IN_LIKES - 1);
      assertThat(hubIn.countEdges(Vertex.DIRECTION.IN, "Tags")).isEqualTo(HUB_IN_TAGS - 1);
      assertThat(ids(hubIn.getVertices(Vertex.DIRECTION.IN))).doesNotContain(deletedPerson)
          .hasSize(HUB_IN_KNOWS + HUB_IN_LIKES + HUB_IN_TAGS - 3);

      final Vertex hubOut = hubOut();
      assertThat(ids(hubOut.getVertices(Vertex.DIRECTION.OUT, "Parent"))).containsExactly(HUB_OUT_PARENT_TO[1]);
      assertThat(ids(hubOut.getVertices(Vertex.DIRECTION.OUT, "Knows"))).doesNotContain(deletedPerson).hasSize(HUB_OUT_KNOWS - 1);

      assertThat(database.countType("Person", false)).isEqualTo(PERSONS - 1);
      assertThat(database.countType("Parent", false)).isEqualTo(HUB_OUT_PARENT_TO.length - 1);
      // Knows: into hubIn, from hubOut, and the two chain edges around the deleted person.
      assertThat(database.countType("Knows", false)).isEqualTo(HUB_IN_KNOWS + HUB_OUT_KNOWS + CHAIN - 4);
      assertThat(person(deletedPerson - 1).countEdges(Vertex.DIRECTION.OUT, "Knows")).isEqualTo(1); // only to hubIn
    });
    assertCheckDatabaseClean();
  }

  private Path open(final String version) throws IOException {
    final Path dbDir = tempDir.resolve("compat-" + version);
    unzip("/compat/db-" + version + ".zip", dbDir);
    database = new DatabaseFactory(dbDir.toString()).open();
    return dbDir;
  }

  private void assertCheckDatabaseClean() {
    try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
      assertThat(result.hasNext()).isTrue();
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
        assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords").isEmpty();
        assertThat((Long) row.getProperty("invalidLinks")).as("invalidLinks").isZero();
        assertThat(row.<Collection<?>>getProperty("warnings")).as("warnings").isEmpty();
      }
    }
  }

  private Vertex hubIn() {
    return hub("hubIn");
  }

  private Vertex hubOut() {
    return hub("hubOut");
  }

  private Vertex hub(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Hub WHERE name = ?", name)) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  private Vertex person(final int id) {
    return database.lookupByKey("Person", "id", id).next().asVertex();
  }

  private static RID inHead(final Vertex v) {
    return ((VertexInternal) v).getInEdgesHeadChunk();
  }

  private static RID outHead(final Vertex v) {
    return ((VertexInternal) v).getOutEdgesHeadChunk();
  }

  /** Person ids of the walked vertices; a Hub reads as -1. */
  private static List<Integer> ids(final Iterable<Vertex> vertices) {
    final List<Integer> ids = new ArrayList<>();
    for (final Vertex v : vertices)
      ids.add("Person".equals(v.getTypeName()) ? v.getInteger("id") : -1);
    return ids;
  }

  private static int count(final Iterable<?> iterable) {
    int n = 0;
    for (final Object ignored : iterable)
      ++n;
    return n;
  }

  private static void unzip(final String resource, final Path dir) throws IOException {
    final Path target = dir.toAbsolutePath().normalize();
    Files.createDirectories(target);
    try (final InputStream in = BackwardCompatibilityFixturesTest.class.getResourceAsStream(resource)) {
      assertThat(in).as("fixture " + resource).isNotNull();
      try (final ZipInputStream zip = new ZipInputStream(in)) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          final Path file = target.resolve(entry.getName()).normalize();
          assertThat(file.startsWith(target)).as("zip entry " + entry.getName()).isTrue();
          if (entry.isDirectory())
            Files.createDirectories(file);
          else {
            Files.createDirectories(file.getParent());
            Files.copy(zip, file);
          }
        }
      }
    }
  }
}
