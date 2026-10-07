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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Writes the backward-compatibility fixture database read by {@code BackwardCompatibilityFixturesTest} (#9265).
 * <p>
 * This file is NOT compiled by the build: it runs against the RELEASED engine jar of the version the fixture
 * represents (see {@code generate-fixture.sh} and the README next to it), so it must only use API that exists in
 * every release it is run against. The graph shape below is mirrored by the constants in
 * {@code BackwardCompatibilityFixturesTest}: change both together, then regenerate every fixture.
 * <p>
 * Usage: {@code java -cp <engine-release-classpath> BackwardCompatFixtureGenerator.java <expected-version> <output-zip>}
 */
public class BackwardCompatFixtureGenerator {
  // Low threshold so the hubs promote to the striped layout (type-7 StripeDirectory) with a small fixture.
  static final int SUPERNODE_THRESHOLD = 64;
  static final int PERSONS             = 500;
  static final int HUB_IN_KNOWS        = 400; // Person[i] -Knows{w:i}-> hubIn,     i < 400
  static final int HUB_IN_LIKES        = 250; // Person[i] -Likes->       hubIn,     i < 250
  static final int HUB_IN_TAGS         = 120; // Person[i] -Tags (light)-> hubIn,    i < 120
  static final int HUB_IN_PARENT_FROM  = 7;   // Person[7] -Parent->      hubIn      (the needle in the haystack)
  static final int HUB_OUT_KNOWS       = 300; // hubOut -Knows{w:i}->    Person[i],  i < 300
  static final int[] HUB_OUT_PARENT_TO = { 3, 5 }; // hubOut -Parent->   Person[3], Person[5]
  static final int CHAIN               = 99;  // Person[i] -Knows{w:-1}-> Person[i+1], i < 99 (low-degree vertices)
  static final int BATCH               = 50;

  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: BackwardCompatFixtureGenerator <expected-version> <output-zip>");
      System.exit(1);
    }
    final String expectedVersion = args[0];
    final File outputZip = new File(args[1]).getAbsoluteFile();

    if (!Constants.getRawVersion().equals(expectedVersion))
      throw new IllegalStateException(
          "Classpath carries ArcadeDB " + Constants.getRawVersion() + ", expected " + expectedVersion);

    final Path work = Files.createTempDirectory("arcadedb-compat-fixture");
    final File dbDir = work.resolve("compat").toFile();

    // Process-global setting: fine for this standalone launcher, but it would leak into anything else sharing the JVM.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(SUPERNODE_THRESHOLD);

    try {
      try (final DatabaseFactory factory = new DatabaseFactory(dbDir.getAbsolutePath())) {
        final Database db = factory.create();
        try {
          populate(db);
          verifyPromoted(db);
        } finally {
          db.close();
        }
      }
      zipDirectory(dbDir.toPath(), outputZip);
    } finally {
      deleteRecursively(work);
    }
    System.out.println("Fixture written by ArcadeDB " + Constants.getVersion() + " -> " + outputZip);
  }

  private static void populate(final Database db) {
    db.transaction(() -> {
      final Schema schema = db.getSchema();
      schema.createVertexType("Person", 1).createProperty("id", Type.INTEGER);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");
      schema.createVertexType("Hub", 1).createProperty("name", Type.STRING);
      schema.createEdgeType("Knows", 1).createProperty("w", Type.INTEGER);
      schema.createEdgeType("Likes", 1);
      schema.createEdgeType("Parent", 1);
      schema.createEdgeType("Tags", 1);
    });

    final RID[] persons = new RID[PERSONS];
    final RID[] hubs = new RID[2];
    db.transaction(() -> {
      for (int i = 0; i < PERSONS; i++)
        persons[i] = db.newVertex("Person").set("id", i).save().getIdentity();
      hubs[0] = db.newVertex("Hub").set("name", "hubIn").save().getIdentity();
      hubs[1] = db.newVertex("Hub").set("name", "hubOut").save().getIdentity();
    });

    // Interleave the edge types so each hub's promoted list mixes them, and commit in small batches so the
    // promotion happens on a later append, the way it does on a live database.
    for (int start = 0; start < PERSONS; start += BATCH) {
      final int from = start;
      db.transaction(() -> {
        final Vertex hubIn = hubs[0].asVertex();
        final MutableVertex hubOut = hubs[1].asVertex().modify();
        for (int i = from; i < Math.min(from + BATCH, PERSONS); i++) {
          final MutableVertex person = persons[i].asVertex().modify();
          if (i < HUB_IN_KNOWS)
            person.newEdge("Knows", hubIn, "w", i);
          if (i < HUB_IN_LIKES)
            person.newEdge("Likes", hubIn);
          if (i < HUB_IN_TAGS)
            person.newLightEdge("Tags", hubIn);
          if (i == HUB_IN_PARENT_FROM)
            person.newEdge("Parent", hubIn);
          if (i < HUB_OUT_KNOWS)
            hubOut.newEdge("Knows", persons[i], "w", i);
          for (final int p : HUB_OUT_PARENT_TO)
            if (i == p)
              hubOut.newEdge("Parent", persons[i]);
          if (i < CHAIN)
            person.newEdge("Knows", persons[i + 1], "w", -1);
        }
      });
    }

    // One edge between the two supernodes.
    db.transaction(() -> hubs[0].asVertex().modify().newEdge("Likes", hubs[1]));
  }

  private static void verifyPromoted(final Database db) {
    db.transaction(() -> {
      final List<String> failures = new ArrayList<>();
      try (final var rs = db.query("sql", "SELECT FROM Hub ORDER BY name")) {
        while (rs.hasNext()) {
          final VertexInternal hub = (VertexInternal) rs.next().getVertex().get();
          final String name = hub.getString("name");
          final RID head = "hubIn".equals(name) ? hub.getInEdgesHeadChunk() : hub.getOutEdgesHeadChunk();
          final Identifiable record = db.lookupByRID(head, true);
          if (!(record instanceof StripeDirectory))
            failures.add(name + " head " + head + " is " + record.getClass().getSimpleName());
        }
      }
      if (!failures.isEmpty())
        throw new IllegalStateException("Hubs were not promoted to the striped layout: " + failures);
    });
  }

  private static void zipDirectory(final Path root, final File outputZip) throws IOException {
    outputZip.getParentFile().mkdirs();
    final List<Path> files;
    try (final Stream<Path> walk = Files.walk(root)) {
      files = walk.filter(Files::isRegularFile).sorted().toList();
    }
    try (final ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(outputZip))) {
      for (final Path file : files) {
        final String name = root.relativize(file).toString().replace(File.separatorChar, '/');
        if (name.endsWith(".lck"))
          continue;
        final ZipEntry entry = new ZipEntry(name);
        entry.setTime(0L);
        zip.putNextEntry(entry);
        Files.copy(file, zip);
        zip.closeEntry();
      }
    }
  }

  private static void deleteRecursively(final Path root) throws IOException {
    try (final Stream<Path> walk = Files.walk(root)) {
      for (final Path p : walk.sorted(Comparator.reverseOrder()).toList())
        Files.delete(p);
    }
  }
}
