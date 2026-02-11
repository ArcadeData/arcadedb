# GraphBenchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an LDBC SNB-inspired graph benchmark for ArcadeDB that measures creation, lookups, and traversal performance across SQL and OpenCypher.

**Architecture:** Single JUnit 5 test class (`GraphBenchmark`) in the engine module's performance package. Uses `@Tag("benchmark")` for selective execution. Database is preserved between runs via `close()` instead of `drop()`. Micrometer `SimpleMeterRegistry` collects timing histograms. Data generation follows LDBC SNB dependency order across 8 vertex types and 14 edge types.

**Tech Stack:** Java 21, JUnit 5, Micrometer 1.16.2, ArcadeDB engine API (Database, Schema, Graph, Query)

**Design doc:** `docs/plans/2026-02-11-graph-benchmark-design.md`

---

### Task 1: Add Micrometer dependency and create class skeleton

**Files:**
- Modify: `engine/pom.xml` (add micrometer-core test dependency)
- Create: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Add Micrometer dependency to engine/pom.xml**

Add after the existing JMH dependencies (after line ~206 in `engine/pom.xml`):

```xml
        <!-- Micrometer for benchmark metrics -->
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
            <version>${micrometer.version}</version>
            <scope>test</scope>
        </dependency>
```

Note: `${micrometer.version}` is already defined as `1.16.2` in the parent `pom.xml`.

**Step 2: Create the skeleton class**

Create `engine/src/test/java/performance/GraphBenchmark.java` with the class structure, constants, lifecycle methods, and empty test methods. Include all imports upfront.

```java
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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * LDBC Social Network Benchmark-inspired graph benchmark for ArcadeDB.
 *
 * Measures performance across 4 phases:
 * - Phase 1: Graph creation (8 vertex types, 14 edge types)
 * - Phase 2: Simple lookups (indexed and non-indexed)
 * - Phase 3: Simple traversals (1-hop)
 * - Phase 4: Complex traversals (multi-hop, pattern matching)
 *
 * Queries run in both SQL and OpenCypher side by side.
 * Database is preserved between runs -- only the first execution pays generation cost.
 *
 * Run with: mvn test -pl engine -Dtest=GraphBenchmark -Dgroups=benchmark
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("benchmark")
class GraphBenchmark {

  // === SCALE CONSTANTS (adjust for smaller/larger benchmarks) ===
  private static final int NUM_PERSONS       = 30_000;
  private static final int NUM_POSTS         = 150_000;
  private static final int NUM_COMMENTS      = 600_000;
  private static final int NUM_FORUMS        = 5_000;
  private static final int NUM_TAGS          = 2_000;
  private static final int NUM_TAG_CLASSES   = 100;
  private static final int NUM_PLACES        = 1_500;
  private static final int NUM_ORGANISATIONS = 3_000;

  // Edge density
  private static final int AVG_KNOWS_PER_PERSON     = 40;
  private static final int AVG_LIKES_PER_PERSON      = 30;
  private static final int AVG_TAGS_PER_POST          = 3;
  private static final int AVG_INTERESTS_PER_PERSON   = 5;
  private static final int AVG_MEMBERS_PER_FORUM      = 20;

  // Runtime config
  private static final int    PARALLEL     = 4;
  private static final int    COMMIT_EVERY = 5_000;
  private static final String DB_PATH      = "target/databases/graph-benchmark";

  // Benchmark iterations
  private static final int WARMUP_ITERATIONS = 5;
  private static final int LOOKUP_ITERATIONS = 1_000;
  private static final int SIMPLE_TRAVERSAL_ITERATIONS = 500;
  private static final int COMPLEX_TRAVERSAL_ITERATIONS = 200;
  private static final int SHORTEST_PATH_ITERATIONS = 100;

  // Synthetic data pools
  private static final String[] FIRST_NAMES = {
      "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry", "Irene", "Jack",
      "Karen", "Leo", "Maria", "Nick", "Olivia", "Peter", "Quinn", "Rachel", "Steve", "Tina",
      "Uma", "Victor", "Wendy", "Xavier", "Yuki", "Zara", "Ahmed", "Bianca", "Carlos", "Diana"
  };
  private static final String[] LAST_NAMES = {
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
      "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
  };
  private static final String[] GENDERS = {"male", "female"};
  private static final String[] BROWSERS = {"Firefox", "Chrome", "Safari", "Edge", "Opera"};
  private static final String[] LANGUAGES = {"en", "es", "fr", "de", "it", "pt", "ja", "zh", "ko", "ar"};
  private static final String[] CONTINENTS = {"Africa", "Asia", "Europe", "NorthAmerica", "Oceania", "SouthAmerica"};
  private static final String[] COUNTRIES = {
      "Germany", "France", "Italy", "Spain", "UK", "Poland", "Netherlands", "Belgium", "Sweden", "Austria",
      "USA", "Canada", "Mexico", "Brazil", "Argentina", "China", "Japan", "India", "Australia", "SouthKorea",
      "Egypt", "Nigeria", "SouthAfrica", "Kenya", "Morocco", "Turkey", "Iran", "Iraq", "Thailand", "Vietnam",
      "Colombia", "Peru", "Chile", "Indonesia", "Philippines", "Pakistan", "Bangladesh", "Russia", "Ukraine", "Norway",
      "Denmark", "Finland", "Switzerland", "Portugal", "Ireland", "CzechRepublic", "Romania", "Hungary", "Greece", "NewZealand"
  };
  private static final String[] TAG_CLASS_NAMES = {
      "Sport", "Politics", "Technology", "Science", "Music", "Art", "Literature", "History",
      "Philosophy", "Economics", "Medicine", "Law", "Education", "Religion", "Nature",
      "Food", "Travel", "Fashion", "Cinema", "Photography"
  };

  // Instance state
  private Database database;
  private MeterRegistry registry;
  private boolean freshlyCreated;

  // Cached sample IDs for benchmark queries (populated after generation or on open)
  private long[] samplePersonIds;
  private long[] samplePostIds;
  private long[] sampleForumIds;
  private String[] sampleCityNames;
  private String[] sampleFirstNames;

  @BeforeAll
  void setup() {
    GlobalConfiguration.PROFILE.setValue("high-performance");
    registry = new SimpleMeterRegistry();

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      database = factory.open();
      freshlyCreated = false;
      System.out.println("Database opened from " + DB_PATH + " (reusing existing data)");
    } else {
      database = factory.create();
      freshlyCreated = true;
      System.out.println("Creating new database at " + DB_PATH);
      final Timer.Sample sample = Timer.start(registry);
      createSchema();
      populateGraph();
      sample.stop(registry.timer("benchmark.creation"));
    }

    prepareSampleIds();
    printDatasetStats();
  }

  @AfterAll
  void teardownAndReport() {
    printReport();
    if (database != null && database.isOpen())
      database.close();
  }

  @Test
  @Order(1)
  void phase2_lookups() {
    // TODO: Task 6
  }

  @Test
  @Order(2)
  void phase3_simpleTraversals() {
    // TODO: Task 7
  }

  @Test
  @Order(3)
  void phase4_complexTraversals() {
    // TODO: Task 8
  }

  // --- Schema creation (Task 2) ---

  private void createSchema() {
    // TODO: Task 2
  }

  // --- Data generation (Tasks 3-5) ---

  private void populateGraph() {
    // TODO: Tasks 3, 4, 5
  }

  // --- Sample ID preparation ---

  private void prepareSampleIds() {
    // TODO: Task 6 (prepare random IDs for benchmark queries)
  }

  // --- Benchmark helper ---

  private void benchmark(final String phase, final String name, final int iterations,
      final String sql, final String cypher) {
    // TODO: Task 6
  }

  // --- Reporting ---

  private void printDatasetStats() {
    // TODO: Task 9
  }

  private void printReport() {
    // TODO: Task 9
  }
}
```

**Step 3: Compile to verify skeleton**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS (no compilation errors)

**Step 4: Commit**

```bash
git add engine/pom.xml engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: add GraphBenchmark skeleton with Micrometer dependency"
```

---

### Task 2: Implement schema creation

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java` (replace `createSchema()` method)

**Step 1: Implement `createSchema()`**

Replace the `createSchema()` stub with the full implementation. Create all 8 vertex types with properties and indexes, and all 14 edge types.

```java
  private void createSchema() {
    System.out.println("Creating LDBC SNB schema...");
    final Schema schema = database.getSchema();

    // --- Vertex types ---

    // TagClass
    final VertexType tagClassType = schema.buildVertexType().withName("TagClass").withTotalBuckets(PARALLEL).create();
    tagClassType.createProperty("id", Type.LONG);
    tagClassType.createProperty("name", Type.STRING);
    tagClassType.createProperty("url", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "TagClass", "id");

    // Tag
    final VertexType tagType = schema.buildVertexType().withName("Tag").withTotalBuckets(PARALLEL).create();
    tagType.createProperty("id", Type.LONG);
    tagType.createProperty("name", Type.STRING);
    tagType.createProperty("url", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Tag", "id");

    // Place
    final VertexType placeType = schema.buildVertexType().withName("Place").withTotalBuckets(PARALLEL).create();
    placeType.createProperty("id", Type.LONG);
    placeType.createProperty("name", Type.STRING);
    placeType.createProperty("url", Type.STRING);
    placeType.createProperty("type", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Place", "id");
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Place", "type");

    // Organisation
    final VertexType orgType = schema.buildVertexType().withName("Organisation").withTotalBuckets(PARALLEL).create();
    orgType.createProperty("id", Type.LONG);
    orgType.createProperty("name", Type.STRING);
    orgType.createProperty("url", Type.STRING);
    orgType.createProperty("type", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Organisation", "id");
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Organisation", "type");

    // Person
    final VertexType personType = schema.buildVertexType().withName("Person").withTotalBuckets(PARALLEL).create();
    personType.createProperty("id", Type.LONG);
    personType.createProperty("firstName", Type.STRING);
    personType.createProperty("lastName", Type.STRING);
    personType.createProperty("gender", Type.STRING);
    personType.createProperty("birthday", Type.STRING);
    personType.createProperty("creationDate", Type.LONG);
    personType.createProperty("locationIP", Type.STRING);
    personType.createProperty("browserUsed", Type.STRING);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");

    // Post
    final VertexType postType = schema.buildVertexType().withName("Post").withTotalBuckets(PARALLEL).create();
    postType.createProperty("id", Type.LONG);
    postType.createProperty("imageFile", Type.STRING);
    postType.createProperty("creationDate", Type.LONG);
    postType.createProperty("locationIP", Type.STRING);
    postType.createProperty("browserUsed", Type.STRING);
    postType.createProperty("language", Type.STRING);
    postType.createProperty("content", Type.STRING);
    postType.createProperty("length", Type.INTEGER);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Post", "id");

    // Comment
    final VertexType commentType = schema.buildVertexType().withName("Comment").withTotalBuckets(PARALLEL).create();
    commentType.createProperty("id", Type.LONG);
    commentType.createProperty("creationDate", Type.LONG);
    commentType.createProperty("locationIP", Type.STRING);
    commentType.createProperty("browserUsed", Type.STRING);
    commentType.createProperty("content", Type.STRING);
    commentType.createProperty("length", Type.INTEGER);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Comment", "id");

    // Forum
    final VertexType forumType = schema.buildVertexType().withName("Forum").withTotalBuckets(PARALLEL).create();
    forumType.createProperty("id", Type.LONG);
    forumType.createProperty("title", Type.STRING);
    forumType.createProperty("creationDate", Type.LONG);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Forum", "id");

    // --- Edge types ---
    schema.createEdgeType("KNOWS");
    schema.createEdgeType("HAS_CREATOR");
    schema.createEdgeType("REPLY_OF");
    schema.createEdgeType("HAS_TAG");
    schema.createEdgeType("LIKES");
    schema.createEdgeType("CONTAINER_OF");
    schema.createEdgeType("HAS_MEMBER");
    schema.createEdgeType("HAS_MODERATOR");
    schema.createEdgeType("WORKS_AT");
    schema.createEdgeType("STUDY_AT");
    schema.createEdgeType("IS_LOCATED_IN");
    schema.createEdgeType("HAS_INTEREST");
    schema.createEdgeType("IS_PART_OF");
    schema.createEdgeType("IS_SUBCLASS_OF");

    System.out.println("Schema created: 8 vertex types, 14 edge types");
  }
```

**Step 2: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement LDBC SNB schema creation (8 vertex, 14 edge types)"
```

---

### Task 3: Implement reference data generation (TagClass, Tag, Place, Organisation)

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Implement generation methods for reference data**

Add these private methods and wire them into `populateGraph()`:

```java
  private void populateGraph() {
    System.out.println("Populating graph...");
    final long start = System.currentTimeMillis();

    generateTagClasses();
    generateTags();
    generatePlaces();
    generateOrganisations();
    generatePersons();
    generateKnows();
    generateForums();
    generatePosts();
    generateComments();
    generateLikes();

    System.out.println("Graph populated in " + (System.currentTimeMillis() - start) + " ms");
  }

  private void generateTagClasses() {
    System.out.println("  Generating " + NUM_TAG_CLASSES + " TagClasses...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final int numRoots = TAG_CLASS_NAMES.length;

    database.begin();
    for (int i = 0; i < NUM_TAG_CLASSES; i++) {
      final MutableVertex tc = database.newVertex("TagClass");
      tc.set("id", (long) i);
      if (i < numRoots)
        tc.set("name", TAG_CLASS_NAMES[i]);
      else
        tc.set("name", TAG_CLASS_NAMES[rnd.nextInt(numRoots)] + "_Sub" + i);
      tc.set("url", "http://dbpedia.org/resource/" + tc.getString("name"));
      tc.save();

      // IS_SUBCLASS_OF: non-root classes point to a random root
      if (i >= numRoots) {
        final int parentId = rnd.nextInt(numRoots);
        final IndexCursor parentCursor = database.lookupByKey("TagClass", "id", (long) parentId);
        if (parentCursor.hasNext())
          tc.newEdge("IS_SUBCLASS_OF", parentCursor.next().asVertex(), false);
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  private void generateTags() {
    System.out.println("  Generating " + NUM_TAGS + " Tags...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    database.begin();
    for (int i = 0; i < NUM_TAGS; i++) {
      final MutableVertex tag = database.newVertex("Tag");
      tag.set("id", (long) i);
      tag.set("name", "Tag_" + i);
      tag.set("url", "http://dbpedia.org/resource/Tag_" + i);
      tag.save();

      // IS_PART_OF: each tag belongs to a random TagClass
      final int tagClassId = rnd.nextInt(NUM_TAG_CLASSES);
      final IndexCursor tcCursor = database.lookupByKey("TagClass", "id", (long) tagClassId);
      if (tcCursor.hasNext())
        tag.newEdge("IS_PART_OF", tcCursor.next().asVertex(), false);

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  private void generatePlaces() {
    System.out.println("  Generating " + NUM_PLACES + " Places...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    database.begin();
    int placeId = 0;

    // Continents (ids 0..5)
    for (final String continent : CONTINENTS) {
      final MutableVertex place = database.newVertex("Place");
      place.set("id", (long) placeId);
      place.set("name", continent);
      place.set("url", "http://dbpedia.org/resource/" + continent);
      place.set("type", "Continent");
      place.save();
      placeId++;
    }

    // Countries (ids 6..55) -- each IS_PART_OF a continent
    final int numCountries = Math.min(COUNTRIES.length, NUM_PLACES / 3);
    for (int i = 0; i < numCountries; i++) {
      final MutableVertex place = database.newVertex("Place");
      place.set("id", (long) placeId);
      place.set("name", COUNTRIES[i]);
      place.set("url", "http://dbpedia.org/resource/" + COUNTRIES[i]);
      place.set("type", "Country");
      place.save();

      // IS_PART_OF continent
      final int continentId = i % CONTINENTS.length;
      final IndexCursor cursor = database.lookupByKey("Place", "id", (long) continentId);
      if (cursor.hasNext())
        place.newEdge("IS_PART_OF", cursor.next().asVertex(), false);

      placeId++;
    }

    // Cities (remaining) -- each IS_PART_OF a country
    final int firstCountryId = CONTINENTS.length;
    while (placeId < NUM_PLACES) {
      final MutableVertex place = database.newVertex("Place");
      place.set("id", (long) placeId);
      place.set("name", "City_" + placeId);
      place.set("url", "http://dbpedia.org/resource/City_" + placeId);
      place.set("type", "City");
      place.save();

      // IS_PART_OF country
      final int countryId = firstCountryId + rnd.nextInt(numCountries);
      final IndexCursor cursor = database.lookupByKey("Place", "id", (long) countryId);
      if (cursor.hasNext())
        place.newEdge("IS_PART_OF", cursor.next().asVertex(), false);

      placeId++;
      if (placeId % COMMIT_EVERY == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  private void generateOrganisations() {
    System.out.println("  Generating " + NUM_ORGANISATIONS + " Organisations...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    // Pre-compute city and country ID ranges
    final int firstCountryId = CONTINENTS.length;
    final int numCountries = Math.min(COUNTRIES.length, NUM_PLACES / 3);
    final int firstCityId = firstCountryId + numCountries;
    final int numCities = NUM_PLACES - firstCityId;

    database.begin();
    for (int i = 0; i < NUM_ORGANISATIONS; i++) {
      final boolean isUniversity = i < NUM_ORGANISATIONS / 2;
      final MutableVertex org = database.newVertex("Organisation");
      org.set("id", (long) i);
      org.set("type", isUniversity ? "University" : "Company");
      org.set("name", (isUniversity ? "University_" : "Company_") + i);
      org.set("url", "http://dbpedia.org/resource/Org_" + i);
      org.save();

      // IS_LOCATED_IN: Universities in countries, Companies in cities
      final int placeId;
      if (isUniversity)
        placeId = firstCountryId + rnd.nextInt(numCountries);
      else
        placeId = firstCityId + (numCities > 0 ? rnd.nextInt(numCities) : 0);

      final IndexCursor cursor = database.lookupByKey("Place", "id", (long) placeId);
      if (cursor.hasNext())
        org.newEdge("IS_LOCATED_IN", cursor.next().asVertex(), false);

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }
```

**Step 2: Add stub methods for remaining generators**

Add empty stubs so it compiles (these will be filled in Tasks 4-5):

```java
  private void generatePersons() {
    System.out.println("  Generating Persons... (TODO)");
  }

  private void generateKnows() {
    System.out.println("  Generating KNOWS... (TODO)");
  }

  private void generateForums() {
    System.out.println("  Generating Forums... (TODO)");
  }

  private void generatePosts() {
    System.out.println("  Generating Posts... (TODO)");
  }

  private void generateComments() {
    System.out.println("  Generating Comments... (TODO)");
  }

  private void generateLikes() {
    System.out.println("  Generating LIKES... (TODO)");
  }
```

**Step 3: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement reference data generation (TagClass, Tag, Place, Organisation)"
```

---

### Task 4: Implement Person generation with social relationships

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java` (replace `generatePersons()` stub)

**Step 1: Implement `generatePersons()`**

Replace the stub with the full implementation. Each Person gets IS_LOCATED_IN (city), WORKS_AT, STUDY_AT, and HAS_INTEREST edges.

```java
  private void generatePersons() {
    System.out.println("  Generating " + NUM_PERSONS + " Persons...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    // Pre-compute city and org ID ranges
    final int firstCountryId = CONTINENTS.length;
    final int numCountries = Math.min(COUNTRIES.length, NUM_PLACES / 3);
    final int firstCityId = firstCountryId + numCountries;
    final int numCities = NUM_PLACES - firstCityId;
    final int numUniversities = NUM_ORGANISATIONS / 2;

    database.begin();
    for (int i = 0; i < NUM_PERSONS; i++) {
      final MutableVertex person = database.newVertex("Person");
      person.set("id", (long) i);
      person.set("firstName", FIRST_NAMES[rnd.nextInt(FIRST_NAMES.length)]);
      person.set("lastName", LAST_NAMES[rnd.nextInt(LAST_NAMES.length)]);
      person.set("gender", GENDERS[rnd.nextInt(GENDERS.length)]);
      person.set("birthday", (1960 + rnd.nextInt(40)) + "-" + (1 + rnd.nextInt(12)) + "-" + (1 + rnd.nextInt(28)));
      person.set("creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 5));
      person.set("locationIP", rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256));
      person.set("browserUsed", BROWSERS[rnd.nextInt(BROWSERS.length)]);
      person.save();

      // IS_LOCATED_IN a city
      if (numCities > 0) {
        final int cityId = firstCityId + rnd.nextInt(numCities);
        final IndexCursor cursor = database.lookupByKey("Place", "id", (long) cityId);
        if (cursor.hasNext())
          person.newEdge("IS_LOCATED_IN", cursor.next().asVertex(), false);
      }

      // WORKS_AT a company (70% of persons)
      if (rnd.nextInt(100) < 70) {
        final int companyId = numUniversities + rnd.nextInt(NUM_ORGANISATIONS - numUniversities);
        final IndexCursor cursor = database.lookupByKey("Organisation", "id", (long) companyId);
        if (cursor.hasNext())
          person.newEdge("WORKS_AT", cursor.next().asVertex(), false, "workFrom", 1990 + rnd.nextInt(30));
      }

      // STUDY_AT a university (40% of persons)
      if (rnd.nextInt(100) < 40 && numUniversities > 0) {
        final int uniId = rnd.nextInt(numUniversities);
        final IndexCursor cursor = database.lookupByKey("Organisation", "id", (long) uniId);
        if (cursor.hasNext())
          person.newEdge("STUDY_AT", cursor.next().asVertex(), false, "classYear", 2000 + rnd.nextInt(20));
      }

      // HAS_INTEREST tags
      final int numInterests = 1 + rnd.nextInt(AVG_INTERESTS_PER_PERSON * 2);
      for (int j = 0; j < numInterests; j++) {
        final int tagId = rnd.nextInt(NUM_TAGS);
        final IndexCursor cursor = database.lookupByKey("Tag", "id", (long) tagId);
        if (cursor.hasNext())
          person.newEdge("HAS_INTEREST", cursor.next().asVertex(), false);
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }

      if (i % 10_000 == 0 && i > 0)
        System.out.println("    " + i + " / " + NUM_PERSONS + " persons...");
    }
    database.commit();
  }
```

**Step 2: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement Person generation with IS_LOCATED_IN, WORKS_AT, STUDY_AT, HAS_INTEREST"
```

---

### Task 5: Implement KNOWS, Forum, Post, Comment, and LIKES generation

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java` (replace remaining stubs)

**Step 1: Implement `generateKnows()`**

Power-law distribution for KNOWS edges. Use bidirectional edges.

```java
  private void generateKnows() {
    final int totalKnows = NUM_PERSONS * AVG_KNOWS_PER_PERSON / 2; // bidirectional, so /2
    System.out.println("  Generating ~" + totalKnows + " KNOWS edges (bidirectional)...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < totalKnows; i++) {
      final int fromId = rnd.nextInt(NUM_PERSONS);
      int toId = rnd.nextInt(NUM_PERSONS);
      if (fromId == toId)
        toId = (toId + 1) % NUM_PERSONS;

      final IndexCursor fromCursor = database.lookupByKey("Person", "id", (long) fromId);
      final IndexCursor toCursor = database.lookupByKey("Person", "id", (long) toId);
      if (fromCursor.hasNext() && toCursor.hasNext()) {
        final Vertex from = fromCursor.next().asVertex();
        final Vertex to = toCursor.next().asVertex();
        from.asVertex(true).newEdge("KNOWS", to, true,
            "creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 3));
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }

      if (i % 100_000 == 0 && i > 0)
        System.out.println("    " + i + " / " + totalKnows + " KNOWS edges...");
    }
    database.commit();
  }
```

**Step 2: Implement `generateForums()`**

```java
  private void generateForums() {
    System.out.println("  Generating " + NUM_FORUMS + " Forums...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < NUM_FORUMS; i++) {
      final MutableVertex forum = database.newVertex("Forum");
      forum.set("id", (long) i);
      forum.set("title", "Forum_" + i);
      forum.set("creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 4));
      forum.save();

      // HAS_MODERATOR: one random Person
      final int modId = rnd.nextInt(NUM_PERSONS);
      final IndexCursor modCursor = database.lookupByKey("Person", "id", (long) modId);
      if (modCursor.hasNext())
        forum.newEdge("HAS_MODERATOR", modCursor.next().asVertex(), false);

      // HAS_MEMBER: random Persons
      final int numMembers = 2 + rnd.nextInt(AVG_MEMBERS_PER_FORUM * 2);
      for (int j = 0; j < numMembers; j++) {
        final int memberId = rnd.nextInt(NUM_PERSONS);
        final IndexCursor cursor = database.lookupByKey("Person", "id", (long) memberId);
        if (cursor.hasNext())
          forum.newEdge("HAS_MEMBER", cursor.next().asVertex(), false,
              "joinDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 3));
      }

      // HAS_TAG: random Tags
      final int numTags = 1 + rnd.nextInt(5);
      for (int j = 0; j < numTags; j++) {
        final int tagId = rnd.nextInt(NUM_TAGS);
        final IndexCursor cursor = database.lookupByKey("Tag", "id", (long) tagId);
        if (cursor.hasNext())
          forum.newEdge("HAS_TAG", cursor.next().asVertex(), false);
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }
```

**Step 3: Implement `generatePosts()`**

```java
  private void generatePosts() {
    System.out.println("  Generating " + NUM_POSTS + " Posts...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < NUM_POSTS; i++) {
      final MutableVertex post = database.newVertex("Post");
      post.set("id", (long) i);
      post.set("imageFile", rnd.nextInt(100) < 30 ? "photo_" + i + ".jpg" : "");
      post.set("creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 3));
      post.set("locationIP", rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256));
      post.set("browserUsed", BROWSERS[rnd.nextInt(BROWSERS.length)]);
      post.set("language", LANGUAGES[rnd.nextInt(LANGUAGES.length)]);
      post.set("content", "Post content " + i);
      post.set("length", 10 + rnd.nextInt(2000));
      post.save();

      // HAS_CREATOR: one random Person
      final int creatorId = rnd.nextInt(NUM_PERSONS);
      final IndexCursor creatorCursor = database.lookupByKey("Person", "id", (long) creatorId);
      if (creatorCursor.hasNext())
        post.newEdge("HAS_CREATOR", creatorCursor.next().asVertex(), false);

      // CONTAINER_OF: this post belongs to a random Forum (edge from Forum to Post)
      final int forumId = rnd.nextInt(NUM_FORUMS);
      final IndexCursor forumCursor = database.lookupByKey("Forum", "id", (long) forumId);
      if (forumCursor.hasNext())
        forumCursor.next().asVertex(true).newEdge("CONTAINER_OF", post, false);

      // HAS_TAG
      final int numTags = 1 + rnd.nextInt(AVG_TAGS_PER_POST * 2);
      for (int j = 0; j < numTags; j++) {
        final int tagId = rnd.nextInt(NUM_TAGS);
        final IndexCursor cursor = database.lookupByKey("Tag", "id", (long) tagId);
        if (cursor.hasNext())
          post.newEdge("HAS_TAG", cursor.next().asVertex(), false);
      }

      // IS_LOCATED_IN: same as creator's location (use a random city for simplicity)
      final int firstCityId = CONTINENTS.length + Math.min(COUNTRIES.length, NUM_PLACES / 3);
      final int numCities = NUM_PLACES - firstCityId;
      if (numCities > 0) {
        final int cityId = firstCityId + rnd.nextInt(numCities);
        final IndexCursor cursor = database.lookupByKey("Place", "id", (long) cityId);
        if (cursor.hasNext())
          post.newEdge("IS_LOCATED_IN", cursor.next().asVertex(), false);
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }

      if (i % 50_000 == 0 && i > 0)
        System.out.println("    " + i + " / " + NUM_POSTS + " posts...");
    }
    database.commit();
  }
```

**Step 4: Implement `generateComments()`**

```java
  private void generateComments() {
    System.out.println("  Generating " + NUM_COMMENTS + " Comments...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < NUM_COMMENTS; i++) {
      final MutableVertex comment = database.newVertex("Comment");
      comment.set("id", (long) i);
      comment.set("creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 2));
      comment.set("locationIP", rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256) + "." + rnd.nextInt(256));
      comment.set("browserUsed", BROWSERS[rnd.nextInt(BROWSERS.length)]);
      comment.set("content", "Comment " + i);
      comment.set("length", 5 + rnd.nextInt(500));
      comment.save();

      // HAS_CREATOR
      final int creatorId = rnd.nextInt(NUM_PERSONS);
      final IndexCursor creatorCursor = database.lookupByKey("Person", "id", (long) creatorId);
      if (creatorCursor.hasNext())
        comment.newEdge("HAS_CREATOR", creatorCursor.next().asVertex(), false);

      // REPLY_OF: 70% reply to a Post, 30% reply to an earlier Comment
      if (rnd.nextInt(100) < 70 || i == 0) {
        final int postId = rnd.nextInt(NUM_POSTS);
        final IndexCursor cursor = database.lookupByKey("Post", "id", (long) postId);
        if (cursor.hasNext())
          comment.newEdge("REPLY_OF", cursor.next().asVertex(), false);
      } else {
        final int replyToId = rnd.nextInt(i); // earlier comment
        final IndexCursor cursor = database.lookupByKey("Comment", "id", (long) replyToId);
        if (cursor.hasNext())
          comment.newEdge("REPLY_OF", cursor.next().asVertex(), false);
      }

      // HAS_TAG
      final int numTags = rnd.nextInt(AVG_TAGS_PER_POST * 2);
      for (int j = 0; j < numTags; j++) {
        final int tagId = rnd.nextInt(NUM_TAGS);
        final IndexCursor cursor = database.lookupByKey("Tag", "id", (long) tagId);
        if (cursor.hasNext())
          comment.newEdge("HAS_TAG", cursor.next().asVertex(), false);
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }

      if (i % 100_000 == 0 && i > 0)
        System.out.println("    " + i + " / " + NUM_COMMENTS + " comments...");
    }
    database.commit();
  }
```

**Step 5: Implement `generateLikes()`**

```java
  private void generateLikes() {
    final int totalLikes = NUM_PERSONS * AVG_LIKES_PER_PERSON;
    System.out.println("  Generating " + totalLikes + " LIKES edges...");
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long now = System.currentTimeMillis();

    database.begin();
    for (int i = 0; i < totalLikes; i++) {
      final int personId = rnd.nextInt(NUM_PERSONS);
      final IndexCursor personCursor = database.lookupByKey("Person", "id", (long) personId);
      if (!personCursor.hasNext())
        continue;
      final Vertex person = personCursor.next().asVertex();

      // 60% like Posts, 40% like Comments
      if (rnd.nextInt(100) < 60) {
        final int postId = rnd.nextInt(NUM_POSTS);
        final IndexCursor cursor = database.lookupByKey("Post", "id", (long) postId);
        if (cursor.hasNext())
          person.asVertex(true).newEdge("LIKES", cursor.next().asVertex(), false,
              "creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 2));
      } else {
        final int commentId = rnd.nextInt(NUM_COMMENTS);
        final IndexCursor cursor = database.lookupByKey("Comment", "id", (long) commentId);
        if (cursor.hasNext())
          person.asVertex(true).newEdge("LIKES", cursor.next().asVertex(), false,
              "creationDate", now - rnd.nextLong(365L * 24 * 3600 * 1000 * 2));
      }

      if (i % COMMIT_EVERY == 0 && i > 0) {
        database.commit();
        database.begin();
      }

      if (i % 100_000 == 0 && i > 0)
        System.out.println("    " + i + " / " + totalLikes + " LIKES edges...");
    }
    database.commit();
  }
```

**Step 6: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 7: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement KNOWS, Forum, Post, Comment, LIKES generation"
```

---

### Task 6: Implement benchmark helper and Phase 2 (Lookups)

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Implement `prepareSampleIds()` and `benchmark()` helper**

```java
  private void prepareSampleIds() {
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final int sampleSize = 100;

    samplePersonIds = new long[sampleSize];
    samplePostIds = new long[sampleSize];
    sampleForumIds = new long[sampleSize];
    sampleCityNames = new String[sampleSize];
    sampleFirstNames = new String[sampleSize];

    for (int i = 0; i < sampleSize; i++) {
      samplePersonIds[i] = rnd.nextLong(NUM_PERSONS);
      samplePostIds[i] = rnd.nextLong(NUM_POSTS);
      sampleForumIds[i] = rnd.nextLong(NUM_FORUMS);
      sampleCityNames[i] = "City_" + (CONTINENTS.length + Math.min(COUNTRIES.length, NUM_PLACES / 3) + rnd.nextInt(Math.max(1, NUM_PLACES - CONTINENTS.length - Math.min(COUNTRIES.length, NUM_PLACES / 3))));
      sampleFirstNames[i] = FIRST_NAMES[rnd.nextInt(FIRST_NAMES.length)];
    }
  }

  // Benchmark results storage
  private final List<String[]> reportRows = new ArrayList<>();

  private void benchmark(final String phase, final String name, final int iterations,
      final String sql, final String cypher) {

    // SQL benchmark
    if (sql != null) {
      final String timerName = "benchmark." + phase + "." + name.replace(" ", "_") + ".sql";

      // Warmup
      for (int i = 0; i < WARMUP_ITERATIONS; i++)
        runQuery("sql", sql, i);

      // Measure
      final long[] times = new long[iterations];
      long totalResults = 0;
      for (int i = 0; i < iterations; i++) {
        final long start = System.nanoTime();
        totalResults += runQuery("sql", sql, i);
        times[i] = System.nanoTime() - start;
        registry.timer(timerName).record(times[i], TimeUnit.NANOSECONDS);
      }
      registry.counter(timerName + ".results").increment(totalResults);

      Arrays.sort(times);
      reportRows.add(formatRow(phase, name, "SQL", iterations, times));
    }

    // Cypher benchmark
    if (cypher != null) {
      final String timerName = "benchmark." + phase + "." + name.replace(" ", "_") + ".cypher";

      // Warmup
      for (int i = 0; i < WARMUP_ITERATIONS; i++)
        runQuery("opencypher", cypher, i);

      // Measure
      final long[] times = new long[iterations];
      long totalResults = 0;
      for (int i = 0; i < iterations; i++) {
        final long start = System.nanoTime();
        totalResults += runQuery("opencypher", cypher, i);
        times[i] = System.nanoTime() - start;
        registry.timer(timerName).record(times[i], TimeUnit.NANOSECONDS);
      }
      registry.counter(timerName + ".results").increment(totalResults);

      Arrays.sort(times);
      reportRows.add(formatRow(phase, name, "Cypher", iterations, times));
    }
  }

  private int runQuery(final String language, final String query, final int iteration) {
    final int idx = iteration % samplePersonIds.length;
    final Map<String, Object> params = Map.of(
        "id", samplePersonIds[idx],
        "postId", samplePostIds[idx],
        "forumId", sampleForumIds[idx],
        "city", sampleCityNames[idx],
        "name", sampleFirstNames[idx],
        "id1", samplePersonIds[idx],
        "id2", samplePersonIds[(idx + 1) % samplePersonIds.length]
    );

    int count = 0;
    try (final ResultSet rs = database.query(language, query, params)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private String[] formatRow(final String phase, final String name, final String lang,
      final int ops, final long[] sortedNanos) {
    final double avgMs = Arrays.stream(sortedNanos).average().orElse(0) / 1_000_000.0;
    final double p50Ms = sortedNanos[sortedNanos.length / 2] / 1_000_000.0;
    final double p95Ms = sortedNanos[(int) (sortedNanos.length * 0.95)] / 1_000_000.0;
    final double p99Ms = sortedNanos[(int) (sortedNanos.length * 0.99)] / 1_000_000.0;
    return new String[] { phase, name, lang, String.valueOf(ops),
        String.format("%.3f", avgMs), String.format("%.3f", p50Ms),
        String.format("%.3f", p95Ms), String.format("%.3f", p99Ms) };
  }
```

**Step 2: Implement `phase2_lookups()`**

```java
  @Test
  @Order(1)
  void phase2_lookups() {
    System.out.println("\n=== Phase 2: Simple Lookups ===");

    // 2a: Person by ID (indexed)
    benchmark("2a", "Person by ID", LOOKUP_ITERATIONS,
        "SELECT FROM Person WHERE id = :id",
        "MATCH (p:Person {id: $id}) RETURN p");

    // 2b: Post by ID (indexed)
    benchmark("2b", "Post by ID", LOOKUP_ITERATIONS,
        "SELECT FROM Post WHERE id = :postId",
        "MATCH (p:Post {id: $postId}) RETURN p");

    // 2c: Person by firstName (non-indexed scan)
    benchmark("2c", "Person by firstName", 100,
        "SELECT FROM Person WHERE firstName = :name",
        "MATCH (p:Person) WHERE p.firstName = $name RETURN p");

    // 2d: Count vertices per type
    benchmark("2d", "Count Persons", 10,
        "SELECT COUNT(*) as cnt FROM Person",
        "MATCH (p:Person) RETURN COUNT(p) as cnt");
  }
```

**Step 3: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement benchmark helper and Phase 2 lookups (SQL + Cypher)"
```

---

### Task 7: Implement Phase 3 (Simple Traversals)

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Implement `phase3_simpleTraversals()`**

```java
  @Test
  @Order(2)
  void phase3_simpleTraversals() {
    System.out.println("\n=== Phase 3: Simple Traversals ===");

    // 3a: Direct friends (1-hop KNOWS)
    benchmark("3a", "Direct friends", SIMPLE_TRAVERSAL_ITERATIONS,
        "SELECT expand(both('KNOWS')) FROM Person WHERE id = :id",
        "MATCH (p:Person {id: $id})-[:KNOWS]-(friend) RETURN friend");

    // 3b: Posts created by a Person
    benchmark("3b", "Posts by Person", SIMPLE_TRAVERSAL_ITERATIONS,
        "SELECT expand(in('HAS_CREATOR')) FROM Person WHERE id = :id",
        "MATCH (p:Person {id: $id})<-[:HAS_CREATOR]-(post:Post) RETURN post");

    // 3c: Tags of a Post
    benchmark("3c", "Tags of Post", SIMPLE_TRAVERSAL_ITERATIONS,
        "SELECT expand(out('HAS_TAG')) FROM Post WHERE id = :postId",
        "MATCH (p:Post {id: $postId})-[:HAS_TAG]->(t:Tag) RETURN t");

    // 3d: Members of a Forum
    benchmark("3d", "Forum members", SIMPLE_TRAVERSAL_ITERATIONS,
        "SELECT expand(out('HAS_MEMBER')) FROM Forum WHERE id = :forumId",
        "MATCH (f:Forum {id: $forumId})-[:HAS_MEMBER]->(p:Person) RETURN p");
  }
```

**Step 2: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement Phase 3 simple traversal benchmarks"
```

---

### Task 8: Implement Phase 4 (Complex Traversals)

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Implement `phase4_complexTraversals()`**

```java
  @Test
  @Order(3)
  void phase4_complexTraversals() {
    System.out.println("\n=== Phase 4: Complex Traversals ===");

    // 4a: Friends-of-friends (2-hop KNOWS, exclude direct friends)
    benchmark("4a", "Friends of friends", COMPLEX_TRAVERSAL_ITERATIONS,
        "SELECT expand(both('KNOWS').both('KNOWS')) FROM Person WHERE id = :id",
        "MATCH (p:Person {id: $id})-[:KNOWS]-()-[:KNOWS]-(fof) " +
            "WHERE fof <> p AND NOT (p)-[:KNOWS]-(fof) RETURN DISTINCT fof");

    // 4b: Posts by friends in a city
    benchmark("4b", "Posts by friends in city", COMPLEX_TRAVERSAL_ITERATIONS,
        null, // SQL MATCH pattern too complex, Cypher only for this one
        "MATCH (p:Person {id: $id})-[:KNOWS]-(friend)-[:IS_LOCATED_IN]->(c:Place {name: $city}), " +
            "(friend)<-[:HAS_CREATOR]-(post:Post) RETURN post, friend.firstName");

    // 4c: Common tags between two Persons' posts
    benchmark("4c", "Common tags", COMPLEX_TRAVERSAL_ITERATIONS,
        null, // Cypher only -- complex pattern match
        "MATCH (a:Person {id: $id1})<-[:HAS_CREATOR]-(p1)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(p2)-[:HAS_CREATOR]->(b:Person {id: $id2}) " +
            "RETURN t.name, count(*) AS freq ORDER BY freq DESC");

    // 4d: Shortest path via KNOWS
    benchmark("4d", "Shortest path", SHORTEST_PATH_ITERATIONS,
        null, // Cypher only -- shortestPath
        "MATCH path = shortestPath((a:Person {id: $id1})-[:KNOWS*]-(b:Person {id: $id2})) " +
            "RETURN length(path)");

    // 4e: Forum recommendation (forums where most friends are members)
    benchmark("4e", "Forum recommendation", COMPLEX_TRAVERSAL_ITERATIONS,
        null, // Cypher only -- aggregation pattern
        "MATCH (p:Person {id: $id})-[:KNOWS]-(friend), " +
            "(forum:Forum)-[:HAS_MEMBER]->(friend) " +
            "RETURN forum.title, count(friend) AS friendCount " +
            "ORDER BY friendCount DESC LIMIT 10");
  }
```

**Step 2: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement Phase 4 complex traversal benchmarks"
```

---

### Task 9: Implement dataset stats and report output

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java`

**Step 1: Implement `printDatasetStats()` and `printReport()`**

```java
  private void printDatasetStats() {
    System.out.println();
    System.out.println("=== ArcadeDB Graph Benchmark (LDBC-inspired) ===");
    System.out.println("Database: " + DB_PATH);

    final long totalVertices = database.countType("Person", false)
        + database.countType("Post", false)
        + database.countType("Comment", false)
        + database.countType("Forum", false)
        + database.countType("Tag", false)
        + database.countType("TagClass", false)
        + database.countType("Place", false)
        + database.countType("Organisation", false);

    System.out.println("Total vertices: " + totalVertices);
    System.out.println("  Person: " + database.countType("Person", false));
    System.out.println("  Post: " + database.countType("Post", false));
    System.out.println("  Comment: " + database.countType("Comment", false));
    System.out.println("  Forum: " + database.countType("Forum", false));
    System.out.println("  Tag: " + database.countType("Tag", false));
    System.out.println("  TagClass: " + database.countType("TagClass", false));
    System.out.println("  Place: " + database.countType("Place", false));
    System.out.println("  Organisation: " + database.countType("Organisation", false));
    System.out.println("Freshly created: " + freshlyCreated);

    if (freshlyCreated) {
      final Timer creationTimer = registry.find("benchmark.creation").timer();
      if (creationTimer != null)
        System.out.println("Creation time: " + String.format("%.1f", creationTimer.totalTime(TimeUnit.SECONDS)) + " s");
    }
    System.out.println();
  }

  private void printReport() {
    System.out.println();
    System.out.println("╔══════════════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                        ArcadeDB Graph Benchmark Results (LDBC-inspired)                        ║");
    System.out.println("╠═══════╤════════════════════════════╤════════╤══════╤═════════╤═════════╤═════════╤═════════════╣");
    System.out.println("║ Phase │ Query                      │ Lang   │  Ops │  Avg ms │  p50 ms │  p95 ms │     p99 ms  ║");
    System.out.println("╟───────┼────────────────────────────┼────────┼──────┼─────────┼─────────┼─────────┼─────────────╢");

    for (final String[] row : reportRows) {
      System.out.printf("║ %-5s │ %-26s │ %-6s │ %4s │ %7s │ %7s │ %7s │ %11s ║%n",
          row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]);
    }

    System.out.println("╚═══════╧════════════════════════════╧════════╧══════╧═════════╧═════════╧═════════╧═════════════╝");
    System.out.println();
  }
```

**Step 2: Compile**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn compile test-compile -pl engine -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: implement benchmark reporting with Micrometer metrics"
```

---

### Task 10: First full test run and fixes

**Files:**
- Modify: `engine/src/test/java/performance/GraphBenchmark.java` (any fixes needed)

**Step 1: Run the full benchmark with a small scale**

Before running the full benchmark, temporarily reduce the scale constants to verify everything works end-to-end quickly. Change constants to:

```java
  private static final int NUM_PERSONS       = 1_000;
  private static final int NUM_POSTS         = 5_000;
  private static final int NUM_COMMENTS      = 10_000;
  private static final int NUM_FORUMS        = 200;
  private static final int NUM_TAGS          = 100;
  private static final int NUM_TAG_CLASSES   = 20;
  private static final int NUM_PLACES        = 100;
  private static final int NUM_ORGANISATIONS = 200;
```

Also reduce iterations:

```java
  private static final int LOOKUP_ITERATIONS = 50;
  private static final int SIMPLE_TRAVERSAL_ITERATIONS = 20;
  private static final int COMPLEX_TRAVERSAL_ITERATIONS = 10;
  private static final int SHORTEST_PATH_ITERATIONS = 5;
```

**Step 2: Run the benchmark**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn test -pl engine -Dtest=GraphBenchmark -Dgroups=benchmark -DfailIfNoTests=false`
Expected: All 3 test methods pass, report table is printed to stdout.

**Step 3: Fix any issues**

If any queries fail (SQL syntax, Cypher syntax, missing data), fix them and re-run. Common issues:
- SQL parameter syntax (`:id` vs `?`) -- check ArcadeDB SQL docs. The existing `QueryLanguageBenchmark` uses `?` positional parameters for SQL, but we're using named parameters via `Map`. Verify ArcadeDB supports `:paramName` syntax.
- Cypher parameter syntax (`$id`) with `Map.of("id", value)` -- this is confirmed working in `QueryLanguageBenchmark.java:682`.
- `shortestPath` might fail if no path exists between the two random persons. Add a try-catch or verify connectivity.

**Step 4: Restore full scale constants**

Change constants back to the medium scale values from the design.

**Step 5: Commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "fix: resolve benchmark query issues found during test run"
```

---

### Task 11: Full-scale verification run

**Step 1: Delete the small-scale test database**

Run: `rm -rf /Users/frank/projects/arcade/worktrees/ldbc-bechmark/engine/target/databases/graph-benchmark`

**Step 2: Run the full benchmark**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn test -pl engine -Dtest=GraphBenchmark -Dgroups=benchmark -DfailIfNoTests=false`
Expected: Generation takes several minutes, then all benchmark phases run and print the report table.

**Step 3: Run a second time to verify database reuse**

Run: `cd /Users/frank/projects/arcade/worktrees/ldbc-bechmark && mvn test -pl engine -Dtest=GraphBenchmark -Dgroups=benchmark -DfailIfNoTests=false`
Expected: "Database opened from target/databases/graph-benchmark (reusing existing data)" message. No generation phase. Benchmark queries run immediately.

**Step 4: Final commit**

```bash
git add engine/src/test/java/performance/GraphBenchmark.java
git commit -m "feat: GraphBenchmark complete -- LDBC-inspired benchmark with full schema and metrics"
```
