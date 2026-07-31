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
package com.arcadedb.partitioning;

import com.arcadedb.TestHelper;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static com.arcadedb.partitioning.WarningCapture.captureSevere;
import static com.arcadedb.partitioning.WarningCapture.captureWarnings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5637, the two loose ends of the partitioned-strategy series (#5589, #5595, #5603).
 * <p>
 * Both are about what happens to a partitioned type <i>after</i> it has been configured. #5603 made the moment of
 * configuration honest - an unsuitable partition is refused, a partly useful one is reported - but said nothing about
 * the two ways a type can leave that moment behind: a restart, which used to drop the strategy entirely, and a later
 * {@code CREATE INDEX}, which can undo the very suitability that was just checked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedStrategyLifecycleTest extends TestHelper {

  private static final int BUCKETS = 3;

  private void createPartitionedType(final String typeName) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".k STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".code STRING");
      database.command("sql", "CREATE INDEX ON " + typeName + "(k) UNIQUE");
    });
    database.transaction(
        () -> database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('k')`"));
  }

  private JSONObject persistedType(final String typeName) throws IOException {
    final JSONObject schemaJson = new JSONObject(
        FileUtils.readFileAsString(((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile()));
    return schemaJson.getJSONObject("types").getJSONObject(typeName);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 1. The strategy has to reach schema.json on its own.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * {@code ALTER TYPE ... BucketSelectionStrategy} set the strategy in memory and nothing wrote it out, so a type
   * partitioned by that DDL alone came back round-robin after a restart: new records placed round-robin among rows
   * that were placed by the partition hash, every partition-aware lookup silently fanning out, and no warning
   * anywhere. Whether it survived depended on whether some later, unrelated schema mutation happened to flush the
   * configuration first.
   */
  @Test
  void theStrategyReachesSchemaJsonWithNoLaterSchemaMutation() throws IOException {
    createPartitionedType("Persisted");

    assertThat(persistedType("Persisted").has("bucketSelectionStrategy"))
        .as("the DDL must persist the strategy itself, not wait for an unrelated mutation to flush it")
        .isTrue();
  }

  @Test
  void theStrategySurvivesAReopen() {
    createPartitionedType("Reopened");
    reopenDatabase();

    assertThat(database.getSchema().getType("Reopened").getBucketSelectionStrategy().getName())
        .as("a type partitioned by DDL alone must still be partitioned after a restart")
        .isEqualTo("partitioned");
  }

  /** Setting the same strategy back to the default has to be persisted too, or the reopen resurrects the old one. */
  @Test
  void revertingToRoundRobinIsPersistedAsWell() throws IOException {
    createPartitionedType("Reverted");
    database.transaction(() -> database.command("sql", "ALTER TYPE Reverted BucketSelectionStrategy `round-robin`"));

    assertThat(persistedType("Reverted").has("bucketSelectionStrategy"))
        .as("round-robin is the default and is deliberately not written out")
        .isFalse();

    reopenDatabase();
    assertThat(database.getSchema().getType("Reverted").getBucketSelectionStrategy().getName())
        .isEqualTo("round-robin");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 2. An index created after the strategy can undo the suitability that was checked when it was assigned.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * #5603 refuses a case-insensitive partition index at assignment time, but the same state is reachable by
   * reordering the DDL: attach the strategy first, then recollate the index underneath it. Correctness holds - the
   * strategy declines to prune, so lookups fan out and UNIQUE stays global - but nothing said the partitioning had
   * stopped doing anything.
   */
  @Test
  void recollatingThePartitionIndexToCaseInsensitiveIsReported() {
    createPartitionedType("Recollated");

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "DROP INDEX `Recollated[k]`");
      database.command("sql", "CREATE INDEX ON Recollated (k COLLATE CI) UNIQUE");
    }));

    assertThat(warnings)
        .as("an index change that makes the partition unprunable must say so when it happens")
        .anyMatch(m -> m.contains("Recollated") && m.contains("COLLATE CI"));
  }

  /**
   * The same asymmetry on the advisory half: a second index on non-partition properties draws the fan-out advisory
   * when the strategy is assigned after it, and used to draw nothing at all when it arrived afterwards.
   */
  @Test
  void anIndexOnNonPartitionPropertiesArrivingLaterIsReported() {
    createPartitionedType("LateIndex");

    final List<String> warnings = captureWarnings(
        () -> database.transaction(() -> database.command("sql", "CREATE INDEX ON LateIndex(code) UNIQUE")));

    assertThat(warnings).anyMatch(m -> m.contains("LateIndex") && m.contains("code"));
  }

  /**
   * The control that keeps the two tests above honest: an index change that leaves the partition exactly as suitable
   * as it was must stay quiet, or the report is just noise on every {@code CREATE INDEX}.
   */
  @Test
  void anIndexChangeThatChangesNothingStaysQuiet() {
    createPartitionedType("Quiet");

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "DROP INDEX `Quiet[k]`");
      database.command("sql", "CREATE INDEX ON Quiet(k) UNIQUE");
    }));

    assertThat(warnings).as("re-creating the partition index unchanged is not worth a line")
        .noneMatch(m -> m.contains("Quiet"));
  }

  /** And a type that is not partitioned at all must never be diagnosed. */
  @Test
  void aRoundRobinTypeIsNeverDiagnosedOnIndexCreation() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Plain").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY Plain.k STRING");
      database.command("sql", "CREATE PROPERTY Plain.code STRING");
    });

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Plain(k) UNIQUE");
      database.command("sql", "CREATE INDEX ON Plain(code COLLATE CI) UNIQUE");
    }));

    assertThat(warnings).noneMatch(m -> m.contains("Plain"));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 3. Persisting the strategy makes the DROP INDEX state reachable across a restart.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The unique index the strategy is assigned against can be dropped afterwards - nothing re-checks it, and
   * {@link com.arcadedb.partitioning.PartitionedBoxedKeyLookupTest} depends on being able to drop and recreate it.
   * Now that the strategy is persisted, that state reaches the next open, where the schema loader binds the strategy
   * back onto the type. If binding still demands the index, the database simply does not open.
   */
  @Test
  void aPartitionedTypeWhoseIndexWasDroppedStillOpens() {
    createPartitionedType("Orphaned");
    database.transaction(() -> {
      database.command("sql", "DROP INDEX `Orphaned[k]`");
      database.newDocument("Orphaned").set("k", "acme").save();
    });

    final List<String> warnings = captureWarnings(this::reopenDatabase);

    assertThat(warnings).as("and says what is missing rather than opening silently degraded")
        .anyMatch(m -> m.contains("Orphaned") && m.contains("unique automatic index"));
    assertThat(database.getSchema().getType("Orphaned").getBucketSelectionStrategy().getName())
        .as("the strategy is kept: it still places records, and swapping it for round-robin would scatter every "
            + "record written from here on among rows the partition hash placed")
        .isEqualTo("partitioned");
    assertThat(database.query("sql", "SELECT FROM Orphaned").stream().count()).isEqualTo(1);
  }

  /**
   * The same fault isolation, from the other direction: a strategy whose implementation class cannot be resolved at
   * all. Nothing about it is recoverable, but it must not take the rest of the schema down - the strategy block runs
   * near the end of the loader, so an exception escaping it drops everything the loader has not reached yet.
   * <p>
   * The extension is what pins that, rather than a second type: types are registered before the strategy block, so
   * one would survive either way, while extensions are read strictly after it.
   */
  @Test
  void anUnresolvableStrategyDoesNotCostTheRestOfTheSchema() throws IOException {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Broken").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY Broken.k STRING");
      database.getSchema().setExtension("lifecycle-probe", new JSONObject().put("loaded", true));
    });

    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final File schemaFile = schema.getConfigurationFile();
    database.close();

    final JSONObject schemaJson = new JSONObject(FileUtils.readFileAsString(schemaFile));
    schemaJson.getJSONObject("types").getJSONObject("Broken")
        .put("bucketSelectionStrategy", new JSONObject().put("name", "com.acme.NoSuchStrategy"));
    try (final FileWriter writer = new FileWriter(schemaFile)) {
      writer.write(schemaJson.toString());
    }

    final List<String> severe = captureSevere(() -> database = factory.open());

    assertThat(database.getSchema().getType("Broken").getBucketSelectionStrategy().getName())
        .as("an unusable strategy falls back to the default").isEqualTo("round-robin");
    assertThat(database.getSchema().getExtension("lifecycle-probe"))
        .as("and everything the loader reads after the strategies is still there").isNotNull();

    // The catch that provides the isolation has to be broad, so the level is what separates "this database is
    // configured that way" from "the bind path is broken". A strategy declining to be restored is the former, and
    // reporting it at SEVERE would leave a real fault here indistinguishable from it.
    assertThat(severe).as("a refused strategy is a property of the database, not an engine fault")
        .noneMatch(m -> m.contains("Broken"));
    assertThat(severe).as("and the schema is emphatically not reset")
        .noneMatch(m -> m.contains("The schema will be reset"));
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 4. Inheritance, which reaches the assignment path without any ALTER TYPE.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * {@code addSuperType} copies the super type's strategy onto the subtype, which now routes through the same
   * refusal as an explicit {@code ALTER TYPE} rather than through the check that used to live in
   * {@code setType}. The happy path has to keep working: the subtype gets the parent's index first, so the
   * inherited partition is suitable, prunes, and is persisted on the subtype in its own right.
   * <p>
   * The subtype declares the same bucket count as its parent deliberately. A partitioned type whose subtype has a
   * DIFFERENT one crashes on the first indexed insert, because {@code TypeIndex.getIndexesByKeys} applies the bucket
   * index it derived from the parent's modulus to {@code s.getBuckets(false)} of each subtype. That is unrelated to
   * this issue - the line predates the whole partitioned-strategy series and nothing here touches it - so it is
   * reported as issue #5645 rather than pinned by a test that would fail for a different reason than it claims.
   */
  @Test
  void aSubtypeInheritsAPartitionedStrategyAndItStillPrunes() throws IOException {
    createPartitionedType("Base");
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base BUCKETS " + BUCKETS));

    final LocalDocumentType derived = (LocalDocumentType) database.getSchema().getType("Derived");
    assertThat(derived.getBucketSelectionStrategy().getName()).isEqualTo("partitioned");
    assertThat(persistedType("Derived").has("bucketSelectionStrategy"))
        .as("an inherited strategy is the subtype's own state and has to be persisted as such").isTrue();

    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) derived.getBucketSelectionStrategy();
    assertThat(strategy.getBucketIdByKeys(List.of("k"), new Object[] { "acme" }, false))
        .as("the inherited partition must be suitable, not merely attached").isNotEqualTo(-1);

    database.transaction(() -> database.newDocument("Derived").set("k", "acme").save());
    assertThat(database.query("sql", "SELECT FROM Derived WHERE k = 'acme'").stream().count()).isEqualTo(1);
  }

  /**
   * The other order, which is how a subtype can reach the inheritance copy without the parent's index having been
   * created for it: the subtype exists before the parent is partitioned, so nothing re-runs on it until a
   * {@code DROP TYPE} in the middle of the hierarchy re-attaches it to the grandparent and copies the strategy
   * down. This is the one caller that passes {@code createIndexes = false}.
   */
  @Test
  void droppingAPartitionedTypeInTheMiddleOfAHierarchyDoesNotBreakTheSubtype() {
    createPartitionedType("Grand");
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Middle EXTENDS Grand");
      database.command("sql", "CREATE DOCUMENT TYPE Leaf EXTENDS Middle");
      database.command("sql", "DROP TYPE Middle");
    });

    assertThat(database.getSchema().existsType("Leaf")).isTrue();
    assertThat(database.getSchema().getType("Leaf").getSuperTypes())
        .as("the leaf is re-attached to the grandparent")
        .extracting(DocumentType::getName).containsExactly("Grand");
    assertThat(database.getSchema().getType("Leaf").getBucketSelectionStrategy().getName())
        .isEqualTo("partitioned");
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/PartitionedStrategyLifecycleTest";
  }

}
