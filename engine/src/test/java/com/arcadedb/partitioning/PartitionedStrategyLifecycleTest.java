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

import static com.arcadedb.log.WarningCapture.captureSevere;
import static com.arcadedb.log.WarningCapture.captureWarnings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

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
  void theStrategyReachesSchemaJsonWithNoLaterSchemaMutation() throws Exception {
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
  void revertingToRoundRobinIsPersistedAsWell() throws Exception {
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

  // ---------------------------------------------------------------------------------------------------------------
  // 3b. Issue #5646: a DROP INDEX that leaves a partitioned type without its partition index is reported when it
  //     happens, not only on the next open.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The single-statement / auto-commit case: there is no enclosing transaction for the diagnosis to defer to, so it
   * has to run immediately, exactly like the pre-#5646 {@code CREATE INDEX} side already does.
   */
  @Test
  void dropIndexOnAnOrphanedPartitionIsReportedImmediatelyOutsideATransaction() {
    createPartitionedType("Immediate");

    final List<String> warnings = captureWarnings(() -> database.command("sql", "DROP INDEX `Immediate[k]`"));

    assertThat(warnings)
        .as("dropping the partition's only unique automatic index must say so right away, not wait for a restart")
        .anyMatch(m -> m.contains("Immediate") && m.contains("unique automatic index"));
  }

  /**
   * Inside an explicit transaction the diagnosis must wait for the commit: reporting mid-transaction would describe
   * a state the transaction might still walk back from (recollating is exactly a DROP followed by a CREATE).
   */
  @Test
  void dropIndexOnAnOrphanedPartitionInATransactionIsReportedOnCommit() {
    createPartitionedType("Deferred");

    // captureWarnings nests: the inner capture sees only what is logged while the DROP INDEX statement itself runs,
    // the outer capture (which is still installed as the inner one's delegate) also sees everything logged
    // afterwards, including at commit.
    final List<String>[] midTransactionWarnings = new List[1];
    final List<String> afterCommitWarnings = captureWarnings(() -> database.transaction(
        () -> midTransactionWarnings[0] = captureWarnings(() -> database.command("sql", "DROP INDEX `Deferred[k]`"))));

    assertThat(midTransactionWarnings[0])
        .as("the diagnosis must not fire before the transaction that dropped the index commits")
        .noneMatch(m -> m.contains("Deferred"));
    assertThat(afterCommitWarnings)
        .as("the blocker is reported once the transaction that dropped the index commits")
        .anyMatch(m -> m.contains("Deferred") && m.contains("unique automatic index"));
  }

  /**
   * The case the synchronous hook could not handle: DROP followed by a re-CREATE of the identical index in one
   * transaction must settle quietly, the same way {@link #anIndexChangeThatChangesNothingStaysQuiet} already pins
   * for the CREATE side. Driving it from the DROP side is what #5646 is about - this used to be silent only because
   * DROP INDEX never reported anything at all, not because the settled state was correctly recognised as unchanged.
   */
  @Test
  void dropThenRecreateInOneTransactionReportsOnlyTheSettledState() {
    createPartitionedType("Recreated");

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "DROP INDEX `Recreated[k]`");
      database.command("sql", "CREATE INDEX ON Recreated(k) UNIQUE");
    }));

    assertThat(warnings).as("re-creating the partition index unchanged is not worth a line, even driven from DROP")
        .noneMatch(m -> m.contains("Recreated"));
  }

  /**
   * {@code DROP TYPE} drops every one of the type's indexes (via the same {@code dropIndexInternal} the two tests
   * above exercise) before removing the type itself from the schema. Without suppressing the report for that
   * cascade, dropping a partitioned type would trigger the exact "no unique automatic index" blocker this issue
   * fixes for {@code DROP INDEX} - except about a type that no longer exists by the time anyone reads it. Found by
   * review on PR #5946: reusing the type-survives assertion style of the two tests above, driven from
   * {@code DROP TYPE} outside a transaction, which is the immediate-report path and the one the fix's first
   * attempt (an existence check made only at commit time) still missed.
   */
  @Test
  void dropTypeOfAPartitionedTypeReportsNothingAboutTheTypeItJustRemoved() {
    createPartitionedType("Vanishing");

    final List<String> warnings = captureWarnings(() -> database.command("sql", "DROP TYPE Vanishing"));

    assertThat(warnings)
        .as("the type is gone by the time this report would be read - it must not be diagnosed at all")
        .noneMatch(m -> m.contains("Vanishing"));
    assertThat(database.getSchema().existsType("Vanishing")).isFalse();
  }

  /**
   * The cross-statement variant: a {@code DROP INDEX} inside a transaction schedules the after-commit callback
   * (the deferred case above), and a {@code DROP TYPE} of the same type later in the same transaction removes the
   * type before that callback fires. This is what the existence re-check inside the deferred diagnosis itself
   * (rather than only at scheduling time) is for.
   */
  @Test
  void dropTypeAfterDropIndexInTheSameTransactionReportsNothing() {
    createPartitionedType("VanishingDeferred");

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "DROP INDEX `VanishingDeferred[k]`");
      database.command("sql", "DROP TYPE VanishingDeferred");
    }));

    assertThat(warnings)
        .as("the DROP INDEX callback must not fire once the same transaction went on to drop the type entirely")
        .noneMatch(m -> m.contains("VanishingDeferred"));
  }

  /**
   * Found on review of this fix (PR #5946): {@code reportPartitionSuitabilityAfterSchemaChange()} schedules a
   * callback bound to {@code this} - the {@code LocalDocumentType} instance live when the DROP INDEX ran.
   * {@code CREATE TYPE} always builds a brand-new instance ({@code LocalSchema}, {@code case "d" ->}), so a
   * DROP-then-CREATE of the same name in one transaction leaves the scheduled callback pointing at a stale,
   * no-longer-registered object by the time it fires. A bare {@code schema.existsType(name)} re-check would have
   * come back {@code true} anyway - off the *new* type sharing the name - and gone on to read the *old* instance's
   * fields. Re-resolving the live type by name before reading anything is what closes that: the new type here is
   * plain (round-robin, the default), so if the diagnosis were still reading the old, partitioned instance's
   * fields, this would report; reading the live instance's, it must not.
   */
  @Test
  void dropTypeThenRecreateWithTheSameNameInOneTransactionDiagnosesTheLiveInstance() {
    createPartitionedType("VanishingRecreated");

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> {
      database.command("sql", "DROP INDEX `VanishingRecreated[k]`");
      database.command("sql", "DROP TYPE VanishingRecreated");
      database.command("sql", "CREATE DOCUMENT TYPE VanishingRecreated");
    }));

    assertThat(warnings)
        .as("the callback must diagnose the new, plain (round-robin) type actually registered under the name, "
            + "not the dropped partitioned instance it was scheduled against")
        .noneMatch(m -> m.contains("VanishingRecreated"));
    assertThat(database.getSchema().getType("VanishingRecreated").getBucketSelectionStrategy().getName())
        .as("sanity check that the recreated type really is the plain default, not still partitioned")
        .isEqualTo("round-robin");
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
  void anUnresolvableStrategyDoesNotCostTheRestOfTheSchema() throws Exception {
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
  void aSubtypeInheritsAPartitionedStrategyAndItStillPrunes() throws Exception {
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
   * Issue #5645. {@code bucketIndex} is only meaningful modulo the bucket count it was derived against - the
   * parent's. A subtype declares its OWN bucket count, and when that count is SMALLER than the parent's,
   * {@code TypeIndex.getIndexesByKeys} used to apply the parent-derived index straight into the subtype's own
   * (shorter) bucket list and throw {@code IndexOutOfBoundsException} on the very first indexed insert - exactly
   * the reproduction from the issue: a partitioned {@code Base BUCKETS 3} and a {@code Derived} that inherits the
   * strategy but gets the schema's default single bucket.
   */
  @Test
  void aSubtypeWithFewerBucketsThanItsParentDoesNotCrashOnIndexedInsert() {
    createPartitionedType("Base");
    // NO "BUCKETS" CLAUSE: DERIVED GETS THE DEFAULT (1), STRICTLY FEWER THAN THE PARENT'S 3
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base"));

    assertThat(database.getSchema().getType("Derived").getBuckets(false)).hasSize(1);

    assertThatNoException().isThrownBy(
        () -> database.transaction(() -> database.newDocument("Derived").set("k", "acme").save()));

    assertThat(database.query("sql", "SELECT FROM Derived WHERE k = 'acme'").stream().count())
        .as("the record must be both insertable and findable through the index despite the bucket-count mismatch")
        .isEqualTo(1);
  }

  /**
   * Issue #5645, the other direction. When the subtype's bucket count is LARGER than the parent's, the same reused
   * {@code bucketIndex} never goes out of range - it silently prunes to a bucket the record was never placed in
   * instead, which is the #5589 failure mode again: no exception, the record just never comes back through the
   * index. The keys below are chosen so {@code hash(k) % 3 != hash(k) % 5}, which is exactly the condition under
   * which the parent's 3-bucket-derived index and the subtype's own 5-bucket placement disagree.
   */
  @Test
  void aSubtypeWithMoreBucketsThanItsParentIsNotSilentlyMissed() {
    createPartitionedType("Base");
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE Wider EXTENDS Base BUCKETS 5"));

    assertThat(database.getSchema().getType("Wider").getBuckets(false)).hasSize(5);

    final List<String> keys = List.of("alpha", "bravo", "delta", "echo");
    database.transaction(() -> {
      for (final String k : keys)
        database.newDocument("Wider").set("k", k).save();
    });

    for (final String k : keys)
      assertThat(database.query("sql", "SELECT FROM Wider WHERE k = ?", k).stream().count())
          .as("key '%s' must be found through the index, not silently pruned to a bucket it was never placed in", k)
          .isEqualTo(1);
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
