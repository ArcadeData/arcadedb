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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

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

    database = factory.open();

    assertThat(database.getSchema().getType("Broken").getBucketSelectionStrategy().getName())
        .as("an unusable strategy falls back to the default").isEqualTo("round-robin");
    assertThat(database.getSchema().getExtension("lifecycle-probe"))
        .as("and everything the loader reads after the strategies is still there").isNotNull();
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/PartitionedStrategyLifecycleTest";
  }

  /**
   * Runs {@code action} with the engine's own {@link Logger} swapped for one that records, and returns the WARNING
   * (or worse) messages it saw. Deliberately not a {@code java.util.logging} handler: the test resources set
   * {@code com.arcadedb.level=SEVERE}, so whether a WARNING reaches a JUL handler depends on which loggers the rest
   * of the suite happened to reconfigure first.
   */
  private static List<String> captureWarnings(final Runnable action) {
    final CapturingLogger capturing = new CapturingLogger(LogManager.instance().getLogger());
    LogManager.instance().setLogger(capturing);
    try {
      action.run();
    } finally {
      LogManager.instance().setLogger(capturing.delegate);
    }
    return capturing.messages;
  }

  private static final class CapturingLogger implements Logger {
    private final Logger       delegate;
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private CapturingLogger(final Logger delegate) {
      this.delegate = delegate;
    }

    private void record(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      try {
        messages.add(args.length > 0 ? message.formatted(args) : message);
      } catch (final Exception ignored) {
        messages.add(message);
      }
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      record(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      record(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
