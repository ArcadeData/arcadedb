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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class AlterDatabaseSettingTest {
  /**
   * Regression for #6875.
   * <p>
   * Issue #6875: {@code ALTER DATABASE ... SETTING} is the third writer into a {@code ContextConfiguration}, after the
   * {@code set_server_setting} MCP tool and the {@code "set server setting"} HTTP command, and it stored the evaluated
   * SQL expression with the same plain map put the other two used. It now goes through
   * {@link GlobalConfiguration#coerce(Object)} as well, so a value that is not the setting's declared type is refused
   * by the statement that names it rather than by whichever component reads the setting next.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class SettingCoercion extends TestHelper {

    @Test
    void alterDatabaseRefusesAValueThatIsNotTheSettingType() {
      assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.asyncWorkerThreads` 'abc'"))
          .hasMessageContaining("arcadedb.asyncWorkerThreads");

      // the refused statement is not the one that breaks a later read
      assertThatCode(() -> database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
          .doesNotThrowAnyException();
    }

    @Test
    void alterDatabaseStoresACoercedTypedValue() {
      try (final ResultSet result = database.command("sql", "alter database `arcadedb.asyncWorkerThreads` '6'")) {
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(6);
      }

      assertThat((Object) database.getConfiguration().getValue(GlobalConfiguration.ASYNC_WORKER_THREADS))
          .isInstanceOf(Integer.class).isEqualTo(6);
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(6);
    }

    /**
     * The SQL value is an arbitrary expression, so an unquoted {@code 6.7} reaches the setting already boxed as a
     * floating-point number rather than as text. It must be refused, not truncated to 6.
     */
    @Test
    void alterDatabaseRefusesAFractionalValueForAnIntegralSetting() {
      assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.asyncWorkerThreads` 6.7"))
          .hasMessageContaining("arcadedb.asyncWorkerThreads");

      assertThatCode(() -> database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
          .doesNotThrowAnyException();
    }

    /**
     * A String setting is unaffected: it takes any text, quotes stripped by the expression evaluator as before. The text is a
     * directory inside the database's own, the only place this setting may point to without a server-level value.
     */
    @Test
    void alterDatabaseStillAcceptsAnyTextForAStringSetting() {
      final String text = database.getDatabasePath() + "/not a number at all";
      database.command("sql", "alter database `arcadedb.externalPropertyBucketPath` '" + text + "'").close();
      assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH))
          .isEqualTo(text);
    }
  }

  /**
   * Regression for #7124.
   * <p>
   * Issue #7124: {@code ALTER DATABASE ... SETTING} is the fourth administrative writer into a
   * {@code ContextConfiguration}, and it converted through the permissive {@code GlobalConfiguration.coerce}, which
   * reads a boolean it cannot parse as {@code false}. So
   * {@code ALTER DATABASE `arcadedb.txWAL` 'ture'} answered with a result row saying {@code newValue: false} and
   * turned the write-ahead log OFF for the database - a durability hazard produced by a typo, reported as a success,
   * and saved to the database configuration.
   * <p>
   * {@code TX_WAL} is used here rather than an arbitrary boolean setting precisely because the consequence of getting
   * it wrong is the worst one in the set.
   */
  @Nested
  class BooleanSetting extends TestHelper {

    @Test
    void alterDatabaseRefusesABooleanTypoInsteadOfTurningTheSettingOff() {
      final boolean before = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);

      assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.txWAL` 'ture'")).hasMessageContaining(
          "arcadedb.txWAL");

      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).as(
          "a refused statement must leave the setting exactly as it was, not at the value the typo would have produced")
          .isEqualTo(before);
    }

    @Test
    void alterDatabaseStillStoresBothBooleanLiterals() {
      try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` 'false'")) {
        assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.FALSE);
      }
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();

      try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` 'TRUE'")) {
        assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.TRUE);
      }
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isTrue();
    }

    @Test
    void anUnquotedBooleanLiteralStillWorks() {
      // THE SQL EXPRESSION EVALUATES TO A Boolean RATHER THAN TEXT: THE STRICT PARSE MUST LET IT THROUGH UNTOUCHED.
      try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` false")) {
        assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.FALSE);
      }
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();

      try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` true")) {
        assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.TRUE);
      }
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isTrue();
    }
  }

  /**
   * Regression for #9763.
   * <p>
   * Issue #9763, the engine half: the settings {@link LocalDatabase#pendingConfigurationChanges()} computes and
   * {@link LocalDatabase#applyConfigurationChanges(String)} applies, which is what a Raft node replicates and takes after
   * an {@code ALTER DATABASE}, and the rollback {@code ALTER DATABASE} now performs when the configuration cannot be
   * saved - under HA that save is what replicates it, and a setting left in force on one node
   * only is the divergence the issue reports.
   */
  @Nested
  class DatabaseConfiguration extends TestHelper {

    /** The rollback of a failed save: one setting back, side effect included, every other setting untouched. */
    @Test
    void restoringASettingPutsBackItsValueAndItsPerDatabaseEffect() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
      // In memory only, as changes whose save is under way hold them
      local.getConfiguration().setValue(GlobalConfiguration.DATE_IMPLEMENTATION, "java.util.Date");
      local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 5);
      local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 6);

      local.restoreSetting(GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(), 4);
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
      assertThat(local.getSerializer().getDateImplementation())
          .as("the per-database effects are re-derived from the configuration in force").isEqualTo(Date.class);

      local.restoreSetting(GlobalConfiguration.DATE_IMPLEMENTATION.getKey(), null);

      assertThat(local.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.DATE_IMPLEMENTATION.getKey());
      assertThat(local.getSerializer().getDateImplementation())
          .isEqualTo(GlobalConfiguration.DATE_IMPLEMENTATION.getDefValue());
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)).isEqualTo(5);
    }

    /** A save that fails must not leave the setting, or its side effect, in force anyway. */
    @Test
    void aSettingWhoseSaveFailsIsRolledBack() {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int bucketsBefore = db.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
      final Class<?> dateImplementationBefore = db.getSerializer().getDateImplementation();
      assertThat(dateImplementationBefore).isNotEqualTo(Date.class);

      final File configuration = new File(db.getDatabasePath() + File.separator + LocalDatabase.CONFIGURATION_FILE_NAME);
      // A directory where the file belongs fails the save on every platform and for every user (see SqlQueryDiagnosticsTest.Diagnostics)
      assertThat(configuration.delete() || !configuration.exists()).isTrue();
      assertThat(configuration.mkdir()).isTrue();
      try {
        assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 13"))
            .isInstanceOf(CommandExecutionException.class);
        assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
            .isInstanceOf(CommandExecutionException.class);
      } finally {
        assertThat(configuration.delete()).isTrue();
      }

      assertThat(db.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(bucketsBefore);
      assertThat(db.getSerializer().getDateImplementation()).isEqualTo(dateImplementationBefore);
    }

    /** What under HA travels to every node: the settings memory holds that the file does not, and nothing else. */
    @Test
    void pendingChangesNameOnlyWhatDiffersFromTheFile() throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
      assertThat(local.pendingConfigurationChanges()).as("saved, so nothing pending").isNull();

      local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);
      local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, null);

      final JSONObject changes = new JSONObject(local.pendingConfigurationChanges());
      assertThat(changes.getJSONObject("configuration").keySet()).containsExactly("txRetries");
      assertThat(changes.getJSONObject("configuration").getInt("txRetries")).isEqualTo(9);
      assertThat(changes.getJSONArray("removed").toList()).containsExactly("typeDefaultBuckets");
    }

    /**
     * Applying replicated changes touches only the settings they name, in memory and on disk, and writes the file from
     * its own content: a change still on its way through the replication on this node is neither lost nor written ahead.
     */
    @Test
    void applyingChangesKeepsEverySettingTheyDoNotName() throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      database.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
      database.command("sql", "ALTER DATABASE `arcadedb.txWalFlush` 2");
      // In memory only, as an ALTER DATABASE whose replication has not committed yet holds it
      local.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 9);

      local.applyConfigurationChanges(
          "{\"configuration\":{\"dateImplementation\":\"java.util.Date\"},\"removed\":[\"txWalFlush\"]}");

      assertThat(local.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
      assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);
      assertThat(local.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_WAL_FLUSH.getKey());
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(4);
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
          .as("the local change in flight stays in force").isEqualTo(9);

      final JSONObject file = new JSONObject(FileUtils.readFileAsString(
          new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME))).getJSONObject("configuration");
      assertThat(file.getString("dateImplementation")).isEqualTo("java.util.Date");
      assertThat(file.has("txWalFlush")).isFalse();
      assertThat(file.getInt("typeDefaultBuckets")).isEqualTo(4);
      assertThat(file.has("txRetries")).as("not written ahead of its own replication").isFalse();
      assertThat(new JSONObject(local.pendingConfigurationChanges()).getJSONObject("configuration").keySet())
          .containsExactly("txRetries");

      local.applyConfigurationChanges("{\"configuration\":{},\"removed\":[\"dateImplementation\"]}");
      assertThat(local.getSerializer().getDateImplementation())
          .isEqualTo(GlobalConfiguration.DATE_IMPLEMENTATION.getDefValue());
    }

    /**
     * A replicated change whose file cannot be written is in force in memory anyway, and the failure is reported: the
     * HA apply turns it into a logged, non-halting entry failure rather than a crash loop on a full or read-only volume.
     */
    @Test
    void aChangeWhoseFileCannotBeWrittenIsInForceAndReported() {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final File configuration = new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
      assertThat(configuration.delete() || !configuration.exists()).isTrue();
      assertThat(configuration.mkdir()).isTrue();
      try {
        assertThatThrownBy(() -> local.applyConfigurationChanges("{\"configuration\":{\"txRetries\":8},\"removed\":[]}"))
            .isInstanceOf(IOException.class);
      } finally {
        assertThat(configuration.delete()).isTrue();
      }
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)).isEqualTo(8);
    }

    /** A configuration.json that is not valid JSON does not block replicated changes: the next one writes a valid file. */
    @Test
    void aCorruptFileCountsAsEmpty() throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final File configuration = new File(local.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
      FileUtils.writeFile(configuration, "{\"configuration\":{\"txRetries\":");

      local.applyConfigurationChanges("{\"configuration\":{\"txRetries\":8},\"removed\":[]}");

      assertThat(new JSONObject(FileUtils.readFileAsString(configuration)).getJSONObject("configuration").getInt("txRetries"))
          .isEqualTo(8);
    }
  }

  /**
   * Regression for #7922.
   * <p>
   * Regression test for issue #7922: {@code SQL_MAX_EXPRESSION_DEPTH} and {@code CYPHER_MAX_EXPRESSION_DEPTH} are both
   * declared {@code SCOPE.DATABASE}, and the refusal a user gets tells them to raise exactly that setting - but the
   * parsers read it off the enum, JVM-wide, so {@code ALTER DATABASE} had no effect at all and the only way to allow
   * one legitimately deep query on one database was to lower the protection for every database on the server.
   * <p>
   * Same shape, and same fix, as #7786.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PerDatabaseExpressionDepth extends TestHelper {

    @Test
    void alterDatabaseMovesTheSqlExpressionDepthLimit() {
      database.command("sql", "CREATE DOCUMENT TYPE T");

      final int jvmDefault = GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getValueAsInteger();
      final String tooDeep = "SELECT FROM T WHERE " + nested(jvmDefault + 20, "1 = 1");

      assertThatThrownBy(() -> database.query("sql", tooDeep))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("Expression nesting exceeds the maximum allowed depth of " + jvmDefault);

      database.command("sql", "ALTER DATABASE `arcadedb.sql.maxExpressionDepth` " + (jvmDefault + 100));
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH))
          .isEqualTo(jvmDefault + 100);

      // The per-database value is what the parser must consult now; the JVM default is untouched.
      try (final ResultSet rs = database.query("sql", tooDeep)) {
        assertThat(rs.hasNext()).isFalse();
      }
      assertThat(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getValueAsInteger())
          .as("raising it for one database must not touch the JVM-wide default")
          .isEqualTo(jvmDefault);
    }

    /**
     * Lowering it must bite too, which is what proves the value is read per parse rather than frozen when the database
     * - and with it the statement cache and the parser it holds - was opened.
     * <p>
     * The query after the ALTER is deliberately a DIFFERENT text from the one before it: a statement already in the
     * per-database cache is served from the AST and never re-parsed, so re-running the identical text would prove
     * nothing either way. That is not specific to this setting - it is how every parse-time setting behaves - and it
     * is why the refusal in the issue is recoverable: a query that was refused never made it into the cache.
     */
    @Test
    void alterDatabaseCanAlsoLowerTheSqlExpressionDepthLimit() {
      database.command("sql", "CREATE DOCUMENT TYPE T");

      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE " + nested(10, "1 = 1"))) {
        assertThat(rs.hasNext()).isFalse();
      }

      database.command("sql", "ALTER DATABASE `arcadedb.sql.maxExpressionDepth` 3");

      assertThatThrownBy(() -> database.query("sql", "SELECT FROM T WHERE " + nested(10, "2 = 2")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("maximum allowed depth of 3");
    }

    @Test
    void alterDatabaseMovesTheCypherExpressionDepthLimit() {
      database.command("sql", "CREATE VERTEX TYPE Person");

      final int jvmDefault = GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger();
      final String tooDeep = "MATCH (p:Person) WHERE " + nested(jvmDefault + 20, "p.n = 1") + " RETURN p";

      assertThatThrownBy(() -> database.query("opencypher", tooDeep))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("too deeply nested");

      database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` " + (jvmDefault + 200));

      try (final ResultSet rs = database.query("opencypher", tooDeep)) {
        assertThat(rs.hasNext()).isFalse();
      }
      assertThat(GlobalConfiguration.CYPHER_MAX_EXPRESSION_DEPTH.getValueAsInteger()).isEqualTo(jvmDefault);
    }

    /** The Cypher twin of {@link #alterDatabaseCanAlsoLowerTheSqlExpressionDepthLimit}, cache caveat included. */
    @Test
    void alterDatabaseCanAlsoLowerTheCypherExpressionDepthLimit() {
      database.command("sql", "CREATE VERTEX TYPE Person");

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person) WHERE " + nested(8, "p.n = 1") + " RETURN p")) {
        assertThat(rs.hasNext()).isFalse();
      }

      database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` 3");

      assertThatThrownBy(() -> database.query("opencypher",
          "MATCH (p:Person) WHERE " + nested(8, "p.n = 2") + " RETURN p"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("maximum allowed depth of 3");
    }

    /**
     * The other arm of the same guard: a long chain of {@code OR} terms, which nests deeply without nesting a single
     * parenthesis. The {@code ExpressionRewriter} behind it reads the same bound value - see
     * {@code ExpressionRewriterDepthTest} for that reader on its own, since in practice the parse listener
     * here refuses first and the rewriter's own guard is the backstop underneath it.
     */
    @Test
    void theChainedExpressionGuardAlsoReadsThePerDatabaseLimit() {
      database.command("sql", "CREATE VERTEX TYPE Person");

      final StringBuilder chained = new StringBuilder("MATCH (p:Person) WHERE p.n = 0");
      for (int i = 1; i < 40; i++)
        chained.append(" OR p.n = ").append(i);
      chained.append(" RETURN p");

      try (final ResultSet rs = database.query("opencypher", chained.toString())) {
        assertThat(rs.hasNext()).isFalse();
      }

      database.command("sql", "ALTER DATABASE `arcadedb.cypher.maxExpressionDepth` 5");

      final StringBuilder chainedAgain = new StringBuilder("MATCH (p:Person) WHERE p.n = 100");
      for (int i = 101; i < 140; i++)
        chainedAgain.append(" OR p.n = ").append(i);
      chainedAgain.append(" RETURN p");

      assertThatThrownBy(() -> database.query("opencypher", chainedAgain.toString()))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("maximum allowed depth of 5");
    }

    private static String nested(final int depth, final String innermost) {
      return "(".repeat(depth) + innermost + ")".repeat(depth);
    }
  }
}
