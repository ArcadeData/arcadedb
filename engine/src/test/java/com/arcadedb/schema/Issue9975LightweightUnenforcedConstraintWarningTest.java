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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9975: a database written before #9846 may pair a LIGHTWEIGHT edge type with a MANDATORY or
 * default-valued property it inherits. It must still open - refusing would be worse than the defect - but the constraint is
 * declared and never applied, so the load logs a WARNING naming the type, the property and the two ways out, and
 * {@code CHECK DATABASE} reports the same finding.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9975LightweightUnenforcedConstraintWarningTest extends TestHelper {

  private static final String PARENT = "Issue9975Follows";
  private static final String CHILD  = "Issue9975CloseFollows";

  @Test
  void openingASchemaThatHoldsTheCombinationLogsAWarning() throws IOException {
    writeLegacySchema();

    final List<String> warnings = new ArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      database = factory.open();
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(((LocalEdgeType) database.getSchema().getType(CHILD)).isLightweight()).isTrue();
    assertThat(warnings).filteredOn(w -> w.contains(CHILD)).singleElement().satisfies(w -> assertThat(w)//
        .contains("LIGHTWEIGHT").contains("MANDATORY").contains(PARENT + ".since").contains("not enforced"));

    // the way out the warning names, so the integrity check that closes the test finds nothing
    database.getSchema().getType(PARENT).getProperty("since").setMandatory(false);
  }

  @Test
  void checkDatabaseReportsTheUnenforcedConstraint() throws IOException {
    writeLegacySchema();
    database = factory.open();

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result result = rs.next();
      final Collection<String> warnings = result.getProperty("warnings");
      assertThat(warnings).filteredOn(w -> w.contains(CHILD)).singleElement()
          .satisfies(w -> assertThat(w).contains(PARENT + ".since").contains("MANDATORY"));
    }

    // once the property is loosened there is nothing left to report
    database.getSchema().getType(PARENT).getProperty("since").setMandatory(false);
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Collection<String> warnings = rs.next().getProperty("warnings");
      assertThat(warnings).noneMatch(w -> w.contains(CHILD));
    }
  }

  @Test
  void everyUnenforcedPropertyIsNamedWithItsOwnWayOut() throws IOException {
    database.getSchema().createEdgeType(PARENT);
    database.command("sql", "CREATE PROPERTY " + PARENT + ".since DATETIME (MANDATORY TRUE)").close();
    database.command("sql", "CREATE PROPERTY " + PARENT + ".weight INTEGER (DEFAULT 5)").close();
    database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);
    database.close();
    markChildLightweight();
    database = factory.open();

    final String warning = LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD));
    assertThat(warning).contains("MANDATORY property '" + PARENT + ".since' (inherited: make the property optional)")
        .contains("default-valued property '" + PARENT + ".weight' (inherited: remove the property's default value (DEFAULT null))")
        .contains("declare the type without LIGHTWEIGHT");

    // each named way out clears its own part of the finding
    database.command("sql", "ALTER PROPERTY " + PARENT + ".since MANDATORY false").close();
    assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).doesNotContain(".since")
        .contains(".weight");
    database.command("sql", "ALTER PROPERTY " + PARENT + ".weight DEFAULT null").close();
    assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).isNull();
  }

  /**
   * {@code DEFAULT null} is kept as the text "null" and fills in nothing, so it is not a default a lightweight edge fails to
   * apply: it was refused as one, which left the SQL way out of the finding refused too.
   */
  @Test
  void aSqlNullDefaultIsNotADefaultUnderALightweightType() {
    database.getSchema().createEdgeType(PARENT);
    database.command("sql", "CREATE PROPERTY " + PARENT + ".weight INTEGER (DEFAULT null)").close();
    database.command("sql", "CREATE EDGE TYPE " + CHILD + " EXTENDS " + PARENT + " LIGHTWEIGHT").close();

    database.command("sql", "ALTER PROPERTY " + PARENT + ".weight DEFAULT null").close();
    database.command("sql", "CREATE PROPERTY " + PARENT + ".other STRING (DEFAULT null)").close();
    assertThat(LocalEdgeType.unenforcedConstraintWarning(database.getSchema().getType(CHILD))).isNull();

    // the STRING 'null' is a value like any other, and still refused
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + PARENT + ".text STRING (DEFAULT 'null')").close())
        .hasMessageContaining(CHILD).hasMessageContaining("keep the property without a default");
    assertThat(Property.fillsInAValue("'null'")).isTrue();
    assertThat(Property.fillsInAValue(" NULL ")).isFalse();
  }

  @Test
  void aSchemaWithoutTheCombinationLogsNothing() {
    database.getSchema().createEdgeType(PARENT).createProperty("since", Type.DATETIME);
    database.getSchema().buildEdgeType().withName(CHILD).withSuperType(PARENT).withLightweight(true).create();
    database.close();

    final List<String> warnings = new ArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings));
    try {
      database = factory.open();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    assertThat(warnings).noneMatch(w -> w.contains(CHILD));
  }

  /**
   * The state the DDL could produce before #9846: a lightweight type under a super type with a MANDATORY property, written by
   * hand because every DDL path now refuses it.
   */
  private void writeLegacySchema() throws IOException {
    final EdgeType parent = database.getSchema().createEdgeType(PARENT);
    parent.createProperty("since", Type.DATETIME).setMandatory(true);
    database.getSchema().createEdgeType(CHILD).addSuperType(PARENT);
    database.close();
    markChildLightweight();
  }

  private void markChildLightweight() throws IOException {
    final Path schemaFile = Path.of(getDatabasePath(), "schema.json");
    final JSONObject schemaJson = new JSONObject(Files.readString(schemaFile));
    schemaJson.getJSONObject("types").getJSONObject(CHILD).put("lightweight", true);
    Files.writeString(schemaFile, schemaJson.toString());
  }

  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;

    private CapturingLogger(final List<String> warnings) {
      this.warnings = warnings;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      log(requester, level, message, exception, context,
          new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public synchronized void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      if (level == Level.WARNING && message != null)
        warnings.add(args == null || args.length == 0 ? message : String.format(message, args));
    }

    @Override
    public void flush() {
    }
  }
}
