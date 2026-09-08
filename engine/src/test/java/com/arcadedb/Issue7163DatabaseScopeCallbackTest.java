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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #7163: {@link ContextConfiguration} ran a SCOPE.SERVER setting's callback but
 * not a SCOPE.DATABASE one, so the settings whose callback does real work were stored per database and never
 * applied - {@code arcadedb.maxPageRAM} kept an unclamped page-cache size, and {@code arcadedb.dateImplementation}
 * kept a raw class name that {@code coerce()} had no case for either.
 * <p>
 * The hook now covers every scope but JVM, its result is what the overlay stores (so a callback that normalises
 * normalises on every channel), and the class-name conversion moved into {@code coerce()} where the strict admin
 * parse reaches it too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7163DatabaseScopeCallbackTest {
  private static final String DB_PATH = "./target/databases/issue-7163-database-scope-callback";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    GlobalConfiguration.MAX_PAGE_RAM.reset();
    GlobalConfiguration.DATE_IMPLEMENTATION.reset();
    GlobalConfiguration.DATE_TIME_IMPLEMENTATION.reset();
  }

  /**
   * The clamp is the whole point of {@code MAX_PAGE_RAM}'s callback: a page cache above 80% of the heap is cut
   * to half the heap. Written into an overlay it used to be stored verbatim, callback unrun.
   */
  @Test
  void maxPageRamIsClampedWhenWrittenIntoAnOverlay() {
    final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

    final ContextConfiguration overlay = new ContextConfiguration();
    overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM, absurd);

    assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM))
        .as("clamped, as the enum's own setValue clamps it").isLessThan(absurd)
        .isEqualTo(Runtime.getRuntime().maxMemory() / 2 / 1024 / 1024);
  }

  /** The same write through the untyped overload, which is what the admin commands use. */
  @Test
  void maxPageRamIsClampedThroughTheUntypedOverloadToo() {
    final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

    final ContextConfiguration overlay = new ContextConfiguration();
    overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM.getKey(), Long.toString(absurd));

    assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isLessThan(absurd);
  }

  /** A value inside the budget is stored as it was asked for. */
  @Test
  void aMaxPageRamInsideTheBudgetIsLeftAlone() {
    final ContextConfiguration overlay = new ContextConfiguration();
    overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM, 16L);

    assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isEqualTo(16L);
  }

  /** And so does the database's own configuration, reached through ALTER DATABASE. */
  @Test
  void alterDatabaseClampsMaxPageRamAndReportsTheValueThatTookEffect() {
    final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

    try (final ResultSet resultSet = database.command("sql", "ALTER DATABASE `arcadedb.maxPageRAM` " + absurd)) {
      final Result row = resultSet.next();
      assertThat(((Number) row.getProperty("newValue")).longValue())
          .as("the result row reports the value that took effect, not the one asked for").isLessThan(absurd);
    }

    assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isLessThan(absurd);
  }

  /** {@code coerce()} now knows the Class case, so the enum and the strict admin parse share one conversion. */
  @Test
  void aClassTypedSettingCoercesFromItsClassName() {
    assertThat(GlobalConfiguration.DATE_IMPLEMENTATION.coerce("java.util.Date")).isEqualTo(Date.class);
    assertThat(GlobalConfiguration.DATE_IMPLEMENTATION.coerce(Date.class)).isEqualTo(Date.class);
    assertThat(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.coerceFromAdminCommand("java.time.LocalDateTime"))
        .isEqualTo(LocalDateTime.class);
  }

  /** An unknown class name is refused where it enters instead of surfacing from the next reader. */
  @Test
  void anUnknownClassNameIsRefused() {
    assertThatThrownBy(() -> GlobalConfiguration.DATE_IMPLEMENTATION.coerceFromAdminCommand("no.such.Clazz"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.dateImplementation");

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'no.such.Clazz'").close())
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * ALTER DATABASE keeps working end to end: the class name is validated, stored by NAME so the database's
   * JSON configuration can hold it, and this database's serializer is pointed at it.
   */
  @Test
  void alterDatabaseSetsTheDateImplementationAndPersistsItByName() {
    database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'").close();

    assertThat(((DatabaseInternal) database).getSerializer().getDateImplementation()).isEqualTo(Date.class);
    assertThat(database.getConfiguration().<Object>getValue(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
    assertThat(database.getConfiguration().toJSON()).contains("java.util.Date");

    final ContextConfiguration reloaded = new ContextConfiguration();
    reloaded.fromJSON(database.getConfiguration().toJSON());
    assertThat(reloaded.<Object>getValue(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
  }

  /**
   * Every write into an overlay resolves the key through {@code GlobalConfiguration.findByKey}, which this
   * change turned from a linear scan of {@code values()} into a hash lookup. A key that stopped resolving would
   * silently take a setting back to "stored and never applied", which is the whole subject of this issue.
   */
  @Test
  void everySettingResolvesByItsOwnKey() {
    for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
      assertThat(GlobalConfiguration.findByKey(setting.getKey())).as(setting.getKey()).isSameAs(setting);
      final String suffix = setting.getKey().substring(GlobalConfiguration.PREFIX.length());
      assertThat(GlobalConfiguration.findByKey(GlobalConfiguration.PREFIX + suffix.toUpperCase(Locale.ENGLISH)))
          .as("case insensitive, as it always was: " + setting.getKey()).isSameAs(setting);
      assertThat(GlobalConfiguration.findByKey(suffix))
          .as("and reachable without the prefix: " + setting.getKey()).isSameAs(setting);
    }
    assertThat(GlobalConfiguration.findByKey("arcadedb.thereIsNoSuchSetting")).isNull();
  }

  /** A SCOPE.JVM setting is deliberately NOT reached through an overlay: it is not what an overlay holds. */
  @Test
  void aJvmScopedSettingIsNotAppliedThroughAnOverlay() {
    final String before = GlobalConfiguration.PROFILE.getValueAsString();

    final ContextConfiguration overlay = new ContextConfiguration();
    overlay.setValue(GlobalConfiguration.PROFILE, "low-ram");

    assertThat(overlay.getValueAsString(GlobalConfiguration.PROFILE)).isEqualTo("low-ram");
    assertThat(GlobalConfiguration.PROFILE.getValueAsString())
        .as("a per-database write must not reconfigure the whole JVM").isEqualTo(before);
  }
}
