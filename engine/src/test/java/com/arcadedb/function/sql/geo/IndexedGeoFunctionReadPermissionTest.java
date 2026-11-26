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
package com.arcadedb.function.sql.geo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A user who may not read a type is refused by a walk of its buckets, and by a {@code geo.*} predicate answered by a GEOSPATIAL index of
 * the type, with the same refusal for any area. The read permission of the type and of its subtypes is asked before the index is opened.
 * <p>
 * Every statement is run with an area that holds records of the denied type and with one that holds none.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class IndexedGeoFunctionReadPermissionTest {
  private static final String PATH = "target/databases/IndexedGeoFunctionReadPermissionTest";

  private static final String PRESENT = "POLYGON ((9 9, 11 9, 11 11, 9 11, 9 9))";
  private static final String ABSENT  = "POLYGON ((50 50, 51 50, 51 51, 50 51, 50 50))";

  /** The statements, each written with the area as its only parameter. */
  private static final List<String> STATEMENTS = List.of(
      "SELECT coords FROM %s WHERE geo.within(coords, geo.geomFromText(?)) = true",
      "SELECT coords FROM %s WHERE geo.intersects(coords, geo.geomFromText(?)) = true",
      "SELECT coords FROM %s WHERE geo.contains(geo.geomFromText(?), coords) = true",
      "SELECT coords FROM %s WHERE geo.dWithin(coords, geo.geomFromText(?), 1.0) = true",
      "SELECT coords FROM %s WHERE geo.within(coords, geo.geomFromText(?)) = true AND coords IS NOT NULL",
      "SELECT count(*) AS c FROM %s WHERE geo.within(coords, geo.geomFromText(?)) = true");

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    // without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    for (final String name : new String[] { "Open", "Secret", "Place" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + name + " BUCKETS 2");
      database.command("sql", "CREATE PROPERTY " + name + ".coords STRING");
      database.command("sql", "CREATE INDEX ON " + name + " (coords) GEOSPATIAL");
    }
    database.command("sql", "CREATE DOCUMENT TYPE Hidden EXTENDS Place BUCKETS 2");

    database.transaction(() -> {
      for (final String type : new String[] { "Open", "Secret", "Hidden" })
        for (int i = 0; i < 4; i++)
          database.newDocument(type).set("coords", "POINT (" + (9.5 + i * 0.3) + " 10)").save();
      database.newDocument("Place").set("coords", "POINT (80 80)").save();
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheStatementWhateverTheAreaIs() {
    deny("Secret");

    final List<String> notRefused = new ArrayList<>();
    for (final String statement : STATEMENTS)
      for (final String area : new String[] { PRESENT, ABSENT })
        if (!refused(String.format(statement, "Secret"), area))
          notRefused.add(String.format(statement, "Secret") + " with " + area);
    assertThat(notRefused).as("a denied type refuses the statement for any area").isEmpty();
  }

  @Test
  void aSubTypeTheUserMayNotReadRefusesTheStatementOfItsSuperTypeWhateverTheAreaIs() {
    deny("Hidden");

    // the walk of Place opens the buckets of Hidden, and so is refused for every area
    assertThat(refused("SELECT coords FROM Place", null)).as("the walk of the super type").isTrue();
    for (final String statement : STATEMENTS)
      for (final String area : new String[] { PRESENT, ABSENT, "POLYGON ((79 79, 81 79, 81 81, 79 81, 79 79))" })
        assertThat(refused(String.format(statement, "Place"), area)).as(String.format(statement, "Place") + " with " + area).isTrue();
  }

  @Test
  void aTypeTheUserMayReadIsAnsweredWhileAnotherIsDenied() {
    deny("Secret", "Hidden");

    assertThat(count("SELECT coords FROM Open WHERE geo.within(coords, geo.geomFromText(?)) = true", PRESENT)).isEqualTo(4);
    assertThat(count("SELECT coords FROM Open WHERE geo.within(coords, geo.geomFromText(?)) = true", ABSENT)).isZero();
    assertThat(count("SELECT coords FROM Open WHERE geo.intersects(coords, geo.geomFromText(?)) = true", PRESENT)).isEqualTo(4);
    assertThat(count("SELECT coords FROM Open WHERE geo.dWithin(coords, geo.geomFromText(?), 1.0) = true", PRESENT)).isEqualTo(4);
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    // Place holds the 4 records of its subtype Hidden in the area, and one of its own outside it
    for (final String type : new String[] { "Secret", "Place" }) {
      assertThat(count(String.format(STATEMENTS.get(0), type), PRESENT)).isEqualTo(4);
      assertThat(count(String.format(STATEMENTS.get(0), type), ABSENT)).isZero();
      assertThat(count(String.format(STATEMENTS.get(1), type), PRESENT)).isEqualTo(4);
    }
  }

  @Test
  void aStatementWithNoBoundUserIsAnswered() {
    // an embedded call has no user bound: nothing is denied
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);

    assertThat(count("SELECT coords FROM Secret WHERE geo.within(coords, geo.geomFromText(?)) = true", PRESENT)).isEqualTo(4);
    assertThat(count("SELECT coords FROM Secret WHERE geo.within(coords, geo.geomFromText(?)) = true", ABSENT)).isZero();
  }

  /** The indexed full-text functions search the buckets the user may read: a denied type is answered the same for any term. */
  @Test
  void aFullTextSearchOfADeniedTypeIsAnsweredTheSameForAnyTerm() {
    database.command("sql", "CREATE DOCUMENT TYPE Note BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Note.body STRING");
    database.command("sql", "CREATE INDEX ON Note (body) FULL_TEXT");
    database.transaction(() -> {
      database.newDocument("Note").set("body", "alpha beta").save();
      database.newDocument("Note").set("body", "gamma delta").save();
    });
    deny("Note");

    final Throwable present = catchThrowable(() -> count("SELECT body FROM Note WHERE SEARCH_INDEX('Note[body]', ?) = true", "alpha"));
    final Throwable absent = catchThrowable(() -> count("SELECT body FROM Note WHERE SEARCH_INDEX('Note[body]', ?) = true", "omega"));

    assertThat(present == null).as("both terms are refused or both are answered").isEqualTo(absent == null);
    if (present == null) {
      assertThat(count("SELECT body FROM Note WHERE SEARCH_INDEX('Note[body]', ?) = true", "alpha")).isZero();
      assertThat(count("SELECT body FROM Note WHERE SEARCH_INDEX('Note[body]', ?) = true", "omega")).isZero();
    }
  }

  private boolean refused(final String statement, final String area) {
    return catchThrowable(() -> count(statement, area)) instanceof SecurityException;
  }

  /** The number of rows of the statement. */
  private long count(final String statement, final String area) {
    long rows = 0;
    try (final ResultSet rs = area == null ? database.query("sql", statement) : database.query("sql", statement, area)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
