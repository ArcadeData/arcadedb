package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.DuplicatedKeyException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4454: a UNIQUE constraint declared on a single label (e.g. Foo) must keep
 * rejecting duplicates inserted through a multi-label composite type (e.g. Foo:Water) after the database
 * is restarted. Before the fix the inherited unique index was not re-attached to the composite type's
 * buckets on schema reload, so the duplicate was incorrectly accepted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMultiLabelConstraintReloadTest {
  private Database       database;
  private DatabaseFactory factory;
  private String         databasePath;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    databasePath = "./target/databases/testopencypher-multilabel-reload-" + testInfo.getTestMethod().get().getName();
    factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (!database.isOpen())
        database = factory.open();
      database.drop();
      database = null;
    }
  }

  @Test
  void uniqueConstraintEnforcedOnMultiLabelAfterReload() {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS UNIQUE");

    database.transaction(() -> database.command("opencypher", "CREATE (n:Foo {id: 'inter-001'})"));

    // Duplicate through the composite type Foo:Water must fail (this also creates the Foo~Water type).
    assertThatThrownBy(() -> database.transaction(() -> //
        database.command("opencypher", "CREATE (n:Foo:Water {id: 'inter-001'})"))) //
        .isInstanceOf(DuplicatedKeyException.class);

    // Simulate a docker restart: close and reopen the database.
    database.close();
    database = factory.open();

    // A plain Foo duplicate must still fail.
    assertThatThrownBy(() -> database.transaction(() -> //
        database.command("opencypher", "CREATE (n:Foo {id: 'inter-001'})"))) //
        .isInstanceOf(DuplicatedKeyException.class);

    // The composite Foo:Water duplicate must still fail after restart (issue #4454: it was wrongly succeeding).
    assertThatThrownBy(() -> database.transaction(() -> //
        database.command("opencypher", "CREATE (n:Foo:Water {id: 'inter-001'})"))) //
        .isInstanceOf(DuplicatedKeyException.class);

    // A brand new composite type Foo:Earth must also enforce the inherited Foo.id unique constraint.
    assertThatThrownBy(() -> database.transaction(() -> //
        database.command("opencypher", "CREATE (n:Foo:Earth {id: 'inter-001'})"))) //
        .isInstanceOf(DuplicatedKeyException.class);

    // Sanity check: a unique value through the composite type is accepted.
    database.transaction(() -> database.command("opencypher", "CREATE (n:Foo:Water {id: 'inter-002'})"));

    final long count = database.command("opencypher", "MATCH (n:Foo) RETURN count(n) AS c").next().<Long>getProperty("c");
    assertThat(count).isEqualTo(2L);
  }
}
