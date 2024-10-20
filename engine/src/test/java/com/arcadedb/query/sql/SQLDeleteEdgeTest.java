package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.utility.CollectionUtils;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SQLDeleteEdgeTest extends TestHelper {
  @Test
  public void testDeleteFromTo() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE testFromToOneE");
      database.command("sql", "CREATE EDGE TYPE testFromToTwoE");
      database.command("sql", "CREATE VERTEX TYPE testFromToV");

      database.command("sql", "create vertex testFromToV set name = 'Luca'");
      database.command("sql", "create vertex testFromToV set name = 'Luca'");

      final List<Document> result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select from testFromToV"));

      database.command("sql", "CREATE EDGE testFromToOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());
      database.command("sql", "CREATE EDGE testFromToTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());

      List<Document> resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(2);

      database.command("sql", "DELETE EDGE testFromToTwoE from " + result.get(1).getIdentity() + " to" + result.get(0).getIdentity());

      resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(1);

      database.command("sql", "DELETE FROM testFromToOneE unsafe");
      database.command("sql", "DELETE FROM testFromToTwoE unsafe");
      final long deleted = database.command("sql", "DELETE VERTEX from testFromToV").nextIfAvailable().getProperty("count", 0);
    });
  }

  @Test
  public void testDeleteFrom() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE testFromOneE");
      database.command("sql", "CREATE EDGE TYPE testFromTwoE");
      database.command("sql", "CREATE VERTEX TYPE testFromV");

      database.command("sql", "create vertex testFromV set name = 'Luca'");
      database.command("sql", "create vertex testFromV set name = 'Luca'");

      final List<Document> result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select from testFromV"));

      database.command("sql", "CREATE EDGE testFromOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());
      database.command("sql", "CREATE EDGE testFromTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());

      List<Document> resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(2);

      database.command("sql", "DELETE EDGE testFromTwoE from " + result.get(1).getIdentity());

      resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(1);

      database.command("sql", "DELETE FROM testFromOneE unsafe");
      database.command("sql", "DELETE FROM testFromTwoE unsafe");
      final long deleted = database.command("sql", "DELETE VERTEX from testFromV").nextIfAvailable().getProperty("count", 0L);
    });
  }

  @Test
  public void testDeleteTo() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE testToOneE");
      database.command("sql", "CREATE EDGE TYPE testToTwoE");
      database.command("sql", "CREATE VERTEX TYPE testToV");

      database.command("sql", "create vertex testToV set name = 'Luca'");
      database.command("sql", "create vertex testToV set name = 'Luca'");

      final List<Document> result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select from testToV"));

      database.command("sql", "CREATE EDGE testToOneE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());
      database.command("sql", "CREATE EDGE testToTwoE from " + result.get(1).getIdentity() + " to " + result.get(0).getIdentity());

      List<Document> resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(2);

      database.command("sql", "DELETE EDGE testToTwoE to " + result.get(0).getIdentity());

      resultTwo = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "select expand(outE()) from " + result.get(1).getIdentity()));
      assertThat(resultTwo.size()).isEqualTo(1);

      database.command("sql", "DELETE FROM testToOneE unsafe");
      database.command("sql", "DELETE FROM testToTwoE unsafe");
      final long deleted = database.command("sql", "DELETE VERTEX from testToV").nextIfAvailable().getProperty("count", 0);
    });
  }

  @Test
  public void testDropTypeVandEwithUnsafe() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE SuperE");
      database.command("sql", "CREATE VERTEX TYPE SuperV");

      final Identifiable v1 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex SuperV set name = 'Luca'"));
      final Identifiable v2 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex SuperV set name = 'Mark'"));
      database.command("sql", "CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity());
    });

    database.transaction(() -> {
      try {
        database.command("sql", "DROP TYPE SuperV");
        fail("Expected CommandExecutionException");
      } catch (final CommandExecutionException e) {
        assertThat(true).isTrue();
      }

      try {
        database.command("sql", "DROP TYPE SuperE");
        fail("Expected CommandExecutionException");
      } catch (final CommandExecutionException e) {
        assertThat(true).isTrue();
      }

      try {
        database.command("sql", "DROP TYPE SuperV unsafe");
        assertThat(true).isTrue();
      } catch (final CommandExecutionException e) {
        fail("Unexpected CommandExecutionException");
      }

      try {
        database.command("sql", "DROP TYPE SuperE UNSAFE");
        assertThat(true).isTrue();
      } catch (final CommandExecutionException e) {
        fail("Unexpected CommandExecutionException");
      }
    });
  }

  @Test
  public void testDropTypeVandEwithDeleteElements() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE SuperE");
      database.command("sql", "CREATE VERTEX TYPE SuperV");

      final Identifiable v1 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex SuperV set name = 'Luca'"));
      final Identifiable v2 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex SuperV set name = 'Mark'"));
      database.command("sql", "CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity());

      try {
        database.command("sql", "DROP TYPE SuperV");
        fail("Expected CommandExecutionException");
      } catch (final CommandExecutionException e) {
        assertThat(true).isTrue();
      }

      try {
        database.command("sql", "DROP TYPE SuperE");
        fail("Expected CommandExecutionException");
      } catch (final CommandExecutionException e) {
        assertThat(true).isTrue();
      }

      final long deleted = database.command("sql", "DELETE FROM SuperV").nextIfAvailable().getProperty("count", 0);

      try {
        database.command("sql", "DROP TYPE SuperV");
        assertThat(true).isTrue();
      } catch (final CommandExecutionException e) {
        fail("Unexpected CommandExecutionException");
      }

      try {
        database.command("sql", "DROP TYPE SuperE");
        assertThat(true).isTrue();
      } catch (final CommandExecutionException e) {
        fail("Unexpected CommandExecutionException");
      }
    });
  }

  @Test
  public void testFromInString() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE FromInStringE");
      database.command("sql", "CREATE VERTEX TYPE FromInStringV");

      final Identifiable v1 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex FromInStringV set name = ' from '"));
      final Identifiable v2 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex FromInStringV set name = ' FROM '"));
      final Identifiable v3 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex FromInStringV set name = ' TO '"));

      database.command("sql", "create edge FromInStringE from " + v1.getIdentity() + " to " + v2.getIdentity());
      database.command("sql", "create edge FromInStringE from " + v1.getIdentity() + " to " + v3.getIdentity());

      List<Document> result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "SELECT expand(out()[name = ' FROM ']) FROM FromInStringV"));
      assertThat(result.size()).isEqualTo(1);

      result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "SELECT expand(in()[name = ' from ']) FROM FromInStringV"));
      assertThat(result.size()).isEqualTo(2);

      result = CollectionUtils.resultsetToListOfDocuments(database.query("sql", "SELECT expand(out()[name = ' TO ']) FROM FromInStringV"));
      assertThat(result.size()).isEqualTo(1);
    });
  }

  @Test
  public void testDeleteVertexWithReturn() {
    database.transaction(() -> {
      database.command("sql", "create vertex type V");
      final Document v1 = CollectionUtils.getFirstResultAsDocument(database.command("sql", "create vertex V set returning = true"));

      final List<Document> v2s = CollectionUtils.resultsetToListOfDocuments(
              database.command("sql", "delete vertex from V return before where returning = true"));

      assertThat(v2s.size()).isEqualTo(1);
      assertThat(v2s).contains(v1);
    });
  }
}
