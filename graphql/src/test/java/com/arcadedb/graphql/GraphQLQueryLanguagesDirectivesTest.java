package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class GraphQLQueryLanguagesDirectivesTest extends AbstractGraphQLTest {

  @Test
  public void testSQLUseTypeDefinitionForReturn() {
    executeTest((database) -> {
      defineTypes(database);

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(bookNameParameter: \"Harry Potter and the Philosopher's Stone\")}")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());
        Assertions.assertEquals("Harry Potter and the Philosopher's Stone", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(bookNameParameter: \"Mr. brain\") }")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());
        Assertions.assertEquals("Mr. brain", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void testSQLCustomDefinitionForReturn() {
    executeTest((database) -> {
      defineTypes(database);

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(bookNameParameter: \"Harry Potter and the Philosopher's Stone\"){ id name pageCount } }")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(3, record.getPropertyNames().size());
        Assertions.assertEquals("Harry Potter and the Philosopher's Stone", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      try (ResultSet resultSet = database.query("graphql", "{ bookByName(bookNameParameter: \"Mr. brain\"){ id name pageCount } }")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();
        Assertions.assertEquals(3, record.getPropertyNames().size());
        Assertions.assertEquals("Mr. brain", record.getProperty("name"));
        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.command("graphql", "type Query {\n" +//
        "  bookById(id: String): Book\n" +//
        "  bookByName(bookNameParameter: String): Book @sql(statement: \"name = :bookNameParameter\")\n" +//
        "}");
  }
}
