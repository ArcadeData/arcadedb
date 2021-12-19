package com.arcadedb.graphql;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public abstract class AbstractGraphQLNativeLanguageDirectivesTest extends AbstractGraphQLTest {
  @Test
  public void testUseTypeDefinitionForReturn() {
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
  public void testCustomDefinitionForReturn() {
    executeTest((database) -> {
      defineTypes(database);

      try (ResultSet resultSet = database.query("graphql",
          "{ bookByName(bookNameParameter: \"Harry Potter and the Philosopher's Stone\"){ id name pageCount } }")) {
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
}
