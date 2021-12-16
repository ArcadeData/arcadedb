package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

public class GraphQLQueries {

  private static final String DB_PATH = "./target/testgraphql";

  @Test
  public void ridMapping() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "}";

      database.command("graphql", types);

      RID rid = null;
      try (ResultSet resultSet = database.query("graphql", "{ bookById(id: \"book-1\"){" +//
          "  rid @rid" +//
          "  id" +//
          "  name" +//
          "  pageCount" +//
          "  authors {" +//
          "    firstName" +//
          "    lastName" +//
          "  }" +//
          "}" +//
          "}")) {
        Assertions.assertTrue(resultSet.hasNext());
        final Result record = resultSet.next();

        System.out.println(record.toJSON());

        rid = record.getIdentity().get();
        Assertions.assertNotNull(rid);

        Assertions.assertEquals(5, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void allBooks() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: String!): [Book!]!\n" +//
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "}";

      database.command("graphql", types);

      try (ResultSet resultSet = database.query("graphql", "{ books }")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertTrue(resultSet.hasNext());
        record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());
        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @Test
  public void queryWhereCondition() {
    executeTest((database) -> {
      final String types = "type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  books(where: WHERE): [Book!]!\n" +//
          "}\n\n" +//
          "type Book {\n" +//
          "  id: String\n" +//
          "  name: String\n" +//
          "  pageCount: Int\n" +//
          "  authors: [Author] @relationship(type: \"IS_AUTHOR_OF\", direction: IN)\n" +//
          "}\n\n" +//
          "type Author {\n" +//
          "  id: String\n" +//
          "  firstName: String\n" +//
          "  lastName: String\n" +//
          "}";

      database.command("graphql", types);

      try (ResultSet resultSet = database.query("graphql", "{ books( where: \"name = 'Mr. brain'\" ) }")) {
        Assertions.assertTrue(resultSet.hasNext());
        Result record = resultSet.next();
        Assertions.assertEquals(4, record.getPropertyNames().size());

        Assertions.assertEquals("book-2", record.getProperty("id"));
        Assertions.assertEquals("Mr. brain", record.getProperty("name"));
        Assertions.assertEquals(422, (Integer) record.getProperty("pageCount"));

        Assertions.assertEquals(1, ((Collection) record.getProperty("authors")).size());

        Assertions.assertFalse(resultSet.hasNext());
      }

      return null;
    });
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  protected void executeTest(final Callable<Void, Database> callback) {
    try (DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      if (factory.exists())
        factory.open().drop();

      final Database database = factory.create();
      try {
        database.transaction(() -> {
          Schema schema = database.getSchema();
          schema.getOrCreateVertexType("Book");
          schema.getOrCreateVertexType("Author");
          schema.getOrCreateEdgeType("IS_AUTHOR_OF");

          MutableVertex author1 = database.newVertex("Author");
          author1.set("id", "author-1");
          author1.set("firstName", "Joanne");
          author1.set("lastName", "Rowling");
          author1.save();

          MutableVertex book1 = database.newVertex("Book");
          book1.set("id", "book-1");
          book1.set("name", "Harry Potter and the Philosopher's Stone");
          book1.set("pageCount", 223);
          book1.save();

          MutableVertex book2 = database.newVertex("Book");
          book2.set("id", "book-2");
          book2.set("name", "Mr. brain");
          book2.set("pageCount", 422);
          book2.save();

          author1.newEdge("IS_AUTHOR_OF", book1, true);
          author1.newEdge("IS_AUTHOR_OF", book2, true);
        });

        database.transaction(() -> {
          callback.call(database);
        });
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }
}
