package com.arcadedb.graphql.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GraphQLParserSchemaTest {
  @Test
  public void testTypes() throws ParseException {
    final Document ast = GraphQLParser.parse("type Query {\n" +//
        "  bookById(id: ID): Book\n" +//
        "}\n" +//
        "\n" +//
        "type Book {\n" +//
        "  id: ID\n" +//
        "  name: String\n" +//
        "  pageCount: Int\n" + "  author: Author\n" +//
        "}\n" +//
        "\n" +//
        "type Author {\n" +//
        "  id: ID\n" +//
        "  firstName: String\n" +//
        "  lastName: String\n" +//
        "}");

    Assertions.assertTrue(ast.children.length > 0);
  }

  @Test
  public void errorInvalidCharacters() {
    try {
      final Document ast = GraphQLParser.parse("type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  bookByName(name: String): Book { id name pageCount authors }\n" +//
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
          "  wrote: [Book] @relationship(type: \"IS_AUTHOR_OF\", direction: OUT)\n" +//
          "} dsfjsd fjsdkjf sdk");
      Assertions.fail(ast.treeToString(""));
    } catch (ParseException e) {
      // EXPECTED
    }
  }

  @Test
  public void errorInvalidCharactersWithDirective() {
    try {
      final Document ast = GraphQLParser.parse("type Query {\n" +//
          "  bookById(id: String): Book\n" +//
          "  bookByName(name: String): Book @sql(statement: \"select from Book where name = :name\", a = 3 ) { id name pageCount authors }\n" +//
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
          "  wrote: [Book] @relationship(type: \"IS_AUTHOR_OF\", direction: OUT)\n" +//
          "} dsfjsd fjsdkjf sdk");
      Assertions.fail(ast.treeToString(""));
    } catch (ParseException e) {
      // EXPECTED
    }
  }
}
