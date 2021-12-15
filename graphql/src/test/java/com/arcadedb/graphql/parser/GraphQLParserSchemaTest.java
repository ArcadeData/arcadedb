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

    ast.dump("-");
  }
}
