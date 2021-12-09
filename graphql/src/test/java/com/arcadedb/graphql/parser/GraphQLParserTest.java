package com.arcadedb.graphql.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GraphQLParserTest {
  @Test
  public void testBasic() throws ParseException {
    GraphQLParser.parse("{ hero { name friends { name } }}");
  }

  @Test
  public void testLookup() throws ParseException {
    final Document ast = GraphQLParser.parse("{ bookById(id: \"book-1\"){" +//
        "  id" +//
        "      name" +//
        "  pageCount" +//
        "  author {" +//
        "    firstName" +//
        "        lastName" +//
        "  }" +//
        "}" +//
        "}");

    Assertions.assertTrue(ast.children.length > 0);

    ast.dump("-");
  }
}
