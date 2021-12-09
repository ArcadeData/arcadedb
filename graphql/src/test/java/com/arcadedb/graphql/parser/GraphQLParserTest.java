package com.arcadedb.graphql.parser;

import org.junit.jupiter.api.Test;

public class GraphQLParserTest {

 @Test
 public void test() throws ParseException {
 GraphQLParser.parse("{ hero { name friends { name } }}");
 }
}
