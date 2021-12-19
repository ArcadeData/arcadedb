package com.arcadedb.graphql;

import com.arcadedb.database.Database;

public class GraphQLSQLDirectivesTest extends AbstractGraphQLNativeLanguageDirectivesTest {
  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.command("graphql", "type Query {\n" +//
        "  bookById(id: String): Book\n" +//
        "  bookByName(bookNameParameter: String): Book @sql(statement: \"select from Book where name = :bookNameParameter\")\n" +//
        "}");
  }
}
