    package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class ImportDatabaseStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("IMPORT DATABASE http://www.foo.bar");
    checkRightSyntax("import database http://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE https://www.foo.bar");
    checkRightSyntax("IMPORT DATABASE file:///foo/bar/");

    checkWrongSyntax("import database ");
    checkWrongSyntax("import database file:///foo/bar/ foo bar");
    checkWrongSyntax("import database http://www.foo.bar asdf ");
    checkWrongSyntax("IMPORT DATABASE https://www.foo.bar asd ");

  }
}
