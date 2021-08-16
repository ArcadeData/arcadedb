package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class CreatePropertyStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("CREATE PROPERTY Foo.bar STRING");
    checkRightSyntax("create property Foo.bar STRING");

    checkRightSyntax("CREATE PROPERTY Foo.bar LINK Bar");
    checkRightSyntax("CREATE PROPERTY Foo.bar LINK Bar unsafe");

    checkRightSyntax("CREATE PROPERTY `Foo bar`.`bar baz` LINK Bar unsafe");

    checkRightSyntax(
        "CREATE PROPERTY Foo.bar Integer (MANDATORY, READONLY, NOTNULL, MAX 5, MIN 3, DEFAULT 7) UNSAFE");
    checkRightSyntax(
        "CREATE PROPERTY Foo.bar Integer (MANDATORY, READONLY, NOTNULL, MAX 5, MIN 3, DEFAULT 7)");

    checkRightSyntax(
        "CREATE PROPERTY Foo.bar LINK Bar (MANDATORY, READONLY, NOTNULL, MAX 5, MIN 3, DEFAULT 7)");

    checkRightSyntax(
        "CREATE PROPERTY Foo.bar LINK Bar (MANDATORY true, READONLY false, NOTNULL true, MAX 5, MIN 3, DEFAULT 7) UNSAFE");
  }

  @Test
  public void testIfNotExists() {
    checkRightSyntax("CREATE PROPERTY Foo.bar if not exists STRING");
    checkWrongSyntax("CREATE PROPERTY Foo.bar if exists STRING");
    checkWrongSyntax("CREATE PROPERTY Foo.bar if not exists");
  }
}
