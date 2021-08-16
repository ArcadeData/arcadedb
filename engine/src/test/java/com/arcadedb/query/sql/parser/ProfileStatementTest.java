package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class ProfileStatementTest extends ParserTestAbstract {

  @Test
  public void test() {
    checkRightSyntax("profile select from V");
    checkRightSyntax("profile MATCH {as:v, type:V} RETURN $elements");
    checkRightSyntax("profile UPDATE V SET name = 'foo'");
    checkRightSyntax("profile INSERT INTO V SET name = 'foo'");
    checkRightSyntax("profile DELETE FROM Foo");
  }
}
