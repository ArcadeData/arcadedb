package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

public class BackupDatabaseStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("BACKUP DATABASE http://www.foo.bar");
    checkRightSyntax("backup database http://www.foo.bar");
    checkRightSyntax("BACKUP DATABASE https://www.foo.bar");
    checkRightSyntax("BACKUP DATABASE file:///foo/bar/");
    checkRightSyntax("backup database "); // USE THE DEFAULT FILE NAME

    checkWrongSyntax("backup database file:///foo/bar/ foo bar");
    checkWrongSyntax("backup database http://www.foo.bar asdf ");
    checkWrongSyntax("BACKUP DATABASE https://www.foo.bar asd ");
  }
}
