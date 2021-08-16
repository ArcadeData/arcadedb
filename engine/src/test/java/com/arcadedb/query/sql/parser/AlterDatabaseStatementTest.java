package com.arcadedb.query.sql.parser;

public class AlterDatabaseStatementTest extends ParserTestAbstract {

  //@Test
  public void testPlain() {
    checkRightSyntax("ALTER DATABASE BUCKETSELECTION 'default'");
    checkRightSyntax("alter database BUCKETSELECTION 'default'");

    checkWrongSyntax("alter database ");
    checkWrongSyntax("alter database bar baz zz");
  }
}
