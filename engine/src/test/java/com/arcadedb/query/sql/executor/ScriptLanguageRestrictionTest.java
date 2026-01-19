package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ScriptLanguageRestrictionTest extends TestHelper {

  @Test
  public void testForeachRejectedInSqlLanguage() {
    assertThrows(CommandSQLParsingException.class, () -> {
      database.command("sql", "FOREACH ($i IN [1,2,3]) { RETURN $i; }");
    });
  }

  @Test
  public void testWhileRejectedInSqlLanguage() {
    assertThrows(CommandSQLParsingException.class, () -> {
      database.command("sql", "WHILE (1 = 1) { RETURN 'test'; }");
    });
  }

  @Test
  public void testForeachWorksInSqlscriptLanguage() {
    database.command("sqlscript", "FOREACH ($i IN [1,2,3]) { RETURN $i; }");
  }

  @Test
  public void testWhileWorksInSqlscriptLanguage() {
    database.command("sqlscript", "LET $i = 0; WHILE ($i < 1) { LET $i = 1; RETURN 'test'; }");
  }
}
