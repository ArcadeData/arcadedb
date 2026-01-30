package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class ScriptLanguageRestrictionTest extends TestHelper {

  @Test
  void foreachRejectedInSqlLanguage() {
    assertThatExceptionOfType(CommandSQLParsingException.class).isThrownBy(() ->
        database.command("sql", "FOREACH ($i IN [1,2,3]) { RETURN $i; }"));
  }

  @Test
  void whileRejectedInSqlLanguage() {
    assertThatExceptionOfType(CommandSQLParsingException.class).isThrownBy(() ->
        database.command("sql", "WHILE (1 = 1) { RETURN 'test'; }"));
  }

  @Test
  void foreachWorksInSqlscriptLanguage() {
    database.command("sqlscript", "FOREACH ($i IN [1,2,3]) { RETURN $i; }");
  }

  @Test
  void whileWorksInSqlscriptLanguage() {
    database.command("sqlscript", "LET $i = 0; WHILE ($i < 1) { LET $i = 1; RETURN 'test'; }");
  }
}
