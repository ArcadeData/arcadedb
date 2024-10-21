package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class JavaMethods {
  public JavaMethods() {
  }

  public int sum(final int a, final int b) {
    return a + b;
  }

  public static int SUM(final int a, final int b) {
    return a + b;
  }

  public static void hello() {
    // EMPTY METHOD
  }
}

public class JavaQueryTest extends TestHelper {
  @Test
  public void testRegisteredMethod() {
    assertThat(database.getQueryEngine("java").getLanguage()).isEqualTo("java");

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  public void testRegisteredMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::SUM");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  public void testRegisteredClass() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  public void testUnRegisteredMethod() {
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof SecurityException).isTrue();
    }
  }

  @Test
  public void testNotExistentMethod() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::totallyInvented", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof NoSuchMethodException).isTrue();
    }
  }

  @Test
  public void testAnalyzeQuery() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("java").analyze("com.arcadedb.query.java.JavaMethods::totallyInvented");
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  public void testUnsupportedMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.query("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      fail("");
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      final HashMap map = new HashMap();
      map.put("name", 1);
      database.getQueryEngine("java").command("com.arcadedb.query.java.JavaMethods::hello", null, map);
      fail("");
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.getQueryEngine("java").query("com.arcadedb.query.java.JavaMethods::sum", null, new HashMap<>());
      fail("");
    } catch (final UnsupportedOperationException e) {
      // EXPECTED
    }
  }
}
