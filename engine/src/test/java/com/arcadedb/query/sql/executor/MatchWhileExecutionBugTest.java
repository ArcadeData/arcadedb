package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal test case to reproduce MATCH with while clause execution bug.
 *
 * The bug: MATCH queries with while clauses return correct results when executed standalone,
 * but return 0 results when nested in SELECT FROM (MATCH...) subqueries.
 *
 * Expected: Both test methods should pass
 * Actual on sql-antlr: testMatchWithWhileStandalone passes, testMatchWithWhileInSubquery fails
 * Actual on main: Both tests pass
 */
public class MatchWhileExecutionBugTest extends TestHelper {

  public MatchWhileExecutionBugTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    // Minimal schema setup
    database.command("sql", "CREATE VERTEX TYPE Employee");
    database.command("sql", "CREATE VERTEX TYPE Department");
    database.command("sql", "CREATE EDGE TYPE WorksAt");
    database.command("sql", "CREATE EDGE TYPE ManagerOf");
    database.command("sql", "CREATE EDGE TYPE ParentDepartment");

    // Minimal data setup matching the original test:
    // p10 works at department7
    // department7 -> ParentDepartment -> department3 (no manager)
    // department3 -> ParentDepartment -> department1 (has manager "c")
    // The while clause should traverse through department3 (no manager) and find department7 (has manager "c")

    database.command("sql", "CREATE VERTEX Employee SET name = 'p10'");
    database.command("sql", "CREATE VERTEX Employee SET name = 'c'");

    database.command("sql", "CREATE VERTEX Department SET name = 'department7'");
    database.command("sql", "CREATE VERTEX Department SET name = 'department3'");
    database.command("sql", "CREATE VERTEX Department SET name = 'department1'");

    // p10 works at department7
    database.command("sql",
        """
        CREATE EDGE WorksAt FROM (SELECT FROM Employee WHERE name = 'p10') \
        TO (SELECT FROM Department WHERE name = 'department7')""");

    // c manages department7 (this is the manager we're looking for)
    database.command("sql",
        """
        CREATE EDGE ManagerOf FROM (SELECT FROM Employee WHERE name = 'c') \
        TO (SELECT FROM Department WHERE name = 'department7')""");

    // Department hierarchy: 7 -> 3 -> 1
    database.command("sql",
        """
        CREATE EDGE ParentDepartment FROM (SELECT FROM Department WHERE name = 'department7') \
        TO (SELECT FROM Department WHERE name = 'department3')""");

    database.command("sql",
        """
        CREATE EDGE ParentDepartment FROM (SELECT FROM Department WHERE name = 'department3') \
        TO (SELECT FROM Department WHERE name = 'department1')""");

    database.commit();
    database.begin();
  }

  /**
   * Test MATCH with while clause executed standalone.
   * This test PASSES on both main and sql-antlr branches.
   */
  @Test
  void matchWithWhileStandalone() {
    String query = """
        MATCH {type:Employee, where: (name = 'p10')}\
        .out('WorksAt')\
        .out('ParentDepartment'){\
            while: (in('ManagerOf').size() == 0),\
            where: (in('ManagerOf').size() > 0)\
        }\
        .in('ManagerOf'){as: manager}\
        RETURN manager""";

    ResultSet rs = database.query("sql", query);

    assertThat(rs.hasNext())
        .as("MATCH with while clause should return 1 result when executed standalone")
        .isTrue();

    Result result = rs.next();
    assertThat(result).isNotNull();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /**
   * Test MATCH with while clause nested in SELECT FROM subquery.
   * This test PASSES on main branch but FAILS on sql-antlr branch.
   *
   * The bug: The exact same MATCH query returns 0 results when nested in SELECT FROM (MATCH...)
   */
  @Test
  void matchWithWhileInSubquery() {
    String query = """
        SELECT expand(manager) FROM (\
        MATCH {type:Employee, where: (name = 'p10')}\
        .out('WorksAt')\
        .out('ParentDepartment'){\
            while: (in('ManagerOf').size() == 0),\
            where: (in('ManagerOf').size() > 0)\
        }\
        .in('ManagerOf'){as: manager}\
        RETURN manager)""";

    ResultSet rs = database.query("sql", query);

    boolean hasNext = rs.hasNext();
    assertThat(hasNext)
        .as("MATCH with while clause should return 1 result when nested in SELECT FROM subquery")
        .isTrue();

    Result result = rs.next();
    assertThat(result).isNotNull();
    assertThat(result.<String>getProperty("name")).isEqualTo("c");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /**
   * Test the full nested SELECT query from the original failing test.
   * This test PASSES on main branch but FAILS on sql-antlr branch.
   */
  @Test
  void nestedSelectWithMatchAndWhile() {
    String query = """
        SELECT @type FROM (\
        SELECT expand(manager) FROM (\
        MATCH {type:Employee, where: (name = 'p10')}\
        .out('WorksAt')\
        .out('ParentDepartment'){\
            while: (in('ManagerOf').size() == 0),\
            where: (in('ManagerOf').size() > 0)\
        }\
        .in('ManagerOf'){as: manager}\
        RETURN manager))""";

    ResultSet rs = database.query("sql", query);

    boolean hasNext = rs.hasNext();
    assertThat(hasNext)
        .as("Nested SELECT with MATCH and while clause should return 1 result")
        .isTrue();

    Result result = rs.next();
    assertThat(result).isNotNull();
    assertThat(result.<String>getProperty("@type")).isEqualTo("Employee");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /**
   * Control test: MATCH WITHOUT while clause in subquery.
   * This test PASSES on both branches - proves the bug is specific to while clause.
   */
  @Test
  void matchWithoutWhileInSubquery() {
    String query = """
        SELECT expand(manager) FROM (\
        MATCH {type:Employee, where: (name = 'p10')}\
        .out('WorksAt')\
        .in('ManagerOf'){as: manager}\
        RETURN manager)""";

    ResultSet rs = database.query("sql", query);

    boolean hasNext = rs.hasNext();
    assertThat(hasNext)
        .as("MATCH without while clause should work correctly in subquery")
        .isTrue();

    Result result = rs.next();
    assertThat(result).isNotNull();
    assertThat(result.<String>getProperty("name")).isEqualTo("c");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }
}
