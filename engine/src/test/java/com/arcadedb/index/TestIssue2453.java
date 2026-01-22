package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TestIssue2453 extends TestHelper {
  @Test
  void testIsNullWithDefaultSkipStrategy() {
    // Reproduce issue #2453
    database.command("SQL", "CREATE VERTEX TYPE vec");
    database.command("SQL", "CREATE PROPERTY vec.lnk LINK");
    database.command("SQL", "CREATE INDEX ON vec (lnk) NOTUNIQUE");  // Default SKIP strategy

    database.transaction(() -> {
      database.command("SQL", "INSERT INTO vec");
      database.command("SQL", "INSERT INTO vec");
      database.command("SQL", "INSERT INTO vec");
    });

    // This should return 3 records, but issue #2453 says it returns 0
    database.transaction(() -> {
      final var result = database.query("SQL", "SELECT FROM vec WHERE lnk IS NULL");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      System.out.println("Records found with IS NULL: " + count);
      assertThat(count).isEqualTo(3);
    });
  }
}
