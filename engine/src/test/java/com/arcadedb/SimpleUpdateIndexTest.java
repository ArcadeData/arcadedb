package com.arcadedb;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleUpdateIndexTest extends TestHelper {

  @Test
  public void testCompositeIndexUpdate() {
    // CREATE Order type
    final DocumentType orderType = database.getSchema().createDocumentType("Order");
    orderType.createProperty("id", Type.INTEGER);
    orderType.createProperty("status", Type.STRING);

    // CREATE unique index on id (like in the failing test)
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Order", "id");

    // CREATE unique composite index on (status, id) - DISABLE FOR NOW
    // database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Order", "status", "id");

    // INSERT - Test that initial INSERT works
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Order SET id = 1, status = 'PENDING'");
    });

    System.out.println("\n>>> After INSERT:");
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'PENDING'")) {
      System.out.println(">>>   Query found: " + rs.hasNext());
    }

    // Verify can query by status
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'PENDING'")) {
      assertThat(rs.hasNext()).as("After INSERT").isTrue();
      Result r = rs.next();
      assertThat((Integer) r.getProperty("id")).isEqualTo(1);
      assertThat((String) r.getProperty("status")).isEqualTo("PENDING");
    }

    // UPDATE to ERROR
    System.out.println("\n>>> BEFORE transaction UPDATE to ERROR");
    database.transaction(() -> {
      System.out.println(">>> INSIDE transaction UPDATE to ERROR");
      database.command("sql", "UPDATE Order SET status = 'ERROR' WHERE id = 1");
      System.out.println(">>> AFTER command, still in transaction");
    });
    System.out.println(">>> AFTER transaction committed");

    System.out.println("\n>>> After UPDATE to ERROR - querying:");
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'ERROR'")) {
      System.out.println(">>>   Query found: " + rs.hasNext());
    }

    // Verify can query by new status
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'ERROR'")) {
      assertThat(rs.hasNext()).as("After UPDATE to ERROR").isTrue();
      Result r = rs.next();
      assertThat((Integer) r.getProperty("id")).isEqualTo(1);
      assertThat((String) r.getProperty("status")).isEqualTo("ERROR");
    }

    // UPDATE back to PENDING
    database.transaction(() -> {
      database.command("sql", "UPDATE Order SET status = 'PENDING' WHERE id = 1");
    });

    // Try direct query to see what we get
    System.out.println("\n>>> Attempting direct SELECT after UPDATE:");
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'PENDING'")) {
      System.out.println(">>>   hasNext: " + rs.hasNext());
      if (rs.hasNext()) {
        Result r = rs.next();
        System.out.println(">>>   Found: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
      }
    }

    // Try scanning without index
    System.out.println("\n>>> Attempting table scan:");
    try (ResultSet rs = database.query("sql", "SELECT FROM Order")) {
      while (rs.hasNext()) {
        Result r = rs.next();
        System.out.println(">>>   Record: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
      }
    }

    // List all indexes and their contents
    System.out.println("\n>>> Checking indexes after UPDATE back to PENDING:");
    for (com.arcadedb.index.Index idx : database.getSchema().getIndexes()) {
      if (idx.getTypeName() != null && idx.getTypeName().equals("Order")) {
        System.out.println(">>>   Index: " + idx.getName() + " (unique=" + idx.isUnique() + ", props=" +
            idx.getPropertyNames() + ")");
        try {
          if (idx instanceof com.arcadedb.index.RangeIndex) {
            com.arcadedb.index.RangeIndex rangeIdx = (com.arcadedb.index.RangeIndex) idx;
            com.arcadedb.index.IndexCursor cursor = rangeIdx.iterator(true);
            int count = 0;
            while (cursor.hasNext() && count < 10) {
              com.arcadedb.database.Identifiable id = cursor.next();
              System.out.println(">>>     Entry: " + id.getIdentity());
              count++;
            }
            if (count == 0) {
              System.out.println(">>>     (empty)");
            }
          } else {
            System.out.println(">>>     (not a RangeIndex)");
          }
        } catch (Exception e) {
          System.out.println(">>>     Error iterating: " + e.getMessage());
        }
      }
    }

    // Verify can query by original status again - THIS IS WHERE IT FAILS
    try (ResultSet rs = database.query("sql", "SELECT FROM Order WHERE status = 'PENDING'")) {
      assertThat(rs.hasNext()).as("Should find record with status='PENDING'").isTrue();
      Result r = rs.next();
      assertThat((Integer) r.getProperty("id")).isEqualTo(1);
      assertThat((String) r.getProperty("status")).isEqualTo("PENDING");
    }
  }
}
