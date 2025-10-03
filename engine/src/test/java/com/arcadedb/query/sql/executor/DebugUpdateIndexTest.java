package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DebugUpdateIndexTest extends TestHelper {

  @Test
  public void testSelectAfterUpdateDebug() {
    database.transaction(() -> {
      DocumentType dtOrders = database.getSchema().buildDocumentType().withName("Order").create();
      dtOrders.createProperty("id", Type.INTEGER);
      dtOrders.createProperty("processor", Type.STRING);
      dtOrders.createProperty("status", Type.STRING);
      dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      dtOrders.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "status", "id");
      dtOrders.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
    });

    String INSERT_ORDER = "INSERT INTO Order SET id = ?, status = ?";
    database.begin();
    try (ResultSet resultSet = database.command("sql", INSERT_ORDER, 1, "PENDING")) {
      assertThat(resultSet.hasNext()).isTrue();
      System.out.println("INSERTED: " + resultSet.next());
    }
    database.commit();

    // Verify initial state
    System.out.println("\n=== AFTER INSERT ===");
    try (ResultSet resultSet = database.query("sql", "SELECT id, status FROM Order WHERE id = 1")) {
      assertThat(resultSet.hasNext()).isTrue();
      Result r = resultSet.next();
      System.out.println("Record: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
    }

    // update order to ERROR
    database.begin();
    String UPDATE_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE id = ?";
    try (ResultSet resultSet = database.command("sql", UPDATE_ORDER, "ERROR", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
      Result r = resultSet.next();
      System.out.println("\n=== AFTER UPDATE TO ERROR ===");
      System.out.println("Updated: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
    }
    database.commit();

    // Verify ERROR state
    System.out.println("\n=== VERIFY ERROR STATE ===");
    try (ResultSet resultSet = database.query("sql", "SELECT id, status FROM Order WHERE id = 1")) {
      assertThat(resultSet.hasNext()).isTrue();
      Result r = resultSet.next();
      System.out.println("Record: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
    }

    // resubmit order 1 (change back to PENDING)
    database.begin();
    String RESUBMIT_ORDER = "UPDATE Order SET status = ? RETURN AFTER WHERE (status != ? AND status != ?) AND id = ?";
    try (ResultSet resultSet = database.command("sql", RESUBMIT_ORDER, "PENDING", "PENDING", "PROCESSING", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
      Result r = resultSet.next();
      System.out.println("\n=== AFTER RESUBMIT TO PENDING ===");
      System.out.println("Resubmitted: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
    }
    database.commit();

    // Verify PENDING state after resubmit
    System.out.println("\n=== VERIFY PENDING STATE (by ID) ===");
    try (ResultSet resultSet = database.query("sql", "SELECT id, status FROM Order WHERE id = 1")) {
      assertThat(resultSet.hasNext()).isTrue();
      Result r = resultSet.next();
      System.out.println("Record: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
    }

    // Try querying by status alone
    System.out.println("\n=== QUERY BY STATUS='PENDING' ===");
    try (ResultSet resultSet = database.query("sql", "SELECT id, status FROM Order WHERE status = 'PENDING'")) {
      if (resultSet.hasNext()) {
        Result r = resultSet.next();
        System.out.println("Found: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
      } else {
        System.out.println("NOT FOUND!");
      }
    }

    // Try the composite index query
    System.out.println("\n=== COMPOSITE INDEX QUERY (status='PENDING' OR status='READY') ===");
    String RETRIEVE_NEXT_ELIGIBLE_ORDERS = "SELECT id, status FROM Order WHERE status = 'PENDING' OR status = 'READY' ORDER BY id ASC LIMIT 1";
    try (ResultSet resultSet = database.query("sql", RETRIEVE_NEXT_ELIGIBLE_ORDERS)) {
      if (resultSet.hasNext()) {
        Result r = resultSet.next();
        System.out.println("Found: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
      } else {
        System.out.println("NOT FOUND! This is the bug.");
      }
      assertThat(resultSet.hasNext()).as("Should find the PENDING record").isTrue();
    }
  }
}
