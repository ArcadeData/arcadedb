package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

public class DebugUpdateIndexTest3 extends TestHelper {

  @Test
  public void testSelectAfterUpdateDebugWithIndexInspection() {
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
    }
    database.commit();

    // update order to ERROR
    database.begin();
    String UPDATE_ORDER = "UPDATE Order SET status = ? WHERE id = ?";
    try (ResultSet resultSet = database.command("sql", UPDATE_ORDER, "ERROR", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    database.commit();

    // resubmit order 1 (change back to PENDING) - THE PROBLEMATIC UPDATE
    database.begin();
    System.out.println("\n=== STARTING RESUBMIT TRANSACTION ===");

    String RESUBMIT_ORDER = "UPDATE Order SET status = ? WHERE (status != ? AND status != ?) AND id = ?";
    try (ResultSet resultSet = database.command("sql", RESUBMIT_ORDER, "PENDING", "PENDING", "PROCESSING", 1)) {
      assertThat(resultSet.hasNext()).isTrue();
    }

    // Inspect TX context BEFORE commit
    System.out.println("\n=== TX CONTEXT BEFORE COMMIT ===");
    inspectTransactionContext();

    database.commit();

    // Try the composite index query
    System.out.println("\n=== QUERY BY STATUS='PENDING' ===");
    try (ResultSet resultSet = database.query("sql", "SELECT id, status FROM Order WHERE status = 'PENDING'")) {
      if (resultSet.hasNext()) {
        Result r = resultSet.next();
        System.out.println("Found: id=" + r.getProperty("id") + ", status=" + r.getProperty("status"));
        assertThat(true).isTrue();
      } else {
        System.out.println("NOT FOUND! Bug confirmed.");
        assertThat(false).as("Should find the PENDING record").isTrue();
      }
    }
  }

  private void inspectTransactionContext() {
    var txContext = ((DatabaseInternal) database).getTransaction().getIndexChanges();
    Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> indexEntries = txContext.toMap();

    if (indexEntries.isEmpty()) {
      System.out.println("(TX context is empty)");
      return;
    }

    for (Map.Entry<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> entry : indexEntries.entrySet()) {
      System.out.println("Index: " + entry.getKey());
      for (Map.Entry<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> keyEntry : entry.getValue().entrySet()) {
        System.out.println("  ComparableKey: " + java.util.Arrays.toString(keyEntry.getKey().values));
        for (TransactionIndexContext.IndexKey indexKey : keyEntry.getValue().values()) {
          System.out.println("    Operation: " + indexKey.operation + ", KeyValues: " + java.util.Arrays.toString(indexKey.keyValues) + ", RID: " + indexKey.rid);
        }
      }
    }
  }
}
