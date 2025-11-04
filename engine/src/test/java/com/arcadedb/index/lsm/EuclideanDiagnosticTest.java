package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Diagnostic test for EUCLIDEAN metric sorting.
 */
public class EuclideanDiagnosticTest extends TestHelper {

  @Test
  public void diagnoseEuclideanSorting() {
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("euclidean_diagnostic_idx")
          .withDimensions(3)
          .withSimilarity("EUCLIDEAN")
          .create();

      // Test vectors
      final float[] v1 = new float[]{1.0f, 0.0f, 0.0f};
      final float[] v2 = new float[]{1.1f, 0.0f, 0.0f};

      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);

      System.out.println("\n=== EUCLIDEAN Metric Test ===");
      System.out.println("v1 = [1.0, 0.0, 0.0] → rid1 = " + rid1);
      System.out.println("v2 = [1.1, 0.0, 0.0] → rid2 = " + rid2);

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });

      // Manual distance calculations
      System.out.println("\n=== Manual EUCLIDEAN Distance Check ===");

      // distance(v1, v1) = 0
      float dist1 = 0.0f;
      System.out.println("distance(v1, v1) = " + dist1 + " (should be 0.0)");

      // distance(v1, v2) = sqrt((1.0-1.1)^2) = sqrt(0.01) = 0.1
      float diff = 1.0f - 1.1f;
      float dist2 = (float)Math.sqrt(diff * diff);
      System.out.println("distance(v1, v2) = " + dist2 + " (should be 0.1)");

      // Perform KNN search
      System.out.println("\n=== KNN Search for v1 with k=1 ===");
      final List<LSMVectorIndexMutable.VectorSearchResult> results = index.knnSearch(v1, 1);

      System.out.println("Results count: " + results.size());
      for (int i = 0; i < results.size(); i++) {
        final LSMVectorIndexMutable.VectorSearchResult result = results.get(i);
        System.out.println("\nResult " + i + ":");
        System.out.println("  Vector: " + java.util.Arrays.toString(result.vector));
        System.out.println("  Distance: " + result.distance);
        System.out.println("  RIDs: " + result.rids);
      }

      System.out.println("\n=== Expected vs Actual ===");
      System.out.println("Expected result[0]: v1 with distance=0.0, rid=" + rid1);
      if (!results.isEmpty()) {
        System.out.println("Actual result[0]: distance=" + results.get(0).distance + ", rids=" + results.get(0).rids);
      }
    });
  }
}
