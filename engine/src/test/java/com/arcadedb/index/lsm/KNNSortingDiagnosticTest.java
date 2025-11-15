package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Diagnostic test to analyze KNN sorting problem.
 */
public class KNNSortingDiagnosticTest extends TestHelper {

  @Test
  public void diagnoseKNNSorting() {
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("diagnostic_idx")
          .withDimensions(3)
          .withSimilarity(VectorSimilarityFunction.COSINE)
          .create();

      // Test vectors
      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);
      final RID rid3 = new RID(database, 1, 2);

      final float[] v1 = { 1.0f, 0.0f, 0.0f };
      final float[] v2 = { 0.9f, 0.1f, 0.0f };
      final float[] v3 = { 0.0f, 1.0f, 0.0f };

      System.out.println("\n=== Inserting vectors ===");
      System.out.println("v1 = [1.0, 0.0, 0.0] → rid1 = " + rid1);
      System.out.println("v2 = [0.9, 0.1, 0.0] → rid2 = " + rid2);
      System.out.println("v3 = [0.0, 1.0, 0.0] → rid3 = " + rid3);

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });
      index.put(new Object[] { v3 }, new RID[] { rid3 });

      // Manual cosine similarity calculations
      System.out.println("\n=== Manual COSINE Similarity Check ===");

      // cos(v1, v1)
      float dot1 = 1.0f * 1.0f;
      float norm1_1 = (float)Math.sqrt(1.0f);
      float norm1_2 = (float)Math.sqrt(1.0f);
      float cos1 = dot1 / (norm1_1 * norm1_2);
      System.out.println("cos(v1, v1) = " + cos1 + " (should be 1.0)");

      // cos(v1, v2)
      float dot2 = 1.0f * 0.9f;
      float norm2_1 = (float)Math.sqrt(1.0f);
      float norm2_2 = (float)Math.sqrt(0.9f*0.9f + 0.1f*0.1f);
      float cos2 = dot2 / (norm2_1 * norm2_2);
      System.out.println("cos(v1, v2) = " + cos2 + " (should be ~0.993)");

      // cos(v1, v3)
      float dot3 = 0.0f;
      float norm3_1 = (float)Math.sqrt(1.0f);
      float norm3_2 = (float)Math.sqrt(1.0f);
      float cos3 = dot3 / (norm3_1 * norm3_2);
      System.out.println("cos(v1, v3) = " + cos3 + " (should be 0.0)");

      // Perform KNN search
      System.out.println("\n=== KNN Search for v1 = [1.0, 0.0, 0.0] ===");
      final List<LSMVectorIndexMutable.VectorSearchResult> results = index.knnSearch(v1, 2);

      System.out.println("Results count: " + results.size());
      for (int i = 0; i < results.size(); i++) {
        final LSMVectorIndexMutable.VectorSearchResult result = results.get(i);
        System.out.println("\nResult " + i + ":");
        System.out.println("  Vector: " + java.util.Arrays.toString(result.vector));
        System.out.println("  Distance/Similarity: " + result.distance);
        System.out.println("  RIDs: " + result.rids);
      }

      System.out.println("\n=== Expected vs Actual ===");
      System.out.println("Expected result[0].rids: " + rid1 + " (v1, similarity=1.0)");
      System.out.println("Actual result[0].rids: " + results.get(0).rids);
      System.out.println("Expected result[1].rids: " + rid2 + " (v2, similarity~0.993)");
      System.out.println("Actual result[1].rids: " + results.get(1).rids);
    });
  }
}
