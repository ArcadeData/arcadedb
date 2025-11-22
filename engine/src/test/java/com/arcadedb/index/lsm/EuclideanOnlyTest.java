package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test EUCLIDEAN metric in isolation.
 */
public class EuclideanOnlyTest extends TestHelper {

  @Test
  public void testEuclideanOnly() {
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = schema.buildLSMVectorIndex("TestVector", "embedding")
          .withIndexName("test_euclidean_only_idx")
          .withDimensions(3)
          .withSimilarity(VectorSimilarityFunction.EUCLIDEAN)
          .create();

      // Create test vectors
      final float[] v1 = new float[3];
      final float[] v2 = new float[3];
      v1[0] = 1.0f;
      v2[0] = 1.1f;

      final RID rid1 = new RID(database, 1, 0);
      final RID rid2 = new RID(database, 1, 1);

      index.put(new Object[] { v1 }, new RID[] { rid1 });
      index.put(new Object[] { v2 }, new RID[] { rid2 });

      // KNN search should return v1 first (distance 0.0)
      final List<LSMVectorIndexMutable.VectorSearchResult> results = index.knnSearch(v1, 1);

      assertThat(results).isNotEmpty();
      assertThat(results.getFirst().rids).contains(rid1);
    });
  }
}
