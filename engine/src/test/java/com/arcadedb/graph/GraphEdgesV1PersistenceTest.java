package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphEdgesV1PersistenceTest extends TestHelper {
  private static final String VERTEX_TYPE = "TestVertex";
  private static final String EDGE_TYPE1 = "TestEdge1";

  @Override
  public void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(VERTEX_TYPE))
        database.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(1).create();

      if (!database.getSchema().existsType(EDGE_TYPE1))
        database.getSchema().buildEdgeType().withName(EDGE_TYPE1).withTotalBuckets(1).create();
    });
  }

  @Test
  public void debugPersistence() {
    final RID[] v1RIDHolder = new RID[1];

    // Transaction 1: Create vertices and edges
    database.transaction(() -> {
      // System.out.println("\n=== Transaction 1: Creating vertices and edges ===");

      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).save();
      // System.out.println("v1 RID: " + v1.getIdentity());
      // System.out.println("v1 outEdgeBuckets before edges: " + v1.getOutEdgeBuckets());

      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).save();
      // System.out.println("Created v2 RID: " + v2.getIdentity());

      // Add edge
      final MutableEdge edge = v1.newEdge(EDGE_TYPE1, v2).save();
      // System.out.println("Created edge, RID: " + edge.getIdentity() + ", bucket: " + edge.getIdentity().getBucketId());

      // Check v1 after edge creation
      // System.out.println("v1 outEdgeBuckets after edge: " + v1.getOutEdgeBuckets());
      // System.out.println("v1 dirty: " + v1.isDirty());

      // Count edges before commit
      final long countBefore = v1.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1);
      // System.out.println("Edge count before commit: " + countBefore);
      assertThat(countBefore).isEqualTo(1);

      v1RIDHolder[0] = v1.getIdentity();
    });

    final RID v1RID = v1RIDHolder[0];
    // System.out.println("\n=== Transaction 1 committed ===");

    // Transaction 2: Reload and check
    database.transaction(() -> {
      // System.out.println("\n=== Transaction 2: Reloading vertex ===");

      final Vertex v1Reloaded = v1RID.asVertex();
      // System.out.println("Reloaded v1, RID: " + v1Reloaded.getIdentity());
      // System.out.println("Reloaded v1 class: " + v1Reloaded.getClass().getSimpleName());

      final MutableVertex mv = (MutableVertex) v1Reloaded.modify();
      // System.out.println("outEdgeBuckets: " + mv.getOutEdgeBuckets());

      // Check if we can get the edge segment
      for (final Integer bucketId : mv.getOutEdgeBuckets()) {
        // System.out.println("Bucket ID: " + bucketId);
        final RID headChunk = mv.getOutEdgesHeadChunk(bucketId);
        // System.out.println("  Head chunk RID: " + headChunk);
        if (headChunk != null) {
          try {
            final EdgeSegment segment = (EdgeSegment) database.lookupByRID(headChunk, true);
            // System.out.println("  EdgeSegment found: " + segment);
            // System.out.println("  EdgeSegment total count: " + segment.getTotalCount());
          } catch (Exception e) {
            // System.out.println("  Error loading EdgeSegment: " + e.getMessage());
          }
        }
      }

      // Try counting edges
      final long countAfter = v1Reloaded.countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE1);
      // System.out.println("Edge count after reload: " + countAfter);

      // Also try without type filter
      final long countAllAfter = v1Reloaded.countEdges(Vertex.DIRECTION.OUT);
      // System.out.println("All edge count after reload: " + countAllAfter);

      assertThat(countAfter).isEqualTo(1);
    });
  }
}
