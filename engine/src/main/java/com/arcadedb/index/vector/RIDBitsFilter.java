package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import io.github.jbellis.jvector.util.Bits;

import java.util.Set;

/**
 * Custom Bits implementation for filtering vector search by RID.
 * Maps graph ordinals to vector IDs, then checks if the corresponding RID is in the allowed set.
 */
public class RIDBitsFilter implements Bits {
  private final Set<RID> allowedRIDs;
  private final int[] ordinalToVectorIdSnapshot;
  private final VectorLocationIndex vectorIndexSnapshot;

  RIDBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
                final VectorLocationIndex vectorIndexSnapshot) {
    this.allowedRIDs = allowedRIDs;
    this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
    this.vectorIndexSnapshot = vectorIndexSnapshot;
  }

  @Override
  public boolean get(final int ordinal) {
    // Check if ordinal is within bounds
    if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length)
      return false;

    // Map ordinal to vector ID
    final int vectorId = ordinalToVectorIdSnapshot[ordinal];

    // Get the RID for this vector ID
    final VectorLocationIndex.VectorLocation loc = vectorIndexSnapshot.getLocation(vectorId);
    if (loc == null || loc.deleted)
      return false;

    // Check if this RID is in the allowed set
    return allowedRIDs.contains(loc.rid);
  }
}
