package com.arcadedb.index.vector;

import java.util.Arrays;

/**
 * Comparable wrapper for float[] to use in transaction tracking.
 * Vectors are compared by their hash code for uniqueness in the transaction map.
 */
public class ComparableVector implements Comparable<ComparableVector> {
  final float[] vector;
  final int hashCode;

  ComparableVector(final float[] vector) {
    this.vector = vector;
    this.hashCode = Arrays.hashCode(vector);
  }

  @Override
  public int compareTo(final ComparableVector other) {
    // First compare by hash code for performance
    final int hashCompare = Integer.compare(this.hashCode, other.hashCode);
    if (hashCompare != 0)
      return hashCompare;

    // If hash codes are equal, perform lexicographical comparison of vector elements
    // to maintain the Comparable contract: compareTo == 0 iff equals == true
    final int minLength = Math.min(this.vector.length, other.vector.length);
    for (int i = 0; i < minLength; i++) {
      final int elementCompare = Float.compare(this.vector[i], other.vector[i]);
      if (elementCompare != 0)
        return elementCompare;
    }
    // If all compared elements are equal, compare by length
    return Integer.compare(this.vector.length, other.vector.length);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ComparableVector other))
      return false;
    return Arrays.equals(vector, other.vector);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
