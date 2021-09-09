package com.orientechnologies.orient.core.storage.cache.chm;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A probabilistic multiset for estimating the popularity of an element within a time window. The
 * maximum frequency of an element is limited to 15 (4-bits) and an aging process periodically
 * halves the popularity of all elements.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class FrequencySketch implements Admittor {

  /*
   * This class maintains a 4-bit CountMinSketch [1] with periodic aging to provide the popularity
   * history for the TinyLfu admission policy [2]. The time and space efficiency of the sketch
   * allows it to cheaply estimate the frequency of an entry in a stream of cache access events.
   *
   * The counter matrix is represented as a single dimensional array holding 16 counters per slot. A
   * fixed depth of four balances the accuracy and cost, resulting in a width of four times the
   * length of the array. To retain an accurate estimation the array's length equals the maximum
   * number of entries in the cache, increased to the closest power-of-two to exploit more efficient
   * bit masking. This configuration results in a confidence of 93.75% and error bound of e / width.
   *
   * The frequency of all entries is aged periodically using a sampling window based on the maximum
   * number of entries in the cache. This is referred to as the reset operation by TinyLfu and keeps
   * the sketch fresh by dividing all counters by two and subtracting based on the number of odd
   * counters found. The O(n) cost of aging is amortized, ideal for hardware prefetching, and uses
   * inexpensive bit manipulations per array location.
   *
   * A per instance smear is used to help protect against hash flooding [3], which would result
   * in the admission policy always rejecting new candidates. The use of a pseudo random hashing
   * function resolves the concern of a denial of service attack by exploiting the hash codes.
   *
   * [1] An Improved Data Stream Summary: The Count-Min Sketch and its Applications
   * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
   * [2] TinyLFU: A Highly Efficient Cache Admission Policy
   * http://arxiv.org/pdf/1512.00727.pdf
   * [3] Denial of Service via Algorithmic Complexity Attack
   * https://www.usenix.org/legacy/events/sec03/tech/full_papers/crosby/crosby.pdf
   */

  private static final long[] SEED =
      new long[] { // A mixture of seeds from FNV-1a, CityHash, and Murmur3
        0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L
      };
  private static final long RESET_MASK = 0x7777777777777777L;
  private static final long ONE_MASK = 0x1111111111111111L;

  private final int randomSeed;

  private int sampleSize;
  private int tableMask;
  private long[] table;
  private int size;

  /**
   * Creates a lazily initialized frequency sketch, requiring {@link #ensureCapacity} be called when
   * the maximum size of the cache has been determined.
   */
  @SuppressWarnings("NullAway.Init")
  FrequencySketch() {
    final int seed = ThreadLocalRandom.current().nextInt();
    this.randomSeed = ((seed & 1) == 0) ? seed + 1 : seed;
  }

  /**
   * Initializes and increases the capacity of this <tt>FrequencySketch</tt> instance, if necessary,
   * to ensure that it can accurately estimate the popularity of elements given the maximum size of
   * the cache. This operation forgets all previous counts when resizing.
   *
   * @param maximumSize the maximum size of the cache
   */
  public void ensureCapacity(final long maximumSize) {
    final int maximum = (int) Math.min(maximumSize, Integer.MAX_VALUE >>> 1);
    if ((table != null) && (table.length >= maximum)) {
      return;
    }

    table = new long[(maximum == 0) ? 1 : ceilingNextPowerOfTwo(maximum)];
    tableMask = Math.max(0, table.length - 1);
    sampleSize = (maximumSize == 0) ? 10 : (10 * maximum);
    if (sampleSize <= 0) {
      sampleSize = Integer.MAX_VALUE;
    }
    size = 0;
  }

  /**
   * Returns the estimated number of occurrences of an element, up to the maximum (15).
   *
   * @param hash the hash code of element to count occurrences of
   * @return the estimated number of occurrences of the element; possibly zero but never negative
   */
  @Override
  public int frequency(int hash) {
    hash = spread(hash);
    final int start = (hash & 3) << 2;
    int frequency = Integer.MAX_VALUE;
    for (int i = 0; i < 4; i++) {
      final int index = indexOf(hash, i);
      final int count = (int) ((table[index] >>> ((start + i) << 2)) & 0xfL);
      frequency = Math.min(frequency, count);
    }
    return frequency;
  }

  /**
   * Increments the popularity of the element if it does not exceed the maximum (15). The popularity
   * of all elements will be periodically down sampled when the observed events exceeds a threshold.
   * This process provides a frequency aging to allow expired long term entries to fade away.
   *
   * @param hash the hash code of element
   */
  @Override
  public void increment(int hash) {
    hash = spread(hash);
    final int start = (hash & 3) << 2;

    // Loop unrolling improves throughput by 5m ops/s
    final int index0 = indexOf(hash, 0);
    final int index1 = indexOf(hash, 1);
    final int index2 = indexOf(hash, 2);
    final int index3 = indexOf(hash, 3);

    boolean added = incrementAt(index0, start);
    added |= incrementAt(index1, start + 1);
    added |= incrementAt(index2, start + 2);
    added |= incrementAt(index3, start + 3);

    if (added && (++size == sampleSize)) {
      reset();
    }
  }

  /**
   * Increments the specified counter by 1 if it is not already at the maximum value (15).
   *
   * @param i the table index (16 counters)
   * @param j the counter to increment
   * @return if incremented
   */
  private boolean incrementAt(final int i, final int j) {
    final int offset = j << 2;
    final long mask = (0xfL << offset);
    if ((table[i] & mask) != mask) {
      table[i] += (1L << offset);
      return true;
    }
    return false;
  }

  /** Reduces every counter by half of its original value. */
  private void reset() {
    int count = 0;
    for (int i = 0; i < table.length; i++) {
      count += Long.bitCount(table[i] & ONE_MASK);
      table[i] = (table[i] >>> 1) & RESET_MASK;
    }
    size = (size >>> 1) - (count >>> 2);
  }

  /**
   * Returns the table index for the counter at the specified depth.
   *
   * @param item the element's hash
   * @param i the counter depth
   * @return the table index
   */
  private int indexOf(final int item, final int i) {
    long hash = SEED[i] * item;
    hash += hash >> 32;
    return ((int) hash) & tableMask;
  }

  /**
   * Applies a supplemental hash function to a given hashCode, which defends against poor quality
   * hash functions.
   */
  private int spread(int x) {
    x = ((x >>> 16) ^ x) * 0x45d9f3b;
    x = ((x >>> 16) ^ x) * randomSeed;
    return (x >>> 16) ^ x;
  }

  private static int ceilingNextPowerOfTwo(final int x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(x - 1));
  }
}
