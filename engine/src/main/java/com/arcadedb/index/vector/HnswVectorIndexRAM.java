package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.schema.VectorIndexBuilder;
import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.Index;
import com.github.jelmerk.knn.Item;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.SizeLimitExceededException;
import com.github.jelmerk.knn.util.ArrayBitSet;
import com.github.jelmerk.knn.util.ClassLoaderObjectInputStream;
import com.github.jelmerk.knn.util.GenericObjectPool;
import com.github.jelmerk.knn.util.Murmur3;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * This work is derived from the excellent work made by Jelmer Kuperus on https://github.com/jelmerk/hnswlib. We forked the entire class only because it was
 * not extensible (private members).
 * <p>
 * Implementation of {@link Index} that implements the hnsw algorithm.
 *
 * @param <TId>       Type of the external identifier of an item
 * @param <TVector>   Type of the vector to perform distance calculation on
 * @param <TItem>     Type of items stored in the index
 * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
 *
 * @see <a href="https://arxiv.org/abs/1603.09320">
 * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
public class HnswVectorIndexRAM<TId, TVector, TItem extends Item<TId, TVector>, TDistance> implements Index<TId, TVector, TItem, TDistance> {

  private static final byte VERSION_1 = 0x01;

  private static final long serialVersionUID = 1L;

  private static final int NO_NODE_ID = -1;

  private DistanceFunction<TVector, TDistance> distanceFunction;
  private Comparator<TDistance>                distanceComparator;
  private MaxValueComparator<TDistance>        maxValueDistanceComparator;

  private int    dimensions;
  private int    maxItemCount;
  private int    m;
  private int    maxM;
  private int    maxM0;
  private double levelLambda;
  private int    ef;
  private int    efConstruction;
  int nodeCount;

  protected volatile Node<TItem> entryPoint;

  private AtomicReferenceArray<Node<TItem>> nodes;
  private MutableObjectIntMap<TId>          lookup;
  private Map<TId, Object>                  locks;

  private ReentrantLock globalLock;

  private GenericObjectPool<ArrayBitSet> visitedBitSetPool;

  private ArrayBitSet excludedCandidates;

  private HnswVectorIndexRAM(final Builder<TId, TVector, TItem, TDistance> builder) {
    this.dimensions = builder.dimensions;
    this.maxItemCount = builder.maxItemCount;
    this.distanceFunction = builder.distanceFunction;
    this.distanceComparator = builder.distanceComparator;
    this.maxValueDistanceComparator = new MaxValueComparator<>(this.distanceComparator);

    this.m = builder.m;
    this.maxM = builder.m;
    this.maxM0 = builder.m * 2;
    this.levelLambda = 1 / Math.log(this.m);
    this.efConstruction = Math.max(builder.efConstruction, m);
    this.ef = builder.ef;

    this.nodes = new AtomicReferenceArray<>(this.maxItemCount);

    this.lookup = new ObjectIntHashMap<>();
    this.locks = new HashMap<>();

    this.globalLock = new ReentrantLock();

    this.visitedBitSetPool = new GenericObjectPool<>(() -> new ArrayBitSet(this.maxItemCount), Runtime.getRuntime().availableProcessors());

    this.excludedCandidates = new ArrayBitSet(this.maxItemCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    globalLock.lock();
    try {
      return lookup.size();
    } finally {
      globalLock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<TItem> get(TId id) {
    globalLock.lock();
    try {
      int nodeId = lookup.getIfAbsent(id, NO_NODE_ID);

      if (nodeId == NO_NODE_ID) {
        return Optional.empty();
      } else {
        return Optional.of(nodes.get(nodeId).item);
      }
    } finally {
      globalLock.unlock();
    }
  }

  @Override
  public Collection<TItem> items() {
    globalLock.lock();
    try {
      List<TItem> results = new ArrayList<>(size());

      Iterator<TItem> iter = new HnswVectorIndexRAM.ItemIterator();

      while (iter.hasNext()) {
        results.add(iter.next());
      }

      return results;
    } finally {
      globalLock.unlock();
    }
  }

  public ItemIterator iterateNodes() {
    return new HnswVectorIndexRAM.ItemIterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(TId id, long version) {
    globalLock.lock();
    try {
      int internalNodeId = lookup.getIfAbsent(id, NO_NODE_ID);

      if (internalNodeId == NO_NODE_ID) {
        return false;
      }

      Node<TItem> node = nodes.get(internalNodeId);

      if (version < node.item.version()) {
        return false;
      }

      node.deleted = true;

      lookup.remove(id);

      return true;
    } finally {
      globalLock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean add(TItem item) {
    if (item.dimensions() != dimensions)
      throw new IllegalArgumentException("Item has dimensionality of " + item.dimensions() + " instead of " + dimensions);

    final int randomLevel = assignLevel(item.id(), this.levelLambda);

    IntArrayList[] connections = new IntArrayList[randomLevel + 1];

    for (int level = 0; level <= randomLevel; level++) {
      int levelM = level == 0 ? maxM0 : maxM;
      connections[level] = new IntArrayList(levelM);
    }

    globalLock.lock();

    try {
      int existingNodeId = lookup.getIfAbsent(item.id(), NO_NODE_ID);

      if (existingNodeId != NO_NODE_ID) {
        Node<TItem> node = nodes.get(existingNodeId);

        if (item.version() < node.item.version())
          return false;

        if (Objects.deepEquals(node.item.vector(), item.vector())) {
          node.item = item;
          return true;
        } else {
          remove(item.id(), item.version());
        }
      }

      if (nodeCount >= this.maxItemCount) {
        throw new SizeLimitExceededException("The number of elements exceeds the specified limit.");
      }

      int newNodeId = nodeCount++;

      synchronized (excludedCandidates) {
        excludedCandidates.add(newNodeId);
      }

      Node<TItem> newNode = new Node<>(newNodeId, connections, item, false);

      nodes.set(newNodeId, newNode);
      lookup.put(item.id(), newNodeId);

      Object lock = locks.computeIfAbsent(item.id(), k -> new Object());

      Node<TItem> entryPointCopy = entryPoint;

      try {
        synchronized (lock) {
          synchronized (newNode) {

            if (entryPoint != null && randomLevel <= entryPoint.maxLevel()) {
              globalLock.unlock();
            }

            Node<TItem> currObj = entryPointCopy;

            if (currObj != null) {

              if (newNode.maxLevel() < entryPointCopy.maxLevel()) {

                TDistance curDist = distanceFunction.distance(item.vector(), currObj.item.vector());

                for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > newNode.maxLevel(); activeLevel--) {

                  boolean changed = true;

                  while (changed) {
                    changed = false;

                    synchronized (currObj) {
                      MutableIntList candidateConnections = currObj.connections[activeLevel];

                      for (int i = 0; i < candidateConnections.size(); i++) {

                        int candidateId = candidateConnections.get(i);

                        Node<TItem> candidateNode = nodes.get(candidateId);

                        TDistance candidateDistance = distanceFunction.distance(item.vector(), candidateNode.item.vector());

                        if (lt(candidateDistance, curDist)) {
                          curDist = candidateDistance;
                          currObj = candidateNode;
                          changed = true;
                        }
                      }
                    }
                  }
                }
              }

              for (int level = Math.min(randomLevel, entryPointCopy.maxLevel()); level >= 0; level--) {
                PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, item.vector(), efConstruction, level);

                if (entryPointCopy.deleted) {
                  TDistance distance = distanceFunction.distance(item.vector(), entryPointCopy.item.vector());
                  topCandidates.add(new NodeIdAndDistance<>(entryPointCopy.id, distance, maxValueDistanceComparator));

                  if (topCandidates.size() > efConstruction) {
                    topCandidates.poll();
                  }
                }

                mutuallyConnectNewElement(newNode, topCandidates, level);

              }
            }

            // zoom out to the highest level
            if (entryPoint == null || entryPointCopy == null || newNode.maxLevel() > entryPointCopy.maxLevel()) {
              // this is thread safe because we get the global lock when we add a level
              this.entryPoint = newNode;
            }

            return true;
          }
        }
      } finally {
        synchronized (excludedCandidates) {
          excludedCandidates.remove(newNodeId);
        }
      }
    } finally {
      if (globalLock.isHeldByCurrentThread()) {
        globalLock.unlock();
      }
    }
  }

  private void mutuallyConnectNewElement(Node<TItem> newNode, PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, int level) {

    int bestN = level == 0 ? this.maxM0 : this.maxM;

    int newNodeId = newNode.id;
    TVector newItemVector = newNode.item.vector();
    MutableIntList newItemConnections = newNode.connections[level];

    getNeighborsByHeuristic2(topCandidates, m);

    while (!topCandidates.isEmpty()) {
      int selectedNeighbourId = topCandidates.poll().nodeId;

      synchronized (excludedCandidates) {
        if (excludedCandidates.contains(selectedNeighbourId)) {
          continue;
        }
      }

      newItemConnections.add(selectedNeighbourId);

      Node<TItem> neighbourNode = nodes.get(selectedNeighbourId);

      synchronized (neighbourNode) {

        TVector neighbourVector = neighbourNode.item.vector();

        MutableIntList neighbourConnectionsAtLevel = neighbourNode.connections[level];

        if (neighbourConnectionsAtLevel.size() < bestN) {
          neighbourConnectionsAtLevel.add(newNodeId);
        } else {
          // finding the "weakest" element to replace it with the new one

          TDistance dMax = distanceFunction.distance(newItemVector, neighbourNode.item.vector());

          Comparator<NodeIdAndDistance<TDistance>> comparator = Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed();

          PriorityQueue<NodeIdAndDistance<TDistance>> candidates = new PriorityQueue<>(comparator);
          candidates.add(new NodeIdAndDistance<>(newNodeId, dMax, maxValueDistanceComparator));

          neighbourConnectionsAtLevel.forEach(id -> {
            TDistance dist = distanceFunction.distance(neighbourVector, nodes.get(id).item.vector());

            candidates.add(new NodeIdAndDistance<>(id, dist, maxValueDistanceComparator));
          });

          getNeighborsByHeuristic2(candidates, bestN);

          neighbourConnectionsAtLevel.clear();

          while (!candidates.isEmpty()) {
            neighbourConnectionsAtLevel.add(candidates.poll().nodeId);
          }
        }
      }
    }
  }

  private void getNeighborsByHeuristic2(PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates, int m) {

    if (topCandidates.size() < m) {
      return;
    }

    PriorityQueue<NodeIdAndDistance<TDistance>> queueClosest = new PriorityQueue<>();
    List<NodeIdAndDistance<TDistance>> returnList = new ArrayList<>();

    while (!topCandidates.isEmpty()) {
      queueClosest.add(topCandidates.poll());
    }

    while (!queueClosest.isEmpty()) {
      if (returnList.size() >= m) {
        break;
      }

      NodeIdAndDistance<TDistance> currentPair = queueClosest.poll();

      TDistance distToQuery = currentPair.distance;

      boolean good = true;
      for (NodeIdAndDistance<TDistance> secondPair : returnList) {

        TDistance curdist = distanceFunction.distance(nodes.get(secondPair.nodeId).item.vector(), nodes.get(currentPair.nodeId).item.vector());

        if (lt(curdist, distToQuery)) {
          good = false;
          break;
        }

      }
      if (good) {
        returnList.add(currentPair);
      }
    }

    topCandidates.addAll(returnList);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<SearchResult<TItem, TDistance>> findNearest(TVector destination, int k) {

    if (entryPoint == null) {
      return Collections.emptyList();
    }

    Node<TItem> entryPointCopy = entryPoint;

    Node<TItem> currObj = entryPointCopy;

    TDistance curDist = distanceFunction.distance(destination, currObj.item.vector());

    for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > 0; activeLevel--) {

      boolean changed = true;

      while (changed) {
        changed = false;

        synchronized (currObj) {
          MutableIntList candidateConnections = currObj.connections[activeLevel];

          for (int i = 0; i < candidateConnections.size(); i++) {

            int candidateId = candidateConnections.get(i);

            TDistance candidateDistance = distanceFunction.distance(destination, nodes.get(candidateId).item.vector());
            if (lt(candidateDistance, curDist)) {
              curDist = candidateDistance;
              currObj = nodes.get(candidateId);
              changed = true;
            }
          }
        }

      }
    }

    PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = searchBaseLayer(currObj, destination, Math.max(ef, k), 0);

    while (topCandidates.size() > k) {
      topCandidates.poll();
    }

    List<SearchResult<TItem, TDistance>> results = new ArrayList<>(topCandidates.size());
    while (!topCandidates.isEmpty()) {
      NodeIdAndDistance<TDistance> pair = topCandidates.poll();
      results.addFirst(new SearchResult<>(nodes.get(pair.nodeId).item, pair.distance, maxValueDistanceComparator));
    }

    return results;
  }

  /**
   * Changes the maximum capacity of the index.
   *
   * @param newSize new size of the index
   */
  public void resize(int newSize) {
    globalLock.lock();
    try {
      this.maxItemCount = newSize;

      this.visitedBitSetPool = new GenericObjectPool<>(() -> new ArrayBitSet(this.maxItemCount), Runtime.getRuntime().availableProcessors());

      AtomicReferenceArray<Node<TItem>> newNodes = new AtomicReferenceArray<>(newSize);
      for (int i = 0; i < this.nodes.length(); i++) {
        newNodes.set(i, this.nodes.get(i));
      }
      this.nodes = newNodes;

      this.excludedCandidates = new ArrayBitSet(this.excludedCandidates, newSize);
    } finally {
      globalLock.unlock();
    }
  }

  private PriorityQueue<NodeIdAndDistance<TDistance>> searchBaseLayer(Node<TItem> entryPointNode, TVector destination, int k, int layer) {

    ArrayBitSet visitedBitSet = visitedBitSetPool.borrowObject();

    try {
      PriorityQueue<NodeIdAndDistance<TDistance>> topCandidates = new PriorityQueue<>(Comparator.<NodeIdAndDistance<TDistance>>naturalOrder().reversed());
      PriorityQueue<NodeIdAndDistance<TDistance>> candidateSet = new PriorityQueue<>();

      TDistance lowerBound;

      if (!entryPointNode.deleted) {
        TDistance distance = distanceFunction.distance(destination, entryPointNode.item.vector());
        NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, distance, maxValueDistanceComparator);

        topCandidates.add(pair);
        lowerBound = distance;
        candidateSet.add(pair);

      } else {
        lowerBound = MaxValueComparator.maxValue();
        NodeIdAndDistance<TDistance> pair = new NodeIdAndDistance<>(entryPointNode.id, lowerBound, maxValueDistanceComparator);
        candidateSet.add(pair);
      }

      visitedBitSet.add(entryPointNode.id);

      while (!candidateSet.isEmpty()) {

        NodeIdAndDistance<TDistance> currentPair = candidateSet.poll();

        if (gt(currentPair.distance, lowerBound)) {
          break;
        }

        Node<TItem> node = nodes.get(currentPair.nodeId);

        synchronized (node) {

          MutableIntList candidates = node.connections[layer];

          for (int i = 0; i < candidates.size(); i++) {

            int candidateId = candidates.get(i);

            if (!visitedBitSet.contains(candidateId)) {

              visitedBitSet.add(candidateId);

              Node<TItem> candidateNode = nodes.get(candidateId);

              TDistance candidateDistance = distanceFunction.distance(destination, candidateNode.item.vector());

              if (topCandidates.size() < k || gt(lowerBound, candidateDistance)) {

                NodeIdAndDistance<TDistance> candidatePair = new NodeIdAndDistance<>(candidateId, candidateDistance, maxValueDistanceComparator);

                candidateSet.add(candidatePair);

                if (!candidateNode.deleted) {
                  topCandidates.add(candidatePair);
                }

                if (topCandidates.size() > k) {
                  topCandidates.poll();
                }

                if (!topCandidates.isEmpty()) {
                  lowerBound = topCandidates.peek().distance;
                }
              }
            }
          }

        }
      }

      return topCandidates;
    } finally {
      visitedBitSet.clear();
      visitedBitSetPool.returnObject(visitedBitSet);
    }
  }

  /**
   * Returns the dimensionality of the items stored in this index.
   *
   * @return the dimensionality of the items stored in this index
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Returns the number of bi-directional links created for every new element during construction.
   *
   * @return the number of bi-directional links created for every new element during construction
   */
  public int getM() {
    return m;
  }

  /**
   * The size of the dynamic list for the nearest neighbors (used during the search)
   *
   * @return The size of the dynamic list for the nearest neighbors
   */
  public int getEf() {
    return ef;
  }

  /**
   * Set the size of the dynamic list for the nearest neighbors (used during the search)
   *
   * @param ef The size of the dynamic list for the nearest neighbors
   */
  public void setEf(int ef) {
    this.ef = ef;
  }

  /**
   * Returns the parameter has the same meaning as ef, but controls the index time / index precision.
   *
   * @return the parameter has the same meaning as ef, but controls the index time / index precision
   */
  public int getEfConstruction() {
    return efConstruction;
  }

  /**
   * Returns the distance function.
   *
   * @return the distance function
   */
  public DistanceFunction<TVector, TDistance> getDistanceFunction() {
    return distanceFunction;
  }

  /**
   * Returns the comparator used to compare distances.
   *
   * @return the comparator used to compare distance
   */
  public Comparator<TDistance> getDistanceComparator() {
    return distanceComparator;
  }

  /**
   * Returns the maximum number of items the index can hold.
   *
   * @return the maximum number of items the index can hold
   */
  public int getMaxItemCount() {
    return maxItemCount;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void save(OutputStream out) throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(this);
    }
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from a File.
   *
   * @param file        File to restore the index from
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return The restored index
   *
   * @throws IOException in case of an I/O exception
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(File file)
      throws IOException {
    return load(new FileInputStream(file));
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from a File.
   *
   * @param file        File to restore the index from
   * @param classLoader the classloader to use
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return The restored index
   *
   * @throws IOException in case of an I/O exception
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(File file,
      ClassLoader classLoader) throws IOException {
    return load(new FileInputStream(file), classLoader);
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from a Path.
   *
   * @param path        Path to restore the index from
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return The restored index
   *
   * @throws IOException in case of an I/O exception
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(Path path)
      throws IOException {
    return load(Files.newInputStream(path));
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from a Path.
   *
   * @param path        Path to restore the index from
   * @param classLoader the classloader to use
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return The restored index
   *
   * @throws IOException in case of an I/O exception
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(Path path,
      ClassLoader classLoader) throws IOException {
    return load(Files.newInputStream(path), classLoader);
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from an InputStream.
   *
   * @param inputStream InputStream to restore the index from
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ...).
   *
   * @return The restored index
   *
   * @throws IOException              in case of an I/O exception
   * @throws IllegalArgumentException in case the file cannot be read
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(InputStream inputStream)
      throws IOException {
    return load(inputStream, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Restores a {@link HnswVectorIndexRAM} from an InputStream.
   *
   * @param inputStream InputStream to restore the index from
   * @param classLoader the classloader to use
   * @param <TId>       Type of the external identifier of an item
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TItem>     Type of items stored in the index
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ...).
   *
   * @return The restored index
   *
   * @throws IOException              in case of an I/O exception
   * @throws IllegalArgumentException in case the file cannot be read
   */
  @SuppressWarnings("unchecked")
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> HnswVectorIndexRAM<TId, TVector, TItem, TDistance> load(InputStream inputStream,
      ClassLoader classLoader) throws IOException {

    try (ObjectInputStream ois = new ClassLoaderObjectInputStream(classLoader, inputStream)) {
      return (HnswVectorIndexRAM<TId, TVector, TItem, TDistance>) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Could not read input file.", e);
    }
  }

  /**
   * Start the process of building a new HNSW index.
   *
   * @param dimensions       the dimensionality of the vectors stored in the index
   * @param distanceFunction the distance function
   * @param maxItemCount     maximum number of items the index can hold
   * @param <TVector>        Type of the vector to perform distance calculation on
   * @param <TDistance>      Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return a builder
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance extends Comparable<TDistance>> Builder<TId, TVector, TItem, TDistance> newBuilder(
      int dimensions, DistanceFunction<TVector, TDistance> distanceFunction, int maxItemCount) {

    Comparator<TDistance> distanceComparator = Comparator.naturalOrder();

    return new Builder<>(dimensions, distanceFunction, distanceComparator, maxItemCount);
  }

  /**
   * Start the process of building a new HNSW index.
   *
   * @param dimensions         the dimensionality of the vectors stored in the index
   * @param distanceFunction   the distance function
   * @param distanceComparator used to compare distances
   * @param maxItemCount       maximum number of items the index can hold
   * @param <TVector>          Type of the vector to perform distance calculation on
   * @param <TDistance>        Type of distance between items (expect any numeric type: float, double, int, ..)
   *
   * @return a builder
   */
  public static <TId, TVector, TItem extends Item<TId, TVector>, TDistance> Builder<TId, TVector, TItem, TDistance> newBuilder(final int dimensions,
      final DistanceFunction<TVector, TDistance> distanceFunction, final Comparator<TDistance> distanceComparator, final int maxItemCount) {

    return new Builder<>(dimensions, distanceFunction, distanceComparator, maxItemCount);
  }

  private int assignLevel(final TId value, final double lambda) {
    // by relying on the external id to come up with the level, the graph construction should be a lot mor stable
    // see : https://github.com/nmslib/hnswlib/issues/28
    int hashCode = value.hashCode();

    byte[] bytes = new byte[] { (byte) (hashCode >> 24), (byte) (hashCode >> 16), (byte) (hashCode >> 8), (byte) hashCode };

    double random = Math.abs((double) Murmur3.hash32(bytes) / (double) Integer.MAX_VALUE);

    double r = -Math.log(random) * lambda;
    return (int) r;
  }

  private boolean lt(TDistance x, TDistance y) {
    return maxValueDistanceComparator.compare(x, y) < 0;
  }

  private boolean gt(TDistance x, TDistance y) {
    return maxValueDistanceComparator.compare(x, y) > 0;
  }

  public VectorIndexBuilder createPersistentIndex(final Database database) {
    return new VectorIndexBuilder(database, this);
  }

  public Integer getEntryPoint() {
    return entryPoint != null ? entryPoint.id : null;
  }

  public class ItemIterator implements Iterator<Node<TItem>> {

    private int done  = 0;
    private int index = 0;

    @Override
    public boolean hasNext() {
      return done < HnswVectorIndexRAM.this.size();
    }

    @Override
    public Node<TItem> next() {
      HnswVectorIndexRAM.Node<TItem> node;

      do {
        node = HnswVectorIndexRAM.this.nodes.get(index++);
      } while (node == null || node.deleted);

      done++;

      return node;
    }
  }

  public static class Node<TItem extends Item> implements Serializable {

    private static final long serialVersionUID = 1L;

    public final int id;

    final MutableIntList[] connections;

    public volatile TItem item;

    volatile boolean deleted;

    Node(int id, MutableIntList[] connections, TItem item, boolean deleted) {
      this.id = id;
      this.connections = connections;
      this.item = item;
      this.deleted = deleted;
    }

    public int maxLevel() {
      return this.connections.length - 1;
    }

    public MutableIntList[] connections() {
      return connections;
    }
  }

  static class NodeIdAndDistance<TDistance> implements Comparable<NodeIdAndDistance<TDistance>> {

    final int                   nodeId;
    final TDistance             distance;
    final Comparator<TDistance> distanceComparator;

    NodeIdAndDistance(int nodeId, TDistance distance, Comparator<TDistance> distanceComparator) {
      this.nodeId = nodeId;
      this.distance = distance;
      this.distanceComparator = distanceComparator;
    }

    @Override
    public int compareTo(NodeIdAndDistance<TDistance> o) {
      return distanceComparator.compare(distance, o.distance);
    }
  }

  static class MaxValueComparator<TDistance> implements Comparator<TDistance>, Serializable {

    private static final long serialVersionUID = 1L;

    private final Comparator<TDistance> delegate;

    MaxValueComparator(Comparator<TDistance> delegate) {
      this.delegate = delegate;
    }

    @Override
    public int compare(TDistance o1, TDistance o2) {
      return o1 == null ? o2 == null ? 0 : 1 : o2 == null ? -1 : delegate.compare(o1, o2);
    }

    static <TDistance> TDistance maxValue() {
      return null;
    }
  }

  /**
   * Builder for initializing an {@link HnswVectorIndexRAM} instance.
   *
   * @param <TVector>   Type of the vector to perform distance calculation on
   * @param <TDistance> Type of distance between items (expect any numeric type: float, double, int, ..)
   */
  public static class Builder<TId, TVector, TItem extends Item<TId, TVector>, TDistance> {

    public static final int     DEFAULT_M               = 10;
    public static final int     DEFAULT_EF              = 10;
    public static final int     DEFAULT_EF_CONSTRUCTION = 200;
    public static final boolean DEFAULT_REMOVE_ENABLED  = false;

    int                                  dimensions;
    DistanceFunction<TVector, TDistance> distanceFunction;
    Comparator<TDistance>                distanceComparator;
    int                                  maxItemCount;
    int                                  m              = DEFAULT_M;
    int                                  ef             = DEFAULT_EF;
    int                                  efConstruction = DEFAULT_EF_CONSTRUCTION;

    /**
     * Constructs a new {@link Builder} instance.
     *
     * @param dimensions       the dimensionality of the vectors stored in the index
     * @param distanceFunction the distance function
     * @param maxItemCount     the maximum number of elements in the index
     */
    Builder(int dimensions, DistanceFunction<TVector, TDistance> distanceFunction, Comparator<TDistance> distanceComparator, int maxItemCount) {
      this.dimensions = dimensions;
      this.distanceFunction = distanceFunction;
      this.distanceComparator = distanceComparator;
      this.maxItemCount = maxItemCount;
    }

    /**
     * Sets the number of bi-directional links created for every new element during construction. Reasonable range
     * for m is 2-100. Higher m work better on datasets with high intrinsic dimensionality and/or high recall,
     * while low m work better for datasets with low intrinsic dimensionality and/or low recalls. The parameter
     * also determines the algorithm's memory consumption.
     * As an example for d = 4 random vectors optimal m for search is somewhere around 6, while for high dimensional
     * datasets (word embeddings, good face descriptors), higher M are required (e.g. m = 48, 64) for optimal
     * performance at high recall. The range mM = 12-48 is ok for the most of the use cases. When m is changed one
     * has to update the other parameters. Nonetheless, ef and efConstruction parameters can be roughly estimated by
     * assuming that m  efConstruction is a constant.
     *
     * @param m the number of bi-directional links created for every new element during construction
     *
     * @return the builder.
     */
    public Builder<TId, TVector, TItem, TDistance> withM(int m) {
      this.m = m;
      return this;
    }

    /**
     * `
     * The parameter has the same meaning as ef, but controls the index time / index precision. Bigger efConstruction
     * leads to longer construction, but better index quality. At some point, increasing efConstruction does not
     * improve the quality of the index. One way to check if the selection of ef_construction was ok is to measure
     * a recall for M nearest neighbor search when ef = efConstruction: if the recall is lower than 0.9, then
     * there is room for improvement.
     *
     * @param efConstruction controls the index time / index precision
     *
     * @return the builder
     */
    public Builder<TId, TVector, TItem, TDistance> withEfConstruction(int efConstruction) {
      this.efConstruction = efConstruction;
      return this;
    }

    /**
     * The size of the dynamic list for the nearest neighbors (used during the search). Higher ef leads to more
     * accurate but slower search. The value ef of can be anything between k and the size of the dataset.
     *
     * @param ef size of the dynamic list for the nearest neighbors
     *
     * @return the builder
     */
    public Builder<TId, TVector, TItem, TDistance> withEf(int ef) {
      this.ef = ef;
      return this;
    }

    /**
     * Build the index.
     *
     * @return the hnsw index instance
     */
    public HnswVectorIndexRAM<TId, TVector, TItem, TDistance> build() {
      return new HnswVectorIndexRAM<>(this);
    }
  }
}
