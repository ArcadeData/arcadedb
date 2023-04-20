/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.vector;/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.arcadedb.log.LogManager;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.zip.*;

public class SortedVectorUniverse<T extends Comparable> {
  private static final int FILE_FORMAT_VERSION = 0; // FOR VERSIONING AND EVOLUTION OF THE FILE FORMAT

  public static class SortedDimension {
    float[] sortedDimensionValues;
    int[]   sortedDimensionPointers;

    public SortedDimension(final int entries) {
      this.sortedDimensionValues = new float[entries];
      this.sortedDimensionPointers = new int[entries];
    }

    public void set(final int entryIndex, final float key, final int value) {
      this.sortedDimensionValues[entryIndex] = key;
      this.sortedDimensionPointers[entryIndex] = value;
    }
  }

  private SortedDimension[] sortedDimensions;
  private VectorUniverse<?> universe;

  public SortedVectorUniverse(final VectorUniverse<?> universe) {
    this.universe = universe;
  }

  public SortedVectorUniverse<T> compute() {
    final int entries = universe.size();
    final int dimensions = universe.dimensions();

    sortedDimensions = new SortedDimension[dimensions];
    for (int dimension = 0; dimension < dimensions; dimension++) {
      sortedDimensions[dimension] = computeDimension(universe, entries, dimension);
      LogManager.instance().log(this, Level.INFO, "Calculate sorted dimension %d/%d", dimension, dimensions);
    }
    return this;
  }

  public SortedVectorUniverse<T> computeAndWriteToFile(final File file) throws IOException {
    final int entries = universe.size();
    final int dimensions = universe.dimensions();

    try (FileOutputStream fos = new FileOutputStream(file);
        GZIPOutputStream gos = new GZIPOutputStream(fos);
        BufferedOutputStream bos = new BufferedOutputStream(gos);
        DataOutputStream dos = new DataOutputStream(bos)) {

      dos.writeInt(FILE_FORMAT_VERSION);

      dos.writeInt(dimensions);

      // WRITE ALL THE ENTRIES
      dos.writeInt(entries);
      for (int i = 0; i < entries; i++) {
        final IndexableVector<?> vector = universe.get(i);
        dos.writeUTF(vector.subject.toString());
      }

      for (int dimension = 0; dimension < dimensions; dimension++) {
        final SortedDimension sortedDimension = computeDimension(universe, entries, dimension);

        for (int i = 0; i < entries; i++) {
          dos.writeFloat(sortedDimension.sortedDimensionValues[i]);
          dos.writeInt(sortedDimension.sortedDimensionPointers[i]);
        }

        LogManager.instance().log(this, Level.INFO, "Written sorted dimension %d/%d", dimension, dimensions);
      }
    }

    return this;
  }

  public SortedVectorUniverse<T> readFromFile(final File file) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Reading vector index from file %s...", file);

    final long begin = System.currentTimeMillis();

    try (FileInputStream fis = new FileInputStream(file);
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedInputStream bis = new BufferedInputStream(gis);
        DataInputStream dis = new DataInputStream(bis)) {

      final List<WordVector> list = new ArrayList<>();

      final int fileFormatVersion = dis.readInt();

      final int dimensions = dis.readInt();

      // IGNORE ENTRIES SUBJECTS
      // TODO: REMOVE WRITING OF ENTRIES IN FILE
      final int entries = dis.readInt();
      for (int i = 0; i < entries; i++) {
        dis.readUTF();
      }

      sortedDimensions = new SortedDimension[dimensions];

      final SortedDimension sortedDimension = new SortedDimension(entries);

      // READ VALUES AND POINTERS
      for (int dimension = 0; dimension < dimensions; dimension++) {
        for (int i = 0; i < entries; i++) {
          final float value = dis.readFloat();
          final int pointer = dis.readInt();
          sortedDimension.set(i, value, pointer);
        }

        sortedDimensions[dimension] = sortedDimension;

        LogManager.instance().log(this, Level.INFO, "Read sorted dimension %d/%d", dimension, dimensions);
      }
    }

    LogManager.instance().log(this, Level.INFO, "Reading of vector index completed in %d sec", (System.currentTimeMillis() - begin) / 1000);

    return this;
  }

  public VectorUniverse<?> getUniverse() {
    return universe;
  }

  private SortedDimension computeDimension(final VectorUniverse<?> universe, final int entries, final int dimension) {
    final TreeMap<Float, List<Integer>> treeMap = new TreeMap<>();

    // ORDER THE ENTRIES USING A TREEMAP
    for (int entryIndex = 0; entryIndex < entries; entryIndex++) {
      final IndexableVector<?> vector = universe.get(entryIndex);

      final float value = vector.getVector()[dimension];
      final List<Integer> pointers = treeMap.computeIfAbsent(value, k -> new ArrayList<>());
      pointers.add(entryIndex);
    }

    // CONVERT THE SORTED TREEMAP IN 2 ARRAYS (LESS RAM, FASTER BROWSING)
    final SortedDimension sortedDimension = new SortedDimension(entries);
    int i = 0;
    for (Map.Entry<Float, List<Integer>> entry : treeMap.entrySet()) {
      for (Integer entryIndex : entry.getValue()) {
        sortedDimension.set(i, entry.getKey(), entryIndex);
        ++i;
      }
    }
    return sortedDimension;
  }
}
