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

package com.arcadedb.vector.universe;/*
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
import com.arcadedb.vector.WordVector;

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

    public float getValue(final int entryIndex) {
      return sortedDimensionValues[entryIndex];
    }

    public int getPointer(final int entryIndex) {
      return sortedDimensionPointers[entryIndex];
    }

    public int searchFirstValue(final float v) {
      int pos = Arrays.binarySearch(sortedDimensionValues, v);
      if (pos > 0 && v == sortedDimensionValues[pos - 1]) {
        // SEARCH FOR THE FIRST ELEMENT WITH THIS VALUE IN THE ARRAY
        while (pos > 0)
          if (sortedDimensionValues[pos - 1] == v)
            --pos;
          else
            break;
      }
      return pos;
    }

    public int size() {
      return sortedDimensionValues.length;
    }
  }

  private SortedDimension[] sortedDimensions;
  private VectorUniverse<?> universe;

  public SortedVectorUniverse(final VectorUniverse<?> universe) {
    this.universe = universe;
  }

  public SortedDimension getSortedDimensions(final int dimension) {
    return sortedDimensions[dimension];
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

      // READ VALUES AND POINTERS
      for (int dimension = 0; dimension < dimensions; dimension++) {
        final SortedDimension sortedDimension = new SortedDimension(entries);

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
}
