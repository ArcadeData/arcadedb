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

package com.arcadedb.vector.parser;

import com.arcadedb.log.LogManager;
import com.arcadedb.vector.VectorUniverse;
import com.arcadedb.vector.WordVector;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.zip.*;

/**
 * Parses text and binary files into a map of words/vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorParser {
  public Map<String, WordVector> loadFromTextFile(final File file, final VectorParserCallback callback) throws IOException {
    final Map<String, WordVector> words = new HashMap<>();

    InputStream fis = new FileInputStream(file);
    if (file.getName().endsWith(".gz"))
      fis = new GZIPInputStream(fis);

    try {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
        String line;

        for (int i = 0; (line = br.readLine()) != null; ++i) {
          final String[] tokens = line.split("\\s+");
          final String word = tokens[0];
          final float[] vector = new float[tokens.length - 1];
          for (int token = 1; token < tokens.length; token++)
            vector[token - 1] = Float.parseFloat(tokens[token]);

          final WordVector wv = new WordVector(word, vector);
          words.put(wv.getSubject(), wv);

          if (callback != null)
            if (!callback.onWord(i, -1, word, vector))
              break;
        }
      }
    } finally {
      fis.close();
    }

    return words;
  }

  public VectorUniverse<String> loadFromBinaryFile(final File file, final boolean lineBreak, final VectorParserCallback callback) throws IOException {
    InputStream fis = new FileInputStream(file);
    if (file.getName().endsWith(".gz"))
      fis = new GZIPInputStream(fis);

    final byte[] floatBuffer = new byte[4];
    final StringBuilder stringBuilder = new StringBuilder();

    try (final BufferedInputStream bis = new BufferedInputStream(fis)) {
      final int totalWords = Integer.parseInt(readString(bis, stringBuilder));
      final int vectorSize = Integer.parseInt(readString(bis, stringBuilder));

      final List<WordVector> words = new ArrayList<>(totalWords);

      LogManager.instance().log(this, Level.INFO, "Parsing Word2Vec file %s containing %d words with vector size %d", file.getName(), totalWords, vectorSize);

      for (int i = 0; i < totalWords; i++) {
        final String word = readString(bis, stringBuilder);

        int realSize = vectorSize + vectorSize % 8;

        final float[] vector = new float[realSize];
        for (int j = 0; j < vectorSize; j++)
          vector[j] = readFloat(bis, floatBuffer);

        words.add(new WordVector(word, vector));

        if (callback != null)
          if (!callback.onWord(i, totalWords, word, vector))
            break;

        if (lineBreak)
          bis.read();
      }

      return new VectorUniverse<>(words);

    } finally {
      fis.close();
    }
  }

  private static float readFloat(final InputStream is, final byte[] floatBuffer) throws IOException {
    is.read(floatBuffer);
    int accum = 0;
    accum = accum | (floatBuffer[0] & 0xff);
    accum = accum | (floatBuffer[1] & 0xff) << 8;
    accum = accum | (floatBuffer[2] & 0xff) << 16;
    accum = accum | (floatBuffer[3] & 0xff) << 24;
    return Float.intBitsToFloat(accum);
  }

  private static String readString(final InputStream dis, final StringBuilder stringBuffer) throws IOException {
    stringBuffer.setLength(0);

    byte b = (byte) dis.read();
    while (b != 32 && b != 10) {
      stringBuffer.append((char) b);
      b = (byte) dis.read();
    }
    return stringBuffer.toString();
  }
}
