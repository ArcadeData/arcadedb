/*
 * Copyright 2021 Arcade Data Ltd
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

package com.arcadedb.console;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class DatabaseExporter {
  public static void export(final String fileName, Database database, final PrintWriter writer) throws IOException {

    final long beginTime = System.currentTimeMillis();

    final File outputFile = new File(fileName);
    if (outputFile.exists())
      outputFile.delete();

    final FileOutputStream fos = new FileOutputStream(outputFile);
    final OutputStream os = fileName.endsWith(".tgz") ? new GZIPOutputStream(fos) : fos;

    try {

      writer.printf("Exporting database to file %s...\n", fileName);

      os.write(("#ARCADEDB EXPORT v1.0\n").getBytes());
      writer.flush();

      long written = 0;

      final List<Bucket> orderedBuckets = new ArrayList<>(database.getSchema().getBuckets());

      int moved = 0;
      for (int i = 0; i < orderedBuckets.size() - moved; ++i) {
        final String bName = orderedBuckets.get(i).getName();
        if (bName.endsWith(GraphEngine.OUT_EDGES_SUFFIX) || bName.endsWith(GraphEngine.IN_EDGES_SUFFIX)) {
          orderedBuckets.add(orderedBuckets.remove(i));
          ++moved;
          --i;
        }
      }

      for (Bucket bucket : orderedBuckets) {
        writer.printf("- Exporting bucket %s...", bucket.getName());
        writer.flush();

        os.write(("#BUCKET " + bucket.getName() + "\n").getBytes());

        final byte[] sep1 = "=".getBytes();
        final byte[] sep2 = ":".getBytes();
        final byte[] lf = "\n".getBytes();

        try {
          int i = 0;
          for (Iterator<Record> it = bucket.iterator(); it.hasNext(); ) {
            final Record rec = it.next();

            byte[] buffer = rec.getIdentity().toString().getBytes();
            os.write(buffer);
            written += buffer.length;

            os.write(sep1);
            written += sep1.length;

            os.write(("" + rec.getRecordType()).getBytes());
            written += 1;

            os.write(sep2);
            written += sep2.length;

            buffer = rec.toJSON().toString().getBytes();
            os.write(buffer);
            written += buffer.length;

            os.write(lf);
            written += lf.length;

            if (++i % 1000 == 0) {
              writer.print(".");
              writer.flush();
            }
          }

          writer.print("\n");

        } catch (Exception e) {
          writer.printf("!Error on exporting bucket %s: %s\n", bucket.getName(), e.getMessage());
          writer.flush();
        }
      }

      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

      writer.printf("Export completed in %d seconds. Written %s (%dMB/sec), final file size is %s...\n", elapsedInSecs, FileUtils.getSizeAsString(written),
          written / elapsedInSecs / FileUtils.MEGABYTE, FileUtils.getSizeAsString(outputFile.length()));
      writer.flush();

    } finally {
      os.close();
      fos.close();
    }
  }
}
