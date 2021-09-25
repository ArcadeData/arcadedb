/*
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
package com.arcadedb.console;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.DocumentType;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.zip.*;

public class DatabaseImporter {
  public static void export(final String fileName, Database database, final PrintWriter writer) throws IOException {

    final long beginTime = System.currentTimeMillis();

    final File outputFile = new File(fileName);
    if (outputFile.exists())
      outputFile.delete();

    final FileInputStream fis = new FileInputStream(outputFile);
    final InputStream is = fileName.endsWith(".tgz") ? new GZIPInputStream(fis) : fis;
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, DatabaseFactory.getDefaultCharset()), 8192 * 16);

    try {

      writer.printf("Importing database from file %s...%n", fileName);

      long totalRecords = 0;
      Bucket currentBucket;
      DocumentType currentType = null;

      final Map<RID, RID> rids = new HashMap<>();

      while (reader.ready()) {
        final String line = reader.readLine();

        if (line.startsWith("#ARCADEDB EXPORT ")) {
          final String version = line.substring("#ARCADEDB EXPORT ".length());
          writer.printf("- Found version %s%n", version);
        } else if (line.startsWith("#BUCKET ")) {
          final String bucketName = line.substring("#BUCKET ".length());
          if (database.getSchema().existsBucket(bucketName))
            currentBucket = database.getSchema().getBucketByName(bucketName);
          else
            currentBucket = database.getSchema().createBucket(bucketName);

          currentType = database.getSchema().getType(database.getSchema().getTypeNameByBucketId(currentBucket.getId()));

          writer.printf("- Importing bucket %s...%n", bucketName);
        } else {
          // RECORD
          final int posSep = line.indexOf("=");
          if (posSep < 0) {
            writer.printf("-- Error on importing record %s%n", line);
            continue;
          }

          try {
            final RID origRID = new RID(database, line.substring(0, posSep));

            final byte recordType = (byte) Integer.parseInt(line.substring(posSep + 1, posSep + 2));

            final JSONObject obj = new JSONObject(line.substring(posSep + 2));

            final Record record = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, currentType, null, recordType);

            final RID newRID = null;

            rids.put(origRID, newRID);

            if (++totalRecords % 1000 == 0) {
              writer.print(".");
              writer.flush();
            }
          } catch (Exception e) {
            writer.printf("-- Error on importing record %s%n", line);
            writer.flush();
          }
        }

        writer.flush();
      }

      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

      writer.printf("Import completed in %d seconds. Created %d new records%n", elapsedInSecs, totalRecords);
      writer.flush();

    } finally {
      reader.close();
      is.close();
      fis.close();
    }
  }
}
