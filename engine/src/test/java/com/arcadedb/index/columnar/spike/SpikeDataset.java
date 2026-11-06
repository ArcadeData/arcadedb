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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.columnar.spike;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic dataset of the columnar faceting spike: Zipf distributed brand, skewed status, uniform fileSize.
 */
final class SpikeDataset {
  static final String TYPE              = "Asset";
  static final String BRAND             = "brand";
  static final String STATUS            = "status";
  static final String FILE_SIZE         = "fileSize";
  static final int    BRAND_CARDINALITY = 200;
  static final long   FILE_SIZE_MAX     = 1_000_000L;
  static final String MIME_TYPE          = "mimeType";
  static final int    MIME_CARDINALITY   = 12;
  static final String AUTHOR             = "author";
  static final int    AUTHOR_CARDINALITY = 100_000;
  static final String TAGS               = "tags";
  static final int    TAG_CARDINALITY    = 1_000;
  static final int    MAX_TAGS           = 5;

  private static final int    COMMIT_EVERY = 10_000;
  // 150 characters are always drawn (keeps the first random stream identical to earlier runs), only the leading ones are stored
  private static final int    TITLE_LENGTH = 150;
  private static final int    TITLE_STORED = 100;
  private static final String ALPHABET     = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

  private SpikeDataset() {
  }

  static void createSchema(final Database db) {
    final Schema schema = db.getSchema();
    final DocumentType type = schema.buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    type.createProperty(BRAND, Type.STRING);
    type.createProperty(STATUS, Type.STRING);
    type.createProperty(FILE_SIZE, Type.LONG);
    type.createProperty(MIME_TYPE, Type.STRING);
    type.createProperty(AUTHOR, Type.STRING);
    type.createProperty(TAGS, Type.LIST);
    schema.buildTypeIndex(TYPE, new String[] { FILE_SIZE }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
  }

  static void populate(final Database db, final int records, final long seed, final double missingBrandRatio) {
    final Random random = new Random(seed);
    // the facet properties come from a second stream so the first one stays identical to earlier runs for a given seed
    final Random facets = new Random(seed + 1);
    final double[] cumulative = zipf(BRAND_CARDINALITY, 1.0);
    final double[] mimeCumulative = zipf(MIME_CARDINALITY, 1.0);
    final double[] authorCumulative = zipf(AUTHOR_CARDINALITY, 0.8);
    final double[] tagCumulative = zipf(TAG_CARDINALITY, 1.0);

    final char[] title = new char[TITLE_LENGTH];
    db.begin();
    for (int n = 0; n < records; n++) {
      final MutableDocument doc = db.newDocument(TYPE);
      if (random.nextDouble() >= missingBrandRatio)
        doc.set(BRAND, brandName(draw(cumulative, random)));
      final double s = random.nextDouble();
      doc.set(STATUS, s < 0.7 ? "published" : s < 0.9 ? "draft" : "archived");
      doc.set(FILE_SIZE, (long) (random.nextDouble() * FILE_SIZE_MAX));
      for (int c = 0; c < TITLE_LENGTH; c++)
        title[c] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
      doc.set("title", new String(title, 0, TITLE_STORED));
      doc.set(MIME_TYPE, String.format("mime-%02d", draw(mimeCumulative, facets)));
      doc.set(AUTHOR, String.format("author-%06d", draw(authorCumulative, facets)));
      final int tagCount = 1 + facets.nextInt(MAX_TAGS);
      final List<String> tags = new ArrayList<>(tagCount);
      while (tags.size() < tagCount) {
        final String tag = String.format("tag-%04d", draw(tagCumulative, facets));
        if (!tags.contains(tag))
          tags.add(tag);
      }
      doc.set(TAGS, tags);
      doc.save();
      if ((n + 1) % COMMIT_EVERY == 0) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static double[] zipf(final int cardinality, final double exponent) {
    final double[] cumulative = new double[cardinality];
    double sum = 0;
    for (int i = 0; i < cardinality; i++) {
      sum += 1.0 / Math.pow(i + 1, exponent);
      cumulative[i] = sum;
    }
    for (int i = 0; i < cardinality; i++)
      cumulative[i] /= sum;
    return cumulative;
  }

  private static int draw(final double[] cumulative, final Random random) {
    int idx = Arrays.binarySearch(cumulative, random.nextDouble());
    if (idx < 0)
      idx = -idx - 1;
    return Math.min(idx, cumulative.length - 1);
  }

  /** Deletes every nth record in RID order and returns the deleted positions, ascending. */
  static long[] deleteEvery(final Database db, final int nth) {
    final long[] deleted = new long[(int) (db.countType(TYPE, false) / nth) + 1];
    final int[] count = { 0 };
    db.transaction(() -> {
      final Iterator<Record> it = bucket(db).iterator();
      int i = 0;
      while (it.hasNext()) {
        final Record record = it.next();
        if (++i % nth == 0) {
          deleted[count[0]++] = record.getIdentity().getPosition();
          db.deleteRecord(record);
        }
      }
    });
    return Arrays.copyOf(deleted, count[0]);
  }

  static LocalBucket bucket(final Database db) {
    final List<Integer> ids = db.getSchema().getType(TYPE).getBucketIds(false);
    assertThat(ids).hasSize(1);
    final Bucket bucket = db.getSchema().getBucketById(ids.get(0));
    return (LocalBucket) bucket;
  }

  static String brandName(final int i) {
    return String.format("brand-%03d", i);
  }
}
