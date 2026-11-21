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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12796: the BSON encoder of the MongoDB wire library takes Integer, Long, Double, Decimal128, String, Boolean, Instant, binary,
 * UUID, collections and maps only. The result conversion returned any other class as it was, so a record holding a FLOAT, SHORT, BYTE,
 * BINARY, ARRAY_OF_* , LINK or EMBEDDED value failed to encode, the server closed the connection, and the client could not read the
 * collection any more. Every value a record can hold must convert to something the encoder takes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12796MongoEncodableTypesTest {
  private static final String DB_PATH = "./target/databases/issue12796";

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE DOCUMENT TYPE X");
    database.command("sql", "CREATE PROPERTY X.f FLOAT");
    database.command("sql", "CREATE PROPERTY X.sh SHORT");
    database.command("sql", "CREATE PROPERTY X.bt BYTE");
    database.command("sql", "CREATE PROPERTY X.bin BINARY");
    database.command("sql", "CREATE PROPERTY X.ash ARRAY_OF_SHORTS");
    database.command("sql", "CREATE PROPERTY X.ai ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY X.al ARRAY_OF_LONGS");
    database.command("sql", "CREATE PROPERTY X.af ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY X.ad ARRAY_OF_DOUBLES");
    database.command("sql", "CREATE PROPERTY X.e EMBEDDED");
    database.command("sql", "CREATE PROPERTY X.lk LINK");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V SET n = 1");
      database.command("sql", "INSERT INTO X SET f = 1.1f, sh = 5, bt = 3, ash = [1, 2], ai = [3, 4], al = [5, 6], af = [1.5f, 2.5f],"
          + " ad = [1.25, 2.5], e = {'@type': 'X', 'sh': 9}, lk = (SELECT FROM V LIMIT 1),"
          + " lf = [1.5f, 2, 'a', {'x': 2.5f}], mp = {'k': 7.5f}");
      final MutableDocument binary = database.newDocument("X");
      binary.set("bin", new byte[] { 1, 2, 3 });
      binary.save();
    });
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      while (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void everyValueTypeOfARecordConvertsToWhatTheBsonEncoderTakes() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM X")) {
      int rows = 0;
      while (rs.hasNext()) {
        final Document converted = MongoDBToSqlTranslator.convertDocumentToMongoDB(rs.next().toElement());
        final ByteBuf buffer = Unpooled.buffer();
        try {
          // throws IllegalArgumentException("Unknown type: ...") for a class the encoder does not know
          BsonEncoder.encodeDocument(converted, buffer);
        } finally {
          buffer.release();
        }
        ++rows;
      }
      assertThat(rows).isEqualTo(2);
    }
  }

  @Test
  void numbersConvertToTheBsonNumberOfTheSameValue() {
    final Document converted = convertedRowWith("f");
    assertThat(converted.get("f")).as("a FLOAT reads as the decimal text it was written as").isEqualTo(1.1d);
    assertThat(converted.get("sh")).isEqualTo(5);
    assertThat(converted.get("bt")).isEqualTo(3);
    assertThat(converted.get("ash")).isEqualTo(List.of(1, 2));
    assertThat(converted.get("ai")).isEqualTo(List.of(3, 4));
    assertThat(converted.get("al")).isEqualTo(List.of(5L, 6L));
    assertThat(converted.get("af")).isEqualTo(List.of(1.5d, 2.5d));
    assertThat(converted.get("ad")).isEqualTo(List.of(1.25d, 2.5d));
    assertThat(converted.get("lf")).isEqualTo(List.of(1.5d, 2, "a", new Document("x", 2.5d)));
    assertThat(converted.get("mp")).isEqualTo(new Document("k", 7.5d));
  }

  @Test
  void aLinkReadsAsItsRidAndAnEmbeddedDocumentAsASubDocument() {
    final Document converted = convertedRowWith("f");
    assertThat(converted.get("lk")).isEqualTo(database.query("sql", "SELECT FROM V").next().getIdentity().get().toString());
    assertThat(converted.get("e")).isEqualTo(new Document("sh", 9));
  }

  @Test
  void aBinaryReadsAsBinData() {
    final Document converted = convertedRowWith("bin");
    assertThat(converted.get("bin")).isInstanceOf(BinData.class);
    assertThat(((BinData) converted.get("bin")).getData()).containsExactly(1, 2, 3);
  }

  private Document convertedRowWith(final String property) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM X WHERE " + property + " IS NOT NULL")) {
      return MongoDBToSqlTranslator.convertDocumentToMongoDB(rs.next().toElement());
    }
  }
}
