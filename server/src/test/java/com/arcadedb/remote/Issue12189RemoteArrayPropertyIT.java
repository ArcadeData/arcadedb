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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12189: a property declared ARRAY_OF_SHORTS/INTEGERS/LONGS/FLOATS/DOUBLES read from a record through the remote client
 * was an ArrayList of whatever the JSON parser built, while the embedded API (and a projection of the same property) returns the
 * primitive array: the five array types had no type on the client.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12189RemoteArrayPropertyIT extends BaseGraphServerTest {
  private RemoteDatabase database;
  private RID            rid;

  @Override
  protected String getDatabaseName() {
    return "issue12189";
  }

  @BeforeEach
  void openAndPopulate() {
    database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    database.command("sql", "create document type Doc12189");
    database.command("sql", "create property Doc12189.shorts ARRAY_OF_SHORTS");
    database.command("sql", "create property Doc12189.ints ARRAY_OF_INTEGERS");
    database.command("sql", "create property Doc12189.longs ARRAY_OF_LONGS");
    database.command("sql", "create property Doc12189.floats ARRAY_OF_FLOATS");
    database.command("sql", "create property Doc12189.doubles ARRAY_OF_DOUBLES");

    final MutableDocument doc = database.newDocument("Doc12189");
    doc.set("shorts", new short[] { 1, 2 });
    doc.set("ints", new int[] { 1, 2 });
    doc.set("longs", new long[] { 1L, 5_000_000_000L });
    doc.set("floats", new float[] { 1.5F });
    doc.set("doubles", new double[] { 2.5D });
    doc.save();
    rid = doc.getIdentity();
  }

  @AfterEach
  void closeDatabase() {
    if (database != null)
      database.close();
  }

  @Test
  void theDeclaredTypeIsKnownOnTheClient() {
    final DocumentType type = database.getSchema().getType("Doc12189");
    assertThat(type.getProperty("shorts").getType()).isEqualTo(Type.ARRAY_OF_SHORTS);
    assertThat(type.getProperty("ints").getType()).isEqualTo(Type.ARRAY_OF_INTEGERS);
    assertThat(type.getProperty("longs").getType()).isEqualTo(Type.ARRAY_OF_LONGS);
    assertThat(type.getProperty("floats").getType()).isEqualTo(Type.ARRAY_OF_FLOATS);
    assertThat(type.getProperty("doubles").getType()).isEqualTo(Type.ARRAY_OF_DOUBLES);
  }

  @Test
  void aRecordReadByRidHoldsThePrimitiveArrays() {
    assertArrays(database.lookupByRID(rid, true).asDocument());
  }

  @Test
  void aRecordReadByQueryHoldsThePrimitiveArrays() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc12189")) {
      assertArrays(rs.next().getElement().get());
    }
    assertArrays(database.iterateType("Doc12189", false).next().asDocument());
  }

  private static void assertArrays(final Document doc) {
    assertThat(doc.get("shorts")).isInstanceOf(short[].class);
    assertThat((short[]) doc.get("shorts")).containsExactly((short) 1, (short) 2);
    assertThat(doc.get("ints")).isInstanceOf(int[].class);
    assertThat((int[]) doc.get("ints")).containsExactly(1, 2);
    assertThat(doc.get("longs")).isInstanceOf(long[].class);
    assertThat((long[]) doc.get("longs")).containsExactly(1L, 5_000_000_000L);
    assertThat(doc.get("floats")).isInstanceOf(float[].class);
    assertThat((float[]) doc.get("floats")).containsExactly(1.5F);
    assertThat(doc.get("doubles")).isInstanceOf(double[].class);
    assertThat((double[]) doc.get("doubles")).containsExactly(2.5D);
  }
}
