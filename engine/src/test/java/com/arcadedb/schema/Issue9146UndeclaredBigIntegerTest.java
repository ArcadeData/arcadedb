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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9146: a {@link BigInteger} written to an undeclared property must never lose digits. Inside the long range it
 * is narrowed to an Integer/Long with the value intact, outside of it is kept exactly as a DECIMAL instead of being
 * silently rounded to a DOUBLE.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9146UndeclaredBigIntegerTest extends TestHelper {

  private Document write(final String typeName, final BigInteger value) {
    database.getSchema().createDocumentType(typeName);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set("big", value).save().getIdentity());
    return database.lookupByRID(rid[0], true).asDocument();
  }

  @Test
  void bigIntegerInsideLongRangeKeepsItsValue() {
    assertThat(((Number) write("In1", BigInteger.ONE).get("big")).longValue()).isEqualTo(1L);
    assertThat(write("In2", new BigInteger("9007199254740993")).get("big")).isEqualTo(9007199254740993L);
    assertThat(write("In3", BigInteger.valueOf(Long.MAX_VALUE)).get("big")).isEqualTo(Long.MAX_VALUE);
    assertThat(write("In4", BigInteger.valueOf(Long.MIN_VALUE)).get("big")).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void bigIntegerOutsideLongRangeIsNotRoundedToDouble() {
    final String[] values = { "9223372036854775808", "12345678901234567890", "-9223372036854775809",
        "123456789012345678901234567890123456789012345678901234567890" };
    for (int i = 0; i < values.length; i++) {
      final BigInteger expected = new BigInteger(values[i]);
      final Object read = write("Out" + i, expected).get("big");
      assertThat(read).as(values[i]).isInstanceOf(BigDecimal.class);
      assertThat(((BigDecimal) read).toBigIntegerExact()).as(values[i]).isEqualTo(expected);
    }
  }

  @Test
  void sqlTypeReportsDecimalForOutOfRangeBigInteger() {
    write("Sql1", new BigInteger("12345678901234567890"));
    assertThat(database.query("sql", "SELECT big.type() AS t FROM Sql1").next().<String>getProperty("t")).isEqualTo("DECIMAL");
  }
}
