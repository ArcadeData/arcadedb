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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.Type;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10740: over a TIMESERIES type declared with a precision other than MILLISECOND before issue #9800 (reached today only by a
 * logical restore, or {@code TimeSeriesTypeBuilder.withLegacyPrecision} embedded), no form of a timestamp condition can be applied:
 * the unit of the stored timestamps is not known. A date was refused naming the {@code ALTER} that confirms the unit, but a number
 * was refused with "Compare 'ts' with a date", the very form the type refuses, and a string with the generic text, which offers a
 * date as well.
 * <p>
 * The rule, stated over every operand the delete can be handed on the timestamp: a refusal names a form the same type accepts, or
 * says that none exists and names what changes that. On a type whose unit is confirmed, the accepted form is a date and every
 * refusal says so; on a type whose unit is not, nothing is accepted and every refusal names the {@code ALTER}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10740MongoTimeSeriesLegacyPrecisionDeleteTest extends BaseMongoServerTest {
  private static final long   T0    = 1792396800000L;
  private static final long   T1    = 1792396860000L;
  private static final String ALTER = "ALTER TIMESERIES TYPE Leg PRECISION MILLISECOND";

  private MongoClient               client;
  private MongoCollection<Document> confirmed;
  private MongoCollection<Document> legacy;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE TIMESERIES TYPE Ms TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE)");
    // the only way such a type reaches a running server: the restore of an export written before issue #9800 builds it this way
    database.getSchema().buildTimeSeriesType().withName("Leg").withTimestamp("ts").withLegacyPrecision("SECOND")
        .withTag("k", Type.STRING).withField("v", Type.DOUBLE).withShards(1).create();

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    confirmed = client.getDatabase(getDatabaseName()).getCollection("Ms");
    legacy = client.getDatabase(getDatabaseName()).getCollection("Leg");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  @Test
  void everyRefusalOnTheTimestampNamesAFormTheSameTypeAccepts() {
    final List<String> acceptedOnConfirmed = new ArrayList<>();
    final List<String> acceptedOnLegacy = new ArrayList<>();

    for (final Map.Entry<String, Object> operand : timestampOperands().entrySet()) {
      final String confirmedRefusal = refusalOf(confirmed, "Ms", operand.getValue(), acceptedOnConfirmed, operand.getKey());
      // the confirmed type accepts a date, so a refusal there must send the user to a date
      if (confirmedRefusal != null)
        assertThat(confirmedRefusal).as("refusal of %s on the confirmed type", operand.getKey()).contains("date");

      final String legacyRefusal = refusalOf(legacy, "Leg", operand.getValue(), acceptedOnLegacy, operand.getKey());
      // the legacy type accepts nothing on its timestamp: every refusal must name what changes that, and none may offer a form
      assertThat(legacyRefusal).as("refusal of %s on the legacy type", operand.getKey()).isNotNull().contains(ALTER);
    }

    assertThat(acceptedOnConfirmed).as("forms the confirmed type accepts").contains("date", "$gte date", "$lt date");
    assertThat(acceptedOnLegacy).as("forms the legacy type accepts").isEmpty();
  }

  @Test
  void theLegacyTypeIsStillDeletableByItsTagsAndTheAlterOpensTheTimestamp() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        INSERT INTO Leg SET ts = %d, k = 'a', v = 1.0;
        INSERT INTO Leg SET ts = %d, k = 'b', v = 2.0;
        """.formatted(T0, T1));

    // a condition on a FIELD is refused as on any type, and on this one it names the ALTER too: the time range it offers is not open
    assertThatThrownBy(() -> legacy.deleteMany(new Document("v", 1.0))).isInstanceOf(MongoException.class).hasMessageContaining(ALTER);

    // the tags name a series whatever the unit of its timestamps
    assertThat(legacy.deleteMany(new Document("k", "a")).getDeletedCount()).isEqualTo(1);
    assertThat(legacy.countDocuments()).isEqualTo(1);

    // the instruction the refusal gives is the one that works
    database.command("sql", ALTER);
    assertThat(legacy.deleteMany(new Document("ts", new Document("$lte", new Date(T1)))).getDeletedCount()).isEqualTo(1);
    assertThat(legacy.countDocuments()).isZero();
  }

  /**
   * Every kind of operand a client can put on the timestamp, in both the bare-value and the operator-document form, plus the operator
   * shapes the delete does not read.
   */
  private static Map<String, Object> timestampOperands() {
    final Map<String, Object> operands = new LinkedHashMap<>();
    operands.put("date", new Date(T0));
    operands.put("int32", 5);
    operands.put("int64", T0);
    operands.put("double", 1.5d);
    operands.put("string", "2026-10-18T00:00:00Z");
    operands.put("boolean", true);
    operands.put("null", null);
    operands.put("list", List.of(T0));
    for (final String op : List.of("$eq", "$gt", "$gte", "$lt", "$lte")) {
      operands.put(op + " date", new Document(op, new Date(T0)));
      operands.put(op + " int64", new Document(op, T0));
      operands.put(op + " string", new Document(op, "x"));
    }
    operands.put("$ne date", new Document("$ne", new Date(T0)));
    operands.put("$in dates", new Document("$in", List.of(new Date(T0))));
    operands.put("empty operators", new Document());
    return operands;
  }

  /**
   * Runs {@code deleteMany({ts: operand})} over an empty collection: the message of the refusal, or {@code null} and the operand
   * recorded as accepted.
   */
  private static String refusalOf(final MongoCollection<Document> collection, final String typeName, final Object operand,
      final List<String> accepted, final String label) {
    try {
      collection.deleteMany(new Document("ts", operand));
      accepted.add(label);
      return null;
    } catch (final MongoException e) {
      assertThat(e.getMessage()).as("refusal of %s on %s", label, typeName).contains("TIMESERIES type '" + typeName + "'");
      return e.getMessage();
    }
  }
}
