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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11714, over the wire: a {@code float_array_value} parameter or property is stored as the {@code float[]} an
 * {@code ARRAY_OF_FLOATS} property holds and is what the vector functions compute on; a stored vector comes back packed to a
 * call that sent {@code x-arcade-packed-arrays: true}, including from a client transaction's own thread, and as the
 * {@code list_value} it always was to a call that did not; the {@code list_value} parameter of an older client still works.
 */
public class GrpcPackedArraysIssue11714IT extends BaseGrpcServerTest {
  private static final String TYPE = "Vec11714";

  // Canonical NaN and both zeros, compared by their raw bits after a store and a read
  private static final float[] SPECIAL = { Float.NaN, -0.0f, 0.0f, Float.NEGATIVE_INFINITY, 1.0e-30f, -7.5f };
  private static final float[] VECTOR  = { 0.25f, -0.5f, 0.75f, 1.0f };

  private ManagedChannel                                  channel;
  // An older client: never sends the consent header
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub plain;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub packed;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void schemaAndStubs() {
    final Database database = database();
    database.getSchema().createDocumentType(TYPE);
    database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
    database.getSchema().getType(TYPE).createProperty("weights", Type.ARRAY_OF_DOUBLES);

    database.getSchema().createDocumentType("Schemaless11714");
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
    plain = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    final Metadata consent = new Metadata();
    consent.put(GrpcPackedArrays.HEADER, "true");
    packed = plain.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(consent));
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aPackedParameterIsStoredAsTheFloatsAndFeedsTheVectorFunctions() {
    command(plain, "INSERT INTO " + TYPE + " SET name = 'special', embedding = :v", GrpcPackedArrays.encode(SPECIAL));
    command(plain, "INSERT INTO " + TYPE + " SET name = 'vector', embedding = :v", GrpcPackedArrays.encode(VECTOR));
    // A type with no declared property: the float[] decides the stored type by itself
    command(plain, "INSERT INTO Schemaless11714 SET embedding = :v", GrpcPackedArrays.encode(VECTOR));

    assertThat(rawBits(stored(TYPE, "special"))).containsExactly(rawBits(SPECIAL));
    assertThat(stored(TYPE, "vector")).containsExactly(VECTOR);
    try (final ResultSet rs = database().query("sql", "SELECT embedding FROM Schemaless11714")) {
      assertThat(rs.next().<Object>getProperty("embedding")).isEqualTo(VECTOR);
    }

    // The packed query vector reaches the function as the float[] it computes on
    final ExecuteCommandResponse similarity = command(plain,
        "SELECT vector.cosineSimilarity(embedding, :q) AS s FROM " + TYPE + " WHERE name = 'vector'", GrpcPackedArrays.encode(VECTOR));
    assertThat(similarity.getRecords(0).getPropertiesOrThrow("s").getFloatValue()).isCloseTo(1.0f, within(1e-6f));
  }

  @Test
  void theListEncodingOfAnOlderClientStillStoresTheFloats() {
    final GrpcList.Builder list = GrpcList.newBuilder();
    for (final float value : VECTOR)
      list.addValues(GrpcValue.newBuilder().setFloatValue(value));
    command(plain, "INSERT INTO " + TYPE + " SET name = 'list', embedding = :v", GrpcValue.newBuilder().setListValue(list).build());

    assertThat(stored(TYPE, "list")).containsExactly(VECTOR);
  }

  @Test
  void everyResponseAnnouncesThatTheServerDecodesPackedArrays() {
    final AtomicReference<Metadata> headers = new AtomicReference<>();
    final AtomicReference<Metadata> trailers = new AtomicReference<>();
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub observed = plain.withInterceptors(
        MetadataUtils.newCaptureMetadataInterceptor(headers, trailers));

    // To a caller that did not ask for packed results too: the announcement is what lets a client send them
    command(observed, "SELECT 1 AS one", GrpcValue.getDefaultInstance());
    assertThat(headers.get().get(GrpcPackedArrays.HEADER)).isEqualTo("true");
    assertThat(trailers.get().get(GrpcPackedArrays.HEADER)).isEqualTo("true");

    // A call that fails before any message sends trailers only: they carry it
    headers.set(null);
    trailers.set(null);
    assertThatThrownBy(() -> observed.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setCommand("SELECT FROM NoSuchType11714").build())).isInstanceOf(StatusRuntimeException.class);
    assertThat(trailers.get().get(GrpcPackedArrays.HEADER)).isEqualTo("true");
  }

  @Test
  void aStoredVectorComesBackPackedOnlyToACallThatAskedForIt() {
    insertSpecial();

    final GrpcValue asList = queryEmbedding(plain, null);
    assertThat(asList.getKindCase()).as("a client that did not ask must not get a kind it may not know").isEqualTo(
        GrpcValue.KindCase.LIST_VALUE);
    assertThat(asList.getListValue().getValuesCount()).isEqualTo(SPECIAL.length);

    final GrpcValue asPacked = queryEmbedding(packed, null);
    assertThat(asPacked.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    assertThat(rawBits(GrpcPackedArrays.toFloats(asPacked.getFloatArrayValue()))).containsExactly(rawBits(SPECIAL));
  }

  @Test
  void theConsentReachesTheThreadOfAClientTransaction() {
    insertSpecial();
    final String txId = packed.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    try {
      final GrpcValue inTransaction = queryEmbedding(packed, txId);
      assertThat(inTransaction.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
      assertThat(rawBits(GrpcPackedArrays.toFloats(inTransaction.getFloatArrayValue()))).containsExactly(rawBits(SPECIAL));

      // And a call on the same transaction without the consent is answered with a list
      assertThat(queryEmbedding(plain, txId).getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    } finally {
      packed.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName())).build());
    }
  }

  @Test
  void aRecordWrittenPackedIsReadBackPackedWithItsSchemaTypes() {
    final double[] weights = { Double.NaN, -0.0, Math.E };
    final String rid = packed.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setType(TYPE).setRecord(GrpcRecord.newBuilder()
            .putProperties("name", GrpcValue.newBuilder().setStringValue("record").build())
            // The schema-typed path of CreateRecord: ARRAY_OF_FLOATS and ARRAY_OF_DOUBLES properties, and an undeclared one
            .putProperties("embedding", GrpcPackedArrays.encode(SPECIAL))
            .putProperties("weights", GrpcPackedArrays.encode(weights))
            .putProperties("empty", GrpcPackedArrays.encode(new float[0]))).build()).getRid();

    final Document stored = (Document) database().lookupByRID(new RID(rid), true);
    assertThat(stored.get("embedding")).isInstanceOf(float[].class);
    assertThat(rawBits((float[]) stored.get("embedding"))).containsExactly(rawBits(SPECIAL));
    assertThat(stored.get("weights")).isInstanceOf(double[].class);
    assertThat(rawBits((double[]) stored.get("weights"))).containsExactly(rawBits(weights));
    assertThat(stored.get("empty")).isEqualTo(new float[0]);

    final GrpcRecord looked = packed.lookupByRid(
        LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid).build()).getRecord();
    assertThat(rawBits(GrpcPackedArrays.toFloats(looked.getPropertiesOrThrow("embedding").getFloatArrayValue()))).containsExactly(
        rawBits(SPECIAL));
    assertThat(rawBits(GrpcPackedArrays.toDoubles(looked.getPropertiesOrThrow("weights").getDoubleArrayValue()))).containsExactly(
        rawBits(weights));
    final GrpcValue empty = looked.getPropertiesOrThrow("empty");
    assertThat(empty.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    assertThat(empty.getFloatArrayValue().getValuesCount()).isZero();

    final GrpcRecord lookedPlain = plain.lookupByRid(
        LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid).build()).getRecord();
    assertThat(lookedPlain.getPropertiesOrThrow("embedding").getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    assertThat(lookedPlain.getPropertiesOrThrow("weights").getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  private void insertSpecial() {
    command(plain, "INSERT INTO " + TYPE + " SET name = 'special', embedding = :v", GrpcPackedArrays.encode(SPECIAL));
  }

  private GrpcValue queryEmbedding(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String txId) {
    final ExecuteQueryRequest.Builder request = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setQuery("SELECT embedding FROM " + TYPE + " WHERE name = 'special'");
    if (txId != null)
      request.setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()));
    final ExecuteQueryResponse response = stub.executeQuery(request.build());
    return response.getResults(0).getRecords(0).getPropertiesOrThrow("embedding");
  }

  private ExecuteCommandResponse command(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String sql,
      final GrpcValue parameter) {
    final String name = sql.contains(":q") ? "q" : "v";
    final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setCommand(sql).putParameters(name, parameter).setReturnRows(true).build());
    assertThat(response.getSuccess()).as(response.getMessage()).isTrue();
    return response;
  }

  private float[] stored(final String type, final String name) {
    try (final ResultSet rs = database().query("sql", "SELECT embedding FROM " + type + " WHERE name = ?", name)) {
      final Object embedding = rs.next().getProperty("embedding");
      assertThat(embedding).isInstanceOf(float[].class);
      return (float[]) embedding;
    }
  }

  private Database database() {
    return getServerDatabase(0, getDatabaseName());
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private static int[] rawBits(final float[] values) {
    final int[] bits = new int[values.length];
    for (int i = 0; i < values.length; i++)
      bits[i] = Float.floatToRawIntBits(values[i]);
    return bits;
  }

  private static long[] rawBits(final double[] values) {
    final long[] bits = new long[values.length];
    for (int i = 0; i < values.length; i++)
      bits[i] = Double.doubleToRawLongBits(values[i]);
    return bits;
  }
}
