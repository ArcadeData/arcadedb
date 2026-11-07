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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11382: CreateRecord and UpdateRecord converted the request's value to the declared property type themselves, with a
 * plain narrowing cast and a {@code null} arm for a kind they did not recognise, so the engine's own range-checked refusal never
 * ran. An INT64 4294967297 sent for an INTEGER property was stored as 1, a bool sent for an INTEGER property was stored as null,
 * and a partial update with such a value answered success and erased the stored value. The two single-record RPCs now refuse what
 * the bulk RPCs (which hand the raw value to the engine) refuse.
 */
public class Issue11382GrpcSchemaTypedConversionRefusesTest extends BaseGrpcServerTest {
  private static final String TYPE = "Typed11382";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
          final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, "root");
            headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
            headers.put(DATABASE_HEADER, getDatabaseName());
            super.start(responseListener, headers);
          }
        };
      }
    }));

    final Database database = getServerDatabase(0, getDatabaseName());
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("i", Type.INTEGER);
    type.createProperty("b", Type.BOOLEAN);
    type.createProperty("s", Type.SHORT);
    type.createProperty("y", Type.BYTE);
    type.createProperty("l", Type.LIST);
    type.createProperty("m", Type.MAP);
    type.createProperty("d", Type.DATETIME);
    type.createProperty("dbl", Type.DOUBLE);
    type.createProperty("ints", Type.ARRAY_OF_INTEGERS);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void createRefusesAnOutOfRangeNumberInsteadOfWrappingIt() {
    assertCreateRefused("i", GrpcValue.newBuilder().setInt64Value(4294967297L).build());
    assertCreateRefused("s", GrpcValue.newBuilder().setInt32Value(70000).build());
    assertCreateRefused("y", GrpcValue.newBuilder().setInt64Value(300L).build());
    assertCreateRefused("i", GrpcValue.newBuilder().setDoubleValue(Double.NaN).build());
    assertThat(count()).isZero();
  }

  @Test
  void createRefusesAValueOfTheWrongKindInsteadOfStoringNull() {
    final GrpcValue bool = GrpcValue.newBuilder().setBoolValue(true).build();
    final GrpcValue map = GrpcValue.newBuilder().setMapValue(GrpcMap.newBuilder().putEntries("k", string("v"))).build();
    final GrpcValue list = GrpcValue.newBuilder().setListValue(GrpcList.newBuilder().addValues(string("v"))).build();
    assertCreateRefused("i", bool);
    assertCreateRefused("i", map);
    assertCreateRefused("i", list);
    assertCreateRefused("dbl", bool);
    assertCreateRefused("dbl", map);
    assertCreateRefused("d", bool);
    assertCreateRefused("l", map);
    assertCreateRefused("m", list);
    assertCreateRefused("ints", map);
    assertCreateRefused("b", GrpcValue.newBuilder().setInt32Value(1).build());
    assertThat(count()).isZero();
  }

  @Test
  void createRefusesAStringThatIsNotABoolean() {
    assertCreateRefused("b", string("yes"));
    assertCreateRefused("b", string("1"));
    assertThat(count()).isZero();
  }

  @Test
  void createRefusesAnOutOfRangeArrayItem() {
    assertCreateRefused("ints", GrpcValue.newBuilder()
        .setListValue(GrpcList.newBuilder().addValues(GrpcValue.newBuilder().setInt64Value(4294967297L))).build());
    assertThat(count()).isZero();
  }

  @Test
  void createStillStoresWellFormedValues() {
    final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(TYPE).setRecord(GrpcRecord.newBuilder().setType(TYPE)
            .putProperties("i", GrpcValue.newBuilder().setInt64Value(7L).build())
            .putProperties("s", string("12"))
            .putProperties("y", GrpcValue.newBuilder().setInt32Value(100).build())
            .putProperties("b", string("true"))
            .putProperties("dbl", GrpcValue.newBuilder().setInt32Value(3).build())
            .putProperties("l", GrpcValue.newBuilder().setListValue(GrpcList.newBuilder().addValues(string("a"))).build())
            .putProperties("ints", GrpcValue.newBuilder().setListValue(GrpcList.newBuilder()
                .addValues(GrpcValue.newBuilder().setInt64Value(5L)).addValues(GrpcValue.newBuilder().setInt32Value(6))).build())
            .build()).build()).getRid();

    final Database database = getServerDatabase(0, getDatabaseName());
    final Document doc = database.lookupByRID(database.newRID(rid), true).asDocument();
    assertThat(doc.getInteger("i")).isEqualTo(7);
    assertThat(doc.getShort("s")).isEqualTo((short) 12);
    assertThat(doc.getByte("y")).isEqualTo((byte) 100);
    assertThat(doc.getBoolean("b")).isTrue();
    assertThat(doc.getDouble("dbl")).isEqualTo(3.0);
    assertThat(doc.getList("l")).containsExactly("a");
    assertThat(doc.get("ints")).isNotNull();
  }

  @Test
  void aPartialUpdateWithAWrongKindedValueIsRefusedAndKeepsTheStoredValue() {
    final String rid = createStored();

    assertThatThrownBy(() -> partialUpdate(rid, "i", GrpcValue.newBuilder().setBoolValue(true).build())).satisfies(this::isInvalidArgument);
    assertThatThrownBy(() -> partialUpdate(rid, "dbl", GrpcValue.newBuilder().setMapValue(GrpcMap.newBuilder().putEntries("k", string("v"))).build()))
        .satisfies(this::isInvalidArgument);
    assertThatThrownBy(() -> partialUpdate(rid, "i", GrpcValue.newBuilder().setInt64Value(4294967297L).build()))
        .satisfies(this::isInvalidArgument);

    final Document doc = lookup(rid);
    assertThat(doc.getInteger("i")).isEqualTo(5);
    assertThat(doc.getDouble("dbl")).isEqualTo(2.5);
  }

  @Test
  void aFullUpdateWithAWrongKindedValueIsRefusedAndKeepsTheStoredValue() {
    final String rid = createStored();

    assertThatThrownBy(() -> stub.updateRecord(UpdateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setRid(rid).setRecord(GrpcRecord.newBuilder().setType(TYPE)
            .putProperties("i", GrpcValue.newBuilder().setBoolValue(true).build())
            .putProperties("dbl", GrpcValue.newBuilder().setDoubleValue(2.5).build()).build()).build())).satisfies(this::isInvalidArgument);

    assertThat(lookup(rid).getInteger("i")).isEqualTo(5);
  }

  @Test
  void aPartialUpdateWithAWellFormedValueStillApplies() {
    final String rid = createStored();
    assertThat(partialUpdate(rid, "i", GrpcValue.newBuilder().setInt64Value(9L).build()).getUpdated()).isTrue();
    assertThat(lookup(rid).getInteger("i")).isEqualTo(9);
  }

  private String createStored() {
    return stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TYPE)
        .setRecord(GrpcRecord.newBuilder().setType(TYPE).putProperties("i", GrpcValue.newBuilder().setInt32Value(5).build())
            .putProperties("dbl", GrpcValue.newBuilder().setDoubleValue(2.5).build()).build()).build()).getRid();
  }

  private UpdateRecordResponse partialUpdate(final String rid, final String property, final GrpcValue value) {
    return stub.updateRecord(UpdateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid)
        .setPartial(PropertiesUpdate.newBuilder().putProperties(property, value).build()).build());
  }

  private void assertCreateRefused(final String property, final GrpcValue value) {
    assertThatThrownBy(() -> stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType(TYPE).setRecord(GrpcRecord.newBuilder().setType(TYPE).putProperties(property, value).build()).build()))
        .as("%s <- %s", property, value.getKindCase()).satisfies(this::isInvalidArgument);
  }

  private void isInvalidArgument(final Throwable t) {
    assertThat(t).isInstanceOf(StatusRuntimeException.class);
    assertThat(((StatusRuntimeException) t).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  private Document lookup(final String rid) {
    final Database database = getServerDatabase(0, getDatabaseName());
    return database.lookupByRID(database.newRID(rid), true).asDocument();
  }

  private long count() {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count() AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static GrpcValue string(final String value) {
    return GrpcValue.newBuilder().setStringValue(value).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
