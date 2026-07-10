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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5046 (COR-4): writing an {@code int32}/{@code float}/{@code double} wire
 * value into a schema {@code BYTE} or {@code SHORT} property threw a {@link ClassCastException} on the
 * server.
 *
 * <p>Root cause: {@code convertWithSchemaType} narrowed BYTE/SHORT values with
 * {@code (byte) (long) fromGrpcValue(v)}. Because {@code fromGrpcValue} returns an {@code Integer} for
 * {@code INT32_VALUE} (and {@code Double}/{@code Float} for the floating kinds), the intermediate
 * {@code (long)} unboxing cast required the runtime type to be {@code Long}, so it threw
 * {@code java.lang.ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Long}
 * and the RPC failed with {@code INTERNAL}. The fix narrows via {@link Number#byteValue()} /
 * {@link Number#shortValue()}.
 */
public class Issue5046GrpcByteShortWriteIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final int    HTTP_PORT   = 2480;
  private static final String VERTEX_TYPE = "Issue5046Vertex";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, HTTP_PORT, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.b IF NOT EXISTS BYTE");
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.s IF NOT EXISTS SHORT");
    grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`");
  }

  @AfterEach
  void closeClient() {
    if (grpc != null) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
        // ignore
      }
      grpc.close();
    }
    if (grpcServer != null)
      grpcServer.close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void writingInt32IntoByteAndShortSucceeds() {
    // Small integers naturally travel over the wire as int32_value: exactly the kind that triggered
    // the ClassCastException before the fix.
    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("b", 5);      // Integer -> int32_value into a BYTE property
    v.set("s", 1000);   // Integer -> int32_value into a SHORT property
    v.save();
    grpc.commit();

    v.reload();
    assertThat(v.getInteger("b")).as("BYTE stored from int32 wire value").isEqualTo(5);
    assertThat(v.getInteger("s")).as("SHORT stored from int32 wire value").isEqualTo(1000);
  }

  @Test
  void writingDoubleAndFloatIntoByteAndShortNarrowsWithoutError() {
    // double_value / float_value kinds must also narrow (truncating toward zero) instead of throwing.
    grpc.begin();
    final MutableVertex v = grpc.newVertex(VERTEX_TYPE);
    v.set("b", 3.7d);   // Double -> double_value into a BYTE property
    v.set("s", 12.9f);  // Float -> float_value into a SHORT property
    v.save();
    grpc.commit();

    v.reload();
    assertThat(v.getInteger("b")).as("BYTE narrowed from double wire value").isEqualTo(3);
    assertThat(v.getInteger("s")).as("SHORT narrowed from float wire value").isEqualTo(12);
  }
}
