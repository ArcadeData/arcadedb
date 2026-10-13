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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9782: {@code arcadedb.grpc.compression.enabled} was read and printed in the startup line but never
 * applied. Both registries were installed whatever it said, and {@code compression.force} was not gated on it, so with
 * {@code enabled=false, force=true} every response was compressed and the startup line did not mention compression.
 * <p>
 * The wiring is exercised on a real listening server (the health service, which every build exposes), because what the setting
 * promises is what a client sees on the wire: whether a compressed request is understood and whether a response is compressed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9782GrpcCompressionEnabledTest {
  private static final Metadata.Key<String> GRPC_ENCODING = Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

  @TempDir
  Path tempDir;

  private GrpcServerPlugin plugin;
  private Server           server;
  private ManagedChannel   channel;

  @AfterEach
  void tearDown() throws InterruptedException {
    if (channel != null)
      channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    if (server != null)
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    if (plugin != null)
      plugin.stopService();
  }

  private void start(final boolean enabled, final boolean force) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.GRPC_COMPRESSION_ENABLED, enabled);
    config.setValue(GlobalConfiguration.GRPC_COMPRESSION_FORCE, force);

    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer(tempDir, config);
    plugin = new GrpcServerPlugin();
    plugin.configure(arcadeServer, config);

    final NettyServerBuilder builder = NettyServerBuilder.forPort(0);
    plugin.configureServer(builder, config);
    server = builder.build().start();
    channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
  }

  /** Calls the health service, asking for a compressed request when {@code requestCodec} is set; returns the response encoding. */
  private String check(final String requestCodec) {
    final AtomicReference<String> responseEncoding = new AtomicReference<>();
    final ClientInterceptor capture = new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
              @Override
              public void onHeaders(final Metadata responseHeaders) {
                responseEncoding.set(responseHeaders.get(GRPC_ENCODING));
                super.onHeaders(responseHeaders);
              }
            }, headers);
          }
        };
      }
    };

    HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(ClientInterceptors.intercept(channel, capture))
        .withDeadlineAfter(30, TimeUnit.SECONDS);
    if (requestCodec != null)
      stub = stub.withCompression(requestCodec);

    final HealthCheckResponse response = stub.check(HealthCheckRequest.getDefaultInstance());
    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
    return responseEncoding.get();
  }

  @Test
  void compressionEnabledAcceptsACompressedRequestAndDoesNotCompressUnlessForced() throws Exception {
    start(true, false);

    assertThat(check("gzip")).as("a compressed request is understood").isNotEqualTo("gzip");
    assertThat(check(null)).as("nothing forces the response").isNotEqualTo("gzip");
  }

  @Test
  void compressionEnabledAndForcedCompressesTheResponse() throws Exception {
    start(true, true);

    assertThat(check(null)).as("force compresses a response to a client that accepts gzip").isEqualTo("gzip");
  }

  @Test
  void compressionDisabledRefusesACompressedRequest() throws Exception {
    start(false, false);

    assertThatThrownBy(() -> check("gzip")).isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
    assertThat(check(null)).as("an uncompressed call still works, and is answered uncompressed").isNotEqualTo("gzip");
  }

  @Test
  void compressionDisabledRefusesACompressedStreamingCallToo() throws Exception {
    start(false, false);

    // THE REFUSAL IS MADE WHEN THE STREAM IS CREATED, WHATEVER THE KIND OF CALL: A SERVER-STREAMING ONE (WATCH) IS REFUSED AS WELL
    final HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
        .withCompression("gzip");
    assertThatThrownBy(() -> stub.watch(HealthCheckRequest.getDefaultInstance()).hasNext()).isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));

    final HealthGrpc.HealthBlockingStub plain = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
    assertThat(plain.watch(HealthCheckRequest.getDefaultInstance()).next().getStatus())
        .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
  }

  @Test
  void disabledRegistriesStillUnderstandIdentityWithoutAdvertisingAnything() {
    // grpc-java never sends "grpc-encoding: identity" itself, but other clients do, and the spec has every server accept it
    final DecompressorRegistry disabled = GrpcServerPlugin.decompressorRegistry(false);
    assertThat(disabled.lookupDecompressor("identity")).isNotNull();
    assertThat(disabled.lookupDecompressor("gzip")).isNull();
    assertThat(disabled.getAdvertisedMessageEncodings()).isEmpty();
    assertThat(GrpcServerPlugin.compressorRegistry(false).lookupCompressor("gzip")).isNull();

    final DecompressorRegistry enabled = GrpcServerPlugin.decompressorRegistry(true);
    assertThat(enabled.lookupDecompressor("gzip")).isNotNull();
    assertThat(enabled.getAdvertisedMessageEncodings()).contains("gzip");
    assertThat(GrpcServerPlugin.compressorRegistry(true).lookupCompressor("gzip")).isNotNull();
  }

  @Test
  void forceIsIgnoredWhenCompressionIsDisabled() throws Exception {
    start(false, true);

    assertThat(check(null)).as("compression is off, so nothing is compressed whatever force says").isNotEqualTo("gzip");
  }
}
