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
package com.arcadedb.nativeimage;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.ProtocolMessageEnum;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

import java.lang.reflect.Method;

/**
 * Registers the public methods of every generated ArcadeDB gRPC message (and its {@code Builder}) and enum for
 * reflection.
 *
 * <p>The gRPC interceptors read request fields through the protobuf descriptor rather than through the typed getters
 * ({@code GrpcAdmissionInterceptor} the {@code database} of every data RPC, {@code GrpcAuthInterceptor} the
 * {@code credentials} of every admin RPC), so an RPC added later is covered without being listed. Descriptor access
 * makes protobuf build the message's field-accessor table, which looks up with {@link Class#getMethod} the
 * {@code getX()}, {@code setX()}, {@code hasX()}, ... of the message and of its builder, {@code newBuilder()} of every
 * message-typed field (including the imported well-known types, such as {@code Timestamp}) and
 * {@code valueOf(EnumValueDescriptor)} of every enum-typed field. Without a reflection registration the native image
 * answers that lookup with {@code NoSuchMethodException} and the RPC fails.
 *
 * <p>The registration is driven by reachability, so a message added to the proto is registered without editing a list.
 */
public final class GrpcMessageReflectionFeature implements Feature {
  static final String GRPC_PACKAGE = "com.arcadedb.server.grpc.";

  @Override
  public String getDescription() {
    return "Registers the ArcadeDB gRPC messages and enums for protobuf descriptor (reflective) field access";
  }

  @Override
  public void beforeAnalysis(final BeforeAnalysisAccess access) {
    access.registerSubtypeReachabilityHandler(GrpcMessageReflectionFeature::onReachable, GeneratedMessage.class);
    access.registerSubtypeReachabilityHandler(GrpcMessageReflectionFeature::onReachable, ProtocolMessageEnum.class);
  }

  private static void onReachable(final DuringAnalysisAccess access, final Class<?> type) {
    if (!type.getName().startsWith(GRPC_PACKAGE))
      return;
    registerProtobufType(type);
    // The field types the accessor table resolves: other messages (also outside the package) and enums
    for (final Method method : type.getMethods())
      if (isProtobufType(method.getReturnType()))
        registerProtobufType(method.getReturnType());
  }

  private static boolean isProtobufType(final Class<?> type) {
    return GeneratedMessage.class.isAssignableFrom(type) || ProtocolMessageEnum.class.isAssignableFrom(type);
  }

  private static void registerProtobufType(final Class<?> type) {
    register(type);
    if (GeneratedMessage.class.isAssignableFrom(type))
      for (final Class<?> nested : type.getClasses())
        if (GeneratedMessage.Builder.class.isAssignableFrom(nested))
          register(nested);
  }

  private static void register(final Class<?> type) {
    RuntimeReflection.register(type);
    RuntimeReflection.register(type.getMethods());
  }
}
