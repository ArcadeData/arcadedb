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

import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers the swagger OpenAPI model classes, and the Jackson mix-ins swagger-core applies to them, for reflection.
 *
 * <p>{@code GET /api/v1/openapi.json} serializes the {@code io.swagger.v3.oas.models.OpenAPI} document built by
 * {@code OpenApiSpecGenerator} with swagger-core's {@code Json.mapper()}. Jackson finds the properties of each bean by
 * introspecting its declared methods and fields, and reads the {@code @JsonInclude}, {@code @JsonIgnore},
 * {@code @JsonAnyGetter} and {@code @JsonSerialize(using = ...)} annotations of the mix-ins
 * ({@code io.swagger.v3.core.jackson.mixin}). Without a reflection registration the native image answers that
 * introspection with no members at all, so every model serialized as an empty bean and the endpoint answered
 * {@code 200 {}} while the JVM served the whole document (#10573).
 *
 * <p>The registration is driven by reachability, so a model class added by a swagger upgrade, or first used by a new
 * contributor, is registered without editing a list. The serializers a mix-in names only inside an annotation
 * ({@code PathsSerializer}, {@code ApiResponsesSerializer}, ...), which Jackson instantiates reflectively, are registered
 * together with the class that carries the annotation.
 */
public final class OpenApiModelReflectionFeature implements Feature {
  static final String[] PACKAGES = { "io.swagger.v3.oas.models.", "io.swagger.v3.core.jackson." };

  private final Set<Class<?>> registered = ConcurrentHashMap.newKeySet();

  @Override
  public String getDescription() {
    return "Registers the swagger OpenAPI models and their Jackson mix-ins for reflective (Jackson) serialization";
  }

  @Override
  public void beforeAnalysis(final BeforeAnalysisAccess access) {
    // The model and mix-in classes share no supertype of their own (Schema, PathItem, Paths extends LinkedHashMap, the
    // mix-ins are plain abstract classes), so the handler watches every reachable type and keeps those of the two
    // packages: a prefix test per type, at build time only
    access.registerSubtypeReachabilityHandler((a, type) -> registerIfOpenApiType(type), Object.class);
  }

  static boolean isOpenApiType(final String className) {
    for (final String prefix : PACKAGES)
      if (className.startsWith(prefix))
        return true;
    return false;
  }

  private void register(final Class<?> type) {
    if (!registered.add(type))
      return;

    RuntimeReflection.register(type);
    // Two kinds of registration, both needed. registerAll* lets the member QUERIES Jackson runs
    // (getDeclaredMethods(), getDeclaredFields(), getMethods(), ...) answer in full rather than with only the registered
    // members; register(members) makes the members INVOCABLE, since Jackson then calls the getters and constructors it
    // found. A query registration alone returns members that fail when invoked; an invocation registration alone misses
    // whatever was not listed
    RuntimeReflection.registerAllDeclaredConstructors(type);
    RuntimeReflection.registerAllDeclaredMethods(type);
    RuntimeReflection.registerAllDeclaredFields(type);
    RuntimeReflection.registerAllMethods(type);
    RuntimeReflection.registerAllFields(type);
    RuntimeReflection.register(type.getDeclaredConstructors());
    RuntimeReflection.register(type.getDeclaredMethods());
    RuntimeReflection.register(type.getDeclaredFields());

    registerAnnotationReferencedClasses(type);
    for (final Method method : type.getDeclaredMethods())
      registerAnnotationReferencedClasses(method);
    for (final Field field : type.getDeclaredFields())
      registerAnnotationReferencedClasses(field);
  }

  /**
   * Registers the classes an annotation names as a value, such as {@code @JsonSerialize(using = PathsSerializer.class)}
   * or a {@code Class[]} such as {@code @JsonSubTypes}' types: Jackson instantiates or introspects them by reflection, and
   * nothing else in the image may make them reachable.
   */
  private void registerAnnotationReferencedClasses(final AnnotatedElement element) {
    for (final Annotation annotation : element.getDeclaredAnnotations())
      for (final Method attribute : annotation.annotationType().getDeclaredMethods()) {
        final Class<?> attributeType = attribute.getReturnType();
        if (attribute.getParameterCount() != 0 || (attributeType != Class.class && attributeType != Class[].class))
          continue;
        final Object value;
        try {
          value = attribute.invoke(annotation);
        } catch (final ReflectiveOperationException e) {
          // An annotation type this class cannot access: the build must not hide it, since what it names stays unregistered
          throw new IllegalStateException("Cannot read " + annotation.annotationType().getName() + "." + attribute.getName()
              + " on " + element + " to register the classes it names for reflection", e);
        }
        if (value instanceof Class<?> referenced)
          registerIfOpenApiType(referenced);
        else if (value instanceof Class<?>[] referencedClasses)
          for (final Class<?> referenced : referencedClasses)
            registerIfOpenApiType(referenced);
      }
  }

  private void registerIfOpenApiType(final Class<?> type) {
    if (isOpenApiType(type.getName()))
      register(type);
  }
}
