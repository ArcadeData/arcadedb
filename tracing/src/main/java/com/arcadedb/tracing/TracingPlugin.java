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
package com.arcadedb.tracing;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.monitor.OtelResourceAttributes;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Optional OpenTelemetry tracing plugin. When enabled it bridges an OTel tracer into the server's
 * shared {@link ObservationRegistry}, so the existing Observations also emit OTLP-exported spans
 * (continuing an inbound W3C {@code traceparent} when present). Disabled by default; the OTel SDK
 * is confined to this module and never reaches the core/server compile classpath.
 */
public class TracingPlugin implements ServerPlugin {
  // OpenTelemetry's invalid/no-span trace id (all zeros): treated as "no active trace".
  private static final String           INVALID_TRACE_ID = "00000000000000000000000000000000";
  private boolean                       enabled;
  private String                        endpoint;
  private double                        samplingRate;
  private String                        serviceName;
  private SdkTracerProvider             tracerProvider;
  private DeactivatableObservationHandler attachedHandler;

  /**
   * The enable flag is the whole opt-in: a deployment that sets it does not also have to name this plugin in
   * {@code arcadedb.server.plugins} (issue #7281). Before this, the flag governed what {@code configure()} did and
   * nothing governed whether {@code configure()} ran, so setting it alone produced no span and no log line.
   */
  @Override
  public boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED);
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    enabled = configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS_TRACING_ENABLED);
    if (!enabled)
      return;

    endpoint = configuration.getValueAsString(GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT);
    if (endpoint == null || endpoint.isBlank()) {
      enabled = false;
      LogManager.instance().log(this, Level.WARNING, "OpenTelemetry tracing endpoint not configured, tracing disabled");
      return;
    }
    samplingRate = configuration.getValueAsFloat(GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE);

    // Export off the request thread: BatchSpanProcessor buffers spans and ships them on a background
    // worker, so observation.stop() in the HTTP handler never blocks on a network call. A malformed
    // endpoint must degrade gracefully (tracing disabled) rather than fail server startup.
    try {
      final SpanExporter exporter = OtlpGrpcSpanExporter.builder().setEndpoint(endpoint).build();
      final Map<String, String> resourceAttributes = OtelResourceAttributes.resolve(configuration);
      serviceName = resourceAttributes.get(OtelResourceAttributes.SERVICE_NAME);
      attach(server.getObservationRegistry(), BatchSpanProcessor.builder(exporter).build(), samplingRate,
          resource(resourceAttributes), excludedPaths(configuration));
    } catch (final Exception e) {
      enabled = false;
      serviceName = null;
      if (tracerProvider != null) {
        tracerProvider.close();
        tracerProvider = null;
      }
      LogManager.instance()
          .log(this, Level.SEVERE, "Failed to initialize OpenTelemetry tracing (endpoint=%s), tracing disabled", e, endpoint);
    }
  }

  @Override
  public void startService() {
    if (enabled)
      LogManager.instance()
          .log(this, Level.INFO, "OpenTelemetry tracing enabled (endpoint=%s, samplingRate=%s, serviceName=%s)", endpoint,
              samplingRate, serviceName);
  }

  @Override
  public void stopService() {
    serviceName = null;
    // Deactivate the handler BEFORE closing the provider: the ObservationRegistry has no
    // remove-handler API, so the handler stays registered, but once deactivated it is a no-op and
    // never touches the closed tracer provider.
    LogManager.instance().setTraceContextSupplier(null);
    if (attachedHandler != null) {
      attachedHandler.deactivate();
      attachedHandler = null;
    }
    if (tracerProvider != null) {
      tracerProvider.close();
      tracerProvider = null;
    }
  }

  @Override
  public boolean isActive() {
    return enabled;
  }

  /**
   * Builds an OTel tracer feeding {@code processor} and registers a first-matching composite handler
   * on the registry: the propagating receiver handler claims contexts carrying an inbound
   * {@code traceparent} (continuing the upstream trace); everything else opens a fresh span. The HTTP requests whose
   * path is in {@code excludedPaths} are declined by the tracing handler, so they get no span (issue #7295).
   */
  private void attach(final ObservationRegistry registry, final SpanProcessor processor, final double samplingRate,
      final Resource resource, final String[] excludedPaths) {
    tracerProvider = SdkTracerProvider.builder()
        .setResource(resource)
        .addSpanProcessor(processor)
        .setSampler(Sampler.parentBased(samplingRate >= 1.0 ?
            Sampler.alwaysOn() :
            samplingRate <= 0.0 ? Sampler.alwaysOff() : Sampler.traceIdRatioBased(samplingRate)))
        .build();

    final OpenTelemetry otel = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .build();

    final io.opentelemetry.api.trace.Tracer otelTracer = otel.getTracer("arcadedb");
    final Tracer tracer = new OtelTracer(otelTracer, new OtelCurrentTraceContext(), event -> {
      // no event handling required
    });
    final Propagator propagator = new OtelPropagator(otel.getPropagators(), otelTracer);

    attachedHandler = new DeactivatableObservationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
        new DefaultTracingObservationHandler(tracer)), excludedPaths);
    registry.observationConfig().observationHandler(attachedHandler);

    // Expose the active trace context to the core logger (issue #4466) without the core taking an
    // OpenTelemetry dependency. The HTTP handler reads this when populating its per-request
    // correlation, so JSON/text logs carry the traceId. Returns null when no real span is active so
    // logging degrades cleanly. Cleared in stopService().
    LogManager.instance().setTraceContextSupplier(() -> {
      final Span span = tracer.currentSpan();
      if (span == null)
        return null;
      final TraceContext context = span.context();
      if (context == null)
        return null;
      final String traceId = context.traceId();
      if (traceId == null || traceId.isEmpty() || INVALID_TRACE_ID.equals(traceId))
        return null;
      return new String[] { traceId, context.spanId() };
    });
  }

  /**
   * Test seam: attach a tracer that always samples and exports synchronously (in-process) to the
   * supplied exporter, so tests can assert spans immediately without a background flush.
   */
  void attachForTest(final ObservationRegistry registry, final SpanExporter exporter) {
    attachForTest(registry, exporter, new ContextConfiguration(), Map.of());
  }

  /**
   * Test seam: as {@link #attachForTest(ObservationRegistry, SpanExporter)}, resolving the resource and the excluded
   * paths the way {@link #configure} does, from {@code configuration} and a stand-in for the process environment.
   */
  void attachForTest(final ObservationRegistry registry, final SpanExporter exporter, final ContextConfiguration configuration,
      final Map<String, String> environment) {
    attach(registry, SimpleSpanProcessor.create(exporter), 1.0, resource(OtelResourceAttributes.resolve(configuration, environment)),
        excludedPaths(configuration));
  }

  /**
   * The SDK default resource ({@code telemetry.sdk.*}, and {@code service.name=unknown_service:java}) overridden by the
   * resolved attributes, which always carry a real {@code service.name} (issue #7295).
   */
  static Resource resource(final Map<String, String> attributes) {
    final AttributesBuilder builder = Attributes.builder();
    for (final Map.Entry<String, String> entry : attributes.entrySet())
      builder.put(entry.getKey(), entry.getValue());
    return Resource.getDefault().merge(Resource.create(builder.build()));
  }

  /**
   * Parses {@link GlobalConfiguration#SERVER_METRICS_TRACING_EXCLUDED_PATHS} into an array scanned on every traced
   * request: blank entries are dropped, so an empty setting traces everything, and a trailing {@code /} is removed so
   * {@code /api/v1/ready/} in the setting means the same as {@code /api/v1/ready}.
   */
  static String[] excludedPaths(final ContextConfiguration configuration) {
    final String raw = configuration.getValueAsString(GlobalConfiguration.SERVER_METRICS_TRACING_EXCLUDED_PATHS);
    if (raw == null || raw.isBlank())
      return new String[0];
    return Arrays.stream(raw.split(",")).map(String::trim).filter(p -> !p.isEmpty()).map(TracingPlugin::withoutTrailingSlash)
        .distinct().toArray(String[]::new);
  }

  /**
   * Drops one trailing {@code /} (never from the root path itself), so a request for {@code /api/v1/ready/} matches the
   * {@code /api/v1/ready} entry.
   */
  static String withoutTrailingSlash(final String path) {
    return path.length() > 1 && path.charAt(path.length() - 1) == '/' ? path.substring(0, path.length() - 1) : path;
  }

  /**
   * Wraps the tracing handler so it can be turned off on {@link #stopService()}. The
   * {@link ObservationRegistry} offers no API to remove a handler, so once the plugin stops this
   * gates every callback to a no-op - preventing the (now closed) tracer provider from being used by
   * later Observations.
   * <p>
   * It also declines the HTTP requests whose raw path ({@link AbstractServerHttpHandler#OBSERVATION_REQUEST_PATH}) is
   * one of the excluded paths (issue #7295): the readiness and health probes get no span, while the Observation itself
   * stays alive for any other handler registered on the server. {@code supportsContext} is asked once, when the
   * Observation is created, and the context supplier has already stored the path by then.
   */
  private static final class DeactivatableObservationHandler implements ObservationHandler<Observation.Context> {
    private final ObservationHandler<Observation.Context> delegate;
    private final String[]                                excludedPaths;
    private final AtomicBoolean                                                     active = new AtomicBoolean(true);

    private DeactivatableObservationHandler(final ObservationHandler<Observation.Context> delegate, final String[] excludedPaths) {
      this.delegate = delegate;
      this.excludedPaths = excludedPaths;
    }

    private boolean isExcluded(final Observation.Context context) {
      if (excludedPaths.length == 0)
        return false;
      if (!(context.get(AbstractServerHttpHandler.OBSERVATION_REQUEST_PATH) instanceof String requestPath))
        return false;
      final String path = withoutTrailingSlash(requestPath);
      for (final String excluded : excludedPaths)
        if (excluded.equals(path))
          return true;
      return false;
    }

    private void deactivate() {
      active.set(false);
    }

    @Override
    public boolean supportsContext(final Observation.Context context) {
      return active.get() && !isExcluded(context) && delegate.supportsContext(context);
    }

    @Override
    public void onStart(final Observation.Context context) {
      if (active.get())
        delegate.onStart(context);
    }

    @Override
    public void onError(final Observation.Context context) {
      if (active.get())
        delegate.onError(context);
    }

    @Override
    public void onEvent(final Observation.Event event,
        final Observation.Context context) {
      if (active.get())
        delegate.onEvent(event, context);
    }

    @Override
    public void onScopeOpened(final Observation.Context context) {
      if (active.get())
        delegate.onScopeOpened(context);
    }

    @Override
    public void onScopeClosed(final Observation.Context context) {
      if (active.get())
        delegate.onScopeClosed(context);
    }

    @Override
    public void onScopeReset(final Observation.Context context) {
      if (active.get())
        delegate.onScopeReset(context);
    }

    @Override
    public void onStop(final Observation.Context context) {
      if (active.get())
        delegate.onStop(context);
    }
  }
}
