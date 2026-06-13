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
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Optional OpenTelemetry tracing plugin. When enabled it bridges an OTel tracer into the server's
 * shared {@link ObservationRegistry}, so the existing Observations also emit OTLP-exported spans
 * (continuing an inbound W3C {@code traceparent} when present). Disabled by default; the OTel SDK
 * is confined to this module and never reaches the core/server compile classpath.
 */
public class TracingPlugin implements ServerPlugin {
  private boolean                       enabled;
  private String                        endpoint;
  private double                        samplingRate;
  private SdkTracerProvider             tracerProvider;
  private DeactivatableObservationHandler attachedHandler;

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
      attach(server.getObservationRegistry(), BatchSpanProcessor.builder(exporter).build(), samplingRate);
    } catch (final Exception e) {
      enabled = false;
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
          .log(this, Level.INFO, "OpenTelemetry tracing enabled (endpoint=%s, samplingRate=%s)", endpoint, samplingRate);
  }

  @Override
  public void stopService() {
    // Deactivate the handler BEFORE closing the provider: the ObservationRegistry has no
    // remove-handler API, so the handler stays registered, but once deactivated it is a no-op and
    // never touches the closed tracer provider.
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
   * {@code traceparent} (continuing the upstream trace); everything else opens a fresh span.
   */
  private void attach(final ObservationRegistry registry, final SpanProcessor processor, final double samplingRate) {
    tracerProvider = SdkTracerProvider.builder()
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
        new DefaultTracingObservationHandler(tracer)));
    registry.observationConfig().observationHandler(attachedHandler);
  }

  /**
   * Test seam: attach a tracer that always samples and exports synchronously (in-process) to the
   * supplied exporter, so tests can assert spans immediately without a background flush.
   */
  void attachForTest(final ObservationRegistry registry, final SpanExporter exporter) {
    attach(registry, SimpleSpanProcessor.create(exporter), 1.0);
  }

  /**
   * Wraps the tracing handler so it can be turned off on {@link #stopService()}. The
   * {@link ObservationRegistry} offers no API to remove a handler, so once the plugin stops this
   * gates every callback to a no-op - preventing the (now closed) tracer provider from being used by
   * later Observations.
   */
  private static final class DeactivatableObservationHandler implements ObservationHandler<io.micrometer.observation.Observation.Context> {
    private final ObservationHandler<io.micrometer.observation.Observation.Context> delegate;
    private final AtomicBoolean                                                     active = new AtomicBoolean(true);

    private DeactivatableObservationHandler(final ObservationHandler<io.micrometer.observation.Observation.Context> delegate) {
      this.delegate = delegate;
    }

    private void deactivate() {
      active.set(false);
    }

    @Override
    public boolean supportsContext(final io.micrometer.observation.Observation.Context context) {
      return active.get() && delegate.supportsContext(context);
    }

    @Override
    public void onStart(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onStart(context);
    }

    @Override
    public void onError(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onError(context);
    }

    @Override
    public void onEvent(final io.micrometer.observation.Observation.Event event,
        final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onEvent(event, context);
    }

    @Override
    public void onScopeOpened(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onScopeOpened(context);
    }

    @Override
    public void onScopeClosed(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onScopeClosed(context);
    }

    @Override
    public void onScopeReset(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onScopeReset(context);
    }

    @Override
    public void onStop(final io.micrometer.observation.Observation.Context context) {
      if (active.get())
        delegate.onStop(context);
    }
  }
}
