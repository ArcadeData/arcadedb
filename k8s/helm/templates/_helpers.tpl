{{/*
Expand the name of the chart.
*/}}
{{- define "arcadedb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "arcadedb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "arcadedb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "arcadedb.labels" -}}
app: {{ .Chart.Name }}
helm.sh/chart: {{ include "arcadedb.chart" . }}
{{ include "arcadedb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "arcadedb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arcadedb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "arcadedb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arcadedb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
 Creates kubernetes naming suffix.
*/}}
{{- define "arcadedb.k8sSuffix" -}}
{{- $fullname := (include "arcadedb.fullname" .) -}}
{{- printf ".%s.%s.svc.cluster.local" $fullname .Release.Namespace -}}
{{- end }}

{{/*
Create a list of pod names based the number of replica.
*/}}
{{- define "arcadedb.nodenames" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $names := list -}}
{{- $fullname := (include "arcadedb.fullname" .) -}}
{{- $k8sSuffix := (include "arcadedb.k8sSuffix" .) -}}
{{- $rpcPort := int (default "2424" .Values.service.rpc.port) -}}
{{- range $i, $_ := until $replicas }}
{{- $names = append $names (printf "%s-%d%s:%d" $fullname $i $k8sSuffix $rpcPort) }}
{{- end }}
{{- join "," $names -}}
{{- end }}

{{/*
Preparing a list of plugin ports to build plugin configurations.
*/}}
{{- define "_arcadedb.plugin.ports" -}}
  {{- range $plugin, $config := .Values.arcadedb.plugins -}}
    {{- if $config.enabled }}
      {{- $port := int 0}}
      {{- if eq $plugin "gremlin" }}
        {{- $port = default 8082 $config.port }}
      {{- else if eq $plugin "postgres" }}
        {{- $port = default 5432 $config.port}}
      {{- else if eq $plugin "mongo" }}
        {{- $port = default 27017 $config.port }}
      {{- else if eq $plugin "redis" }}
        {{- $port = default 6379 }}
      {{- else if eq $plugin "prometheus" }}
        {{/*
        Prometheus does not use a port in the plugin configuration. It is accessible from /prometheus endpoint.
        */}}
        {{- $port = -1 }}
      {{- else }}
        {{- if not $config.port }}
          {{- fail (printf "Custom plugin '%s' has no port specified." $plugin) -}}
        {{- end }}
      {{- end }}
{{ $plugin }}:
  port: {{ $port }}
  class: {{ default "" $config.class }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
Create a comma separated list of plugins to be enabled in arcadedb
*/}}
{{- define "arcadedb.plugin.parameters" -}}
{{- $plugins := list -}}
  {{- range $plugin, $config := (include "_arcadedb.plugin.ports" . | fromYaml) }}
    {{- if eq $plugin "gremlin" }}
      {{- $plugins = append $plugins "GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin" }}
    {{- else if eq $plugin "postgres" }}
      {{- $plugins = append $plugins "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin" }}
- {{- printf " -Darcadedb.postgres.port=%d" (int $config.port) -}}
    {{- else if eq $plugin "mongo" }}
      {{- $plugins = append $plugins "MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin" }}
- {{- printf " -Darcadedb.mongo.port=%d" (int $config.port) -}}
    {{- else if eq $plugin "redis" }}
      {{- $plugins = append $plugins "Redis:com.arcadedb.redis.RedisProtocolPlugin" }}
- {{- printf " -Darcadedb.redis.port=%d" (int $config.port) -}}
    {{- else if eq $plugin "prometheus" }}
      {{- $plugins = append $plugins "Prometheus:com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin" }}
    {{- else }}
      {{- $plugins = append $plugins (printf "%s:%s" $plugin $config.class) }}
    {{- end }}
{{- end }}
{{- if gt (len $plugins) 0 }}
- {{- printf " -Darcadedb.server.plugins=%s" (join "," $plugins) -}}
{{- end -}}
{{- end -}}

{{/*
Create service configuration for the enabled plugins
*/}}
{{- define "arcadedb.plugin.service" -}}
  {{- $plugins := (include "_arcadedb.plugin.ports" . | fromYaml) }}
  {{- range $plugin, $config := $plugins }}
    {{- if (gt (int $config.port) 0) }}
- port: {{ $config.port }}
  targetPort: {{ $config.port }}
  protocol: TCP
  name: {{ $plugin }}-port
    {{- end -}}
  {{- end -}}
{{- end -}}
