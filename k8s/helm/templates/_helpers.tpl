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
{{- default ""  (printf ".%s.%s.svc.cluster.local" $fullname .Release.Namespace) -}}
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
{{ join "," $names }}
{{- end }}
