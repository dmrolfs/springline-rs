{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "common.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "common.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "common.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "common.labels" -}}
app.kubernetes.io/name: {{ include "common.name" . }}
helm.sh/chart: {{ include "common.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
pipelineId: {{ .Values.global.pipeline.id }}
jobId: {{ .Values.global.pipeline.jobId }}
pipelineVersionId: {{ .Values.global.pipeline.versionId }}
{{- end -}}

{{/*
Common annotations
*/}}
{{- define "common.annotations" -}}
{{- if .Values.global.pipeline.annotations -}}
{{- range $key, $value := .Values.global.annotations }}
{{ $key }}: {{ $value }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "springline.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "common.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}



{{- define "springline.masterUrl" -}}
{{- if .Values.global.cluster.url -}}
{{ .Values.global.cluster.url }}
{{- else -}}
http://{{ include "pipeline.releaseName" . }}:{{ .Values.global.cluster.port }}
{{- end -}}
{{- end -}}

{{/*
    Helper template to dereference the name of the helper template to use defining the masterUrl
    This way any parent chart can provide its own template for constructing the master URL through
    a named template, and pass it to springline.
*/}}
{{- define "springline.masterUrl.helper" -}}
{{- $helperName := .Values.helpers.masterUrl -}}
{{- include  $helperName . -}}
{{- end -}}
