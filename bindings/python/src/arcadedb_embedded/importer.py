from __future__ import annotations

from dataclasses import dataclass, field
from os import PathLike, fspath
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import jpype

from .exceptions import ArcadeDBError


@dataclass(slots=True)
class ImportResult:
    result: str
    operation: str
    source_url: str | None = None
    statistics: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.statistics.get(key, default)

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self.statistics)
        payload.update(
            {
                "result": self.result,
                "operation": self.operation,
                "source_url": self.source_url,
            }
        )
        return payload


def normalize_import_source(source: str | PathLike[str]) -> str:
    source_text = fspath(source)
    parsed = urlparse(source_text)
    if parsed.scheme and not (len(parsed.scheme) == 1 and source_text[1:3] == ":\\"):
        return source_text
    return Path(source_text).expanduser().resolve(strict=False).as_uri()


def import_documents(
    java_database,
    source: str | PathLike[str],
    *,
    document_type: str,
    file_type: str | None = None,
    delimiter: str | None = None,
    header: str | None = None,
    skip_entries: int | None = None,
    properties_include: str | None = None,
    commit_every: int | None = None,
    parallel: int | None = None,
    wal: bool | None = None,
    verbose_level: int | None = None,
    probe_only: bool | None = None,
    force_database_create: bool | None = None,
    trim_text: bool | None = None,
    extra_settings: Mapping[str, Any] | None = None,
) -> ImportResult:
    source_url = normalize_import_source(source)
    settings: dict[str, Any] = {
        "documents": source_url,
        "documentType": document_type,
    }

    optional_settings = {
        "documentsFileType": file_type,
        "documentsDelimiter": delimiter,
        "documentsHeader": header,
        "documentsSkipEntries": skip_entries,
        "documentPropertiesInclude": properties_include,
        "commitEvery": commit_every,
        "parallel": parallel,
        "wal": wal,
        "verboseLevel": verbose_level,
        "probeOnly": probe_only,
        "forceDatabaseCreate": force_database_create,
        "trimText": trim_text,
    }

    for key, value in optional_settings.items():
        if value is not None:
            settings[key] = value

    if extra_settings:
        for key, value in extra_settings.items():
            if value is not None:
                settings[key] = value

    try:
        Importer = jpype.JClass("com.arcadedb.integration.importer.Importer")
        HashMap = jpype.JClass("java.util.HashMap")

        importer = Importer(java_database, None)
        java_settings = HashMap()
        for key, value in settings.items():
            java_settings.put(key, _stringify_import_setting(value))

        importer.setSettings(java_settings)
        java_stats = _run_import_with_runtime_settings(
            java_database,
            importer,
            parallel=parallel,
            commit_every=commit_every,
            wal=wal,
        )
        statistics = _java_value_to_python(java_stats) if java_stats is not None else {}

        return ImportResult(
            result="PROBE_ONLY" if probe_only and java_stats is None else "OK",
            operation="import documents",
            source_url=source_url,
            statistics=statistics,
        )
    except Exception as e:
        raise ArcadeDBError(f"Document import failed: {e}") from e


def _run_import_with_runtime_settings(
    java_database,
    importer,
    *,
    parallel: int | None,
    commit_every: int | None,
    wal: bool | None,
):
    java_async = java_database.async_()
    saved_read_your_writes = bool(java_database.isReadYourWrites())
    saved_parallel = int(java_async.getParallelLevel())
    saved_commit_every = int(java_async.getCommitEvery())
    saved_use_wal = bool(java_async.isTransactionUseWAL())

    try:
        java_database.setReadYourWrites(False)
        if parallel is not None:
            java_async.setParallelLevel(parallel)
        if commit_every is not None:
            java_async.setCommitEvery(commit_every)
        if wal is not None:
            java_async.setTransactionUseWAL(wal)

        return importer.load()
    finally:
        try:
            java_async.waitCompletion()
        finally:
            java_database.setReadYourWrites(saved_read_your_writes)
            java_async.setParallelLevel(saved_parallel)
            java_async.setCommitEvery(saved_commit_every)
            java_async.setTransactionUseWAL(saved_use_wal)


def _stringify_import_setting(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, PathLike):
        return normalize_import_source(value)
    return str(value)


def _java_value_to_python(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if hasattr(value, "entrySet"):
        converted: dict[str, Any] = {}
        for entry in value.entrySet():
            converted[str(entry.getKey())] = _java_value_to_python(entry.getValue())
        return converted

    if hasattr(value, "toArray") and not isinstance(value, (bytes, bytearray)):
        return [_java_value_to_python(item) for item in value.toArray()]

    return value
