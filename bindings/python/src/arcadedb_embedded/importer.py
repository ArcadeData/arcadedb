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


@dataclass(slots=True)
class _AsyncRuntimeSettings:
    read_your_writes: bool
    parallel: int
    commit_every: int
    wal: bool

    @classmethod
    def capture(cls, java_database, java_async):
        return cls(
            read_your_writes=bool(java_database.isReadYourWrites()),
            parallel=int(java_async.getParallelLevel()),
            commit_every=int(java_async.getCommitEvery()),
            wal=bool(java_async.isTransactionUseWAL()),
        )

    def apply_overrides(
        self,
        java_database,
        java_async,
        *,
        parallel: int | None,
        commit_every: int | None,
        wal: bool | None,
    ) -> None:
        java_database.setReadYourWrites(False)
        if parallel is not None:
            java_async.setParallelLevel(parallel)
        if commit_every is not None:
            java_async.setCommitEvery(commit_every)
        if wal is not None:
            java_async.setTransactionUseWAL(wal)

    def restore(self, java_database, java_async) -> None:
        java_database.setReadYourWrites(self.read_your_writes)
        java_async.setParallelLevel(self.parallel)
        java_async.setCommitEvery(self.commit_every)
        java_async.setTransactionUseWAL(self.wal)


def normalize_import_source(source: str | PathLike[str]) -> str:
    source_text = fspath(source)
    parsed = urlparse(source_text)
    if parsed.scheme and not (len(parsed.scheme) == 1 and source_text[1:3] == ":\\"):
        return source_text
    return Path(source_text).expanduser().resolve(strict=False).as_uri()


def _build_import_settings(
    source_url: str,
    *,
    document_type: str,
    file_type: str | None,
    delimiter: str | None,
    header: str | None,
    skip_entries: int | None,
    properties_include: str | None,
    commit_every: int | None,
    parallel: int | None,
    wal: bool | None,
    verbose_level: int | None,
    probe_only: bool | None,
    force_database_create: bool | None,
    trim_text: bool | None,
    extra_settings: Mapping[str, Any] | None,
) -> dict[str, Any]:
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

    settings.update(
        {key: value for key, value in optional_settings.items() if value is not None}
    )

    if extra_settings:
        settings.update(
            {key: value for key, value in extra_settings.items() if value is not None}
        )

    return settings


def _to_java_settings_map(settings: Mapping[str, Any]):
    HashMap = jpype.JClass("java.util.HashMap")

    java_settings = HashMap()
    for key, value in settings.items():
        java_settings.put(key, _stringify_import_setting(value))
    return java_settings


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
    settings = _build_import_settings(
        source_url,
        document_type=document_type,
        file_type=file_type,
        delimiter=delimiter,
        header=header,
        skip_entries=skip_entries,
        properties_include=properties_include,
        commit_every=commit_every,
        parallel=parallel,
        wal=wal,
        verbose_level=verbose_level,
        probe_only=probe_only,
        force_database_create=force_database_create,
        trim_text=trim_text,
        extra_settings=extra_settings,
    )

    try:
        Importer = jpype.JClass("com.arcadedb.integration.importer.Importer")

        importer = Importer(java_database, None)
        importer.setSettings(_to_java_settings_map(settings))
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
    saved_settings = _AsyncRuntimeSettings.capture(java_database, java_async)

    try:
        saved_settings.apply_overrides(
            java_database,
            java_async,
            parallel=parallel,
            commit_every=commit_every,
            wal=wal,
        )

        return importer.load()
    finally:
        try:
            java_async.waitCompletion()
        finally:
            saved_settings.restore(java_database, java_async)


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
