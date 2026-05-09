from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

from sqlalchemy.types import DateTime, TypeDecorator


class IsoZDateTime(TypeDecorator):
    """
    SQLite (and some drivers) may store timezone-aware datetimes as ISO strings.
    Python's datetime.fromisoformat() does not accept a trailing 'Z' (UTC).
    This type makes result/bind processing tolerant of that form.
    """

    impl = DateTime
    cache_ok = True

    def bind_processor(self, dialect: Any) -> Optional[Callable[[Any], Any]]:
        impl_proc = self.impl_instance.bind_processor(dialect)

        def process(value: Any) -> Any:
            if value is None:
                return None

            if isinstance(value, str):
                value = self._parse_iso(value)

            if isinstance(value, datetime) and value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)

            return impl_proc(value) if impl_proc else value

        return process

    def result_processor(self, dialect: Any, coltype: Any) -> Optional[Callable[[Any], Any]]:
        """
        SQLAlchemy's DateTime impl may try to parse strings before TypeDecorator
        gets a chance. Wrap the impl processor so we can normalize `...Z`.
        """
        impl_proc = self.impl_instance.result_processor(dialect, coltype)

        def process(value: Any) -> Any:
            if value is None or isinstance(value, datetime):
                return value

            if isinstance(value, str) and value.endswith("Z"):
                value = value[:-1] + "+00:00"

            if impl_proc:
                try:
                    return impl_proc(value)
                except ValueError:
                    # If the impl couldn't parse it, fall back to our parser.
                    if isinstance(value, str):
                        return self._parse_iso(value)
                    raise

            if isinstance(value, str):
                return self._parse_iso(value)
            return value

        return process

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, str):
            value = self._parse_iso(value)

        if isinstance(value, datetime):
            # If it's naive, assume UTC to avoid inconsistent storage.
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        return value

    def process_result_value(self, value: Any, dialect: Any) -> Optional[datetime]:
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            return self._parse_iso(value)

        return value

    @staticmethod
    def _parse_iso(value: str) -> datetime:
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            # Fall back to common SQLite datetime formats.
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            raise

