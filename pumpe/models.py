import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import ConfigDict, field_validator, model_validator
from pydantic.alias_generators import to_snake
from sqlalchemy.orm import declared_attr
from sqlmodel import JSON, Field, SQLModel


class PumpMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


class PumpMeta(SQLModel, table=True):
    __tablename__ = "pump_meta"

    id: int | None = Field(default=None, primary_key=True)
    pump: str = Field(index=True)
    mode: PumpMode
    started: datetime
    skipped: int = 0
    created: int = 0
    updated: int = 0
    deleted: int = 0
    elapsed: float | None = None


class PumpModel(SQLModel):
    pump_hash__: str | None = Field(default=None, index=True)
    pump_modified__: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)},
    )
    pump_touched__: bool = Field(default=True)
    pump_extra__: dict[str, Any] | None = Field(
        default=None,
        sa_type=JSON(none_as_null=True),
    )

    # SQLModel types model_config as its private SQLModelConfig, a ConfigDict subtype.
    model_config = ConfigDict(from_attributes=True, extra="allow")  # type: ignore[assignment]

    # Same declaration as SQLModel's own __tablename__, which SQLAlchemy's stubs do not accept.
    @declared_attr  # type: ignore[arg-type]
    def __tablename__(self) -> str:
        return to_snake(self.__name__).removesuffix("_model")

    @field_validator("*", mode="before")
    @classmethod
    def no_null_terminated(cls, value: object) -> object:
        if not isinstance(value, str):
            return value

        return value.replace("\x00", "")

    @field_validator("*", mode="after")
    @classmethod
    def datetime_to_utc(cls, value: object) -> object:
        if not isinstance(value, datetime) or value.utcoffset() is None:
            return value

        return value.astimezone(UTC)

    @model_validator(mode="after")
    def compute_pump_extra_and_hash(self) -> Self:
        # One validator, because the hash must cover the extras: separate "after" validators run in
        # definition order, and hashing first silently skipped rows whose only change was in extras.
        extra = None
        if self.model_config.get("extra", False) and self.__pydantic_extra__:
            extra = self.__pydantic_extra__

        self.__dict__["pump_extra__"] = extra

        # Sorted keys: sources may reorder JSON objects between fetches without changing their content.
        fields = self.get_custom_fields() | {"pump_extra__"}
        dump = json.dumps(self.model_dump(mode="json", include=fields), sort_keys=True, separators=(",", ":"))
        self.__dict__["pump_hash__"] = hashlib.sha256(dump.encode()).hexdigest()

        return self

    @classmethod
    def get_custom_fields(cls) -> set[str]:
        private_fields = {"pump_hash__", "pump_modified__", "pump_touched__", "pump_extra__"}
        return {name for name in cls.model_fields if name not in private_fields}

    @classmethod
    def get_primary_key(cls) -> str:
        pk_fields = [
            field_name
            for field_name, field_info in cls.model_fields.items()
            if hasattr(field_info, "primary_key")
            and field_info.primary_key is True  # workaround for wrong PydanticUndefined conversion to bool
        ]

        if len(pk_fields) != 1:
            raise ValueError(f"Model {cls.__name__} must have exactly one primary key field, found: {pk_fields}")

        return pk_fields[0]
