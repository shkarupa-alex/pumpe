from collections.abc import Iterable
from functools import cached_property
from operator import attrgetter
from typing import Any

from sqlmodel import col, delete, or_, select, update

from pumpe.models import PumpMeta, PumpMode, PumpModel
from pumpe.pumps.base import BasePump


class ModelPump(BasePump):
    _model: type[PumpModel] | None = None

    @cached_property
    def model(self) -> type[PumpModel]:
        if self._model is None:
            raise ValueError("Model should be set via `_model` property")
        if not issubclass(self._model, PumpModel):
            raise TypeError("Model should be a subclass of `PumpModel`")
        if not getattr(self._model, "model_config", {}).get("table", False):
            raise ValueError("Model should have a table backend")

        return self._model

    @property
    def title(self) -> str:
        return self.model.__name__

    @cached_property
    def id(self) -> "attrgetter[Any]":
        return attrgetter(self.model.get_primary_key())

    async def _process_all(self, meta: PumpMeta) -> None:
        await super()._process_all(meta)
        meta.deleted = await self._delete_unseen(meta)

    async def _delete_unseen(self, meta: PumpMeta) -> int:
        if meta.mode == PumpMode.PARTIAL:
            return 0

        # Every row this run fetched carries its token, and runs are serialized, so any other stamp means the row
        # was not in this run's source.
        await self._renew_lease()
        seen = col(self.model.pump_seen__)
        query = delete(self.model).where(or_(seen.is_(None), seen != self._run_token))
        deleted = (await self.session.exec(query)).rowcount
        await self.session.commit()

        return deleted

    async def _process_batch(self, batch: tuple[dict[str, Any], ...], meta: PumpMeta) -> None:
        validated = [self.model.model_validate(record) for record in batch]
        if any(self.id(i) is None for i in validated):
            # Rows are matched by primary key, so records without one would collapse into a single row.
            raise ValueError(f"Source records must carry the primary key: {self.title}")
        items = {self.id(i): i for i in validated}

        query_exist = select(self.model).where(self.id(self.model).in_(items))
        existing = (await self.session.exec(query_exist)).all()

        unchanged = {
            self.id(e): items.pop(self.id(e)) for e in existing if items[self.id(e)].pump_hash__ == e.pump_hash__
        }
        changed = {self.id(e): items.pop(self.id(e)) for e in existing if self.id(e) not in unchanged}

        meta.skipped += len(unchanged)
        meta.created += len(items)
        meta.updated += len(changed)

        if unchanged:
            query_seen = (
                update(self.model)
                .values(pump_seen__=self._run_token, **self._keep_modified)
                .where(self.id(self.model).in_(unchanged))
            )
            await self.session.exec(query_seen)

        await self._process_insert(items.values())
        await self._process_update(changed.values())

    @property
    def _keep_modified(self) -> dict[str, Any]:
        # Scan bookkeeping is not a content change: assigning the column to itself keeps its onupdate from firing.
        return {"pump_modified__": self.model.pump_modified__}

    async def _process_insert(self, items: Iterable[PumpModel]) -> None:
        mappings = [dict(i) | {"pump_seen__": self._run_token} for i in items]
        await self.session.run_sync(lambda s: s.bulk_insert_mappings(self.model, mappings))

    async def _process_update(self, items: Iterable[PumpModel]) -> None:
        mappings = [dict(i) | {"pump_seen__": self._run_token} for i in items]
        await self.session.run_sync(lambda s: s.bulk_update_mappings(self.model, mappings))
