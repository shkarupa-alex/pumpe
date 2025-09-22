from collections.abc import Iterable
from functools import cached_property
from operator import attrgetter
from typing import Any

from sqlmodel import delete, select, update

from pumpe.models import PumpMeta, PumpMode, PumpModel
from pumpe.pumps.base import BasePump


class ModelPump(BasePump):
    _model: type[PumpModel] | None = None

    @cached_property
    def model(self) -> type[PumpModel]:
        if self._model is None:
            raise ValueError("Model should be set via `_model` property")
        if not issubclass(self._model, PumpModel):
            raise ValueError("Model should be a subclass of `PumpModel`")
        if not getattr(self._model, "model_config", {}).get("table", False):
            raise ValueError("Model should have a table backend")

        return self._model

    @property
    def title(self) -> str:
        return self.model.__name__

    @cached_property
    def id(self) -> attrgetter:
        return attrgetter(self.model.get_primary_key())

    async def _process_all(self, meta: PumpMeta) -> PumpMeta:
        await self._untouch_all(meta)
        await super()._process_all(meta)
        meta.deleted = await self._delete_untouched(meta)

        return meta

    async def _untouch_all(self, meta: PumpMeta) -> None:
        if meta.mode == PumpMode.PARTIAL:
            return

        query = update(self.model).values(pump_touched__=False)
        await self.session.exec(query)
        await self.session.commit()

    async def _delete_untouched(self, meta: PumpMeta) -> int:
        if meta.mode == PumpMode.PARTIAL:
            return 0

        query = delete(self.model).where(self.model.pump_touched__.is_(False))
        deleted = (await self.session.exec(query)).rowcount
        await self.session.commit()

        return deleted

    async def _process_batch(self, batch: tuple[dict[str, Any]], meta: PumpMeta) -> None:
        items = map(self.model.model_validate, batch)
        items = {self.id(i): i for i in items}

        query_exist = select(self.model).where(self.id(self.model).in_(items))
        existing = (await self.session.exec(query_exist)).all()

        unchanged = {
            self.id(e): items.pop(self.id(e)) for e in existing if items[self.id(e)].pump_hash__ == e.pump_hash__
        }
        changed = {self.id(e): items.pop(self.id(e)) for e in existing if self.id(e) not in unchanged}

        meta.skipped += len(unchanged)
        meta.created += len(items)
        meta.updated += len(changed)

        if meta.mode == PumpMode.FULL:
            query_touch = update(self.model).values(pump_touched__=True).where(self.id(self.model).in_(unchanged))
            await self.session.exec(query_touch)

        await self._process_insert(items.values())
        await self._process_update(changed.values())
        await self.session.commit()

    async def _process_insert(self, items: Iterable[PumpModel]) -> None:
        await self.session.run_sync(lambda s: s.bulk_insert_mappings(self.model, items))

    async def _process_update(self, items: Iterable[PumpModel]) -> None:
        await self.session.run_sync(lambda s: s.bulk_update_mappings(self.model, items))
