from abc import abstractmethod
from functools import cached_property
from operator import attrgetter
from typing import Any

from sqlmodel import delete, select, update

from pumpe.models import BaseModel, PumpMeta, PumpMode
from pumpe.pumps.base import BasePump


class ModelPump(BasePump):
    @abstractmethod
    def model(self) -> type[BaseModel]:
        pass

    @cached_property
    def _model(self) -> type[BaseModel]:
        model = self.model()
        if not getattr(model, "model_config", {}).get("table", False):
            raise ValueError("Model should have a table backend")

        return model

    @property
    def title(self) -> str:
        return self._model.__name__

    @cached_property
    def _id(self) -> attrgetter:
        return attrgetter(self._model.get_primary_key())

    async def _process_all(self, meta: PumpMeta) -> PumpMeta:
        await self._untouch_all(meta)
        await super()._process_all(meta)
        meta.deleted = await self._delete_untouched(meta)

        return meta

    async def _untouch_all(self, meta: PumpMeta) -> None:
        if meta.mode == PumpMode.PARTIAL:
            return

        query = update(self._model).values(pump_touched__=False)
        await self.session.exec(query)
        await self.session.commit()

    async def _delete_untouched(self, meta: PumpMeta) -> int:
        if meta.mode == PumpMode.PARTIAL:
            return 0

        query = delete(self._model).where(self._model.pump_touched__.is_(False))
        deleted = (await self.session.exec(query)).rowcount
        await self.session.commit()

        return deleted

    async def _process_batch(self, batch: tuple[dict[str, Any]], meta: PumpMeta) -> None:
        items = map(self._model.model_validate, batch)
        items = {self._id(i): i for i in items}

        query_exist = select(self._model).where(self._id(self._model).in_(items))
        existing = (await self.session.exec(query_exist)).all()

        unchanged = {
            self._id(e): items.pop(self._id(e)) for e in existing if items[self._id(e)].pump_hash__ == e.pump_hash__
        }
        changed = {self._id(e): items.pop(self._id(e)) for e in existing if self._id(e) not in unchanged}

        meta.skipped += len(unchanged)
        meta.created += len(items)
        meta.updated += len(changed)

        if meta.mode == PumpMode.FULL:
            query_touch = update(self._model).values(pump_touched__=True).where(self._id(self._model).in_(unchanged))
            await self.session.exec(query_touch)

        await self.session.run_sync(lambda s: s.bulk_insert_mappings(self._model, items.values()))
        await self.session.run_sync(lambda s: s.bulk_update_mappings(self._model, changed.values()))
        await self.session.commit()
