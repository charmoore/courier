from typing import Generic, TypeVar, Type, Any, Optional

from sqlalchemy.orm import Session
from sqlmodel import SQLModel

from app.utils.logging import Logger


ModelType = TypeVar("ModelType", bound=SQLModel)


class CRUDBase(Generic[ModelType]):
    def __init__(self, model: Type[ModelType]):
        self.logger = Logger("base.crud." + __name__)
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def create(self, db: Session, *, db_obj: ModelType) -> ModelType:
        try:
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            return db_obj
        except Exception as e:
            print(e)

    def update(
        self,
        db: Session,
        *,
        db_obj: ModelType,
        obj_in: ModelType,
    ) -> ModelType:
        obj_data = db_obj.dict

        update_data = obj_in.dict(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def remove(self, db: Session, *, id: Any) -> ModelType:
        obj = db.query(self.model).get(id)
        db.delete(obj)
        db.commit()
        return obj

    def exists(self, db: Session, id: Any) -> bool:
        """Check that an ID exists in the database for a given model"""
        return self.get(db=db, id=id) is not None
