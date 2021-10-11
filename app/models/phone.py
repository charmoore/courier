from typing import Optional, Any, Dict

import phonenumbers
from sqlmodel import SQLModel, Field
from pydantic import validator


class Phone(SQLModel):
    raw_number: str
    from_country: Optional[str] = Field(default="US")
    # phonenumber: Optional[phonenumbers.phonenumber.PhoneNumber] = Field(default=None)
    country_code: Optional[str] = Field(default=None)
    national_number: Optional[str] = Field(default=None)
    is_valid: bool = False

    # @validator("phonenumber", pre=True, allow_reuse=True)
    # def get_phone_number(
    #     cls, v: phonenumbers.phonenumber.PhoneNumber, values: Dict[str, Any]
    # ) -> phonenumbers.phonenumber.PhoneNumber:
    #     return phonenumbers.parse(values.get("raw_number"), values.get("country_code"))

    # @validator("country_code", pre=True)
    # def get_country_code(cls, v: str, values: Dict[str, Any]) -> str:
    #     if isinstance(v, str):
    #         return v
    #     return values.get("phonenumber").country_code

    # @validator("national_number", pre=True)
    # def get_national_number(cls, v: str, values: Dict[str, Any]) -> str:
    #     if isinstance(v, str):
    #         return v
    #     return values.get("phonenumber").national_number

    # @validator("is_valid", pre=True)
    # def get_phone_number(cls, v: bool, values: Dict[str, Any]) -> bool:
    #     return bool(
    #         phonenumbers.is_valid(values.get("raw_number"), values.get("country_code"))
    #     )
