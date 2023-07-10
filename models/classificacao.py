from sqlalchemy import MetaData
from sqlalchemy import (Table, Column, 
                        Integer, String, ForeignKey)

from datetime import datetime

metadata_obj = MetaData()


schedulers = Table(
    "classificacao_grupos",
    metadata_obj,
    Column("cod_sugestion", Integer, primary_key=True),
    Column("data", datetime, nullable=False),
 
    schema="als"
)