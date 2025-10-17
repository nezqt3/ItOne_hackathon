from pydantic import BaseModel
from datetime import datetime

class RequestData(BaseModel):
    amount: int
    to: str
    firstly: str
    date: str = datetime.now()