from pydantic import BaseModel

class RequestData(BaseModel):
    amount: int
    to: str
    firstly: str
    date: str