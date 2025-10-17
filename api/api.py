from fastapi import FastAPI, HTTPException
from models.models import *

class Api(object):
    
    def __init__(self):
        self.app = FastAPI(title="Financial Radar")
        self._registr_handlers()
        
    def _registr_handlers(self):
        self.app.post("/purchases/create") (self.create_purchase)
        
    def create_purchase(self, data: RequestData):
        return {"data": data}
        
api = Api()
app = api.app