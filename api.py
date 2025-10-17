from fastapi import FastAPI, HTTPException

class Api(object):
    
    def __init__(self):
        self.app = FastAPI(title="Financial Radar")
        
    def _registr_handlers(self):
        self.app.get("/purchases/create") (self.create_purchase)
        
    def create_purchase(self):
        pass