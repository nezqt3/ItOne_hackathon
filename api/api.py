from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import csv
import tempfile

app = FastAPI(title="Simple JSON to CSV API")
transactions = []
class Transaction(BaseModel):
    transaction_id: str
    timestamp: datetime
    sender_account: str
    receiver_account: str
    amount: float
    transaction_type: str
    merchant_category: Optional[str] = None
    location: Optional[str] = None

@app.post("/transactions/add")
async def add_transaction(transaction: Transaction):
    transactions.append(transaction.model_dump())
    return {"message": "Transaction added", "id": transaction.transaction_id}


@app.post("/transactions/add-batch")
async def add_transactions_batch(transactions_list: List[Transaction]):
    for transaction in transactions_list:
        transactions.append(transaction.model_dump())
    return {"message": f"Added {len(transactions_list)} transactions"}


@app.get("/transactions/export-csv")
async def export_to_csv():
    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions available")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as temp_file:
        writer = csv.writer(temp_file)
        headers = ['transaction_id', 'timestamp', 'sender_account', 'receiver_account',
                   'amount', 'transaction_type', 'merchant_category', 'location']
        writer.writerow(headers)
        for transaction in transactions:
            row = [
                transaction['transaction_id'],
                transaction['timestamp'],
                transaction['sender_account'],
                transaction['receiver_account'],
                transaction['amount'],
                transaction['transaction_type'],
                transaction.get('merchant_category', ''),
                transaction.get('location', '')
            ]
            writer.writerow(row)
        temp_file_path = temp_file.name
    return FileResponse(
        path=temp_file_path,
        filename=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        media_type='text/csv'
    )

@app.get("/transactions/count")
async def get_transaction_count():
    return {"count": len(transactions)}

@app.get("/")
async def root():
    return {
        "message": "Simple JSON to CSV API",
        "endpoints": {
            "add_single": "POST /transactions/add",
            "add_batch": "POST /transactions/add-batch",
            "export_csv": "GET /transactions/export-csv",
            "count": "GET /transactions/count"
        }
    }

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
