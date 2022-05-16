from fastapi import FastAPI
from rabbit import start_queue

app = FastAPI()
start_queue()

@app.get("/")
async def root():
    return {"message": "Hello World"}