from fastapi import FastAPI

from src.api.routers import api_router

app = FastAPI(title='Avito Moderation API', version='0.1.0')
app.include_router(api_router)


@app.get('/')
async def root():
    return {'message': 'Hello, world!'}
