from fastapi import FastAPI
from routers import strava, activities

app = FastAPI(title="EyeSight Backend")

# inclure les routers
app.include_router(strava.router, prefix="/strava", tags=["Strava"])
app.include_router(activities.router, prefix="/activities", tags=["Activités"])

@app.get("/")
def root():
    return {"status": "ok", "msg": "EyeSight API running"}
