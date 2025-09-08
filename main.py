from fastapi import FastAPI
from routers import strava, activities,kpi

app = FastAPI(title="EyeSight Backend")

# inclure les routers
app.include_router(strava.router, prefix="/strava", tags=["Strava"])
app.include_router(activities.router, prefix="/activities", tags=["Activités"])
app.include_router(kpi.router, prefix="/kpis", tags=["KPIs"])


@app.get("/")
def root():
    return {"status": "ok", "msg": "EyeSight API running"}
