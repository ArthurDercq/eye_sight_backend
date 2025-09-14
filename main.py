from fastapi import FastAPI
from routers import plot, strava, activities,kpi

app = FastAPI(title="EyeSight Backend")

# inclure les routers
app.include_router(strava.router, prefix="/strava", tags=["Strava"])
app.include_router(activities.router, prefix="/activities", tags=["Activités"])
app.include_router(kpi.router, prefix="/kpi", tags=["KPIs"])
app.include_router(plot.router, prefix="/plot", tags=["Graphiques"])


@app.get("/")
def root():
    return {"status": "ok", "msg": "EyeSight API running"}
