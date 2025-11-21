from routers import plot, strava, activities, kpi, analysis
from fastapi import FastAPI, Depends, HTTPException, status
from services.auth import authenticate_user, create_access_token, get_current_user, validate_environment
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware


# Validation au démarrage - AVANT la création de l'app
validate_environment()

app = FastAPI(title="EyeSight Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # ton front Vite
    allow_methods=["*"],
    allow_headers=["*"],
)
# inclure les routers
app.include_router(strava.router, prefix="/strava", tags=["Strava"]) #dependencies=[Depends(get_current_user)]
app.include_router(activities.router, prefix="/activities", tags=["Activités"]) #dependencies=[Depends(get_current_user)]
app.include_router(kpi.router, prefix="/kpi", tags=["KPIs"]) #dependencies=[Depends(get_current_user)]
app.include_router(plot.router, prefix="/plot", tags=["Graphiques"]) #dependencies=[Depends(get_current_user)]
app.include_router(analysis.router, prefix="/analysis", tags=["Analyses"]) #dependencies=[Depends(get_current_user)]


@app.get("/")
def root():
    return {"status": "ok", "msg": "EyeSight API running"}


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not authenticate_user(form_data.username, form_data.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token = create_access_token(data={"sub": form_data.username})
    return {"access_token": token, "token_type": "bearer"}
