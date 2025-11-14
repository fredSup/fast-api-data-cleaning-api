from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_cleaning.deduplication import router as dedup_router
from data_cleaning.missing_values import router as missing_router
from data_cleaning.outliers import router as outlier_router
from data_cleaning.full_cleaning import router as full_cleaning_router

app = FastAPI(title="Data Cleaning API")

# --- Configuration CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Enregistrement des routes ---
app.include_router(dedup_router)
app.include_router(missing_router)
app.include_router(outlier_router)
app.include_router(full_cleaning_router)

@app.get("/")
def root():
    return {"message": "Bienvenue sur l'API de nettoyage de données 👋"}
