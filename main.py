from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io
# NOUVEL IMPORT
from fastapi.responses import StreamingResponse 
# import csv is not needed as pandas handles the conversion

app = FastAPI(title="Data Cleaning API")

# Fonction utilitaire
def load_csv(file: UploadFile):
    contents = file.file.read()
    return pd.read_csv(io.BytesIO(contents))

@app.post("/deduplicate")
async def deduplicate(file: UploadFile = File(...)):
    df = load_csv(file)
    before = len(df)
    df_clean = df.drop_duplicates()
    after = len(df_clean)
    return {"rows_before": before, "rows_after": after}

@app.post("/fill-missing")
async def fill_missing(file: UploadFile = File(...)):
    df = load_csv(file)
    missing_before = df.isna().sum().to_dict()
    df_clean = df.fillna(df.median(numeric_only=True))
    missing_after = df_clean.isna().sum().to_dict()
    return {"missing_before": missing_before, "missing_after": missing_after}

@app.post("/remove-outliers")
async def remove_outliers(file: UploadFile = File(...)):
    df = load_csv(file)
    before = len(df)
    # règle simple : revenu < 20000 et age > 0
    df_clean = df[(df["revenu"] < 20000) & (df["age"] > 0)]
    after = len(df_clean)
    return {"rows_before": before, "rows_after": after}

# @app.post("/clean-all")
# async def clean_all(file: UploadFile = File(...)):
#     df = load_csv(file)
#     df = df.drop_duplicates()
#     df = df.fillna(df.median(numeric_only=True))
#     df = df[(df["revenu"] < 20000) & (df["age"] > 0)]
#     return {"rows_after_cleaning": len(df), "columns": list(df.columns)}

@app.post("/clean-all-and-download") # J'ai changé le nom pour être explicite
async def clean_all_and_download(file: UploadFile = File(...)):
    df = load_csv(file)

    # 1. Déduplication
    df = df.drop_duplicates()
    
    # 2. Gestion des valeurs manquantes (imputation par la médiane)
    df = df.fillna(df.median(numeric_only=True))
    
    # 3. Suppression des valeurs aberrantes
    df = df[(df["revenu"] < 20000) & (df["age"] > 0)]
    
    # --- PARTIE CLÉ : CONVERSION ET TÉLÉCHARGEMENT ---
    
    # Convertir le DataFrame en CSV en mémoire (sans l'écrire sur disque)
    # io.StringIO est un tampon de texte en mémoire
    stream = io.StringIO()
    df.to_csv(stream, index=False) # index=False pour ne pas inclure l'index de Pandas
    
    # Créer la réponse en streaming
    response = StreamingResponse(
        # On itère sur le contenu du tampon
        iter([stream.getvalue()]), 
        media_type="text/csv" # Indique au navigateur que c'est un fichier CSV
    )
    
    # Ajouter les headers pour forcer le téléchargement du fichier
    response.headers["Content-Disposition"] = "attachment; filename=cleaned_data.csv"
    
    return response
