from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from .utils import load_file
import pandas as pd
import numpy as np
import io

router = APIRouter()

@router.post("/fill-missing")
async def fill_missing(
    file: UploadFile = File(...),
    method: str = Form("median"),  # median / mean / constant / null
    value: float = Form(None)
):
    """
    Remplit les valeurs manquantes dans les colonnes numériques
    selon la méthode choisie.
    """
    try:
        df, _ = load_file(file)
    except Exception as e:
        return {"error": str(e)}

    df_clean = df.copy()
    num_cols = df_clean.select_dtypes(include=[np.number]).columns
    text_cols = df_clean.select_dtypes(include=["object", "string"]).columns

    if method == "median":
        df_clean[num_cols] = df_clean[num_cols].fillna(df_clean[num_cols].median())
    elif method == "mean":
        df_clean[num_cols] = df_clean[num_cols].fillna(df_clean[num_cols].mean())
    elif method == "constant":
        if value is None:
            return {"error": "Veuillez fournir une valeur constante."}
        df_clean[num_cols] = df_clean[num_cols].fillna(value)
    elif method == "null":
        # df_clean[num_cols] = df_clean[num_cols].fillna(pd.NA)
        df_clean = df_clean.fillna("NULL")

    else:
        return {"error": "Méthode invalide."}
    df_clean[text_cols] = df_clean[text_cols].fillna("NULL")

    # stream = io.StringIO()
    # df_clean.to_csv(stream, index=False)
    # stream.seek(0)

    # response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    # response.headers["Content-Disposition"] = "attachment; filename=filled_data.xlsx"
    # return response
    
    # Conversion en Excel lisible
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine='xlsxwriter') as writer:
        for col in df_clean.select_dtypes(include=['number']).columns:
            df_clean[col] = df_clean[col].apply(lambda x: f"{int(x):.0f}" if pd.notna(x) else "")
        df_clean.to_excel(writer, index=False, sheet_name="Nettoye")
        # Ajuster automatiquement la largeur des colonnes
        for column in df_clean:
            col_width = max(df_clean[column].astype(str).map(len).max(), len(column)) + 2
            writer.sheets["Nettoye"].set_column(df_clean.columns.get_loc(column), df_clean.columns.get_loc(column), col_width)

    stream.seek(0)

    # Création de la réponse
    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response.headers["Content-Disposition"] = "attachment; filename=deduplicated_data.xlsx"
    return response
