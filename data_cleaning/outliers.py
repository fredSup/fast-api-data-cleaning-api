from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from .utils import load_file
import pandas as pd
import numpy as np
import json
import io

router = APIRouter()

@router.post("/remove-outliers")
async def remove_outliers(
    file: UploadFile = File(...),
    method: str = Form("delete"),  # delete / mean / median
    columns: str = Form(None),
    use_custom_bounds: bool = Form(False),
    lower_bound: float = Form(None),
    upper_bound: float = Form(None),
    iqr_factor: float = Form(1.5)
):
    """
    Détecte et traite les valeurs aberrantes selon la méthode choisie.
    """
    try:
        df, _ = load_file(file)
    except Exception as e:
        return {"error": str(e)}

    # Colonnes sélectionnées (si fournies)
    selected_cols = json.loads(columns) if columns else list(df.select_dtypes(include=[np.number]).columns)
    numeric_available = df.select_dtypes(include=[np.number]).columns.tolist()
    cols_to_check = [c for c in selected_cols if c in numeric_available]

    if not cols_to_check:
        return {"error": "Aucune colonne numérique valide trouvée."}

    df_clean = df.copy()
    masks = {}

    # Calcul des bornes pour chaque colonne
    for col in cols_to_check:
        if use_custom_bounds:
            if lower_bound is None or upper_bound is None:
                return {"error": "Bornes personnalisées manquantes."}
            col_lower, col_upper = lower_bound, upper_bound
        else:
            Q1, Q3 = df[col].quantile([0.25, 0.75])
            IQR = Q3 - Q1
            col_lower = Q1 - iqr_factor * IQR
            col_upper = Q3 + iqr_factor * IQR

        masks[col] = (df[col] < col_lower) | (df[col] > col_upper)

    if method == "delete":
        combined_mask = np.column_stack([masks[c] for c in cols_to_check]).any(axis=1)
        df_clean = df_clean[~combined_mask].reset_index(drop=True)
    elif method in ("mean", "median"):
        for col in cols_to_check:
            mask = masks[col]
            if mask.any():
                replacement = df.loc[~mask, col].mean() if method == "mean" else df.loc[~mask, col].median()
                df_clean.loc[mask, col] = replacement
    else:
        return {"error": "Méthode d'outliers invalide."}

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

@router.post("/get-numeric-columns")
async def get_numeric_columns(file: UploadFile = File(...)):
    """
    Retourne la liste des colonnes numériques du fichier pour le frontend.
    """
    try:
        df, _ = load_file(file)
    except Exception as e:
        return {"error": str(e)}

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        return {"error": "Aucune colonne numérique trouvée."}
    return {"numeric_columns": numeric_cols}
