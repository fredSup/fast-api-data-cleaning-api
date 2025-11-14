from fastapi import APIRouter, UploadFile, File
from fastapi.responses import StreamingResponse
from .utils import load_file
import pandas as pd
import io
import unidecode
import re

router = APIRouter()

def normalize_for_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les données pour améliorer la détection des doublons.
    - Nettoie les espaces
    - Uniformise la casse
    - Supprime les accents
    - Normalise les dates
    - Convertit les nombres mal formatés
    - Nettoie les colonnes d'adresse
    """
    df_norm = df.copy()

    for col in df_norm.columns:
        if pd.api.types.is_string_dtype(df_norm[col]):
            # Nettoyage général
            df_norm[col] = df_norm[col].astype(str).str.strip()
            df_norm[col] = df_norm[col].str.replace(r'\s+', ' ', regex=True)
            df_norm[col] = df_norm[col].str.lower()
            df_norm[col] = df_norm[col].apply(unidecode.unidecode)
            df_norm[col] = df_norm[col].str.replace(r'[^a-z0-9\s]', '', regex=True)

             # Normalisation des dates
        if "date" in col.lower():
            def parse_date(x):
                try:
                    return pd.to_datetime(x, dayfirst=True, errors='coerce')
                except:
                    return pd.NaT

            df_norm[col] = df_norm[col].apply(parse_date)
            # Ensuite, formater uniformément et remplacer NaT par "NULL"
            df_norm[col] = df_norm[col].dt.strftime("%Y-%m-%d")
            df_norm[col] = df_norm[col].fillna("NULL")
            
            
        # Conversion des nombres mal formatés
        elif pd.api.types.is_numeric_dtype(df_norm[col]):
            df_norm[col] = pd.to_numeric(df_norm[col], errors='coerce')

        # Remplacer les chaînes vides par NaN
        else:
            df_norm[col] = df_norm[col].replace(r'^\s*$', pd.NA, regex=True)
    return df_norm

@router.post("/deduplicate")
async def deduplicate(file: UploadFile = File(...)):
    """
    Supprime les lignes dupliquées dans un fichier CSV, Excel ou JSON.
    Retourne toujours un fichier Excel propre et lisible.
    """
    try:
        df, _ = load_file(file)
    except Exception as e:
        return {"error": str(e)}

    # Normalisation pour améliorer la détection des doublons
    df_norm = normalize_for_duplicates(df)

    # Suppression des doublons
    df_clean = df_norm.drop_duplicates().reset_index(drop=True)

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















# from fastapi import APIRouter, UploadFile, File
# from fastapi.responses import StreamingResponse
# from .utils import load_file
# import pandas as pd
# import io

# router = APIRouter()

# @router.post("/deduplicate")
# async def deduplicate(file: UploadFile = File(...)):
#     """
#     Supprime les lignes dupliquées dans un fichier CSV ou Excel.
#     Normalise aussi les formats de dates avant suppression.
#     """
#     try:
#         df, _ = load_file(file)
#     except Exception as e:
#         return {"error": str(e)}

#     #  Normalisation des dates (YYYY-MM-DD)
#     for col in df.columns:
#         if "date" in col.lower():
#             try:
#                 df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
#             except Exception:
#                 pass

#     #  Suppression des doublons
#     df_clean = df.drop_duplicates()

#     #  Conversion du DataFrame en fichier Excel (binaire)
#     stream = io.BytesIO()
#     df_clean.to_excel(stream, index=False)
#     stream.seek(0)

#     #  Création de la réponse
#     response = StreamingResponse(
#         iter([stream.getvalue()]),
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#     )
#     response.headers["Content-Disposition"] = "attachment; filename=deduplicated_data.xlsx"
#     return response
