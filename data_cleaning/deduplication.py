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
    Version PRO adaptée : normalise le texte, les dates et les nombres,
    mais conserve les colonnes Email et Message intactes pour éviter de les casser.
    """
    df_norm = df.copy()

    # Colonnes à ne pas toucher
    protected_cols = ["E-mail", "Message"]

    # ---------- FONCTIONS INTERNES ----------

    def clean_text(x):
        if pd.isna(x):
            return ""
        x = str(x).strip()
        x = unidecode.unidecode(x)  # enlever les accents
        x = re.sub(r"\s+", " ", x)  # espaces multiples → 1
        x = x.lower()
        x = re.sub(r"[^a-z0-9\s\-/]", "", x)  # garder lettres, chiffres, -, /
        return x

    def parse_date(x):
        if pd.isna(x):
            return None
        x = str(x).strip()
        for fmt in ["%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d",
                    "%d.%m.%Y", "%Y.%m.%d", "%d%m%Y", "%Y%m%d"]:
            try:
                return pd.to_datetime(x, format=fmt, errors="raise")
            except:
                continue
        try:
            return pd.to_datetime(x, dayfirst=True, errors='coerce')
        except:
            return None

    def parse_number(x):
        if pd.isna(x):
            return None
        x = str(x).strip().replace(" ", "")
        if "," in x and "." not in x:
            x = x.replace(",", ".")
        elif x.count(",") > 1:
            x = x.replace(",", "")
        x = re.sub(r"[^\d\.-]", "", x)
        try:
            return float(x)
        except:
            return None

    # ---------- TRAITEMENT COLONNES ----------
    for col in df_norm.columns:

        # Ignorer les colonnes protégées
        if col in protected_cols:
            continue

        # Colonnes texte
        if df_norm[col].dtype == object:
            df_norm[col] = df_norm[col].apply(clean_text)

        # Colonnes date
        if "date" in col.lower() or "birth" in col.lower() or "nais" in col.lower():
            df_norm[col] = df_norm[col].apply(parse_date)
            df_norm[col] = df_norm[col].dt.strftime("%Y-%m-%d")
            df_norm[col] = df_norm[col].fillna("NULL")
            continue

        # Colonnes numériques
        if pd.api.types.is_numeric_dtype(df_norm[col]):
            df_norm[col] = pd.to_numeric(df_norm[col], errors="coerce")
            continue

        # Colonnes mixtes (texte + chiffres)
        if df_norm[col].dtype == object:
            df_norm[col] = df_norm[col].apply(clean_text)
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
