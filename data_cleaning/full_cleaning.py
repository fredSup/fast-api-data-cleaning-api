from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from typing import Optional
import pandas as pd
import numpy as np
import io, json
import unidecode
from .utils import load_file
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


@router.post("/clean-all-and-download")
async def clean_all_and_download(
    file: UploadFile = File(...),
    missing_method: str = Form("median"),       
    missing_value: Optional[str] = Form(None),  # Changé en Optional[str]
    outlier_method: str = Form("delete"),       
    columns: Optional[str] = Form(None),
    use_custom_bounds: bool = Form(False),
    lower_bound: Optional[str] = Form(None),    # Changé en Optional[str]
    upper_bound: Optional[str] = Form(None),    # Changé en Optional[str]
    iqr_factor: float = Form(1.5)
):
    """Pipeline complet : normalisation → doublons → valeurs manquantes → outliers."""

    try:
        df, _ = load_file(file)
    except Exception as e:
        return {"error": f"Erreur de chargement : {e}"}

    df_clean = df.copy()

    # === 1️⃣ NORMALISATION + DÉDOUBLONNAGE INTELLIGENT ===
    df_clean = normalize_for_duplicates(df_clean)
    df_clean = df_clean.drop_duplicates().reset_index(drop=True)

    # === 2️⃣ TRAITEMENT VALEURS MANQUANTES ===
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    text_cols = df_clean.select_dtypes(include=["object", "string"]).columns.tolist()

    if missing_method == "median":
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df_clean[numeric_cols].median())
    elif missing_method == "mean":
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df_clean[numeric_cols].mean())
    elif missing_method == "constant":
        if missing_value is None or missing_value == "":
            return {"error": "Valeur constante manquante."}
        try:
            constant_val = float(missing_value)
        except ValueError:
            return {"error": "Valeur constante invalide."}
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(constant_val)
    elif missing_method == "null":
        # df_clean[numeric_cols] = df_clean[numeric_cols].fillna("NULL")
            df_clean = df_clean.fillna("NULL")

    else:
        return {"error": "Méthode de valeurs manquantes invalide."}

    # Texte → "NULL"
    df_clean[text_cols] = df_clean[text_cols].fillna("NULL")

    # === 3️⃣ TRAITEMENT OUTLIERS ===
    if columns and columns != "null":
        try:
            selected_cols = json.loads(columns)
        except:
            return {"error": "Format des colonnes invalide."}
    else:
        selected_cols = numeric_cols

    # Vérification
    numeric_available = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    cols_to_check = [c for c in selected_cols if c in numeric_available]

    # Si des colonnes numériques sont sélectionnées, traiter les outliers
    if cols_to_check:
        masks = {}
        for col in cols_to_check:
            if use_custom_bounds:
                if lower_bound is None or lower_bound == "" or upper_bound is None or upper_bound == "":
                    return {"error": "Bornes personnalisées manquantes."}
                try:
                    col_lower = float(lower_bound)
                    col_upper = float(upper_bound)
                except ValueError:
                    return {"error": "Bornes personnalisées invalides."}
            else:
                Q1, Q3 = df_clean[col].quantile([0.25, 0.75])
                IQR = Q3 - Q1
                col_lower = Q1 - iqr_factor * IQR
                col_upper = Q3 + iqr_factor * IQR

            masks[col] = (df_clean[col] < col_lower) | (df_clean[col] > col_upper)

        # Application
        if outlier_method == "delete":
            global_mask = np.column_stack([masks[c] for c in cols_to_check]).any(axis=1)
            df_clean = df_clean[~global_mask].reset_index(drop=True)
        elif outlier_method in ("mean", "median"):
            for col in cols_to_check:
                mask = masks[col]
                valid_vals = df_clean.loc[~mask, col]
                if len(valid_vals) == 0:
                    continue
                replacement = valid_vals.mean() if outlier_method == "mean" else valid_vals.median()
                df_clean.loc[mask, col] = replacement
        else:
            return {"error": "Méthode d'outliers invalide."}

    # === 4️⃣ EXPORT EXCEL PROPRE ===
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine='xlsxwriter') as writer:
        # Formater les nombres sans décimales si ce sont des entiers
        df_display = df_clean.copy()
        for col in df_display.select_dtypes(include=['number']).columns:
            # Vérifier si la colonne contient principalement des entiers
            if (df_display[col].dropna() % 1 == 0).all():
                df_display[col] = df_display[col].apply(lambda x: f"{int(x):d}" if pd.notna(x) else "")
        
        df_display.to_excel(writer, index=False, sheet_name="Nettoye")
        
        # Ajuster automatiquement la largeur des colonnes
        worksheet = writer.sheets["Nettoye"]
        for idx, col in enumerate(df_display.columns):
            max_len = max(
                df_display[col].astype(str).str.len().max(),
                len(col)
            ) + 2
            worksheet.set_column(idx, idx, min(max_len, 50))

    stream.seek(0)

    # Création de la réponse
    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response.headers["Content-Disposition"] = "attachment; filename=donnees_nettoyees.xlsx"
    return response