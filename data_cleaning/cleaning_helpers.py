# import pandas as pd
# import numpy as np

# # --- Normalisation des formats ---
# def normalize_formats(df):
#     normalized_cols = []
#     for col in df.columns:
#         if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
#             # Nettoyer espaces et caractères parasites
#             df[col] = df[col].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)

#             # Capitalisation pour noms propres
#             df[col] = df[col].str.title()

#             # Détection et conversion de formats de dates
#             try:
#                 sample = df[col].dropna().iloc[0]
#                 if any(sep in sample for sep in ['/', '-', '.']):
#                     parsed = pd.to_datetime(df[col], errors='ignore', infer_datetime_format=True)
#                     if pd.api.types.is_datetime64_any_dtype(parsed):
#                         df[col] = parsed.dt.strftime("%Y-%m-%d")
#                         normalized_cols.append(col)
#             except Exception:
#                 pass
#     return df, normalized_cols


# # --- Rapport initial ---
# def init_report():
#     return {
#         "doublons_supprimes": 0,
#         "valeurs_manquantes_remplacees": {},
#         "valeurs_aberrantes_traitees": {},
#         "colonnes_normalisees": [],
#     }


# # --- Suivi des valeurs manquantes ---
# def track_missing_values(before_df, after_df, numeric_cols):
#     before = before_df[numeric_cols].isnull().sum()
#     after = after_df[numeric_cols].isnull().sum()
#     diff = (before - after).to_dict()
#     return {k: int(v) for k, v in diff.items() if v > 0}


# # --- Suivi des valeurs aberrantes ---
# def track_outliers(masks):
#     return {col: int(mask.sum()) for col, mask in masks.items()}
