import pandas as pd
import io
import json
from pandas import json_normalize

def load_file(file):
    """
    Charge un fichier (CSV, Excel, Parquet ou JSON) dans un DataFrame pandas.
    Préserve les colonnes existantes (ex: 'id') et détecte automatiquement le type.
    """
    content = file.file.read()
    filename = file.filename.lower()
    df = None

    try:
        # --- CSV ---
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))

        # --- Excel (.xls, .xlsx) ---
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(io.BytesIO(content))

        # --- Parquet ---
        elif filename.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(content))

        # --- JSON ---
        elif filename.endswith(".json"):
            try:
                # 🔹 JSON déjà bien structuré
                df = pd.read_json(io.BytesIO(content))
            except ValueError:
                # 🔹 JSON imbriqué ou complexe
                data = json.load(io.BytesIO(content))
                if isinstance(data, dict):
                    # Cas: {"data": [ {...}, {...} ]}
                    for key, val in data.items():
                        if isinstance(val, list):
                            df = json_normalize(val)
                            break
                elif isinstance(data, list):
                    # Cas: [ {...}, {...} ]
                    df = json_normalize(data)
                else:
                    raise ValueError("Format JSON non supporté.")

        else:
            raise ValueError(f"Format de fichier non pris en charge : {filename}")

        # --- Validation ---
        if df is None or df.empty:
            raise ValueError("Le fichier est vide ou illisible.")

        # --- Nettoyage de base : suppression des colonnes vides ---
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        # --- Vérifie la présence d'un identifiant ---
        if "id" not in df.columns:
            df.reset_index(inplace=True)
            df.rename(columns={"index": "id"}, inplace=True)

        return df, filename.split(".")[-1]

    except Exception as e:
        raise ValueError(f"Erreur de lecture du fichier : {e}")


# import pandas as pd
# import io

# def load_file(file):
#     """
#     Charge un fichier CSV ou Excel et retourne (DataFrame, type)
#     """
#     contents = file.file.read()
#     filename = file.filename.lower()

#     if filename.endswith(".csv"):
#         df = pd.read_csv(io.BytesIO(contents))
#         file_type = "csv"
#     elif filename.endswith((".xlsx", ".xls")):
#         df = pd.read_excel(io.BytesIO(contents))
#         file_type = "excel"
#     else:
#         raise ValueError("Format non supporté. Fichier CSV ou Excel requis.")
#     return df, file_type


