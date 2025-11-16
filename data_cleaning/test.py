# # import pandas as pd

# # # Données d’exemple
# # data = {
# #     "id": [1, 1, 2, 3, 4, 5, 6, 7],
# #     "nom": ["Alice", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"],
# #     "age": [25, 25, 35, None, None, 22, 30, 28],
# #     "revenu": [1800000000000, 1800000000000, None, -50, 500, 9000, None, 25000],
# #     "ville": ["Paris", "Paris", "Lyon", "Marseille", "Lille", "Paris", None, "Bordeaux"]
# # }

# # df = pd.DataFrame(data)

# # # Création des fichiers
# # df.to_csv("dataset.csv", index=False)
# # df.to_excel("dataset.xlsx", index=False)
# # df.to_json("dataset.json", orient="records", lines=False)
# # df.to_parquet("dataset.parquet", index=False)


# import pandas as pd

# data = {
#     "id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
#     "nom": ["Alice", "Bob", "Charlie", "David", "Eve", "Eve", "Eve", "Alice", "Alice"],
#     "age": [25, 35, None, 40, 22, 22, 22, 25, 25],
#     "revenu": [1.8e12, None, -5, 500, 9000, 9000, 1.7e9, 100, None],
#     "date_naissance": ["12/03/1998", "1985-05-07", "07-08-1980", "1980/07/08", "2001-09-01", "2001-09-01", "2001-09-01", "12/03/1998", None],
#     "ville": [" Paris ", "Londres", "Marseille", "Lille", " Paris ", " Paris ", " Paris ", " Paris ", "Paris"]
# }

# df = pd.DataFrame(data)
# df.to_csv("test_dataset.csv", index=False)
# print("✅ Fichier test_dataset.csv généré avec succès !")



