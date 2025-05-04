# ------------------------------------------------------------
# Script para cargar y visualizar datos con Elasticsearch y GitHub Pages.
#
# Este script utiliza Elasticsearch para indexar y consultar un dataset de Clash Royale.
# Dataset from: https://www.kaggle.com/datasets/hrish4/clash-royale-cards-data?resource=download
# Autor: Iñaki Heras
# Fecha: mayo de 2025
# ------------------------------------------------------------

# Librerias
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import plotly.express as px

# Dataset
df = pd.read_csv("clash_royale_cards.csv")
print(df)

# Sustituir valores nulos NaN por None
df = df.replace({np.nan: None})

# Conexión a Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Creación de indice
index_name = "clash_royale_cards"

if es.indices.exists(index=index_name): 
    es.indices.delete(index=index_name) # Si el indice ya existe, borrarlo

es.indices.create(index=index_name)

# Insertar documentos
for i, row in df.iterrows():
    doc = row.to_dict()
    response = es.index(index=index_name, document=doc)

# Consulta para obtener los datos
res = es.search(
    index=index_name,
    query={"match_all": {}},
    size=1000
)
hits = res["hits"]["hits"]
data = [hit["_source"] for hit in hits]
plot_df = pd.DataFrame(data)

print("Columnas disponibles en plot_df:", plot_df.columns)
print(plot_df.head())

# Agrupar por rareza y calcular el promedio de elixir
plot_df_grouped = plot_df.groupby("rarity", as_index=False)["elixirCost"].mean()

# Crear gráfico
fig = px.bar(
    plot_df_grouped,
    x="rarity",
    y="elixirCost",
    title="Costo promedio de elixir por rareza",
    color="rarity",
    labels={"elixirCost": "Costo promedio de elixir", "rarity": "Rareza"}
)

fig.show()
#fig.write_html("docs/plot.html")