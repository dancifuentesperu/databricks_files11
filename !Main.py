# Databricks notebook source
# MAGIC %run ./0.Variables

# COMMAND ----------

mkdir /Workspace/{workspace_folder}/

# COMMAND ----------

import requests
import os

# ✅ Variables de tu entorno
#catalog_name = "integration_catalog"
#schema_name_raw = "raw_files"
#volume_name_tmp = "tmp"

# 🔗 Repositorio GitHub
github_user = "dancifuentesperu"
# repo_name = "databricks_files4"
branch = "main"

# 🔧 Ruta destino en Volume UC
volume_path = f"/Workspace/{workspace_folder}"

# 🌐 GitHub API para listar archivos del repo
api_url = f"https://api.github.com/repos/{github_user}/{repo_name}/git/trees/{branch}?recursive=1"

# 📥 Obtener lista de archivos
response = requests.get(api_url)
response.raise_for_status()
tree = response.json().get("tree", [])

# 🔁 Descargar y guardar solo los archivos .py
for item in tree:
    if item["type"] == "blob" and item["path"].endswith(".py"):
        file_path = item["path"]
        print(f"📄 Descargando archivo .py: {file_path}")
        
        # Construir URL RAW de GitHub
        raw_url = f"https://raw.githubusercontent.com/{github_user}/{repo_name}/{branch}/{file_path}"
        
        # Descargar el archivo
        r = requests.get(raw_url)
        if r.status_code != 200:
            print(f"❌ Error al descargar: {raw_url}")
            continue

        # Guardar en la ruta del volumen
        tmp_filename = os.path.basename(file_path)
        local_tmp_path = f"{volume_path}/{tmp_filename}"
        
        with open(local_tmp_path, "wb") as f:
            f.write(r.content)

        print(f"✅ Guardado en: {local_tmp_path}")

# COMMAND ----------

result = dbutils.notebook.run(
    f"/Workspace/{workspace_folder}/1.Catalogs",
    300  # tiempo máximo de ejecución en segundos
)

result = dbutils.notebook.run(
    f"/Workspace/{workspace_folder}/2.Files",
    300  # tiempo máximo de ejecución en segundos
)

result = dbutils.notebook.run(
    f"/Workspace/{workspace_folder}/3.Tables",
    300  # tiempo máximo de ejecución en segundos
)