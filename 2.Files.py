# Databricks notebook source
# MAGIC %run ./0.Variables
# MAGIC

# COMMAND ----------

# Copia los archivos del repositorio GitHub a local_tmp_path
import requests
import os

# ✅ Tus variables

# 🔗 Repositorio GitHub (cambia si quieres usar otro)
github_user = "dancifuentesperu"

# repo_name = "databricks_files5"
branch = "main"

# 🔧 Construir paths
volume_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}"
dbfs_path = f"dbfs:{volume_path}"

# 🌐 GitHub API para listar archivos del repo
api_url = f"https://api.github.com/repos/{github_user}/{repo_name}/git/trees/{branch}?recursive=1"

# 📥 Obtener lista de archivos
response = requests.get(api_url)
response.raise_for_status()
tree = response.json().get("tree", [])

# 🔁 Descargar y copiar cada archivo al Volume UC
for item in tree:
    if item["type"] == "blob":  # Asegura que es un archivo
        file_path = item["path"]
        print(f"📄 Descargando: {file_path}")
        
        # Construir URL RAW
        raw_url = f"https://raw.githubusercontent.com/{github_user}/{repo_name}/{branch}/{file_path}"
        
        # Descargar contenido del archivo
        r = requests.get(raw_url)
        if r.status_code != 200:
            print(f"❌ Error al descargar: {raw_url}")
            continue

        # Guardar temporalmente en /Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}
        tmp_filename = os.path.basename(file_path)
        local_tmp_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}/{tmp_filename}"
        with open(local_tmp_path, "wb") as f:
            f.write(r.content)

        # Subir al Volume UC (manteniendo la ruta del archivo)


# COMMAND ----------

path_tmp    = f"dbfs:/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}/"
path_target = f"dbfs:/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_target}/"

# Listar archivos en tmp
files_tmp = dbutils.fs.ls(path_tmp)

# Filtrar y mover solo archivos .parquet
print("🚚 Archivos .parquet movidos:")

for file in files_tmp:
    if file.name.endswith(".parquet"):
        src = file.path
        dst = f"{path_target}{file.name}"
        dbutils.fs.cp(src, dst)
        print(f"✅ {file.name} → {dst}")