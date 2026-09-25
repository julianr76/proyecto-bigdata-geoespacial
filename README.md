# Accidentes de tránsito en EE.UU. - Big Data geoespacial

Trabajo práctico de Big Data. El sistema descarga el dataset US Accidents de Kaggle, lo limpia con Dask, lo guarda en MongoDB como GeoJSON con índice 2dsphere, lo procesa con Spark y lo expone con una API en Flask. Jenkins construye, prueba y despliega todo cada vez que se hace push a `main`.

Dataset: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents (unos 7.7 millones de registros, 3 GB aprox.). Se descarga completo con la API de Kaggle y se cargan en MongoDB 1.000.000 de registros (se puede cambiar con la variable `MAX_REGISTROS`, 0 = todos).

## Servicios

| Servicio | Qué hace | Puerto en el PC |
|---|---|---|
| mongo | Base de datos con los accidentes y los resultados | 27017 (solo localhost) |
| spark-master / spark-worker | Procesamiento distribuido con el MongoDB Spark Connector | 8081 (interfaz web) |
| dask-scheduler / dask-worker (x2) | Lectura particionada, limpieza y carga por lotes | 8787 (dashboard) |
| api | API en Flask | 5000 |
| jenkins | Integración y despliegue continuo | 8080 |

## Requisitos

- Docker Desktop (con WSL2 si es Windows) y al menos 6 GB de memoria asignados a Docker.
- Git.
- Una cuenta de Kaggle con su API token (usuario y key).
- Unos 10 GB libres en disco.

## Levantar el sistema desde cero

1. Clonar el repositorio:

```
git clone https://github.com/julianr76/proyecto-bigdata-geoespacial.git
cd proyecto-bigdata-geoespacial
```

2. Levantar todo con un solo comando:

```
docker compose up -d --build
```

La primera vez se demora porque descarga y construye las imágenes. Con `docker compose ps` se ve que todo quedó arriba.

3. Cargar los datos. Hay dos formas:

**Con Jenkins (la forma normal):** se configura Jenkins como se explica más abajo y se ejecuta el pipeline. El pipeline descarga el dataset, lo limpia, lo carga y corre Spark.

**A mano (sin Jenkins):** copiar `.env.example` a `.env`, poner el usuario y la key de Kaggle, y correr:

```
docker compose run --rm ingesta python descargar.py
docker compose run --rm ingesta python cargar_mongo.py
docker compose exec spark-master /opt/jobs/enviar.sh procesamiento.py
```

El archivo `.env` está en el `.gitignore`, nunca se sube al repositorio.

La descarga se hace una sola vez (el csv queda en el volumen `datos`). La carga también: si ya hay datos cargados se salta, y para forzarla se usa `-e FORZAR_CARGA=1`.

## Configurar Jenkins

1. Abrir http://localhost:8080
2. Sacar la contraseña inicial con `docker compose exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword`
3. Instalar los plugins sugeridos y crear el usuario administrador.
4. En *Administrar Jenkins > Credentials > System > Global credentials > Add Credentials* crear una credencial tipo **Username with password** con:
   - Username: el usuario de Kaggle
   - Password: la key de Kaggle
   - ID: `kaggle-credenciales`
5. Crear un item nuevo tipo **Pipeline**, en *Triggers* marcar **GitHub hook trigger for GITScm polling** y en *Pipeline* elegir **Pipeline script from SCM**, Git, la URL del repositorio, rama `*/main` y Script Path `Jenkinsfile`.
6. En GitHub, en *Settings > Webhooks* agregar `http://DIRECCION_PUBLICA_DE_JENKINS/github-webhook/` con content type `application/json`. Si Jenkins corre en un PC local se necesita una URL pública, por ejemplo con ngrok (`ngrok http 8080`).

Etapas del pipeline: Checkout, Construir imagenes, Pruebas unitarias (pytest), Levantar servicios, Descarga e ingesta, Procesamiento Spark, Pruebas contra la API y Despliegue. Si cualquier etapa falla el despliegue no se hace. Las pruebas contra la API se hacen sobre una copia de la API nueva (`api-pruebas`, puerto 5001) y solo si pasan se reemplaza la API del puerto 5000.

## Endpoints

| Método | Ruta | Parámetros | Consulta |
|---|---|---|---|
| GET | `/salud` | | ping a Mongo |
| GET | `/accidentes/cercanos` | `lat`, `lng`, `radio` (metros), `limite` | `$near` |
| POST | `/accidentes/poligono` | cuerpo: Polygon GeoJSON, `limite` | `$geoWithin` |
| GET | `/accidentes/resumen-radio` | `lat`, `lng`, `radio` | agregación con `$geoNear` |
| GET | `/resultados/<tipo>` | `tipo`: grilla, zonas-calientes, hora, dia, mes; `limite` | resultados de Spark |

Ejemplos:

```
curl "http://localhost:5000/accidentes/cercanos?lat=34.05&lng=-118.24&radio=2000&limite=10"
curl -X POST -H "Content-Type: application/json" --data @ejemplos/poligono_los_angeles.json "http://localhost:5000/accidentes/poligono?limite=10"
curl "http://localhost:5000/accidentes/resumen-radio?lat=40.71&lng=-74.00&radio=5000"
curl "http://localhost:5000/resultados/zonas-calientes"
```

## Comparación Dask vs Spark

Las dos herramientas hacen la misma operación: leer el csv completo, quitar coordenadas nulas o fuera de EE.UU. y contar accidentes por celda de 0.1°. Cada script guarda tiempo y memoria en la colección `benchmark`.

Configuración 1 (un worker en cada motor):

```
docker compose stop jenkins
docker compose up -d --scale dask-worker=1 --scale spark-worker=1 dask-worker spark-worker
docker compose run --rm ingesta python benchmark_dask.py
docker compose exec spark-master /opt/jobs/enviar.sh benchmark_spark.py
```

Configuración 2 (dos workers en cada motor):

```
docker compose up -d --scale dask-worker=2 --scale spark-worker=2 dask-worker spark-worker
docker compose run --rm ingesta python benchmark_dask.py
docker compose exec spark-master /opt/jobs/enviar.sh benchmark_spark.py
```

Ver los resultados:

```
docker compose exec mongo mongosh accidentes_db --quiet --eval "db.benchmark.find({}, {_id: 0}).toArray()"
```

Al terminar se vuelve a la configuración normal con `docker compose up -d`.

## Pruebas

```
docker compose run --rm --no-deps api python -m pytest -q tests
docker compose run --rm --no-deps ingesta python -m pytest -q tests
```

## Estructura

```
proyecto-bigdata-geoespacial/
├── docker-compose.yml
├── Jenkinsfile
├── README.md
├── .env.example
├── .gitignore
├── .gitattributes
├── api/            API Flask, consultas geoespaciales y pruebas
├── ingesta/        descarga de Kaggle, limpieza con Dask, carga a Mongo y benchmark Dask
├── spark/          imagen de Spark, job de agregaciones y benchmark Spark
├── jenkins/        imagen de Jenkins con Docker CLI y plugins
├── ejemplos/       polígono de ejemplo para probar la API
└── docs/           informe técnico
```

## Apagar

```
docker compose down
```

Con `docker compose down -v` también se borran los volúmenes (datos de Mongo, csv descargado y configuración de Jenkins).
