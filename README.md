# Trabajo práctico final 

Los datos utilizados y el proceso llevado adelante corresponde a un proyecto ya implementado a través de Google Colab y Power BI. El mismo puede ser visitado en <https://www.santacruzpatagonia.gob.ar/observatorio-economico/valoracion-online>.

## Objetivo del proyecto
Generar una plataforma de visualización donde se obtenga un análisis cuantitativo y cualitativo de los comentarios y valoraciones volcados por los usuarios en las plataformas de viaje sobre los atractivos turísticos del destino turístico de interés. Para esto se espera genenerar indicadores clave y la aplicación de modelos de procesamiento de lenguaje natural NLP (-por sus siglas en inglés-) orientado a conocer los motivos que explican y justifican el puntaje de las calificaciones.

El análisis de NLP fue implementado mediante la aplicación del modelo propuesto en el siguiente [paper](https://www.cambridge.org/core/services/aop-cambridge-core/content/view/81B3703230D21620B81EB6E2266C7A66/S1047198700002291a.pdf/fightin-words-lexical-feature-selection-and-evaluation-for-identifying-the-content-of-political-conflict.pdf) el cual busca identificar palabras divisorias entre comentarios positivos y negativos. 


Datasets utilizados
- atractivos_dashboard.csv		
- comentarios_dashboard.csv	
- comentarios_nlp.csv (muestra de 1000 casos)

## Objetivo del desarrollo
El objetivo es la implementación de un pipeline que permita: 

1) Carga y subida de los datasets de atractivos y valoración a una base de datos.
2) Procesamiento de dataset de comentarios para la aplicación de modelos de NLP y posterior subida a una base de datos y al bucket asociado al proyecto.
3) Desarrollo de una visualización con los resultados.

El proceso tiene que ser fácilmente reproducible para cada nuevo cliente, mientras que no se contemplan actualizaciones en un periodo inferior al año. 


## Arquitectura implementada

![Arquitectura](https://github.com/fede9124/tp-final-itba-ml/blob/main/Arquitectura/Arquitectura.png?raw=true "Arquitectura")


### Recursos de la infraestructura 

#### Procesamiento
- Instancia de EC2 t2.large (Airflow)
- Instancia de EC2 t2.medium (Superset)

#### Base de datos
- RDS (Postgres)  

#### Redes
- Virtual Private Netwok
- 2 subredes privada 
- 2 subredes públicas
- 2 IPs elásticas
- 1 subred de bases de datos

#### Tecnologías

- Apache Airflow
- Apache Superset

## Pasos realizados para la implementación

1. Creación de bucket y sus carpetas correspondientes (el bucket es externo a la organización por inconvenientes para generar credenciales desde el lab). La primera es raw_data donde se alojarán los archivos originales y la segunda es processed_data donde se alojarán los archivos soporte (pueden llegar a ser entregados a los clientes).  
2. Creación de la VPC (incluye la creación del internet gateway)
3. Creación de las subredes públicas y privadas
4. Lanzamiento de las instancias en EC2.
5. Instalación del software en las instancias.
6. Creación de la base de datos en RDS.
7. Configuración del grupo de subredes de las bases de datos (se añaden las dos subredes privadas).
8. Configuración del grupo de seguridad para dar acceso a las instancias de EC2 a la base de datos.


## Configuración de instancia con Airflow

Se utilizó una imagen de docker compose para su instanlación.

#### Paso 1. Instalación Docker

Se realiza de forma manual a través de una conexión SSH por el puerto 22 desde un acceso local.
Versión: Última versión estable

```
sudo apt-get update

sudo apt-get install \
   ca-certificates \
   curl \
   gnupg \
   lsb-release


sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg`


echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null


# Docker Engine Installation
# Se instala la última versión disponible
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Configuración post instalación
sudo groupadd docker
sudo usermod -aG docker $USER
```

#### Paso 2. Descarga y edición de la imagen oficial de Airflow

La imagen de Airflow se obtuvo de https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html.

Luego se modificó la imagen para no utilizar la última imagen estable por defecto y crear una nueva imagen que mejor se adapte al proyecto.

```
  #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.5.0}
  build: .
```

#### Paso 3. Creación de dockerfile y requirements.txt para crear una nueva imagen de Airflow para la implementación

En el dockerfile se estableció la imagen de airflow 2.5.0 y de python 3.9 ya que la versión por defecto (3.7) ya no tiene soporte con alguna de las librerías requeridas.
En el .txt se añadieron los paquetes requeridos que no se encuentran disponibles en la instalación de Airflow.

#### Paso 4. Clonación repositorio con imagen de docker compose

Ejecutado de forma manual a través de una conexión SSH por el puerto 22 desde un acceso local.

```
git clone https://github.com/fede9124/tp-final-itba-ml
cd /home/ubuntu/tp-final-itba-ml/Airflow

# Se crean las carpetas que se van a utilizar
mkdir -p ./dags ./logs ./plugins ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
```

#### Paso 3. Añadir conexiones en Airflow

Se efectua en la UI de Airflow a la cual se accede a través de su IP pública (puerto 8080)  

- Conexión con S3
- Conexión con RDS de Postgres 

![Conexiones Airflow](https://github.com/fede9124/tp-final-itba-ml/blob/main/Arquitectura/Conexiones_airflow.png?raw=true "Conexiones Airflow")

## Configuración de instancia con Superset

Se realiza de forma manual a través de una conexión SSH por el puerto 22 desde un acceso local.
Versión: Última versión estable

#### Paso 1. Instalación Docker

```
sudo apt-get update

sudo apt-get install \
   ca-certificates \
   curl \
   gnupg \
   lsb-release


sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg`


echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null


# Docker Engine Installation
# Se instala la última versión disponible
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Configuración post instalación
sudo groupadd docker
sudo usermod -aG docker $USER
```

#### Paso 2. Instalación Superset

También de forma manual a través de una conexión SSH por el puerto 22 desde un acceso local.
Versión: Última versión estable

```
git clone https://github.com/apache/superset.git
cd superset
docker compose -f docker-compose-non-dev.yml pull
docker compose -f docker-compose-non-dev.yml up -d
```


Para lograr la visualización de mapas de MAPBOX se obtuvo un token API de https://www.mapbox.com y se cargó manualmente en el archivo de configuraciones
```
cd /home/ubuntu/superset/docker/pythonpath_dev/
vim superset_config.py

# Añadir linea al final con:
MAPBOX_API_KEY = ""
```

## DAGs 

Se implementaron 4 DAGs para ejecutar las diferentes tareas requeridas.



![DAGs](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/dags.png?raw=true "DAGs")


#### DAG atractivos
- Levanta dataset del bucket
- Carga dataset en RDS

![DAG atractivos](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/dag_atractivos.png?raw=true "DAG atractivos")

#### DAG valoraciones
- Levanta dataset del bucket
- Carga dataset en RDS

![DAG valoraciones](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/dag_valoraciones.png?raw=true "DAG valoraciones")


#### DAG comentarios
- Levanta dataset del bucket
- Separa el dataset de acuerdo al idioma de los comentarios
- Hacer preprocesamiento de los comentarios (normalización y tokenización)

![DAG comentarios](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/dag_comentarios.png?raw=true "DAG comentarios")

#### DAG NLP_español
- Levanta el dataset generado por el DAG de comentarios
- Implementa modelo de NLP
- Carga dataset generados a RDS
- Carga dataset generados a bucket


![DAG comentarios](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/dag_nlp_espa%C3%B1ol.PNG?raw=true "DAG comentarios")



## Dashboard construido 

Versión de prueba (primera interacción con la herramienta).

![Dashboard](https://github.com/fede9124/tp-final-itba-ml/blob/main/Im%C3%A1genes%20Arquitectura/Dashboard-superset.jpg?raw=true "Dashboard")



## Tareas pendientes de la implementación

1. Control de dependencias a través de Poetry.
2. Añadir una instancia como bastión host para un único acceso externo a todos los componentes.
3. Añadir procesamientos adicionales de NLP que todavía no se implementaron.
4. Replicar los resultados para comentarios en español en inglés y portugués.
5. Buscar alternativas para permitir acceso público a Superset (¿Servidor web con NGINX?)
6. Analizar otras alternativas de implementación de acuerdo a los costes. La actual fue considerada más económica para una instancia de desarrollo respecto del Amazon Managed Workflows for Apache Airflow (MWAA).