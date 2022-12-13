# Trabajo práctico final 


## Objetivo del proyecto
Generar una plataforma de visualización donde se obtenga un análisis cuantitativo y cualitativo de los comentarios y valoraciones volcados por los usuarios en las plataformas de viaje sobre los atractivos turísticos del destino turístico de interés. Para esto se espera genenerar indicadores clave y la aplicación de modelos de procesamiento de lenguaje natural NLP - por sus siglas en inglés) orientado a conocer los motivos que explican y justifican el puntaje de las calificaciones.


## Objetivo del desarrollo
El objetivo es la implementación de un pipeline que permita: 

1) Carga y subida de los datasets de atractivos y valoración a una base de datos.
2) Procesamiento de dataset de comentarios para la aplicación de modelos de NLP y posterior subida a una base de datos.
3) Desarrollo de una visualización con los resultados.



### Arquitectura implementada

[Arquitectura](C:\Users\Fede\Documents\GitHub\tp-final-itba-ml\Arquitectura\Arquitectura.png)


Recursos 

Procesamiento
- Instancia de EC2 t2.large (Airflow)
- Instancia de EC2 t2.medium (Superset)

Base de datos
- RDS (Postgres)  


Redes
- Virtual Private Netwok

- Subred privada 

- 2 IPs elásticas

Tecnologías

- Apache Airflow
- Apache Superset

## Pasos a seguir

1. Se crea un bucket y dos carpetas. La primera es raw_data donde se alojarán los archivos originales y la segunda es processed_data donde se alojarán los archivos a ser entregados a los clientes.  
2. Creación de VPC  (ampliar a subnets, etc)
3. Lanzamiento de instancia en EC2.  Ubuntu Server 20.04 LTS  t2.large (2vcpu / 8GB ram)



Instalación docker
Manual 

# VER PORQUE NO FUNCIONA CORRIENDO EL SCRIPT
Instalación docker
cd /home/data/tp-final-itba-ml
sudo chmod +x ./docker_install.sh
./docker_install.sh


# Instalacion Docker Compose



Instalación Airflow
https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml


# VER SI SE NECESITA UNA IP ELÁSTICA

FAQ

¿Por qué una instancia EC2 y no otras alternativas?



## Airflow

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

#### Paso 2. Clonar repositorio con imagen de docker compose

```
git clone https://github.com/fede9124/tp-final-itba-ml
cd /home/ubuntu/tp-final-itba-ml/Airflow

# Se crean las carpetas que se van a utilizar
mkdir -p ./dags ./logs ./plugins ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
```

#### Paso 3. Añadir las conexiones que se van a utilizar en la UI de Airflow

- Conexión con S3
- Conexión con RDS de Postgres 

## Superset

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
```


