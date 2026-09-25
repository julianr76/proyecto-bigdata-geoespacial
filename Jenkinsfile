pipeline {
    agent any

    triggers {
        githubPush()
    }

    options {
        disableConcurrentBuilds()
        timeout(time: 3, unit: 'HOURS')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Construir imagenes') {
            steps {
                sh 'docker compose build api ingesta spark-master'
            }
        }

        stage('Pruebas unitarias') {
            steps {
                sh 'docker compose run --rm --no-deps api python -m pytest -q tests'
                sh 'docker compose run --rm --no-deps ingesta python -m pytest -q tests'
            }
        }

        stage('Levantar servicios') {
            steps {
                sh 'docker compose up -d mongo spark-master spark-worker dask-scheduler dask-worker'
                sh 'docker compose --profile pruebas up -d --force-recreate api-pruebas'
            }
        }

        stage('Descarga e ingesta') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'kaggle-credenciales', usernameVariable: 'KAGGLE_USERNAME', passwordVariable: 'KAGGLE_KEY')]) {
                    sh 'docker compose run --rm -e KAGGLE_USERNAME -e KAGGLE_KEY ingesta python descargar.py'
                }
                sh 'docker compose run --rm ingesta python cargar_mongo.py'
            }
        }

        stage('Procesamiento Spark') {
            steps {
                sh 'docker compose exec -T spark-master /opt/jobs/enviar.sh procesamiento.py'
            }
        }

        stage('Pruebas contra la API') {
            steps {
                sh 'docker compose run --rm --no-deps -e API_URL=http://api-pruebas:5000 api python pruebas_humo.py'
            }
        }

        stage('Despliegue') {
            steps {
                sh 'docker compose up -d --no-deps --force-recreate api'
            }
        }
    }

    post {
        always {
            sh 'docker compose --profile pruebas stop api-pruebas || true'
        }
        success {
            echo 'Pipeline terminado, la API nueva quedo desplegada en el puerto 5000'
        }
        failure {
            echo 'El pipeline fallo, no se desplego la version nueva'
        }
    }
}
