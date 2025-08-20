# Proyecto de Detección de Anomalías

Este proyecto implementa un pipeline de detección de anomalías sobre datos de sensores. El flujo está diseñado para consumir datos desde un bucket AWS S3, con módulos que limpian los datos, calculan parámetros base y detectan valores atípicos.

El proyecto emplea Docker para facilitar su despliegue y ejecución en cualquier entorno. Además, cuenta con integración continua mediante GitHub Actions, lo que asegura que cada cambio en el código sea probado automáticamente con los tests implementados.

### Flujo del Proyecto

Preparación de datos (data_prep.py)

Carga datos crudos desde S3.

Reemplaza valores faltantes usando un promedio móvil.

Guarda la versión limpia en S3.

Detección de anomalías (anomaly_detection.py)

Calcula media y desviación estándar del dataset limpio de entrenamiento.

Carga los datos de prueba y detecta valores fuera del rango esperado.

Exporta las anomalías detectadas a S3.

Ejecución completa (main.py)

Orquesta todo el pipeline (limpieza + detección).

Automatización con GitHub Actions

Cada push o pull request ejecuta los tests definidos en pytest.

Se asegura que el código cumpla con los estándares de calidad y pase la suite de validaciones.

Contenedorización con Docker

Todo el proyecto puede ejecutarse dentro de un contenedor, garantizando portabilidad y consistencia entre entornos.
