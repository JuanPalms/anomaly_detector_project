# Proyecto de Detección de Anomalías

Este proyecto implementa un pipeline de detección de anomalías sobre datos de sensores. El flujo está diseñado para consumir datos desde un bucket AWS S3, con módulos que limpian los datos, calculan parámetros base y detectan valores atípicos.

El proyecto emplea Docker para facilitar su despliegue y ejecución en cualquier entorno. Además, cuenta con integración continua mediante GitHub Actions, lo que asegura que cada cambio en el código sea probado automáticamente con los tests implementados.

## Flujo del Proyecto

1. Preparación de datos (data_prep.py)

- Carga datos crudos desde S3.

- Reemplaza valores faltantes usando un promedio móvil.

- Guarda la versión limpia en S3.

2. Detección de anomalías (anomaly_detection.py)

- Calcula media y desviación estándar del dataset limpio de entrenamiento.

- Carga los datos de prueba y detecta valores fuera del rango esperado.

- Exporta las anomalías detectadas a S3.

3. Ejecución completa (main.py)

- Orquesta todo el pipeline (limpieza + detección).

4. Automatización con GitHub Actions

- Cada push o pull request ejecuta los tests definidos en pytest.

- Se asegura que el código cumpla con los estándares de calidad y pase la suite de validaciones.

5. Contenedorización con Docker

- Todo el proyecto puede ejecutarse dentro de un contenedor, garantizando portabilidad y consistencia entre entornos.

## Estructura del proyecto

``` bash
├── src/
│   ├── data_prep.py              # Limpieza de datos
│   ├── anomaly_detection.py      # Detección de anomalías
│   ├── main.py                   # Orquestador del pipeline
│   ├── outils.py                 # Funciones auxiliares (S3, limpieza, guardado)
│
├── tests/
│   ├── test_outils.py
│   ├── test_data_prep.py
│   ├── test_anomaly_detection.py
│   ├── test_main.py
│   └── conftest.py
│
├── requirements.txt                        # Dependencias principales
├── requirements_testing.txt                # Dependencias de testing
├── env.list                                # Variables de entorno
├── pytest.ini                              # Configuración de pytest
├── Dockerfile                              # Definición del contenedor
└── .github/workflows/dockerbuild.yaml      # Pipeline de GitHub Actions
└── .github/workflows/test_ci.yml           # Pipeline de GitHub Actions


```

## Mejoras con respecto a la primera versión

En esta nueva versión, se implementaron las siguientes mejoras:

1. Separación modular del código

- Antes: todo el flujo (entrenamiento, predicción, reporte) estaba en un único script monolítico.

- Ahora: el código está dividido en módulos (data_prep.py, anomaly_detection.py, outils.py, main.py), facilitando la reutilización y el mantenimiento.

2. Parámetros configurables mediante variables de entorno

- Antes: rutas de archivos, WINDOW_SIZE y THRESHOLD_MULTIPLIER estaban hardcodeados.

- Ahora: se definen en env.list, lo que permite ejecutar el pipeline en distintos entornos sin modificar el código.

3. Integración con AWS S3

- Antes: solo funcionaba con archivos locales (.csv).

- Ahora: se puede cargar y guardar datasets directamente en S3, soportando pipelines de datos en producción.

4. Manejo robusto de valores faltantes

- Antes: los NaN no eran tratados.

- Ahora: se utiliza una función dedicada (handle_missing_values) que aplica un promedio móvil y garantiza datos limpios antes de entrenar o detectar anomalías.

5. Detección de anomalías más eficiente y reutilizable

- Antes: la detección era iterativa fila por fila (iterrows), lo que era ineficiente.

- Ahora: la detección usa operaciones vectorizadas de pandas, encapsuladas en detect_anomalies_df, lo que mejora el rendimiento y la claridad del código.

6. Pipeline orquestado automáticamente

- Antes: se corrían pasos sueltos manualmente.

- Ahora: main.py ejecuta todo el pipeline de forma secuencial y controlada, desde la limpieza hasta la detección.

7. Logging estructurado y robusto

- Antes: solo se usaban print().

- Ahora: se emplea logging, lo que facilita monitoreo y depuración en entornos productivos.

8. Testing automatizado con Pytest y Moto

- Antes: sin pruebas unitarias.

- Ahora: el proyecto incluye un set completo de tests para cada módulo, con simulación de S3 usando moto.

9. Contenedor con Docker

- Antes: se ejecutaba solo en local.

- Ahora: el proyecto cuenta con un Dockerfile que asegura portabilidad y ejecución consistente en cualquier entorno.

10. Integración Continua con GitHub Actions

- Antes: no había validación automática.

- Ahora: cada push o pull request ejecuta el pipeline de CI (tests, linting, verificación de dependencias).

### Prompts empleados para asistentes de IA

1) Quiero que me ayudes a escribir un workflow de GitHub Actions.
El objetivo es construir una imagen de Docker que utilice un archivo requirements.txt para instalar dependencias de Python.

El workflow debe ejecutarse cuando se haga un push a la rama main.

Debe incluir los pasos para:
Configurar Docker.
Construir la imagen utilizando un Dockerfile.
Instalar dependencias desde requirements.txt.
Explica el archivo YAML resultante con comentarios en español para entender cada parte.



