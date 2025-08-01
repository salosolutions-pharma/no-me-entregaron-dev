import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import functions_framework
from flask import Request
from google.cloud import bigquery
import pytz
import traceback
# Configuración
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID") 
TABLE_ID = os.getenv("TABLE_ID")

COLOMBIA_TZ = pytz.timezone('America/Bogota')

DIAS_REVISION = {
    "simple": {
        "reclamacion_eps": 5,      # Nivel 1
        "reclamacion_supersalud": 20   # Nivel 2 y 3
    },
    "priorizado": {
        "reclamacion_eps": 5,      # Nivel 1  
        "reclamacion_supersalud": 20   # Nivel 2 y 3
    },
    "vital": {
        "reclamacion_eps": 1,      # Nivel 1 (24h)
        "reclamacion_supersalud": 1    # Nivel 2 (24h) - No hay nivel 3 múltiple en vital
    }
}

def get_bigquery_client() -> bigquery.Client:
    """Obtiene cliente BigQuery."""
    try:
        logger.info(f"🔧 Intentando crear cliente BigQuery con PROJECT_ID: {PROJECT_ID}")
        if not PROJECT_ID:
            raise ValueError("PROJECT_ID es None o vacío")
            
        client = bigquery.Client(project=PROJECT_ID)
        logger.info("✅ Cliente BigQuery creado exitosamente")
        return client
    except Exception as e:
        logger.error(f"❌ Error creando cliente BigQuery: {e}")
        logger.error(f"❌ Traceback completo: {traceback.format_exc()}")
        raise

def calcular_dias_calendario(fecha_base: datetime.date, dias_totales: int) -> datetime.date:
    """Calcula fecha agregando días calendario (incluye sábados, domingos y festivos)."""
    return fecha_base + timedelta(days=dias_totales)

def obtener_registros_eps_supersalud(client: bigquery.Client) -> List[Dict]:
    """
    Consulta registros EPS y Supersalud con fecha_radicacion = ayer y fecha_revision nula.
    
    LÓGICA DE NIVELES:
    - SIMPLE/PRIORIZADO: Nivel 1 (EPS), Nivel 2 (Supersalud), Nivel 3 (EPS+Supersalud múltiple)
    - VITAL: Nivel 1 (EPS), Nivel 2 (Supersalud) → Directo a Tutela (NO nivel 3 múltiple)
    """
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    print (f"🔍 Consultando registros en {table_reference}")
    query = f"""
    SELECT 
        paciente_clave,
        prescripcion.categoria_riesgo AS categoria_riesgo,
        reclamacion.fecha_radicacion,
        reclamacion.nivel_escalamiento,
        reclamacion.tipo_accion
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    CROSS JOIN UNNEST(reclamaciones) AS reclamacion
    LEFT JOIN UNNEST(prescripciones) AS prescripcion ON TRUE
    WHERE reclamacion.fecha_radicacion = DATE_SUB(DATE(DATETIME(CURRENT_TIMESTAMP(), "America/Bogota")), INTERVAL 1 DAY)
    AND reclamacion.tipo_accion IN ('reclamacion_eps', 'reclamacion_supersalud')
    AND (
        (LOWER(prescripcion.categoria_riesgo) IN ('simple', 'priorizado') AND reclamacion.nivel_escalamiento IN (1, 2, 3))
        OR
        (LOWER(prescripcion.categoria_riesgo) = 'vital' AND reclamacion.nivel_escalamiento IN (1, 2))
    )
    AND reclamacion.fecha_revision IS NULL
    ORDER BY paciente_clave, reclamacion.nivel_escalamiento

    """
    
    try:
        logger.info(f"🔍 Buscando EPS/Supersalud: Simple/Priorizado(1-3), Vital(1-2) con fecha_radicacion = ayer")
        
        results = client.query(query).result()
        print(f"🔍 Registros encontrados: {results}")
        registros = []
                
        for row in results:
            registro = {
                "paciente_clave": row.paciente_clave,
                "fecha_radicacion": row.fecha_radicacion,
                "categoria_riesgo": row.categoria_riesgo or "simple",
                "nivel_escalamiento": row.nivel_escalamiento,
                "tipo_accion": row.tipo_accion
            }
            registros.append(registro)
        
        logger.info(f"📊 Encontrados {len(registros)} registros EPS/Supersalud para procesar")
        return registros
        
    except Exception as e:
        logger.error(f"❌ Error consultando registros: {e}")
        return []

def actualizar_fecha_revision(client: bigquery.Client, paciente_clave: str, 
                            nueva_fecha: datetime.date, nivel_escalamiento: int, tipo_accion: str) -> bool:
    """Actualiza fecha_revision para una reclamación específica dentro del array."""
    table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Convertir date a string para BigQuery
    fecha_str = nueva_fecha.strftime('%Y-%m-%d')
    
    update_query = f"""
        UPDATE `{table_reference}`
        SET reclamaciones = ARRAY(
            SELECT AS STRUCT
                rec.med_no_entregados,
                rec.tipo_accion,
                rec.texto_reclamacion,
                rec.estado_reclamacion,
                rec.nivel_escalamiento,
                rec.url_documento,
                rec.numero_radicado,
                rec.fecha_radicacion,
                CASE 
                    WHEN rec.nivel_escalamiento = @nivel_escalamiento 
                         AND rec.tipo_accion = @tipo_accion
                         AND rec.fecha_radicacion = DATE_SUB(DATE(DATETIME(CURRENT_TIMESTAMP(), "America/Bogota")), INTERVAL 1 DAY)
                         AND rec.fecha_revision IS NULL 
                    THEN DATE(@nueva_fecha)
                    ELSE rec.fecha_revision
                END AS fecha_revision,
                rec.id_session
            FROM UNNEST(reclamaciones) AS rec
        )
        WHERE paciente_clave = @paciente_clave
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("nueva_fecha", "STRING", fecha_str),
            bigquery.ScalarQueryParameter("paciente_clave", "STRING", paciente_clave),
            bigquery.ScalarQueryParameter("nivel_escalamiento", "INTEGER", nivel_escalamiento),
            bigquery.ScalarQueryParameter("tipo_accion", "STRING", tipo_accion)
        ]
    )
    
    try:
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        if query_job.errors:
            logger.error(f"❌ Errores actualizando {paciente_clave}: {query_job.errors}")
            return False
            
        rows_affected = getattr(query_job, 'num_dml_affected_rows', 0)
        if rows_affected > 0:
            logger.info(f"✅ {paciente_clave} nivel {nivel_escalamiento} ({tipo_accion}) -> fecha_revision: {nueva_fecha}")
            return True
        else:
            logger.warning(f"⚠️  No se actualizó {paciente_clave} nivel {nivel_escalamiento}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error actualizando {paciente_clave}: {e}")
        return False

@functions_framework.http
def actualizar_fechas_eps_supersalud(request: Request) -> tuple:
    """
    Cloud Function principal - SOLO procesa EPS y Supersalud.
    Programada para ejecutar a las 4 AM diario.
    """
    inicio = datetime.now(COLOMBIA_TZ)
    logger.info(f"🚀 Iniciando actualización EPS/Supersalud - {inicio}")
    
    try:
        # Validar configuración
        if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
            return "❌ Variables de entorno incompletas", 500
        
        client = get_bigquery_client()
        
        # Obtener registros pendientes (solo EPS y Supersalud)
        registros = obtener_registros_eps_supersalud(client)
        
        if not registros:
            logger.info("ℹ️  No hay registros EPS/Supersalud para procesar")
            return "No hay registros EPS/Supersalud pendientes", 200
        
        # Procesar cada registro
        exitosos = 0
        errores = 0
        
        for registro in registros:
            try:
                # Obtener días según reglas
                categoria = registro["categoria_riesgo"].lower()
                escalamiento = registro["nivel_escalamiento"]
                accion = registro["tipo_accion"]
                
                # Buscar en matriz de reglas
                if categoria not in DIAS_REVISION:
                    logger.warning(f"⚠️  Categoría '{categoria}' no reconocida, usando 'simple'")
                    categoria = "simple"

                if accion not in DIAS_REVISION[categoria]:
                    logger.error(f"❌ Acción '{accion}' no definida para categoría '{categoria}'")
                    errores += 1
                    continue    
                
                dias = DIAS_REVISION[categoria][accion]

                
                # Calcular nueva fecha
                fecha_radicacion = registro["fecha_radicacion"]
                if isinstance(fecha_radicacion, str):
                    fecha_radicacion = datetime.strptime(fecha_radicacion, '%Y-%m-%d').date()

                nueva_fecha_revision = calcular_dias_calendario(fecha_radicacion, dias)

                if actualizar_fecha_revision(client, registro["paciente_clave"], nueva_fecha_revision,
                                            escalamiento, accion):
                    exitosos += 1
                    logger.info(f"✅ {registro['paciente_clave']}: {categoria}+{accion} = {dias}d -> {nueva_fecha_revision}")
                else:
                    errores += 1
                    
            except Exception as e:
                logger.error(f"❌ Error procesando {registro['paciente_clave']}: {e}")
                logger.error(traceback.format_exc())
                errores += 1
        
        # Resultado final
        tiempo_total = (datetime.now(COLOMBIA_TZ) - inicio).total_seconds()
        
        logger.info(f"🎯 Completado en {tiempo_total:.1f}s:")
        logger.info(f"   📊 Total: {len(registros)}")
        logger.info(f"   ✅ Exitosos: {exitosos}")
        logger.info(f"   ❌ Errores: {errores}")
        
        mensaje = f"Procesamiento EPS/Supersalud completado: {exitosos}/{len(registros)} exitosos"
        return mensaje, 200
        
    except Exception as e:
        logger.error(f"❌ Error general: {e}")
        return f"Error: {e}", 500
        logger.error(traceback.format_exc())
        return f"Error: {e}\n{traceback.format_exc()}", 500

@functions_framework.http
def test_configuracion(request: Request) -> tuple:
    """Función de prueba para verificar configuración."""
    try:
        # Test variables
        if not PROJECT_ID:
            return "❌ PROJECT_ID no configurado", 400
        if not DATASET_ID:
            return "❌ DATASET_ID no configurado", 400
        if not TABLE_ID:
            return "❌ TABLE_ID no configurado", 400
            
        # Test BigQuery
        client = get_bigquery_client()
        test_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` LIMIT 1"
        client.query(test_query).result()
        
        return f"✅ Configuración OK - Tabla: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", 200
        
    except Exception as e:
        return f"❌ Error configuración: {e}", 500
        logger.error(traceback.format_exc())
        return f"Error: {e}\n{traceback.format_exc()}", 500

if __name__ == "__main__":
    # Para testing local
    from flask import Flask
    app = Flask(__name__)
    
    @app.route('/test')
    def test(): 
        from unittest.mock import Mock
        return actualizar_fechas_eps_supersalud(Mock())
    
    @app.route('/config')
    def config():
        from unittest.mock import Mock
        return test_configuracion(Mock())
        
    app.run(host='0.0.0.0', port=8080, debug=True)