#!/usr/bin/env python3
"""
Estadísticas Básicas del Bot "No Me Entregaron" - VERSIÓN CORREGIDA

Este script genera estadísticas basadas en el flujo real del bot:
1. Sesiones que autorizaron tratamiento de datos
2. Personas que subieron fórmulas médicas válidas  
3. Personas que completaron información completamente (según el flujo real)

Uso:
    python estadistica_basica.py
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
from collections import defaultdict

import pytz
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Configuración
PROJECT_ID = os.getenv("PROJECT_ID", "")
FIRESTORE_DATABASE_NAME = "historia"
FIRESTORE_COLLECTION_SESSIONS = "sesiones_activas"
BIGQUERY_DATASET_ID = os.getenv("DATASET_ID")
BIGQUERY_TABLE_PATIENTS = os.getenv("TABLE_ID", "pacientes")
BIGQUERY_TABLE_HISTORY = os.getenv("BIGQUERY_TABLE_ID", "historial_conversacion")

COLOMBIA_TZ = pytz.timezone('America/Bogota')


class EstadisticaError(Exception):
    """Excepción para errores en estadísticas."""
    pass


class EstadisticaBasica:
    """Generador de estadísticas básicas del bot según el flujo real."""

    def __init__(self):
        """Inicializa clientes de Firestore y BigQuery."""
        if not PROJECT_ID:
            raise EstadisticaError("PROJECT_ID no está configurado en las variables de entorno.")
        
        try:
            # Cliente Firestore
            self.firestore_client = firestore.Client(
                project=PROJECT_ID, 
                database=FIRESTORE_DATABASE_NAME
            )
            logger.info(f"✅ Conectado a Firestore: {PROJECT_ID}/{FIRESTORE_DATABASE_NAME}")
            
            # Cliente BigQuery
            self.bigquery_client = bigquery.Client(project=PROJECT_ID)
            logger.info(f"✅ Conectado a BigQuery: {PROJECT_ID}")
            
        except Exception as e:
            logger.error(f"❌ Error inicializando clientes: {e}")
            raise EstadisticaError(f"Error de inicialización: {e}")

    def obtener_estadisticas_firestore(self) -> Dict[str, Any]:
        """Obtiene estadísticas de sesiones activas en Firestore según el flujo real."""
        try:
            logger.info("🔍 Consultando sesiones activas en Firestore...")
            
            # Obtener todas las sesiones activas
            sessions_ref = self.firestore_client.collection(FIRESTORE_COLLECTION_SESSIONS)
            sessions = sessions_ref.stream()
            
            total_sesiones = 0
            sesiones_con_consentimiento = 0
            sesiones_autorizadas = 0
            sesiones_con_prescripcion = 0
            sesiones_proceso_completo = 0  # Nueva métrica clave
            
            for session_doc in sessions:
                session_data = session_doc.to_dict()
                total_sesiones += 1
                
                # Verificar consentimiento
                consentimiento = session_data.get('consentimiento')
                if consentimiento is not None:
                    sesiones_con_consentimiento += 1
                    if consentimiento is True:
                        sesiones_autorizadas += 1
                
                # Analizar conversación para determinar el progreso
                conversation = session_data.get('conversation', [])
                
                # Verificar si subieron prescripción
                tiene_prescripcion = any(
                    event.get('event_type') == 'prescription_processed' 
                    for event in conversation 
                    if isinstance(event, dict)
                )
                if tiene_prescripcion:
                    sesiones_con_prescripcion += 1
                
                # ✅ MÉTRICA CLAVE: Verificar si completaron TODO el proceso
                # Según telegram_c.py línea ~543: "process_completed_with_claim"
                # Esto indica que generaron la reclamación exitosamente
                proceso_completo = any(
                    (event.get('message', '').find('Reclamación EPS generada exitosamente') != -1 or
                     event.get('message', '').find('proceso completado exitosamente') != -1 or
                     'process_completed_with_claim' in str(event))
                    for event in conversation 
                    if isinstance(event, dict)
                )
                if proceso_completo:
                    sesiones_proceso_completo += 1
            
            # Calcular porcentajes
            porcentaje_autorizadas = (
                (sesiones_autorizadas / total_sesiones * 100) 
                if total_sesiones > 0 else 0
            )
            
            porcentaje_prescripciones = (
                (sesiones_con_prescripcion / total_sesiones * 100) 
                if total_sesiones > 0 else 0
            )
            
            porcentaje_proceso_completo = (
                (sesiones_proceso_completo / total_sesiones * 100) 
                if total_sesiones > 0 else 0
            )
            
            return {
                'total_sesiones_activas': total_sesiones,
                'sesiones_con_consentimiento': sesiones_con_consentimiento,
                'sesiones_autorizadas': sesiones_autorizadas,
                'porcentaje_autorizadas': porcentaje_autorizadas,
                'sesiones_con_prescripcion': sesiones_con_prescripcion,
                'porcentaje_prescripciones': porcentaje_prescripciones,
                'sesiones_proceso_completo': sesiones_proceso_completo,  # ✅ Nueva métrica
                'porcentaje_proceso_completo': porcentaje_proceso_completo
            }
            
        except GoogleAPIError as e:
            logger.error(f"❌ Error de Firestore: {e}")
            raise EstadisticaError(f"Error consultando Firestore: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado consultando Firestore: {e}")
            raise EstadisticaError(f"Error inesperado: {e}")

    def obtener_estadisticas_bigquery(self) -> Dict[str, Any]:
        """Obtiene estadísticas de pacientes y reclamaciones en BigQuery según el flujo real."""
        try:
            logger.info("🔍 Consultando datos en BigQuery...")
            
            # 1. Estadísticas de pacientes con prescripciones válidas
            query_pacientes = f"""
            SELECT 
                COUNT(*) as total_pacientes,
                COUNT(CASE WHEN ARRAY_LENGTH(prescripciones) > 0 THEN 1 END) as pacientes_con_prescripciones,
                -- ✅ MÉTRICA CLAVE: Pacientes que generaron al menos 1 reclamación
                COUNT(CASE WHEN ARRAY_LENGTH(reclamaciones) > 0 THEN 1 END) as pacientes_con_reclamaciones,
                -- Pacientes con datos básicos completos
                COUNT(CASE 
                    WHEN nombre_paciente IS NOT NULL 
                    AND nombre_paciente != ''
                    AND tipo_documento IS NOT NULL 
                    AND tipo_documento != ''
                    AND numero_documento IS NOT NULL 
                    AND numero_documento != ''
                    AND ARRAY_LENGTH(prescripciones) > 0
                    THEN 1 
                END) as pacientes_datos_basicos_completos,
                -- ✅ NUEVA MÉTRICA: Pacientes que llegaron hasta el final (según ClaimManager)
                -- Esto es cuando tienen reclamación Y todos los campos requeridos por ClaimManager
                COUNT(CASE 
                    WHEN nombre_paciente IS NOT NULL AND nombre_paciente != ''
                    AND tipo_documento IS NOT NULL AND tipo_documento != ''
                    AND numero_documento IS NOT NULL AND numero_documento != ''
                    AND ARRAY_LENGTH(telefono_contacto) > 0
                    AND ARRAY_LENGTH(correo) > 0
                    AND regimen IS NOT NULL AND regimen != ''
                    AND ciudad IS NOT NULL AND ciudad != ''
                    AND direccion IS NOT NULL AND direccion != ''
                    AND eps_estandarizada IS NOT NULL AND eps_estandarizada != ''
                    AND farmacia IS NOT NULL AND farmacia != ''
                    AND ARRAY_LENGTH(informante) > 0
                    AND ARRAY_LENGTH(prescripciones) > 0
                    AND ARRAY_LENGTH(reclamaciones) > 0  -- ✅ CLAVE: Que tengan reclamación generada
                    THEN 1 
                END) as pacientes_proceso_completado_totalmente
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_PATIENTS}`
            """
            
            # 2. Estadísticas específicas de reclamaciones generadas
            query_reclamaciones = f"""
            SELECT 
                COUNT(*) as total_reclamaciones_generadas,
                COUNT(DISTINCT paciente_clave) as pacientes_unicos_con_reclamaciones,
                COUNT(CASE WHEN tipo_accion = 'reclamacion_eps' THEN 1 END) as reclamaciones_eps,
                COUNT(CASE WHEN tipo_accion = 'reclamacion_supersalud' THEN 1 END) as reclamaciones_supersalud,
                COUNT(CASE WHEN tipo_accion = 'tutela' THEN 1 END) as tutelas_generadas,
                COUNT(CASE WHEN estado_reclamacion = 'pendiente_radicacion' THEN 1 END) as pendientes_radicacion,
                COUNT(CASE WHEN estado_reclamacion = 'radicado' THEN 1 END) as radicadas
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_PATIENTS}`,
            UNNEST(reclamaciones) AS reclamacion
            """
            
            # 3. Estadísticas del historial de conversaciones cerradas
            query_historial = f"""
            SELECT 
                COUNT(*) as total_conversaciones_cerradas,
                COUNT(CASE WHEN consentimiento = true THEN 1 END) as conversaciones_autorizadas_historial,
                -- ✅ NUEVA MÉTRICA: Conversaciones que mencionan "proceso completado"
                COUNT(CASE 
                    WHEN (conversacion LIKE '%Reclamación EPS generada exitosamente%' 
                          OR conversacion LIKE '%proceso completado exitosamente%'
                          OR conversacion LIKE '%process_completed_with_claim%')
                    THEN 1 
                END) as conversaciones_proceso_completo_historial
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_HISTORY}`
            """
            
            # Ejecutar consultas
            results_pacientes = self.bigquery_client.query(query_pacientes).result()
            results_reclamaciones = self.bigquery_client.query(query_reclamaciones).result()
            results_historial = self.bigquery_client.query(query_historial).result()
            
            # Procesar resultados de pacientes
            pacientes_stats = None
            for row in results_pacientes:
                pacientes_stats = {
                    'total_pacientes': row.total_pacientes,
                    'pacientes_con_prescripciones': row.pacientes_con_prescripciones,
                    'pacientes_con_reclamaciones': row.pacientes_con_reclamaciones,
                    'pacientes_datos_basicos_completos': row.pacientes_datos_basicos_completos,
                    'pacientes_proceso_completado_totalmente': row.pacientes_proceso_completado_totalmente
                }
                break
            
            # Procesar resultados de reclamaciones
            reclamaciones_stats = None
            for row in results_reclamaciones:
                reclamaciones_stats = {
                    'total_reclamaciones_generadas': row.total_reclamaciones_generadas,
                    'pacientes_unicos_con_reclamaciones': row.pacientes_unicos_con_reclamaciones,
                    'reclamaciones_eps': row.reclamaciones_eps,
                    'reclamaciones_supersalud': row.reclamaciones_supersalud,
                    'tutelas_generadas': row.tutelas_generadas,
                    'pendientes_radicacion': row.pendientes_radicacion,
                    'radicadas': row.radicadas
                }
                break
            
            # Procesar resultados de historial
            historial_stats = None
            for row in results_historial:
                historial_stats = {
                    'total_conversaciones_cerradas': row.total_conversaciones_cerradas,
                    'conversaciones_autorizadas_historial': row.conversaciones_autorizadas_historial,
                    'conversaciones_proceso_completo_historial': row.conversaciones_proceso_completo_historial
                }
                break
            
            if not pacientes_stats or not historial_stats:
                raise EstadisticaError("No se pudieron obtener estadísticas de BigQuery")
            
            if not reclamaciones_stats:
                # Si no hay reclamaciones, llenar con ceros
                reclamaciones_stats = {
                    'total_reclamaciones_generadas': 0,
                    'pacientes_unicos_con_reclamaciones': 0,
                    'reclamaciones_eps': 0,
                    'reclamaciones_supersalud': 0,
                    'tutelas_generadas': 0,
                    'pendientes_radicacion': 0,
                    'radicadas': 0
                }
            
            # Calcular porcentajes
            total_pac = pacientes_stats['total_pacientes']
            porcentaje_con_prescripciones = (
                (pacientes_stats['pacientes_con_prescripciones'] / total_pac * 100) 
                if total_pac > 0 else 0
            )
            porcentaje_con_reclamaciones = (
                (pacientes_stats['pacientes_con_reclamaciones'] / total_pac * 100) 
                if total_pac > 0 else 0
            )
            porcentaje_proceso_completado = (
                (pacientes_stats['pacientes_proceso_completado_totalmente'] / total_pac * 100) 
                if total_pac > 0 else 0
            )
            
            return {
                **pacientes_stats,
                **reclamaciones_stats,
                **historial_stats,
                'porcentaje_con_prescripciones': porcentaje_con_prescripciones,
                'porcentaje_con_reclamaciones': porcentaje_con_reclamaciones,
                'porcentaje_proceso_completado': porcentaje_proceso_completado
            }
            
        except GoogleAPIError as e:
            logger.error(f"❌ Error de BigQuery: {e}")
            raise EstadisticaError(f"Error consultando BigQuery: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado consultando BigQuery: {e}")
            raise EstadisticaError(f"Error inesperado: {e}")

    def generar_reporte_completo(self) -> None:
        """Genera reporte básico con solo las 3 métricas principales."""
        try:
            print("📊 ESTADÍSTICAS BÁSICAS - BOT 'NO ME ENTREGARON'")
            print(f"🕐 {datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Obtener estadísticas
            firestore_stats = self.obtener_estadisticas_firestore()
            bigquery_stats = self.obtener_estadisticas_bigquery()
            
            # 1. AUTORIZACIÓN DE TRATAMIENTO DE DATOS
            total_autorizadas = (
                firestore_stats['sesiones_autorizadas'] + 
                bigquery_stats['conversaciones_autorizadas_historial']
            )
            total_sesiones_globales = (
                firestore_stats['total_sesiones_activas'] + 
                bigquery_stats['total_conversaciones_cerradas']
            )
            porcentaje_global_autorizadas = (
                (total_autorizadas / total_sesiones_globales * 100) 
                if total_sesiones_globales > 0 else 0
            )
            
            print(f"1️⃣ AUTORIZARON TRATAMIENTO DE DATOS:")
            print(f"   📊 {total_autorizadas:,} personas ({porcentaje_global_autorizadas:.1f}%)")
            
            # 2. FÓRMULAS MÉDICAS VÁLIDAS
            print(f"\n2️⃣ SUBIERON FÓRMULAS MÉDICAS VÁLIDAS:")
            print(f"   📊 {bigquery_stats['pacientes_con_prescripciones']:,} personas ({bigquery_stats['porcentaje_con_prescripciones']:.1f}%)")
            
            # 3. COMPLETARON INFORMACIÓN COMPLETAMENTE
            print(f"\n3️⃣ COMPLETARON INFORMACIÓN COMPLETAMENTE:")
            print(f"   📊 {bigquery_stats['pacientes_proceso_completado_totalmente']:,} personas ({bigquery_stats['porcentaje_proceso_completado']:.1f}%)")
            
            print(f"\n✅ Total sesiones: {total_sesiones_globales:,}")
            
        except EstadisticaError:
            raise
        except Exception as e:
            logger.error(f"❌ Error generando reporte: {e}")
            raise EstadisticaError(f"Error generando reporte: {e}")


def main():
    """Función principal para ejecutar las estadísticas básicas."""
    try:
        # Verificar configuración
        if not PROJECT_ID:
            print("❌ ERROR: PROJECT_ID no configurado.")
            sys.exit(1)
        
        # Crear instancia y generar reporte básico
        estadistica = EstadisticaBasica()
        estadistica.generar_reporte_completo()
        
    except EstadisticaError as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Interrumpido por el usuario.")
        sys.exit(0)
    except Exception as e:
        print(f"❌ ERROR INESPERADO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()