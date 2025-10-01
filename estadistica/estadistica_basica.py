#!/usr/bin/env python3
"""
Estadísticas Básicas del Bot "No Me Entregaron" - VERSIÓN ACTUALIZADA

Este script genera las métricas clave del bot:
1. Fórmulas médicas válidas (personas que subieron prescripciones)
2. Completaron información hasta el final (proceso completo)
3. Conversaciones activas en este momento (sesiones en Firestore)
4. Total personas que han escrito al bot (historial completo)

Uso:
    python estadistica_basica.py
    python estadistica_basica.py --debug
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
    """Generador de estadísticas básicas del bot."""

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

    def obtener_conversaciones_activas_firestore(self) -> int:
        """Obtiene el número de conversaciones activas en Firestore."""
        try:
            logger.info("🔍 Contando conversaciones activas en Firestore...")
            
            sessions_ref = self.firestore_client.collection(FIRESTORE_COLLECTION_SESSIONS)
            sessions = sessions_ref.stream()
            
            total_activas = 0
            for session_doc in sessions:
                total_activas += 1
            
            logger.info(f"📱 Conversaciones activas encontradas: {total_activas}")
            return total_activas
            
        except GoogleAPIError as e:
            logger.error(f"❌ Error de Firestore: {e}")
            raise EstadisticaError(f"Error consultando Firestore: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado consultando Firestore: {e}")
            raise EstadisticaError(f"Error inesperado: {e}")

    def obtener_estadisticas_bigquery(self) -> Dict[str, Any]:
        """Obtiene estadísticas de BigQuery: prescripciones, proceso completo y total histórico."""
        try:
            logger.info("🔍 Consultando datos en BigQuery...")
            
            # 1. Estadísticas de fórmulas médicas y proceso completo
            query_pacientes = f"""
            SELECT 
                COUNT(*) as total_pacientes_registrados,
                COUNT(CASE WHEN ARRAY_LENGTH(prescripciones) > 0 THEN 1 END) as formulas_medicas_validas,
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
                    AND ARRAY_LENGTH(reclamaciones) > 0
                    THEN 1 
                END) as completaron_proceso_final
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_PATIENTS}`
            """
            
            # 2. Total de personas que han escrito (historial completo)
            query_historial = f"""
            SELECT 
                COUNT(*) as total_personas_han_escrito
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_HISTORY}`
            """
            
            # Ejecutar consultas
            results_pacientes = self.bigquery_client.query(query_pacientes).result()
            results_historial = self.bigquery_client.query(query_historial).result()
            
            # Procesar resultados de pacientes
            pacientes_stats = None
            for row in results_pacientes:
                pacientes_stats = {
                    'total_pacientes_registrados': row.total_pacientes_registrados,
                    'formulas_medicas_validas': row.formulas_medicas_validas,
                    'completaron_proceso_final': row.completaron_proceso_final
                }
                break
            
            # Procesar resultados de historial
            historial_stats = None
            for row in results_historial:
                historial_stats = {
                    'total_personas_han_escrito': row.total_personas_han_escrito
                }
                break
            
            if not pacientes_stats or not historial_stats:
                # Si no hay datos, llenar con ceros
                if not pacientes_stats:
                    pacientes_stats = {
                        'total_pacientes_registrados': 0,
                        'formulas_medicas_validas': 0,
                        'completaron_proceso_final': 0
                    }
                if not historial_stats:
                    historial_stats = {
                        'total_personas_han_escrito': 0
                    }
            
            return {
                **pacientes_stats,
                **historial_stats
            }
            
        except GoogleAPIError as e:
            logger.error(f"❌ Error de BigQuery: {e}")
            raise EstadisticaError(f"Error consultando BigQuery: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado consultando BigQuery: {e}")
            raise EstadisticaError(f"Error inesperado: {e}")

    def generar_reporte_completo(self) -> None:
        """Genera reporte con las 4 métricas principales."""
        try:
            print("📊 ESTADÍSTICAS BÁSICAS - BOT 'NO ME ENTREGARON'")
            print(f"🕐 {datetime.now(COLOMBIA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Obtener estadísticas
            conversaciones_activas = self.obtener_conversaciones_activas_firestore()
            bigquery_stats = self.obtener_estadisticas_bigquery()
            
            # Calcular total de personas (base para porcentajes)
            total_personas = bigquery_stats['total_personas_han_escrito']
            
            # Calcular porcentajes
            if total_personas > 0:
                porcentaje_formulas = (bigquery_stats['formulas_medicas_validas'] / total_personas * 100)
                porcentaje_completaron = (bigquery_stats['completaron_proceso_final'] / total_personas * 100)
                porcentaje_activas = (conversaciones_activas / total_personas * 100)
            else:
                porcentaje_formulas = 0
                porcentaje_completaron = 0
                porcentaje_activas = 0
            
            # Mostrar métricas
            print(f"1️⃣ FÓRMULAS MÉDICAS VÁLIDAS:")
            print(f"   📊 {bigquery_stats['formulas_medicas_validas']:,} personas ({porcentaje_formulas:.1f}%)")
            
            print(f"\n2️⃣ COMPLETARON INFORMACIÓN HASTA EL FINAL:")
            print(f"   📊 {bigquery_stats['completaron_proceso_final']:,} personas ({porcentaje_completaron:.1f}%)")
            
            print(f"\n3️⃣ CONVERSACIONES ACTIVAS AHORA:")
            print(f"   📊 {conversaciones_activas:,} sesiones ({porcentaje_activas:.1f}%)")
            
            print(f"\n4️⃣ TOTAL PERSONAS QUE HAN ESCRITO:")
            print(f"   📊 {total_personas:,} personas (100.0%)")
            
            print(f"\n" + "="*50)
            print(f"📈 BASE DE CÁLCULO: {total_personas:,} personas totales")
            
        except EstadisticaError:
            raise
        except Exception as e:
            logger.error(f"❌ Error generando reporte: {e}")
            raise EstadisticaError(f"Error generando reporte: {e}")

    # =================== FUNCIONES DE DIAGNÓSTICO ===================
    
    def diagnostico_bigquery(self) -> None:
        """🩺 Diagnóstica la estructura de BigQuery para entender los datos."""
        print("\n🔍 DIAGNÓSTICO DE BIGQUERY")
        print("=" * 60)
        
        try:
            # 1. Verificar qué tablas existen
            print(f"\n📋 TABLAS EN EL DATASET '{BIGQUERY_DATASET_ID}':")
            dataset_ref = self.bigquery_client.dataset(BIGQUERY_DATASET_ID)
            tables = list(self.bigquery_client.list_tables(dataset_ref))
            
            for table in tables:
                # Contar registros en cada tabla
                try:
                    count_query = f"SELECT COUNT(*) as total FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table.table_id}`"
                    result = self.bigquery_client.query(count_query).result()
                    for row in result:
                        print(f"   - {table.table_id}: {row.total:,} registros")
                except Exception as e:
                    print(f"   - {table.table_id}: Error contando - {e}")
            
            # 2. Examinar tabla pacientes específicamente
            print(f"\n📊 ANÁLISIS DE TABLA 'pacientes':")
            table_ref = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_PATIENTS}"
            
            try:
                query_analisis = f"""
                SELECT 
                    COUNT(*) as total_registros,
                    COUNT(CASE WHEN prescripciones IS NOT NULL THEN 1 END) as con_campo_prescripciones,
                    COUNT(CASE WHEN ARRAY_LENGTH(prescripciones) > 0 THEN 1 END) as con_prescripciones_datos,
                    COUNT(CASE WHEN reclamaciones IS NOT NULL THEN 1 END) as con_campo_reclamaciones,
                    COUNT(CASE WHEN ARRAY_LENGTH(reclamaciones) > 0 THEN 1 END) as con_reclamaciones_datos
                FROM `{table_ref}`
                """
                
                result = self.bigquery_client.query(query_analisis).result()
                for row in result:
                    print(f"   - Total registros: {row.total_registros}")
                    print(f"   - Con campo prescripciones: {row.con_campo_prescripciones}")
                    print(f"   - Con datos prescripciones: {row.con_prescripciones_datos}")
                    print(f"   - Con campo reclamaciones: {row.con_campo_reclamaciones}")
                    print(f"   - Con datos reclamaciones: {row.con_reclamaciones_datos}")
                    
            except Exception as e:
                print(f"   ❌ Error analizando pacientes: {e}")
            
            # 3. Examinar tabla historial
            print(f"\n📊 ANÁLISIS DE TABLA 'historial_conversacion':")
            table_historial_ref = f"{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_HISTORY}"
            
            try:
                query_historial = f"""
                SELECT 
                    COUNT(*) as total_conversaciones,
                    COUNT(DISTINCT usuario_id) as usuarios_unicos,
                    MIN(fecha_inicio) as primera_conversacion,
                    MAX(fecha_inicio) as ultima_conversacion
                FROM `{table_historial_ref}`
                """
                
                result = self.bigquery_client.query(query_historial).result()
                for row in result:
                    print(f"   - Total conversaciones: {row.total_conversaciones}")
                    if hasattr(row, 'usuarios_unicos') and row.usuarios_unicos:
                        print(f"   - Usuarios únicos: {row.usuarios_unicos}")
                    if hasattr(row, 'primera_conversacion') and row.primera_conversacion:
                        print(f"   - Primera conversación: {row.primera_conversacion}")
                    if hasattr(row, 'ultima_conversacion') and row.ultima_conversacion:
                        print(f"   - Última conversación: {row.ultima_conversacion}")
                        
            except Exception as e:
                print(f"   ❌ Error analizando historial: {e}")
                
            # 4. Buscar otras tablas con datos
            print(f"\n🔍 OTRAS TABLAS CON DATOS:")
            for table in tables:
                if table.table_id not in ['pacientes', 'historial_conversacion']:
                    try:
                        count_query = f"SELECT COUNT(*) as total FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table.table_id}`"
                        result = self.bigquery_client.query(count_query).result()
                        for row in result:
                            if row.total > 0:
                                print(f"   ✅ {table.table_id}: {row.total:,} registros")
                                
                                # Mostrar muestra si tiene pocos registros
                                if row.total <= 10:
                                    sample_query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table.table_id}` LIMIT 3"
                                    sample_results = self.bigquery_client.query(sample_query).result()
                                    for i, sample_row in enumerate(sample_results, 1):
                                        print(f"      Muestra {i}: {dict(sample_row)}")
                    except Exception as e:
                        pass
                            
        except Exception as e:
            logger.error(f"❌ Error en diagnóstico BigQuery: {e}")

    def diagnostico_firestore(self) -> None:
        """🩺 Diagnóstica las sesiones en Firestore."""
        print("\n🔥 DIAGNÓSTICO DE FIRESTORE")
        print("=" * 60)
        
        try:
            sessions_ref = self.firestore_client.collection(FIRESTORE_COLLECTION_SESSIONS)
            sessions = sessions_ref.limit(5).stream()
            
            print(f"\n📱 MUESTRA DE SESIONES ACTIVAS:")
            
            for i, session_doc in enumerate(sessions, 1):
                session_data = session_doc.to_dict()
                print(f"\n   📋 Sesión {i} (ID: {session_doc.id}):")
                print(f"     - channel: {session_data.get('channel', 'N/A')}")
                print(f"     - user_identifier: {session_data.get('user_identifier', 'N/A')}")
                print(f"     - created_at: {session_data.get('created_at', 'N/A')}")
                
                # Examinar conversación
                conversation = session_data.get('conversation', [])
                print(f"     - Total mensajes: {len(conversation)}")
                
                # Buscar eventos importantes
                eventos_importantes = []
                for event in conversation:
                    if isinstance(event, dict):
                        event_str = str(event).lower()
                        if any(keyword in event_str for keyword in 
                               ['prescription', 'formula', 'patient_key', 'bigquery', 'processed']):
                            eventos_importantes.append(event)
                
                if eventos_importantes:
                    print(f"     - ✅ Eventos importantes: {len(eventos_importantes)}")
                    for evento in eventos_importantes[:2]:
                        print(f"       • {str(evento)[:120]}...")
                else:
                    print(f"     - ❌ No hay eventos de procesamiento")
                        
        except Exception as e:
            logger.error(f"❌ Error en diagnóstico Firestore: {e}")

    def ejecutar_diagnostico_completo(self) -> None:
        """🩺 Ejecuta diagnóstico completo."""
        print("\n" + "="*80)
        print("🩺 DIAGNÓSTICO COMPLETO DEL SISTEMA")
        print("="*80)
        
        self.diagnostico_bigquery()
        self.diagnostico_firestore()
        
        print(f"\n🎯 RECOMENDACIONES:")
        print("1. Verificar si el procesador PIP está funcionando")
        print("2. Revisar logs de errores en el bot")
        print("3. Confirmar que los datos se guardan en BigQuery correctamente")
        print("4. Verificar permisos y configuración de BigQuery")


def main():
    """Función principal para ejecutar las estadísticas básicas."""
    try:
        # Verificar configuración
        if not PROJECT_ID:
            print("❌ ERROR: PROJECT_ID no configurado.")
            sys.exit(1)
        
        # Crear instancia
        estadistica = EstadisticaBasica()
        
        # Verificar si se solicita diagnóstico
        if '--debug' in sys.argv or '--diagnostico' in sys.argv:
            estadistica.ejecutar_diagnostico_completo()
        else:
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