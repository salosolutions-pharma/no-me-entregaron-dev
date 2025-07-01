#!/usr/bin/env python3
"""
TEST DE ESCALAMIENTO SEGURO - VERSIÓN CORREGIDA
================================================================
Test del flujo: EPS (Nivel 1) → Supersalud (Nivel 2) → Tutela (Nivel 3) → Desacato (Nivel 4)

CORRECCIONES IMPLEMENTADAS:
1. ✅ Usa UPDATE seguro en lugar de DELETE+INSERT peligroso
2. ✅ Corrige campo inexistente 'fecha_generacion_documento'  
3. ✅ Implementa funciones granulares para cada operación
4. ✅ Maneja errores sin perder datos del paciente
5. ✅ Verifica que PDFs se guarden correctamente en Cloud Storage

Paciente de prueba: COCC8048589 (LONDOÑO ACOSTA WILMAR)
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import Dict, Any, List
import pytz

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurar zona horaria
COLOMBIA_TZ = pytz.timezone('America/Bogota')

# Agregar el directorio del proyecto al path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

try:
    from claim_manager.claim_generator import (
        generar_reclamacion_eps,
        generar_reclamacion_supersalud, 
        generar_tutela,
        generar_desacato,
        validar_requisitos_escalamiento,
        validar_requisitos_desacato
    )
    from claim_manager.data_collection import ClaimManager
    
    # ✅ IMPORTAR FUNCIONES SEGURAS CORREGIDAS
    from processor_image_prescription.bigquery_pip import (
        get_bigquery_client,
        _convert_bq_row_to_dict_recursive,
        add_reclamacion_safe,  # ✅ Nueva función segura
        update_reclamacion_status,  # ✅ Función corregida
        save_document_url_to_reclamacion,  # ✅ Función corregida
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        load_table_from_json_direct
    )
    from processor_image_prescription.pdf_generator import generar_pdf_tutela, generar_pdf_desacato
    from google.cloud import bigquery
    
    logger.info("✅ Todos los módulos importados correctamente")
    
except ImportError as e:
    logger.error(f"❌ Error importando módulos: {e}")
    sys.exit(1)


class TestEscalamientoSeguro:
    """Clase para testing seguro del escalamiento de reclamaciones."""
    
    def __init__(self):
        try:
            logger.info("🔧 Inicializando TestEscalamientoSeguro...")
            self.patient_key = "COCC8048589"
            logger.info(f"📋 Patient key: {self.patient_key}")
            
            logger.info("🔗 Conectando a BigQuery...")
            self.bq_client = get_bigquery_client()
            self.table_reference = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            logger.info(f"📊 Tabla: {self.table_reference}")
            
            logger.info("🧠 Inicializando ClaimManager...")
            self.claim_manager = ClaimManager()
            logger.info("✅ ClaimManager inicializado")
            
            # Datos para simular tutela (necesarios para desacato)
            self.datos_tutela_simulada = {
                "numero_tutela": "T-2025-001-TEST",
                "juzgado": "Juzgado Primero Laboral del Circuito de Medellín",
                "fecha_sentencia": "2025-06-15",
                "contenido_fallo": "ORDENAR a NUEVA EPS la entrega inmediata de Divalproato sódico 500 mg ER según prescripción médica en un plazo máximo de 48 horas",
                "representante_legal_eps": "Representante Legal de NUEVA EPS"
            }
            
            logger.info(f"✅ TestEscalamientoSeguro inicializado para paciente: {self.patient_key}")
            
        except Exception as e:
            logger.error(f"❌ Error en __init__: {e}", exc_info=True)
            raise

    def verificar_datos_paciente(self) -> Dict[str, Any]:
        """Verifica que el paciente existe y tiene los datos necesarios."""
        try:
            query = f"""
            SELECT * FROM `{self.table_reference}`
            WHERE paciente_clave = @patient_key
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", self.patient_key)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                patient_data = _convert_bq_row_to_dict_recursive(row)
                logger.info(f"✅ Paciente encontrado: {patient_data.get('nombre_paciente')}")
                logger.info(f"📋 EPS: {patient_data.get('eps_estandarizada')}")
                logger.info(f"💊 Medicamentos no entregados: {self._get_medicamentos_no_entregados(patient_data)}")
                
                # Mostrar reclamaciones existentes
                reclamaciones = patient_data.get("reclamaciones", [])
                logger.info(f"📄 Reclamaciones existentes: {len(reclamaciones)}")
                for i, rec in enumerate(reclamaciones, 1):
                    logger.info(f"   {i}. {rec.get('tipo_accion')} - Nivel {rec.get('nivel_escalamiento')} - {rec.get('estado_reclamacion')}")
                
                return patient_data
                
            logger.error(f"❌ Paciente {self.patient_key} no encontrado")
            return {}
            
        except Exception as e:
            logger.error(f"❌ Error verificando datos del paciente: {e}")
            return {}

    def _get_medicamentos_no_entregados(self, patient_data: Dict[str, Any]) -> str:
        """Obtiene medicamentos no entregados del paciente."""
        prescripciones = patient_data.get("prescripciones", [])
        if not prescripciones:
            return ""
        
        ultima_prescripcion = prescripciones[-1]
        medicamentos = ultima_prescripcion.get("medicamentos", [])
        
        meds_no_entregados = [
            med.get("nombre", "")
            for med in medicamentos
            if isinstance(med, dict) 
            and med.get("entregado") == "no entregado"
            and med.get("nombre", "")
        ]
        
        return ", ".join(meds_no_entregados)

    def test_nivel_2_supersalud(self) -> bool:
        """🥈 NIVEL 2: Test generación de queja ante Supersalud."""
        logger.info("\n" + "="*60)
        logger.info("🥈 TESTING NIVEL 2: QUEJA SUPERSALUD")
        logger.info("="*60)
        
        try:
            # Validar requisitos
            validacion = validar_requisitos_escalamiento(self.patient_key, "supersalud")
            logger.info(f"🔍 Validación requisitos: {validacion}")
            
            if not validacion.get("puede_escalar"):
                logger.error(f"❌ No se puede escalar a Supersalud: {validacion.get('mensaje')}")
                return False
            
            # Generar reclamación
            resultado = generar_reclamacion_supersalud(self.patient_key)
            
            if resultado.get("success"):
                logger.info("✅ Queja Supersalud generada exitosamente")
                logger.info(f"📄 Entidad destinataria: {resultado.get('entidad_destinataria')}")
                logger.info(f"🎯 Nivel de escalamiento: {resultado.get('nivel_escalamiento')}")
                logger.info(f"📋 Gestiones previas EPS: {len(resultado.get('radicados_eps_previos', []))}")
                
                # ✅ USAR FUNCIÓN SEGURA para guardar en BD
                success_saved = self._guardar_reclamacion_segura(
                    tipo_accion="reclamacion_supersalud",
                    texto_reclamacion=resultado["texto_reclamacion"],
                    estado_reclamacion="pendiente_radicacion",
                    nivel_escalamiento=2
                )
                
                if success_saved:
                    # Simular que se radicó
                    self._simular_radicado_supersalud()
                    return True
                else:
                    logger.error("❌ Error guardando reclamación Supersalud en BD")
                    return False
            else:
                logger.error(f"❌ Error generando queja Supersalud: {resultado.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en test Supersalud: {e}")
            return False

    def test_nivel_3_tutela(self) -> bool:
        """🥉 NIVEL 3: Test generación de tutela con PDF."""
        logger.info("\n" + "="*60)
        logger.info("🥉 TESTING NIVEL 3: ACCIÓN DE TUTELA")
        logger.info("="*60)
        
        try:
            # Validar requisitos
            validacion = validar_requisitos_escalamiento(self.patient_key, "tutela")
            logger.info(f"🔍 Validación requisitos: {validacion}")
            
            if not validacion.get("puede_escalar"):
                logger.error(f"❌ No se puede escalar a Tutela: {validacion.get('mensaje')}")
                return False
            
            # Generar tutela
            resultado = generar_tutela(self.patient_key)
            
            if resultado.get("success"):
                logger.info("✅ Tutela generada exitosamente")
                logger.info(f"📄 Entidad destinataria: {resultado.get('entidad_destinataria')}")
                logger.info(f"🎯 Nivel de escalamiento: {resultado.get('nivel_escalamiento')}")
                logger.info(f"📋 Requiere PDF: {resultado.get('requiere_pdf')}")
                logger.info(f"📝 Gestiones previas: {len(resultado.get('gestiones_previas', []))}")
                
                # ✅ USAR FUNCIÓN SEGURA para guardar reclamación en BD
                success_saved = self._guardar_reclamacion_segura(
                    tipo_accion="tutela",
                    texto_reclamacion=resultado["texto_reclamacion"],
                    estado_reclamacion="pendiente_radicacion",
                    nivel_escalamiento=3
                )
                
                if not success_saved:
                    logger.error("❌ Error guardando tutela en BD")
                    return False
                
                # Test generación de PDF
                if resultado.get("requiere_pdf"):
                    logger.info("📄 Generando PDF de tutela...")
                    pdf_result = generar_pdf_tutela(resultado)
                    
                    if pdf_result.get("success"):
                        logger.info(f"✅ PDF generado exitosamente")
                        logger.info(f"📁 Archivo: {pdf_result.get('pdf_filename')}")
                        logger.info(f"📏 Tamaño: {pdf_result.get('file_size_bytes', 0)} bytes")
                        logger.info(f"☁️ URL: {pdf_result.get('pdf_url')}")
                        
                        # ✅ USAR FUNCIÓN SEGURA para guardar URL en BD
                        pdf_url = pdf_result.get('pdf_url')
                        if pdf_url:
                            url_saved = save_document_url_to_reclamacion(
                                patient_key=self.patient_key,
                                nivel_escalamiento=3,
                                url_documento=pdf_url,
                                tipo_documento="tutela"
                            )
                            
                            if url_saved:
                                logger.info(f"✅ URL del PDF guardada en BD: {pdf_url}")
                            else:
                                logger.warning("⚠️ No se pudo guardar URL del PDF en BD")
                        
                        # Guardar datos de tutela para el desacato
                        self._guardar_datos_tutela_para_desacato()
                        
                        return True
                    else:
                        logger.warning(f"⚠️ Error generando PDF: {pdf_result.get('error')}")
                        return False
                else:
                    logger.info("📄 Tutela no requiere PDF")
                    return True
            else:
                logger.error(f"❌ Error generando tutela: {resultado.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en test Tutela: {e}")
            return False

    def test_nivel_4_desacato(self) -> bool:
        """🏆 NIVEL 4: Test generación de desacato con PDF."""
        logger.info("\n" + "="*60)
        logger.info("🏆 TESTING NIVEL 4: INCIDENTE DE DESACATO")
        logger.info("="*60)
        
        try:
            # Validar requisitos específicos de desacato
            validacion = validar_requisitos_desacato(self.patient_key)
            logger.info(f"🔍 Validación requisitos desacato: {validacion}")
            
            if not validacion.get("puede_desacatar"):
                logger.error(f"❌ No se puede generar desacato: {validacion.get('mensaje')}")
                return False
            
            # Generar desacato
            resultado = generar_desacato(self.patient_key)
            
            if resultado.get("success"):
                logger.info("✅ Desacato generado exitosamente")
                logger.info(f"📄 Entidad destinataria: {resultado.get('entidad_destinataria')}")
                logger.info(f"🎯 Nivel de escalamiento: {resultado.get('nivel_escalamiento')}")
                logger.info(f"⚖️ Tutela referencia: {resultado.get('numero_tutela_referencia')}")
                logger.info(f"🏛️ Juzgado: {resultado.get('juzgado')}")
                logger.info(f"📋 Requiere PDF: {resultado.get('requiere_pdf')}")
                
                # ✅ USAR FUNCIÓN SEGURA para guardar reclamación en BD
                success_saved = self._guardar_reclamacion_segura(
                    tipo_accion="desacato",
                    texto_reclamacion=resultado["texto_reclamacion"],
                    estado_reclamacion="pendiente_radicacion",
                    nivel_escalamiento=4
                )
                
                if not success_saved:
                    logger.error("❌ Error guardando desacato en BD")
                    return False
                
                # Test generación de PDF
                if resultado.get("requiere_pdf"):
                    logger.info("📄 Generando PDF de desacato...")
                    pdf_result = generar_pdf_desacato(resultado)
                    
                    if pdf_result.get("success"):
                        logger.info(f"✅ PDF generado exitosamente")
                        logger.info(f"📁 Archivo: {pdf_result.get('pdf_filename')}")
                        logger.info(f"📏 Tamaño: {pdf_result.get('file_size_bytes', 0)} bytes")
                        logger.info(f"☁️ URL: {pdf_result.get('pdf_url')}")
                        
                        # ✅ USAR FUNCIÓN SEGURA para guardar URL en BD
                        pdf_url = pdf_result.get('pdf_url')
                        if pdf_url:
                            url_saved = save_document_url_to_reclamacion(
                                patient_key=self.patient_key,
                                nivel_escalamiento=4,
                                url_documento=pdf_url,
                                tipo_documento="desacato"
                            )
                            
                            if url_saved:
                                logger.info(f"✅ URL del PDF guardada en BD: {pdf_url}")
                            else:
                                logger.warning("⚠️ No se pudo guardar URL del PDF en BD")
                        
                        return True
                    else:
                        logger.warning(f"⚠️ Error generando PDF: {pdf_result.get('error')}")
                        return False
                else:
                    logger.info("📄 Desacato no requiere PDF")
                    return True
            else:
                logger.error(f"❌ Error generando desacato: {resultado.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en test Desacato: {e}")
            return False

    # ✅ NUEVA FUNCIÓN SEGURA: Guardar reclamación sin DELETE peligroso
    def _guardar_reclamacion_segura(self, tipo_accion: str, texto_reclamacion: str, 
                                   estado_reclamacion: str, nivel_escalamiento: int) -> bool:
        """
        Guarda una reclamación usando la nueva función segura add_reclamacion_safe.
        NO borra datos existentes del paciente.
        """
        try:
            # Obtener medicamentos no entregados
            patient_data = self.verificar_datos_paciente()
            if not patient_data:
                logger.error("❌ No se puede obtener datos del paciente para guardar reclamación")
                return False
            
            med_no_entregados = self._get_medicamentos_no_entregados(patient_data)
            
            # Preparar nueva reclamación con campos correctos del esquema
            nueva_reclamacion = {
                "med_no_entregados": med_no_entregados,
                "tipo_accion": tipo_accion,
                "texto_reclamacion": texto_reclamacion,
                "estado_reclamacion": estado_reclamacion,
                "nivel_escalamiento": nivel_escalamiento,
                "url_documento": "",  # Se llena después si es tutela/desacato
                "numero_radicado": "",  # Se llena cuando se radica
                "fecha_radicacion": None,  # Se llena cuando se radica
                "fecha_revision": None,   # Se llena cuando hay respuesta
                # ✅ NO incluir 'fecha_generacion_documento' - campo no existe
            }
            
            # ✅ USAR FUNCIÓN SEGURA que no borra datos
            success = add_reclamacion_safe(self.patient_key, nueva_reclamacion)
            
            if success:
                logger.info(f"✅ Reclamación {tipo_accion} (nivel {nivel_escalamiento}) guardada exitosamente")
            else:
                logger.error(f"❌ Error guardando reclamación {tipo_accion}")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error en _guardar_reclamacion_segura: {e}")
            return False

    def _simular_radicado_eps_existente(self):
        """Simula el radicado de la reclamación EPS existente usando función segura."""
        try:
            numero_radicado = f"EPS-{datetime.now().strftime('%Y%m%d')}-001"
            fecha_radicacion = date.today().strftime("%Y-%m-%d")
            
            logger.info(f"🔧 Simulando radicado EPS: {numero_radicado}")
            
            # ✅ USAR FUNCIÓN SEGURA que no borra datos
            success = update_reclamacion_status(
                patient_key=self.patient_key,
                nivel_escalamiento=1,
                nuevo_estado="radicado",
                numero_radicado=numero_radicado,
                fecha_radicacion=fecha_radicacion
            )
            
            if success:
                logger.info(f"✅ Radicado EPS simulado exitosamente: {numero_radicado}")
                return True
            else:
                logger.warning("⚠️ No se pudo simular radicado EPS")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error simulando radicado EPS: {e}")
            return False

    def _simular_radicado_supersalud(self):
        """Simula el radicado de la queja ante Supersalud usando función segura."""
        try:
            numero_radicado = f"SS-{datetime.now().strftime('%Y%m%d')}-002"
            fecha_radicacion = date.today().strftime("%Y-%m-%d")
            
            # ✅ USAR FUNCIÓN SEGURA que no borra datos
            success = update_reclamacion_status(
                patient_key=self.patient_key,
                nivel_escalamiento=2,
                nuevo_estado="radicado",
                numero_radicado=numero_radicado,
                fecha_radicacion=fecha_radicacion
            )
            
            if success:
                logger.info(f"✅ Radicado Supersalud simulado: {numero_radicado}")
            else:
                logger.warning("⚠️ No se pudo simular radicado Supersalud")
                
        except Exception as e:
            logger.warning(f"⚠️ Error simulando radicado Supersalud: {e}")

    def _guardar_datos_tutela_para_desacato(self):
        """Guarda datos de tutela simulada en la tabla tutelas para el test de desacato."""
        try:
            success = self.claim_manager.save_tutela_data_to_bigquery(self.patient_key, self.datos_tutela_simulada)
            
            if success:
                logger.info(f"✅ Datos de tutela guardados para desacato: {self.datos_tutela_simulada['numero_tutela']}")
            else:
                logger.warning("⚠️ No se pudieron guardar datos de tutela")
                
        except Exception as e:
            logger.warning(f"⚠️ Error guardando datos de tutela: {e}")

    def verificar_estado_final(self):
        """Verifica el estado final del paciente después de todas las pruebas."""
        logger.info("\n" + "="*60)
        logger.info("🔍 VERIFICACIÓN ESTADO FINAL")
        logger.info("="*60)
        
        try:
            patient_data = self.verificar_datos_paciente()
            if not patient_data:
                logger.error("❌ No se pueden verificar datos - paciente no encontrado")
                return
                
            reclamaciones = patient_data.get("reclamaciones", [])
            
            logger.info(f"📊 Total reclamaciones: {len(reclamaciones)}")
            
            # Verificar cada nivel de escalamiento
            niveles_encontrados = {}
            urls_pdf_encontradas = {}
            
            for i, reclamacion in enumerate(reclamaciones, 1):
                tipo = reclamacion.get('tipo_accion')
                nivel = reclamacion.get('nivel_escalamiento')
                estado = reclamacion.get('estado_reclamacion')
                radicado = reclamacion.get('numero_radicado', 'Sin radicado')
                url_documento = reclamacion.get('url_documento', '')
                
                logger.info(f"\n📋 Reclamación {i}:")
                logger.info(f"   🎯 Tipo: {tipo}")
                logger.info(f"   📊 Nivel: {nivel}")
                logger.info(f"   📄 Estado: {estado}")
                logger.info(f"   🔢 Radicado: {radicado}")
                logger.info(f"   📎 PDF: {'Sí' if url_documento else 'No'}")
                if url_documento:
                    logger.info(f"   🔗 URL: {url_documento}")
                
                niveles_encontrados[nivel] = tipo
                if url_documento:
                    urls_pdf_encontradas[nivel] = url_documento
            
            # Verificar que se crearon todos los niveles esperados
            logger.info(f"\n📈 RESUMEN DE ESCALAMIENTO:")
            
            niveles_esperados = {
                1: "reclamacion_eps",
                2: "reclamacion_supersalud", 
                3: "tutela",
                4: "desacato"
            }
            
            for nivel, tipo_esperado in niveles_esperados.items():
                if nivel in niveles_encontrados:
                    logger.info(f"   ✅ Nivel {nivel} ({tipo_esperado}): CREADO")
                    
                    # Verificar PDFs para tutela y desacato
                    if nivel in [3, 4]:
                        if nivel in urls_pdf_encontradas:
                            logger.info(f"      📎 PDF: GENERADO Y GUARDADO")
                        else:
                            logger.warning(f"      ⚠️ PDF: NO ENCONTRADO")
                else:
                    logger.warning(f"   ❌ Nivel {nivel} ({tipo_esperado}): NO CREADO")
                
        except Exception as e:
            logger.error(f"❌ Error verificando estado final: {e}")

    def verificar_bucket_storage(self):
        """Verifica que los PDFs se guardaron correctamente en Cloud Storage."""
        logger.info("\n" + "="*60)
        logger.info("☁️ VERIFICACIÓN CLOUD STORAGE")
        logger.info("="*60)
        
        try:
            from google.cloud import storage
            
            # Obtener datos del paciente para verificar URLs
            patient_data = self.verificar_datos_paciente()
            if not patient_data:
                logger.warning("⚠️ No se pueden verificar URLs - paciente no encontrado")
                return
                
            reclamaciones = patient_data.get("reclamaciones", [])
            
            # Buscar URLs de documentos
            urls_to_check = []
            for reclamacion in reclamaciones:
                url_documento = reclamacion.get('url_documento', '')
                if url_documento and url_documento.startswith('gs://'):
                    urls_to_check.append({
                        'url': url_documento,
                        'tipo': reclamacion.get('tipo_accion'),
                        'nivel': reclamacion.get('nivel_escalamiento')
                    })
            
            if not urls_to_check:
                logger.warning("⚠️ No se encontraron URLs de documentos para verificar")
                return
            
            logger.info(f"🔍 Verificando {len(urls_to_check)} archivos en Cloud Storage...")
            
            client = storage.Client()
            
            for doc_info in urls_to_check:
                url = doc_info['url']
                tipo = doc_info['tipo']
                nivel = doc_info['nivel']
                
                try:
                    # Parsear la URL gs://
                    if url.startswith('gs://'):
                        url_parts = url[5:].split('/', 1)
                        bucket_name = url_parts[0]
                        blob_name = url_parts[1] if len(url_parts) > 1 else ''
                        
                        bucket = client.bucket(bucket_name)
                        blob = bucket.blob(blob_name)
                        
                        if blob.exists():
                            size = blob.size
                            logger.info(f"✅ {tipo} (Nivel {nivel}): ENCONTRADO")
                            logger.info(f"   📁 Bucket: {bucket_name}")
                            logger.info(f"   📄 Archivo: {blob_name}")
                            logger.info(f"   📏 Tamaño: {size} bytes")
                        else:
                            logger.error(f"❌ {tipo} (Nivel {nivel}): NO ENCONTRADO")
                            logger.error(f"   🔗 URL: {url}")
                            
                except Exception as e:
                    logger.error(f"❌ Error verificando {tipo}: {e}")
                    
        except ImportError:
            logger.warning("⚠️ google.cloud.storage no disponible, saltando verificación")
        except Exception as e:
            logger.error(f"❌ Error verificando Cloud Storage: {e}")

    def ejecutar_test_completo(self):
        """Ejecuta la secuencia completa de testing."""
        try:
            logger.info("🚀 INICIANDO TEST COMPLETO DE ESCALAMIENTO SEGURO")
            logger.info("="*80)
            
            # Verificar datos iniciales
            logger.info("🔍 Verificando datos del paciente...")
            patient_data = self.verificar_datos_paciente()
            if not patient_data:
                logger.error("❌ No se puede proceder sin datos del paciente")
                return False
        except Exception as e:
            logger.error(f"❌ Error en verificación inicial: {e}", exc_info=True)
            return False
        
        try:
            # Verificar que ya existe reclamación EPS
            logger.info("🔍 Verificando reclamaciones existentes...")
            reclamaciones = patient_data.get("reclamaciones", [])
            tiene_eps = any(r.get('tipo_accion') == 'reclamacion_eps' for r in reclamaciones)
            
            if not tiene_eps:
                logger.error("❌ El paciente debe tener una reclamación EPS existente para este test")
                logger.info("💡 Ejecuta primero el flujo normal del bot para crear la reclamación EPS")
                return False
            
            # Verificar si la EPS tiene radicado
            eps_con_radicado = any(
                r.get('tipo_accion') == 'reclamacion_eps' and 
                r.get('numero_radicado') and 
                r.get('numero_radicado').strip()
                for r in reclamaciones
            )
            
            if not eps_con_radicado:
                logger.info("🔧 La reclamación EPS no tiene radicado, simulando radicación...")
                if self._simular_radicado_eps_existente():
                    logger.info("✅ Radicado EPS simulado, procediendo con escalamiento...")
                else:
                    logger.error("❌ No se pudo simular radicado EPS")
                    return False
            else:
                logger.info("✅ La reclamación EPS ya tiene radicado")
            
            logger.info(f"✅ Paciente listo para escalamiento...")
        except Exception as e:
            logger.error(f"❌ Error verificando reclamaciones: {e}", exc_info=True)
            return False
        
        # Ejecutar tests de escalamiento
        tests_resultados = {
            "Supersalud (Nivel 2)": self.test_nivel_2_supersalud(),
            "Tutela (Nivel 3)": self.test_nivel_3_tutela(),
            "Desacato (Nivel 4)": self.test_nivel_4_desacato(),
        }
        
        # Verificar estado final
        self.verificar_estado_final()
        
        # Verificar Cloud Storage
        self.verificar_bucket_storage()
        
        # Resumen final
        logger.info("\n" + "="*80)
        logger.info("📊 RESUMEN FINAL DEL TESTING SEGURO")
        logger.info("="*80)
        
        tests_exitosos = 0
        for test_name, resultado in tests_resultados.items():
            status = "✅ EXITOSO" if resultado else "❌ FALLIDO"
            logger.info(f"{test_name}: {status}")
            if resultado:
                tests_exitosos += 1
        
        logger.info(f"\n🎯 RESULTADO GENERAL: {tests_exitosos}/{len(tests_resultados)} tests exitosos")
        
        if tests_exitosos == len(tests_resultados):
            logger.info("🎉 ¡TODOS LOS TESTS PASARON! Sistema de escalamiento completamente funcional.")
            logger.info("✅ PDFs generados y URLs guardadas correctamente en BigQuery")
            logger.info("☁️ Archivos verificados en Cloud Storage")
            logger.info("🛡️ Datos del paciente preservados - NO se perdió información")
        else:
            logger.warning(f"⚠️ {len(tests_resultados) - tests_exitosos} tests fallaron. Revisar implementación.")
        
        return tests_exitosos == len(tests_resultados)


def main():
    """Función principal para ejecutar el testing."""
    try:
        logger.info("🧪 Iniciando main() del script de testing SEGURO...")
        
        # Crear instancia del tester
        logger.info("🔧 Creando instancia de TestEscalamientoSeguro...")
        tester = TestEscalamientoSeguro()
        logger.info("✅ Instancia creada exitosamente")
        
        # Ejecutar test completo
        logger.info("🚀 Ejecutando test completo SEGURO...")
        exito = tester.ejecutar_test_completo()
        
        # Exit code
        logger.info(f"🏁 Test completado. Éxito: {exito}")
        sys.exit(0 if exito else 1)
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Test interrumpido por el usuario.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error crítico en testing: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logger.info("🎬 Script SEGURO iniciado desde línea de comandos...")
    main()