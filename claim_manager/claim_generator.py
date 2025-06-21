import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from manual_instrucciones.prompt_manager import prompt_manager
from llm_core import LLMCore
from processor_image_prescription.bigquery_pip import (
    get_bigquery_client,
    _convert_bq_row_to_dict_recursive,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID
)
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class ClaimGeneratorError(Exception):
    """Excepción para errores específicos del generador de reclamaciones."""
    pass


class ClaimGenerator:
    """
    Generador completo de reclamaciones para el sistema de salud colombiano.
    
    Maneja la generación de:
    - Reclamaciones ante EPS
    - Quejas ante Superintendencia Nacional de Salud
    - Acciones de tutela por vulneración del derecho a la salud
    """
    
    # Constantes de clase
    PLAZOS_RESPUESTA = {
        "simple": "5 días hábiles",
        "priorizado": "72 horas", 
        "vital": "24 horas"
    }
    
    PLAZOS_SUPERSALUD = {
        "simple": "15 días hábiles",
        "priorizado": "10 días hábiles",
        "vital": "5 días hábiles"
    }
    
    CAMPOS_REQUERIDOS_BASE = [
        "nombre_paciente",
        "tipo_documento", 
        "numero_documento",
        "eps_estandarizada",
        "med_no_entregados",
        #"diagnostico"
    ]
    
    CAMPOS_ADICIONALES_SUPERSALUD = ["ciudad", "direccion", "telefono_contacto", "correo"]
    CAMPOS_ADICIONALES_TUTELA = ["ciudad", "direccion"]

    def __init__(self):
        """Inicializa el generador con conexiones a LLM y BigQuery."""
        try:
            self.llm_core = LLMCore()
            self.bq_client = get_bigquery_client()
            logger.info("ClaimGenerator inicializado correctamente.")
            
        except Exception as e:
            logger.error(f"Error al inicializar ClaimGenerator: {e}")
            raise ClaimGeneratorError(f"Fallo en inicialización: {e}")

    def obtener_datos_paciente(self, patient_key: str) -> Dict[str, Any]:
        """
        Obtiene todos los datos necesarios del paciente desde BigQuery.
        
        Args:
            patient_key: Clave única del paciente (ej: COCC39287966)
            
        Returns:
            Dict con todos los datos formateados para el prompt
        """
        try:
            query = f"""
            SELECT 
                paciente_clave,
                nombre_paciente,
                tipo_documento,
                numero_documento,
                eps_estandarizada,
                farmacia,
                sede_farmacia,
                ciudad,
                direccion,
                telefono_contacto,
                correo,
                prescripciones,
                reclamaciones
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE paciente_clave = @patient_key
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                patient_data = _convert_bq_row_to_dict_recursive(row)
                return self._formatear_datos_paciente(patient_data)
                
            logger.error(f"No se encontró paciente con clave: {patient_key}")
            raise ClaimGeneratorError(f"Paciente {patient_key} no encontrado")
            
        except Exception as e:
            logger.error(f"Error al obtener datos del paciente {patient_key}: {e}")
            raise ClaimGeneratorError(f"Error de base de datos: {e}")

    def _formatear_datos_paciente(self, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """Formatea los datos del paciente para uso en prompts."""
        diagnostico, categoria_riesgo = self._extraer_datos_prescripcion(patient_data)
        med_no_entregados = self._obtener_medicamentos_no_entregados(patient_data)
        
        diagnostico_texto = diagnostico if diagnostico and diagnostico.strip() else "No especificado en la prescripción"

        return {
            "nombre_paciente": patient_data.get("nombre_paciente", ""),
            "tipo_documento": patient_data.get("tipo_documento", ""),
            "numero_documento": patient_data.get("numero_documento", ""),
            "eps_estandarizada": patient_data.get("eps_estandarizada", ""),
            "ciudad": patient_data.get("ciudad", ""),
            "direccion": patient_data.get("direccion", ""),
            "telefono_contacto": self._format_array_field(
                patient_data.get("telefono_contacto", [])
            ),
            "correo": self._format_array_field(patient_data.get("correo", [])),
            "diagnostico": diagnostico_texto,
            "categoria_riesgo": categoria_riesgo,
            "med_no_entregados": med_no_entregados,
            "farmacia": patient_data.get("farmacia", ""),
            "sede_farmacia": patient_data.get("sede_farmacia", ""),
            "plazo_respuesta": self._obtener_plazo_respuesta(categoria_riesgo),
            "plazo_supersalud": self._obtener_plazo_supersalud(categoria_riesgo),
            "fecha_actual": datetime.now().strftime("%d de %B de %Y")
        }

    def _extraer_datos_prescripcion(self, patient_data: Dict[str, Any]) -> tuple:
        """Extrae diagnóstico y categoría de riesgo de la prescripción más reciente."""
        diagnostico = ""
        categoria_riesgo = ""
        
        prescripciones = patient_data.get("prescripciones")
        if prescripciones:
            ultima_prescripcion = prescripciones[-1]
            diagnostico = ultima_prescripcion.get("diagnostico", "")
            categoria_riesgo = ultima_prescripcion.get("categoria_riesgo", "").lower()
        
        return diagnostico, categoria_riesgo

    def _obtener_medicamentos_no_entregados(self, patient_data: Dict[str, Any]) -> str:
        """Obtiene la lista de medicamentos no entregados de la prescripción más reciente."""
        prescripciones = patient_data.get("prescripciones")
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

    def _obtener_plazo_respuesta(self, categoria_riesgo: str) -> str:
        """Obtiene el plazo de respuesta para EPS según la categoría de riesgo."""
        categoria_clean = categoria_riesgo.lower().strip()
        return self.PLAZOS_RESPUESTA.get(categoria_clean, "5 días hábiles")

    def _obtener_plazo_supersalud(self, categoria_riesgo: str) -> str:
        """Obtiene el plazo de respuesta para Supersalud según la categoría de riesgo."""
        categoria_clean = categoria_riesgo.lower().strip()
        return self.PLAZOS_SUPERSALUD.get(categoria_clean, "15 días hábiles")

    def _format_array_field(self, field_value) -> str:
        """Formatea campos de tipo array para mostrar como texto."""
        if isinstance(field_value, list):
            return ", ".join(str(item) for item in field_value if item)
        return str(field_value) if field_value else ""

    def _validar_campos_requeridos(self, datos: Dict[str, Any], 
                                  campos_adicionales: List[str] = None) -> List[str]:
        """Valida que los campos requeridos estén presentes."""
        campos_requeridos = self.CAMPOS_REQUERIDOS_BASE.copy()
        if campos_adicionales:
            campos_requeridos.extend(campos_adicionales)
        
        campos_faltantes = []
        for campo in campos_requeridos:
            valor = datos.get(campo, "")
            if not valor or (isinstance(valor, str) and not valor.strip()):
                campos_faltantes.append(campo)
        
        if campos_faltantes:
            logger.warning(f"Campos faltantes: {campos_faltantes}")
        
        return campos_faltantes

    def validar_datos_eps(self, datos: Dict[str, Any]) -> List[str]:
        """Valida datos mínimos para reclamación EPS - diagnóstico opcional."""
        campos_requeridos_eps = [
            "nombre_paciente",
            "tipo_documento", 
            "numero_documento",
            "eps_estandarizada",
            "med_no_entregados"
            # 'diagnostico' 
        ]
        
        campos_faltantes = []
        for campo in campos_requeridos_eps:
            valor = datos.get(campo, "")
            if not valor or (isinstance(valor, str) and not valor.strip()):
                campos_faltantes.append(campo)
        
        if campos_faltantes:
            logger.warning(f"Campos faltantes para reclamación EPS: {campos_faltantes}")
        
        return campos_faltantes

    def validar_datos_supersalud(self, datos: Dict[str, Any]) -> List[str]:
        """Valida datos mínimos para queja ante Supersalud - diagnóstico SÍ requerido."""
        campos_adicionales_con_diagnostico = self.CAMPOS_ADICIONALES_SUPERSALUD + ["diagnostico"]
        return self._validar_campos_requeridos(datos, campos_adicionales_con_diagnostico)

    def validar_datos_tutela(self, datos: Dict[str, Any]) -> List[str]:
        """Valida datos mínimos para tutela - diagnóstico SÍ requerido."""
        campos_adicionales_con_diagnostico = self.CAMPOS_ADICIONALES_TUTELA + ["diagnostico"]
        return self._validar_campos_requeridos(datos, campos_adicionales_con_diagnostico)

    def _generar_documento_legal(self, patient_key: str, tipo_documento: str,
                               gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
        """Método genérico para generar documentos legales."""
        try:
            logger.info(f"Iniciando generación de {tipo_documento} para paciente: {patient_key}")
            
            # 1. Obtener datos del paciente
            datos_paciente = self.obtener_datos_paciente(patient_key)
            
            # 2. Validar datos según tipo de documento
            if tipo_documento == "tutela":
                campos_faltantes = self.validar_datos_tutela(datos_paciente)
                if not gestiones_previas:
                    gestiones_previas = [
                        "Reclamación ante EPS sin respuesta satisfactoria",
                        "Queja ante Superintendencia Nacional de Salud",
                        "Múltiples solicitudes presenciales y telefónicas",
                        "Agotamiento de medios ordinarios de reclamación"
                    ]
                datos_paciente["gestiones_previas"] = ". ".join(gestiones_previas)
            elif tipo_documento == "reclamacion_supersalud":
                campos_faltantes = self.validar_datos_supersalud(datos_paciente)
            else:  # reclamacion_eps
                campos_faltantes = self.validar_datos_eps(datos_paciente)
            
            if campos_faltantes:
                return {
                    "success": False,
                    "error": f"Faltan campos requeridos: {', '.join(campos_faltantes)}",
                    "campos_faltantes": campos_faltantes,
                    "patient_key": patient_key,
                    "tipo_documento": tipo_documento
                }
            
            # 3. Obtener y formatear prompt
            prompt_template = prompt_manager.get_prompt_by_module_and_function("CLAIM", tipo_documento)
            if not prompt_template:
                logger.error(f"Prompt CLAIM.{tipo_documento} no encontrado")
                return {
                    "success": False,
                    "error": f"Prompt CLAIM.{tipo_documento} no disponible en el sistema",
                    "patient_key": patient_key,
                    "tipo_documento": tipo_documento
                }
            
            try:
                prompt_formateado = prompt_template.format(**datos_paciente)
                logger.debug(f"Prompt formateado correctamente para paciente {patient_key}")
            except KeyError as e:
                logger.error(f"Error al formatear prompt: variable {e} no encontrada")
                return {
                    "success": False,
                    "error": f"Error en template del prompt: falta variable {e}",
                    "patient_key": patient_key,
                    "tipo_documento": tipo_documento
                }
            
            # 4. Generar texto con LLM
            logger.info(f"Enviando prompt a LLM para generar {tipo_documento}...")
            texto_generado = self.llm_core.ask_text(prompt_formateado)
            
            # 5. Preparar respuesta exitosa
            resultado = {
                "success": True,
                "tipo_reclamacion": tipo_documento,
                "texto_reclamacion": texto_generado.strip(),
                "datos_utilizados": datos_paciente,
                "fecha_generacion": datetime.now().isoformat(),
                "patient_key": patient_key,
                "nivel_riesgo": datos_paciente.get("categoria_riesgo", ""),
                "plazo_respuesta": datos_paciente.get("plazo_respuesta", ""),
                "medicamentos_afectados": datos_paciente.get("med_no_entregados", "")
            }
            
            # Agregar campos específicos según tipo
            if tipo_documento == "reclamacion_supersalud":
                resultado["plazo_supersalud"] = datos_paciente.get("plazo_supersalud", "")
                resultado["entidad_destinataria"] = "Superintendencia Nacional de Salud"
            elif tipo_documento == "tutela":
                resultado["gestiones_previas"] = gestiones_previas
                resultado["entidad_destinataria"] = "Juzgado de Tutela"
            else:
                resultado["entidad_destinataria"] = datos_paciente.get("eps_estandarizada", "")
            
            logger.info(f"{tipo_documento} generada exitosamente para paciente {patient_key}")
            return resultado
            
        except ClaimGeneratorError:
            raise
        except Exception as e:
            logger.error(f"Error inesperado generando {tipo_documento} para {patient_key}: {e}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "tipo_reclamacion": tipo_documento,
                "patient_key": patient_key
            }

    def generar_reclamacion_eps(self, patient_key: str) -> Dict[str, Any]:
        """Genera una reclamación formal ante la EPS."""
        return self._generar_documento_legal(patient_key, "reclamacion_eps")

    def generar_reclamacion_supersalud(self, patient_key: str) -> Dict[str, Any]:
        """Genera una queja formal ante la Superintendencia Nacional de Salud."""
        return self._generar_documento_legal(patient_key, "reclamacion_supersalud")

    def generar_tutela(self, patient_key: str, 
                      gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
        """Genera una acción de tutela por vulneración del derecho a la salud."""
        return self._generar_documento_legal(patient_key, "tutela", gestiones_previas)

    def obtener_preview_datos(self, patient_key: str, 
                             tipo_documento: str = "reclamacion_eps") -> Dict[str, Any]:
        """Obtiene un preview de los datos que se usarían para generar el documento."""
        try:
            datos = self.obtener_datos_paciente(patient_key)
            
            # Validar según tipo de documento
            if tipo_documento == "reclamacion_supersalud":
                campos_faltantes = self.validar_datos_supersalud(datos)
            elif tipo_documento == "tutela":
                campos_faltantes = self.validar_datos_tutela(datos)
            else:
                campos_faltantes = self.validar_datos_eps(datos)
            
            return {
                "success": True,
                "datos_disponibles": datos,
                "campos_faltantes": campos_faltantes,
                "puede_generar": len(campos_faltantes) == 0,
                "patient_key": patient_key,
                "tipo_documento": tipo_documento
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "patient_key": patient_key,
                "tipo_documento": tipo_documento
            }


def _get_claim_generator():
    """Factory function para obtener instancia del generador."""
    try:
        return ClaimGenerator()
    except ClaimGeneratorError as e:
        logger.critical(f"Error crítico al instanciar ClaimGenerator: {e}")
        return None
    except Exception as e:
        logger.critical(f"Error inesperado al instanciar ClaimGenerator: {e}")
        return None


# Instancia global del generador
claim_generator = _get_claim_generator()
if claim_generator:
    logger.info("ClaimGenerator completo instanciado correctamente.")


# Funciones de conveniencia para uso externo
def generar_reclamacion_eps(patient_key: str) -> Dict[str, Any]:
    """Función de conveniencia para generar reclamación EPS."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_reclamacion_eps(patient_key)


def generar_reclamacion_supersalud(patient_key: str) -> Dict[str, Any]:
    """Función de conveniencia para generar queja ante Supersalud."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_reclamacion_supersalud(patient_key)


def generar_tutela(patient_key: str, 
                  gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
    """Función de conveniencia para generar tutela."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_tutela(patient_key, gestiones_previas)


def preview_datos_paciente(patient_key: str, 
                          tipo_documento: str = "reclamacion_eps") -> Dict[str, Any]:
    """Función de conveniencia para preview de datos."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.obtener_preview_datos(patient_key, tipo_documento)

def validar_disponibilidad_supersalud() -> Dict[str, Any]:
    """
    Valida si el sistema puede generar reclamaciones ante Supersalud.
    
    Returns:
        Dict con información sobre disponibilidad
    """
    try:
        if not claim_generator:
            return {
                "disponible": False,
                "error": "ClaimGenerator no inicializado",
                "solucion": "Verificar configuración del sistema"
            }
        
        # Verificar prompt
        prompt_supersalud = prompt_manager.get_prompt_by_module_and_function("CLAIM", "reclamacion_supersalud")
        if not prompt_supersalud:
            return {
                "disponible": False,
                "error": "Prompt para Supersalud no encontrado",
                "solucion": "Ejecutar el INSERT SQL del prompt en BigQuery"
            }
        
        return {
            "disponible": True,
            "mensaje": "Sistema listo para generar reclamaciones ante Supersalud",
            "funciones_disponibles": [
                "generar_reclamacion_supersalud()",
                "validar_datos_supersalud()",
                "preview_datos_paciente(tipo='reclamacion_supersalud')"
            ]
        }
        
    except Exception as e:
        return {
            "disponible": False,
            "error": f"Error verificando disponibilidad: {e}",
            "solucion": "Revisar configuración y logs del sistema"
        }