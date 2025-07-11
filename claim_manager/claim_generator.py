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

REQUIRED_TUTELA_FIELDS = [
    "numero_sentencia",
    "fecha_sentencia", 
    "fecha_radicacion_tutela",
    "juzgado",
    "ciudad"
]

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
    - Incidentes de desacato por incumplimiento de tutela
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

    def _obtener_radicados_previos(self, patient_key: str, tipos_accion: List[str]) -> List[Dict[str, Any]]:
        """
        Obtiene radicados previos de reclamaciones específicas para el mismo paciente y medicamentos.
        
        Args:
            patient_key: Clave del paciente
            tipos_accion: Lista de tipos de acción a buscar (ej: ["reclamacion_eps"])
            
        Returns:
            Lista de reclamaciones con radicados encontrados
        """
        try:
            query = f"""
            SELECT reclamaciones
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE paciente_clave = @patient_key
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                reclamaciones = row.reclamaciones if row.reclamaciones else []
                
                # Filtrar reclamaciones que tengan radicado y sean del tipo solicitado
                radicados_previos = []
                for reclamacion in reclamaciones:
                    if (reclamacion.get("tipo_accion") in tipos_accion and 
                        reclamacion.get("numero_radicado") and 
                        reclamacion.get("numero_radicado").strip()):
                        radicados_previos.append({
                            "tipo_accion": reclamacion.get("tipo_accion"),
                            "numero_radicado": reclamacion.get("numero_radicado"),
                            "fecha_radicacion": reclamacion.get("fecha_radicacion"),
                            "med_no_entregados": reclamacion.get("med_no_entregados", ""),
                            "estado_reclamacion": reclamacion.get("estado_reclamacion")
                        })
                
                return radicados_previos
                
            return []
            
        except Exception as e:
            logger.error(f"Error obteniendo radicados previos para {patient_key}: {e}")
            return []

    def validar_requisitos_escalamiento(self, patient_key: str, tipo_escalamiento: str) -> Dict[str, Any]:
        """
        Valida si un paciente cumple los requisitos para un tipo específico de escalamiento.
        ACTUALIZADA para incluir desacato.
        
        Args:
            patient_key: Clave del paciente
            tipo_escalamiento: "supersalud", "tutela", o "desacato"
            
        Returns:
            Dict con información sobre si puede escalar y qué requisitos faltan
        """
        try:
            requisitos = {
                "supersalud": {
                    "requiere": ["reclamacion_eps"],
                    "nivel": 2,
                    "descripcion": "Queja ante Superintendencia Nacional de Salud"
                },
                "tutela": {
                    "requiere": ["reclamacion_eps"],  # Supersalud es opcional
                    "nivel": 3,
                    "descripcion": "Acción de tutela por vulneración del derecho a la salud"
                },
                "desacato": {
                    "requiere": ["tutela_favorable"],  # Requiere fallo favorable de tutela
                    "nivel": 4,
                    "descripcion": "Incidente de desacato por incumplimiento de fallo de tutela"
                }
            }
            
            if tipo_escalamiento not in requisitos:
                return {
                    "puede_escalar": False,
                    "error": f"Tipo de escalamiento no válido: {tipo_escalamiento}"
                }
            
            config = requisitos[tipo_escalamiento]
            
            # Verificar requisitos específicos
            if "tutela_favorable" in config["requiere"]:
                # Para desacato, usar la validación específica
                validacion_desacato = self.validar_requisitos_desacato(patient_key)
                if not validacion_desacato.get("puede_desacatar"):
                    return {
                        "puede_escalar": False,
                        "requisitos_faltantes": ["tutela_favorable_registrada"],
                        "mensaje": f"Para {config['descripcion']} se requiere una tutela favorable previa registrada en el sistema",
                        "nivel_escalamiento": config["nivel"]
                    }
            elif "reclamacion_eps" in config["requiere"]:
                radicados_eps = self._obtener_radicados_previos(patient_key, ["reclamacion_eps"])
                if not radicados_eps:
                    return {
                        "puede_escalar": False,
                        "requisitos_faltantes": ["reclamacion_eps_radicada"],
                        "mensaje": f"Para {config['descripcion']} se requiere al menos una reclamación EPS previa con radicado",
                        "nivel_escalamiento": config["nivel"]
                    }
            
            return {
                "puede_escalar": True,
                "tipo_escalamiento": tipo_escalamiento,
                "nivel_escalamiento": config["nivel"],
                "descripcion": config["descripcion"],
                "patient_key": patient_key
            }
            
        except Exception as e:
            logger.error(f"Error validando requisitos de escalamiento: {e}")
            return {
                "puede_escalar": False,
                "error": f"Error verificando requisitos: {str(e)}"
            }

    def validar_requisitos_desacato(self, patient_key: str) -> Dict[str, Any]:
        """
        Valida si un paciente tiene datos de tutela para poder solicitar desacato.
        ✅ MODIFICADO: Ahora usa tabla tutelas simplificada.
        
        Args:
            patient_key: Clave del paciente
            
        Returns:
            Dict con información sobre si puede solicitar desacato
        """
        try:
            # ✅ USAR función simplificada para obtener datos
            datos_tutela = _obtener_datos_tutela_para_desacato(patient_key)
            
            if datos_tutela and datos_tutela.get("numero_sentencia") and datos_tutela.get("juzgado"):
                return {
                    "puede_desacatar": True,
                    "numero_sentencia": datos_tutela["numero_sentencia"],
                    "juzgado": datos_tutela["juzgado"],
                    "fecha_sentencia": datos_tutela["fecha_sentencia"],
                    "ciudad": datos_tutela.get("ciudad", ""),
                    "nivel_escalamiento": 5,
                    "patient_key": patient_key
                }
            
            # ✅ NO tiene datos de tutela
            return {
                "puede_desacatar": False,
                "requisitos_faltantes": ["datos_tutela"],
                "mensaje": "Para solicitar desacato se requieren los datos de una tutela favorable previa",
                "nivel_escalamiento": 5
            }
            
        except Exception as e:
            logger.error(f"Error validando requisitos de desacato: {e}")
            return {
                "puede_desacatar": False,
                "error": f"Error verificando requisitos: {str(e)}"
            }

    def _generar_documento_legal(self, patient_key: str, tipo_documento: str,
                               gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
        """Método genérico para generar documentos legales."""
        try:
            logger.info(f"Iniciando generación de {tipo_documento} para paciente: {patient_key}")
            
            # 1. Obtener datos del paciente
            datos_paciente = self.obtener_datos_paciente(patient_key)
            
            # 1.5. Determinar prompt según historial (NUEVO)
            nivel_escalamiento = 1  # Por defecto
            if gestiones_previas and isinstance(gestiones_previas, list) and len(gestiones_previas) > 0:
                # Si hay gestiones previas, intentar extraer nivel
                try:
                    nivel_escalamiento = int(gestiones_previas[0]) if str(gestiones_previas[0]).isdigit() else 1
                except:
                    nivel_escalamiento = 1
            
            tipo_prompt = self._determinar_prompt_escalamiento(patient_key, tipo_documento, nivel_escalamiento)
            logger.info(f"Usando prompt: {tipo_prompt} (base: {tipo_documento})")
            
            # 1.6. Agregar datos específicos según tipo de prompt (NUEVO)
            if tipo_prompt.endswith("_escalado"):
                datos_paciente["gestiones_previas"] = self._obtener_gestiones_previas_texto(patient_key)
            
            if tipo_prompt in ["desacato2", "desacato3"]:
                datos_desacatos = self._obtener_datos_desacatos_previos_metodo(patient_key)
                datos_paciente.update(datos_desacatos)
            
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
            
            # 3. Obtener y formatear prompt (MODIFICADO para usar tipo_prompt)
            prompt_template = prompt_manager.get_prompt_by_module_and_function("CLAIM", tipo_prompt)
            if not prompt_template:
                logger.warning(f"Prompt {tipo_prompt} no encontrado, usando base {tipo_documento}")
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
            
            pdf_url = ""
            pdf_filename = ""
            pdf_result: Dict[str, Any] = {} # Initialize pdf_result here
            
            if tipo_documento in ["tutela", "desacato"]:
                try:
                    logger.info(f"🔄 Generando PDF para {tipo_documento}...")
                    
                    if tipo_documento == "tutela":
                        from processor_image_prescription.pdf_generator import generar_pdf_tutela
                        temp_resultado = {
                            "success": True,
                            "tipo_reclamacion": "tutela",
                            "texto_reclamacion": texto_generado.strip(),
                            "patient_key": patient_key
                        }
                        pdf_result = generar_pdf_tutela(temp_resultado)
                        
                    elif tipo_documento == "desacato":
                        from processor_image_prescription.pdf_generator import generar_pdf_desacato
                        temp_resultado = {
                            "success": True,
                            "tipo_reclamacion": "desacato", 
                            "texto_reclamacion": texto_generado.strip(),
                            "patient_key": patient_key,
                            "numero_sentencia_referencia": datos_paciente.get("numero_sentencia", ""),
                            "juzgado": datos_paciente.get("juzgado", "")
                        }
                        pdf_result = generar_pdf_desacato(temp_resultado)
                    
                    if pdf_result and pdf_result.get("success"):
                        pdf_url = pdf_result["pdf_url"]
                        pdf_filename = pdf_result["pdf_filename"]
                        logger.info(f"✅ PDF generado automáticamente: {pdf_url}")
                    else:
                        error_msg = pdf_result.get("error") if pdf_result else "Unknown error"
                        logger.error(f"❌ Error generando PDF: {error_msg}")
                        
                except Exception as pdf_error:
                    logger.error(f"❌ Error en generación automática de PDF {tipo_documento}: {pdf_error}")
            
            # ✅ PREPARAR respuesta exitosa CON información del PDF
            resultado = {
                "success": True,
                "tipo_reclamacion": tipo_documento,
                "prompt_usado": tipo_prompt,
                "texto_reclamacion": texto_generado.strip(),
                "datos_utilizados": datos_paciente,
                "fecha_generacion": datetime.now().isoformat(),
                "patient_key": patient_key,
                "nivel_riesgo": datos_paciente.get("categoria_riesgo", ""),
                "plazo_respuesta": datos_paciente.get("plazo_respuesta", ""),
                "medicamentos_afectados": datos_paciente.get("med_no_entregados", "")
            }
            
            # ✅ AGREGAR información del PDF si se generó
            if pdf_url:
                resultado["pdf_url"] = pdf_url
                resultado["pdf_filename"] = pdf_filename
                resultado["requiere_pdf"] = True
                resultado["requiere_firma_paciente"] = True
                logger.info(f"✅ Resultado incluye PDF: {pdf_url}")
            else:
                logger.warning(f"⚠️ Resultado SIN PDF para {tipo_documento}")
            
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
        """
        Genera una queja formal ante la Superintendencia Nacional de Salud.
        REQUIERE reclamaciones EPS previas con radicado para el mismo paciente.
        """
        try:
            logger.info(f"Iniciando generación de reclamación Supersalud para paciente: {patient_key}")
            
            # 1. Verificar que existan reclamaciones EPS previas con radicado
            radicados_eps = self._obtener_radicados_previos(patient_key, ["reclamacion_eps"])
            
            if not radicados_eps:
                return {
                    "success": False,
                    "error": "No se encontraron reclamaciones EPS previas con radicado para este paciente",
                    "patient_key": patient_key,
                    "tipo_documento": "reclamacion_supersalud",
                    "nivel_escalamiento": 2,
                    "requisitos_faltantes": ["reclamacion_eps_radicada"]
                }
            
            # 2. Obtener datos del paciente y validar
            datos_paciente = self.obtener_datos_paciente(patient_key)
            campos_faltantes = self.validar_datos_supersalud(datos_paciente)
            
            if campos_faltantes:
                return {
                    "success": False,
                    "error": f"Faltan campos requeridos para Supersalud: {', '.join(campos_faltantes)}",
                    "campos_faltantes": campos_faltantes,
                    "patient_key": patient_key,
                    "tipo_documento": "reclamacion_supersalud",
                    "nivel_escalamiento": 2
                }
            
            # 3. Agregar información de gestiones previas al contexto del prompt
            gestiones_previas = []
            for radicado in radicados_eps:
                fecha_rad = radicado.get("fecha_radicacion", "")
                num_rad = radicado.get("numero_radicado", "")
                gestiones_previas.append(
                    f"Reclamación ante EPS radicada el {fecha_rad} bajo el número {num_rad}"
                )
            
            datos_paciente["gestiones_previas_eps"] = ". ".join(gestiones_previas)
            datos_paciente["radicados_previos"] = radicados_eps
            
            # 4. Generar documento usando el método base
            resultado = self._generar_documento_legal(patient_key, "reclamacion_supersalud")
            
            if resultado["success"]:
                resultado["nivel_escalamiento"] = 2
                resultado["radicados_eps_previos"] = radicados_eps
                resultado["gestiones_previas"] = gestiones_previas
                
            return resultado
            
        except Exception as e:
            logger.error(f"Error inesperado generando reclamación Supersalud para {patient_key}: {e}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "tipo_reclamacion": "reclamacion_supersalud",
                "patient_key": patient_key,
                "nivel_escalamiento": 2
            }

    def generar_tutela(self, patient_key: str, 
                  gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Genera una acción de tutela por vulneración del derecho a la salud.
        REQUIERE reclamaciones EPS y opcionalmente Supersalud previas con radicado.
        ✅ MODIFICADO: Genera PDF automáticamente.
        """
        try:
            logger.info(f"Iniciando generación de tutela para paciente: {patient_key}")
            
            # 1. Verificar gestiones previas obligatorias
            radicados_eps = self._obtener_radicados_previos(patient_key, ["reclamacion_eps"])
            radicados_supersalud = self._obtener_radicados_previos(patient_key, ["reclamacion_supersalud"])
            
            if not radicados_eps:
                return {
                    "success": False,
                    "error": "No se encontraron reclamaciones EPS previas con radicado para generar tutela",
                    "patient_key": patient_key,
                    "tipo_documento": "tutela",
                    "nivel_escalamiento": 3,
                    "requisitos_faltantes": ["reclamacion_eps_radicada"]
                }
            
            # 2. Obtener datos del paciente y validar
            datos_paciente = self.obtener_datos_paciente(patient_key)
            campos_faltantes = self.validar_datos_tutela(datos_paciente)
            
            if campos_faltantes:
                return {
                    "success": False,
                    "error": f"Faltan campos requeridos para tutela: {', '.join(campos_faltantes)}",
                    "campos_faltantes": campos_faltantes,
                    "patient_key": patient_key,
                    "tipo_documento": "tutela",
                    "nivel_escalamiento": 3
                }
            
            # 3. Construir gestiones previas automáticamente si no se proporcionan
            if not gestiones_previas:
                gestiones_previas = []
                
                # Agregar reclamaciones EPS
                for radicado in radicados_eps:
                    fecha_rad = radicado.get("fecha_radicacion", "")
                    num_rad = radicado.get("numero_radicado", "")
                    gestiones_previas.append(
                        f"Reclamación ante {datos_paciente.get('eps_estandarizada', 'EPS')} "
                        f"radicada el {fecha_rad} bajo el número {num_rad} sin respuesta satisfactoria"
                    )
                
                # Agregar reclamaciones Supersalud si existen
                for radicado in radicados_supersalud:
                    fecha_rad = radicado.get("fecha_radicacion", "")
                    num_rad = radicado.get("numero_radicado", "")
                    gestiones_previas.append(
                        f"Queja ante Superintendencia Nacional de Salud "
                        f"radicada el {fecha_rad} bajo el número {num_rad} sin respuesta satisfactoria"
                    )
                
                # Agregar gestiones adicionales estándar
                gestiones_previas.extend([
                    "Múltiples solicitudes presenciales y telefónicas ante la EPS",
                    "Agotamiento de medios ordinarios de reclamación administrativa"
                ])
            
            # 4. Generar documento
            resultado = self._generar_documento_legal(patient_key, "tutela", gestiones_previas)
            
            if resultado["success"]:
                resultado["nivel_escalamiento"] = 3
                resultado["radicados_eps_previos"] = radicados_eps
                resultado["radicados_supersalud_previos"] = radicados_supersalud
                resultado["requiere_pdf"] = True
                resultado["requiere_firma_paciente"] = True
                
                # ✅ AGREGAR: Generar PDF automáticamente para tutela
                try:
                    from processor_image_prescription.pdf_generator import generar_pdf_tutela
                    pdf_result = generar_pdf_tutela(resultado)
                    
                    if pdf_result.get("success"):
                        resultado["pdf_url"] = pdf_result["pdf_url"]
                        resultado["pdf_filename"] = pdf_result["pdf_filename"]
                        logger.info(f"✅ PDF de tutela generado exitosamente: {pdf_result['pdf_url']}")
                    else:
                        logger.error(f"❌ Error generando PDF de tutela: {pdf_result.get('error')}")
                        
                except Exception as e:
                    logger.error(f"Error en generación automática de PDF tutela: {e}")
                
            return resultado
            
        except Exception as e:
            logger.error(f"Error inesperado generando tutela para {patient_key}: {e}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "tipo_reclamacion": "tutela",
                "patient_key": patient_key,
                "nivel_escalamiento": 3
            }

    def generar_desacato(self, patient_key: str, datos_tutela_adicionales: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Genera un incidente de desacato por incumplimiento de tutela.
        ✅ MODIFICADO: Valida TODOS los campos antes de generar.
        """
        try:
            logger.info(f"Iniciando generación de desacato para paciente: {patient_key}")
            logger.info(f"Datos tutela adicionales recibidos: {datos_tutela_adicionales}")

            # 1. ✅ VALIDAR DATOS COMPLETOS ANTES DE CONTINUAR
            logger.info(f"🔍 Validando datos de tutela completos...")
            validacion = validar_datos_tutela_completos(patient_key)
            logger.info(f"Resultado validación: {validacion}")
            
            if not validacion["completo"]:
                logger.warning(f"❌ Datos de tutela incompletos: {validacion['campos_faltantes']}")

                return {
                    "success": False,
                    "error": "Datos de tutela incompletos para generar desacato",
                    "requiere_recoleccion_tutela": True,
                    "campos_faltantes": validacion["campos_faltantes"],
                    "patient_key": patient_key,
                    "tipo_documento": "desacato",
                    "nivel_escalamiento": 5
                }
            logger.info(f"✅ Datos de tutela completos, continuando...")
            # 2. Usar datos validados
            datos_tutela_adicionales = validacion["datos_existentes"]
            logger.info(f"Datos tutela a usar: {datos_tutela_adicionales}")

            logger.info(f"🔍 Obteniendo datos del paciente...")
            datos_paciente = self.obtener_datos_paciente(patient_key)
            logger.info(f"✅ Datos paciente obtenidos")

            campos_faltantes = self.validar_datos_tutela(datos_paciente)
            logger.info(f"Campos faltantes paciente: {campos_faltantes}")
            
            if campos_faltantes:
                return {
                    "success": False,
                    "error": f"Faltan campos requeridos para desacato: {', '.join(campos_faltantes)}",
                    "campos_faltantes": campos_faltantes,
                    "patient_key": patient_key,
                    "tipo_documento": "desacato",
                    "nivel_escalamiento": 5
                }
            
            # 3. Combinar datos del paciente con datos de tutela simplificados
            datos_completos = {**datos_paciente}
            datos_completos.update({
                "numero_sentencia": datos_tutela_adicionales["numero_sentencia"],      # ✅ NUEVO
                "juzgado": datos_tutela_adicionales["juzgado"], 
                "fecha_sentencia": datos_tutela_adicionales["fecha_sentencia"],
                "fecha_radicacion_tutela": datos_tutela_adicionales["fecha_radicacion_tutela"],  # ✅ NUEVO
                "ciudad_tutela": datos_tutela_adicionales.get("ciudad", datos_paciente.get("ciudad", "")),
            })
            
            # Representante legal se construye automáticamente
            datos_completos["representante_legal_eps"] = f"Representante Legal de {datos_paciente.get('eps_estandarizada', 'EPS')}"
            
            # 4. Obtener y formatear prompt
            logger.info(f"🔍 Obteniendo prompt de desacato...")
            prompt_template = prompt_manager.get_prompt_by_module_and_function("CLAIM", "desacato")
            logger.info(f"Prompt template obtenido: {prompt_template is not None}")

            if not prompt_template:
                logger.error("Prompt CLAIM.desacato no encontrado")
                return {
                    "success": False,
                    "error": "Prompt CLAIM.desacato no disponible en el sistema",
                    "patient_key": patient_key,
                    "tipo_documento": "desacato",
                    "nivel_escalamiento": 5
                }
            logger.info(f"🔍 Formateando prompt con datos completos...")
            logger.info(f"Datos completos para prompt: {list(datos_completos.keys())}")
            
            try:
                prompt_formateado = prompt_template.format(**datos_completos)
                logger.debug(f"Prompt formateado correctamente para desacato {patient_key}")
            except KeyError as e:
                logger.error(f"Error al formatear prompt de desacato: variable {e} no encontrada")
                return {
                    "success": False,
                    "error": f"Error en template del prompt: falta variable {e}",
                    "patient_key": patient_key,
                    "tipo_documento": "desacato",
                    "nivel_escalamiento": 5
                }
            
            # 5. Generar texto con LLM
            logger.info(f"Enviando prompt a LLM para generar desacato...")
            texto_generado = self.llm_core.ask_text(prompt_formateado)
            
            # 6. Preparar respuesta exitosa
            resultado = {
                "success": True,
                "tipo_reclamacion": "desacato",
                "texto_reclamacion": texto_generado.strip(),
                "datos_utilizados": datos_completos,
                "fecha_generacion": datetime.now().isoformat(),
                "patient_key": patient_key,
                "nivel_escalamiento": 5,
                "numero_sentencia_referencia": datos_tutela_adicionales["numero_sentencia"],
                "juzgado": datos_tutela_adicionales["juzgado"],
                "requiere_pdf": True,
                "requiere_firma_paciente": True,
                "entidad_destinataria": datos_tutela_adicionales["juzgado"]
            }
            
            try:
                from processor_image_prescription.pdf_generator import generar_pdf_desacato
                pdf_result = generar_pdf_desacato(resultado)
                
                if pdf_result.get("success"):
                    resultado["pdf_url"] = pdf_result["pdf_url"]
                    resultado["pdf_filename"] = pdf_result["pdf_filename"]
                    logger.info(f"✅ PDF de desacato generado exitosamente: {pdf_result['pdf_url']}")
                else:
                    logger.error(f"❌ Error generando PDF de desacato: {pdf_result.get('error')}")
                    
            except Exception as e:
                logger.error(f"Error en generación automática de PDF desacato: {e}")
            
            logger.info(f"Desacato generado exitosamente para paciente {patient_key}")
            return resultado
            
        except Exception as e:
            logger.error(f"Error inesperado generando desacato para {patient_key}: {e}")
            return {
                "success": False,
                "error": f"Error inesperado: {str(e)}",
                "tipo_reclamacion": "desacato",
                "patient_key": patient_key,
                "nivel_escalamiento": 5
            }
        
    def _determinar_prompt_escalamiento(self, patient_key: str, tipo_base: str, 
                                  nivel: int) -> str:
        """
        Determina prompt según historial: base o escalado.
        
        Args:
            patient_key: Clave del paciente
            tipo_base: Tipo base (reclamacion_eps, reclamacion_supersalud, desacato)
            nivel: Nivel actual
            
        Returns:
            Nombre del prompt a usar
        """
        try:
            query = f"""
            SELECT reclamaciones
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE paciente_clave = @patient_key
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                reclamaciones = row.reclamaciones if row.reclamaciones else []
                
                if tipo_base == "reclamacion_eps":
                    count = len([r for r in reclamaciones 
                            if r.get("tipo_accion") == "reclamacion_eps" 
                            and r.get("nivel_escalamiento", 0) < nivel])
                    return "reclamacion_eps_escalado" if count > 0 else "reclamacion_eps"
                    
                elif tipo_base == "reclamacion_supersalud":
                    count = len([r for r in reclamaciones 
                            if r.get("tipo_accion") == "reclamacion_supersalud" 
                            and r.get("nivel_escalamiento", 0) < nivel])
                    return "reclamacion_supersalud_escalado" if count > 0 else "reclamacion_supersalud"
                    
                elif tipo_base == "desacato":
                    desacatos = [r for r in reclamaciones if r.get("tipo_accion") == "desacato"]
                    count = len(desacatos)
                    
                    if count == 0:
                        return "desacato"
                    elif count == 1:
                        return "desacato2"
                    else:
                        return "desacato3"
            
            return tipo_base
            
        except Exception as e:
            logger.error(f"Error determinando prompt escalamiento: {e}")
            return tipo_base
        
    def _obtener_gestiones_previas_texto(self, patient_key: str) -> str:
        """Obtiene texto de gestiones previas para prompts escalados."""
        try:
            radicados_eps = self._obtener_radicados_previos(patient_key, ["reclamacion_eps"])
            radicados_supersalud = self._obtener_radicados_previos(patient_key, ["reclamacion_supersalud"])
            
            gestiones = []
            
            for radicado in radicados_eps:
                fecha = radicado.get("fecha_radicacion", "")
                numero = radicado.get("numero_radicado", "")
                gestiones.append(f"Reclamación ante EPS radicada el {fecha} (No. {numero}) sin respuesta satisfactoria")
            
            for radicado in radicados_supersalud:
                fecha = radicado.get("fecha_radicacion", "")
                numero = radicado.get("numero_radicado", "")
                gestiones.append(f"Queja ante Superintendencia radicada el {fecha} (No. {numero}) sin resolución")
            
            return ". ".join(gestiones) if gestiones else "Gestiones previas realizadas sin éxito"
            
        except Exception as e:
            logger.error(f"Error obteniendo gestiones previas: {e}")
            return "Múltiples gestiones previas realizadas sin respuesta satisfactoria"    
        
    def _obtener_datos_desacatos_previos_metodo(self, patient_key: str) -> Dict[str, str]:
        """Obtiene fechas/números de desacatos previos para desacato2 y desacato3."""
        try:
            query = f"""
            SELECT reclamaciones
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE paciente_clave = @patient_key
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
                ]
            )
            
            results = self.bq_client.query(query, job_config=job_config).result()
            
            for row in results:
                reclamaciones = row.reclamaciones if row.reclamaciones else []
                desacatos = [r for r in reclamaciones if r.get("tipo_accion") == "desacato"]
                desacatos.sort(key=lambda x: x.get("fecha_radicacion", ""))
                
                datos = {}
                if len(desacatos) >= 1:
                    datos["primer_desacato_fecha"] = desacatos[0].get("fecha_radicacion", "")
                    datos["primer_desacato_numero"] = desacatos[0].get("numero_radicado", "")
                    
                if len(desacatos) >= 2:
                    datos["segundo_desacato_fecha"] = desacatos[1].get("fecha_radicacion", "")
                    datos["segundo_desacato_numero"] = desacatos[1].get("numero_radicado", "")
                    
                return datos
            
            return {}
            
        except Exception as e:
            logger.error(f"Error obteniendo datos desacatos previos: {e}")
            return {}
            
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
            elif tipo_documento == "desacato":
                # Para desacato, también validar requisitos de tutela previa
                validacion_desacato = self.validar_requisitos_desacato(patient_key)
                if not validacion_desacato.get("puede_desacatar"):
                    campos_faltantes = ["tutela_favorable_previa"]
                else:
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
def verificar_requisitos_desacato_completos(patient_key: str) -> Dict[str, Any]:
    """
    Función de conveniencia para verificar si se puede generar desacato.
    
    Args:
        patient_key: Clave del paciente
        
    Returns:
        Dict con información completa sobre requisitos
    """
    if not claim_generator:
        return {
            "puede_generar": False,
            "error": "ClaimGenerator no disponible"
        }
    
    try:
        validacion = validar_datos_tutela_completos(patient_key)
        
        return {
            "puede_generar": validacion["completo"],
            "campos_faltantes": validacion.get("campos_faltantes", []),
            "datos_existentes": validacion.get("datos_existentes", {}),
            "mensaje": validacion["mensaje"],
            "patient_key": patient_key
        }
        
    except Exception as e:
        return {
            "puede_generar": False,
            "error": str(e),
            "patient_key": patient_key
        }
    
def generar_reclamacion_eps(patient_key: str) -> Dict[str, Any]:
    """Función de conveniencia para generar reclamación EPS."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_reclamacion_eps(patient_key)


def generar_reclamacion_supersalud(patient_key: str) -> Dict[str, Any]:
    """Función de conveniencia para generar queja ante Supersalud con validación de requisitos."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_reclamacion_supersalud(patient_key)


def generar_tutela(patient_key: str, 
                  gestiones_previas: Optional[List[str]] = None) -> Dict[str, Any]:
    """Función de conveniencia para generar tutela con validación de requisitos."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_tutela(patient_key, gestiones_previas)


def generar_desacato(patient_key: str, datos_tutela_adicionales: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Función de conveniencia para generar desacato con validación de requisitos."""
    if not claim_generator:
        return {"success": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.generar_desacato(patient_key, datos_tutela_adicionales)


def validar_requisitos_escalamiento(patient_key: str, tipo_escalamiento: str) -> Dict[str, Any]:
    """Función de conveniencia para validar requisitos de escalamiento."""
    if not claim_generator:
        return {"puede_escalar": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.validar_requisitos_escalamiento(patient_key, tipo_escalamiento)


def validar_requisitos_desacato(patient_key: str) -> Dict[str, Any]:
    """Función de conveniencia para validar requisitos de desacato."""
    if not claim_generator:
        return {"puede_desacatar": False, "error": "ClaimGenerator no disponible"}
    return claim_generator.validar_requisitos_desacato(patient_key)


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


def validar_disponibilidad_desacato() -> Dict[str, Any]:
    """
    Valida si el sistema puede generar incidentes de desacato.
    
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
        
        # Verificar prompt de desacato
        prompt_desacato = prompt_manager.get_prompt_by_module_and_function("CLAIM", "desacato")
        if not prompt_desacato:
            return {
                "disponible": False,
                "error": "Prompt para desacato no encontrado",
                "solucion": "Ejecutar el INSERT SQL del prompt en BigQuery"
            }
        
        # Verificar prompt de recolección de datos de desacato
        prompt_recoleccion = prompt_manager.get_prompt_by_module_and_function("DATA", "recoleccion_desacato")
        if not prompt_recoleccion:
            return {
                "disponible": False,
                "error": "Prompt para recolección de datos de desacato no encontrado",
                "solucion": "Ejecutar el INSERT SQL del prompt de recolección en BigQuery"
            }
        
        # Verificar tabla de tutelas (simulada - en producción usar BigQuery)
        try:
            query = f"""
            SELECT COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET_ID}.tutelas`
            LIMIT 1
            """
            results = claim_generator.bq_client.query(query).result()
            # Si llega aquí, la tabla existe
        except Exception:
            return {
                "disponible": False,
                "error": "Tabla 'tutelas' no encontrada",
                "solucion": "Crear la tabla tutelas en BigQuery"
            }
        
        return {
            "disponible": True,
            "mensaje": "Sistema listo para generar incidentes de desacato",
            "funciones_disponibles": [
                "generar_desacato()",
                "validar_requisitos_desacato()",
                "preview_datos_paciente(tipo='desacato')"
            ]
        }
        
    except Exception as e:
        return {
            "disponible": False,
            "error": f"Error verificando disponibilidad: {e}",
            "solucion": "Revisar configuración y logs del sistema"
        }

def auto_escalate_patient(session_id: str) -> Dict[str, Any]:
    """
    FUNCIÓN PRINCIPAL DE ESCALAMIENTO AUTOMÁTICO

    Args:
        session_id: ID de la sesión (ej: "TL_573226743144_20250702_091518")

    Returns:
        Dict con resultado del escalamiento automático
    """
    try:
        logger.info(f"🔄 Iniciando escalamiento automático para session_id: {session_id}")
        
        # 1. BUSCAR PATIENT_KEY USANDO SESSION_ID
        patient_key = _obtener_patient_key_por_session_id(session_id)
        if not patient_key:
            return {"success": False, "error": "Paciente no encontrado para esta sesión"}
        
        logger.info(f"✅ Session {session_id} corresponde a patient_key: {patient_key}")
        
        # 2. OBTENER DATOS COMPLETOS DEL PACIENTE
        datos_paciente = _obtener_datos_paciente_para_escalamiento(patient_key)
        if not datos_paciente:
            return {"success": False, "error": "Datos del paciente no encontrados"}
        
        # 3. DETERMINAR AUTOMÁTICAMENTE QUÉ ESCALAMIENTO HACER
        decision_escalamiento = _determinar_siguiente_escalamiento_automatico(datos_paciente)
        
        logger.info(f"Decisión de escalamiento para {patient_key}: {decision_escalamiento}")
        
        if decision_escalamiento["accion"] == "generar":
            tipo = decision_escalamiento["tipo"]
            
            # ✅ Verificar datos para desacato antes de generar
            if tipo == "desacato":
                datos_tutela_existentes = _obtener_datos_tutela_para_desacato(patient_key)
                faltantes = []
                if not datos_tutela_existentes:
                    faltantes = REQUIRED_TUTELA_FIELDS
                else:
                    faltantes = [campo for campo in REQUIRED_TUTELA_FIELDS 
                               if not datos_tutela_existentes.get(campo)]
                if faltantes:
                    # ✅ Faltan datos de tutela → Solicitar recolección
                    return {
                        "success": False,
                        "requiere_recoleccion_tutela": True,
                        "tipo": "desacato",
                        "patient_key": patient_key,
                        "session_id": session_id,
                        "campos_necesarios": faltantes
                    }
            # ✅ Si no es desacato O tiene todos los datos → Continuar normal
            resultado = _ejecutar_escalamiento_especifico(patient_key, tipo)
        
        elif decision_escalamiento["accion"] == "generar_multiple":
            tipos = decision_escalamiento["tipos"]
            # Solo permitir escalamiento múltiple EPS+Supersalud, en ese orden exacto
            if set(tipos) == set(["reclamacion_eps", "reclamacion_supersalud"]):
                resultado = _ejecutar_escalamiento_multiple(patient_key, tipos)
                # Guardar resultado y hacer return inmediato
                if resultado.get("success"):
                    guardado = _guardar_escalamiento_en_bd(
                        patient_key, 
                        resultado, 
                        decision_escalamiento["nivel_escalamiento"],
                        session_id
                    )
                    if guardado:
                        logger.info(f"✅ Escalamiento múltiple EPS+Supersalud completo para {patient_key}")
                        return {
                            "success": True,
                            "tipo": "multiple_reclamacion_eps_reclamacion_supersalud",
                            "nivel_escalamiento": decision_escalamiento["nivel_escalamiento"],
                            "razon": decision_escalamiento["razon"],
                            "patient_key": patient_key
                        }
                    else:
                        return {"success": False, "error": "Error guardando escalamiento múltiple en BigQuery"}
                else:
                    return resultado
            else:
                # Si intenta cualquier otro múltiple, rechaza
                return {
                    "success": False,
                    "error": "Escalamiento múltiple solo permitido para EPS+Supersalud"
                }
        
        elif decision_escalamiento["accion"] == "mantener":
            return {
                "success": True, 
                "tipo": "sin_escalamiento",
                "razon": decision_escalamiento["razon"]
            }
            
        elif decision_escalamiento["accion"] == "error":
            return {
                "success": False,
                "error": decision_escalamiento["razon"]
            }
            
        else:
            return {
                "success": False,
                "error": f"Acción no reconocida: {decision_escalamiento['accion']}"
            }
        
        # 5. GUARDAR RESULTADO EN BIGQUERY SI FUE EXITOSO
        if resultado.get("success"):
            guardado = _guardar_escalamiento_en_bd(
                patient_key, 
                resultado, 
                decision_escalamiento["nivel_escalamiento"],
                session_id
            )
            if guardado:
                logger.info(f"✅ Escalamiento completo para {patient_key}: {resultado.get('tipo_reclamacion', resultado.get('tipo', 'desconocido'))}")
                return {
                    "success": True,
                    "tipo": resultado.get('tipo_reclamacion', resultado.get('tipo', 'desconocido')),
                    "nivel_escalamiento": decision_escalamiento["nivel_escalamiento"],
                    "razon": decision_escalamiento["razon"],
                    "patient_key": patient_key
                }
            else:
                return {"success": False, "error": "Error guardando escalamiento en BigQuery"}
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error en auto_escalate_patient para session_id {session_id}: {e}")
        return {"success": False, "error": str(e)}


def _obtener_patient_key_por_session_id(session_id: str) -> Optional[str]:
    """
    NUEVA FUNCIÓN: Busca el patient_key usando el session_id
    
    Args:
        session_id: ID de la sesión (ej: "TL_573226743144_20250702_091518")
        
    Returns:
        patient_key si se encuentra, None si no existe
    """
    try:
        client = get_bigquery_client()
        
        # Buscar en prescripciones que tengan ese session_id
        sql = f"""
        SELECT 
            paciente_clave
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS t,
             UNNEST(t.prescripciones) AS pres
        WHERE pres.id_session = @session_id
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
            ]
        )
        
        results = client.query(sql, job_config=job_config).result()
        
        for row in results:
            logger.info(f"🔍 Session {session_id} encontrado → patient_key: {row.paciente_clave}")
            return row.paciente_clave
        
        # Si no se encuentra en prescripciones, buscar en reclamaciones
        sql_reclamaciones = f"""
        SELECT 
            paciente_clave
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS t,
             UNNEST(t.reclamaciones) AS rec
        WHERE rec.id_session = @session_id
        LIMIT 1
        """
        
        results_rec = client.query(sql_reclamaciones, job_config=job_config).result()
        
        for row in results_rec:
            logger.info(f"🔍 Session {session_id} encontrado en reclamaciones → patient_key: {row.paciente_clave}")
            return row.paciente_clave
        
        logger.warning(f"❌ No se encontró patient_key para session_id: {session_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error buscando patient_key para session_id {session_id}: {e}")
        return None
    
def _obtener_datos_paciente_para_escalamiento(patient_key: str) -> Optional[Dict]:
    """
    Obtiene TODOS los datos necesarios para determinar escalamiento.
    Equivalente a obtener_datos_paciente_para_escalamiento del EscalamientoAutomatico.
    """
    try:
        client = get_bigquery_client()
        
        sql = f"""
        SELECT 
            paciente_clave,
            nombre_paciente,
            tipo_documento,
            numero_documento,
            ciudad,
            direccion,
            telefono_contacto,
            correo,
            eps_estandarizada,
            farmacia,
            sede_farmacia,
            
            -- Datos de prescripción más reciente
            (
                SELECT presc.categoria_riesgo 
                FROM UNNEST(prescripciones) AS presc 
                ORDER BY presc.fecha_atencion DESC 
                LIMIT 1
            ) as categoria_riesgo,
            
            (
                SELECT presc.diagnostico 
                FROM UNNEST(prescripciones) AS presc 
                ORDER BY presc.fecha_atencion DESC 
                LIMIT 1
            ) as diagnostico,
            
            -- Medicamentos no entregados de la última reclamación
            (
                SELECT rec.med_no_entregados 
                FROM UNNEST(reclamaciones) AS rec 
                ORDER BY rec.fecha_radicacion DESC 
                LIMIT 1
            ) as med_no_entregados,
            
            -- Todas las reclamaciones para análisis de escalamiento
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
        
        results = client.query(sql, job_config=job_config).result()
        
        for row in results:
            return {
                'paciente_clave': row.paciente_clave,
                'nombre_paciente': row.nombre_paciente,
                'tipo_documento': row.tipo_documento,
                'numero_documento': row.numero_documento,
                'ciudad': row.ciudad,
                'direccion': row.direccion,
                'telefono_contacto': _format_array_to_string(row.telefono_contacto),
                'correo': _format_array_to_string(row.correo),
                'eps_estandarizada': row.eps_estandarizada,
                'farmacia': row.farmacia,
                'sede_farmacia': row.sede_farmacia,
                'categoria_riesgo': row.categoria_riesgo or 'simple',
                'diagnostico': row.diagnostico or '',
                'med_no_entregados': row.med_no_entregados or '',
                'reclamaciones': list(row.reclamaciones) if row.reclamaciones else []
            }
        
        logger.warning(f"No se encontraron datos para patient_key: {patient_key}")
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo datos del paciente para escalamiento {patient_key}: {e}")
        return None


def _determinar_siguiente_escalamiento_automatico(datos_paciente: Dict) -> Dict[str, Any]:
    """
    LÓGICA PRINCIPAL DE ESCALAMIENTO AUTOMÁTICO
    
    Replica exactamente la lógica del EscalamientoAutomatico que ya funcionaba.
    Determina automáticamente el siguiente escalamiento según categoría de riesgo y historial.
    """
    try:
        categoria_riesgo = datos_paciente.get('categoria_riesgo', 'simple').lower()
        reclamaciones = datos_paciente.get('reclamaciones', [])
        
        # Ordenar reclamaciones por nivel de escalamiento
        reclamaciones_ordenadas = sorted(
            reclamaciones, 
            key=lambda x: x.get('nivel_escalamiento', 0)
        )
        
        if not reclamaciones_ordenadas:
            # Primer escalamiento - siempre empezar con EPS
            return _generar_accion_inicial(categoria_riesgo)
        
        ultima_reclamacion = reclamaciones_ordenadas[-1]
        nivel_actual = ultima_reclamacion.get('nivel_escalamiento', 1)
        tipo_actual = ultima_reclamacion.get('tipo_accion', '')
        estado_actual = ultima_reclamacion.get('estado_reclamacion', '')
        
        # Si está resuelto, no escalar
        if estado_actual == 'resuelto':
            return {"accion": "mantener", "razon": "Caso resuelto"}
        
        # VERIFICACIÓN TEMPORAL: Prevenir escalamiento inmediato después de escalamiento múltiple
        if nivel_actual == 3 and tipo_actual in ["reclamacion_eps", "reclamacion_supersalud"]:
            from datetime import datetime, timedelta
            import pytz
            
            # Obtener fecha de la última reclamación
            fecha_radicacion = ultima_reclamacion.get('fecha_radicacion')
            
            if fecha_radicacion:
                try:
                    # Convertir string a datetime si es necesario
                    if isinstance(fecha_radicacion, str):
                        fecha_radicacion = datetime.strptime(fecha_radicacion, '%Y-%m-%d').date()
                    elif hasattr(fecha_radicacion, 'date'):
                        fecha_radicacion = fecha_radicacion.date()
                    
                    # Obtener fecha actual en Colombia
                    colombia_tz = pytz.timezone('America/Bogota')
                    fecha_actual = datetime.now(colombia_tz).date()
                    
                    # Si la reclamación se creó hoy, no escalar inmediatamente
                    if fecha_radicacion == fecha_actual:
                        return {
                            "accion": "mantener", 
                            "razon": f"Escalamiento múltiple nivel 3 recién completado hoy ({fecha_actual}). Esperando plazo."
                        }
                        
                except Exception as e:
                    # Si hay error con fechas, continuar con lógica normal
                    logger.warning(f"Error verificando fecha de escalamiento múltiple: {e}")
        
        # Evaluar según categoría de riesgo usando la lógica original
        if categoria_riesgo == "simple":
            return _evaluar_escalamiento_simple(nivel_actual, tipo_actual)
        elif categoria_riesgo == "priorizado":
            # VERIFICACIÓN TEMPORAL: Prevenir escalamiento inmediato después de escalamiento múltiple (PRIORIZADO)
            if nivel_actual == 3 and tipo_actual in ["reclamacion_eps", "reclamacion_supersalud"]:
                from datetime import datetime, timedelta
                import pytz
                
                # Obtener fecha de la última reclamación
                fecha_radicacion = ultima_reclamacion.get('fecha_radicacion')
                
                if fecha_radicacion:
                    try:
                        # Convertir string a datetime si es necesario
                        if isinstance(fecha_radicacion, str):
                            fecha_radicacion = datetime.strptime(fecha_radicacion, '%Y-%m-%d').date()
                        elif hasattr(fecha_radicacion, 'date'):
                            fecha_radicacion = fecha_radicacion.date()
                        
                        # Obtener fecha actual en Colombia
                        colombia_tz = pytz.timezone('America/Bogota')
                        fecha_actual = datetime.now(colombia_tz).date()
                        
                        # Si la reclamación se creó hoy, no escalar inmediatamente
                        if fecha_radicacion == fecha_actual:
                            return {
                                "accion": "mantener", 
                                "razon": f"Escalamiento múltiple nivel 3 PRIORIZADO recién completado hoy ({fecha_actual}). Esperando plazo."
                            }
                            
                    except Exception as e:
                        # Si hay error con fechas, continuar con lógica normal
                        logger.warning(f"Error verificando fecha de escalamiento múltiple PRIORIZADO: {e}")
            
            return _evaluar_escalamiento_priorizado(nivel_actual, tipo_actual)
        elif categoria_riesgo == "vital":
            # VERIFICACIÓN TEMPORAL: Prevenir escalamiento inmediato después de escalamiento múltiple (VITAL)
            if nivel_actual == 3 and tipo_actual == "tutela":
                from datetime import datetime, timedelta
                import pytz
                
                # Obtener fecha de la última reclamación
                fecha_radicacion = ultima_reclamacion.get('fecha_radicacion')
                
                if fecha_radicacion:
                    try:
                        # Convertir string a datetime si es necesario
                        if isinstance(fecha_radicacion, str):
                            fecha_radicacion = datetime.strptime(fecha_radicacion, '%Y-%m-%d').date()
                        elif hasattr(fecha_radicacion, 'date'):
                            fecha_radicacion = fecha_radicacion.date()
                        
                        # Obtener fecha actual en Colombia
                        colombia_tz = pytz.timezone('America/Bogota')
                        fecha_actual = datetime.now(colombia_tz).date()
                        
                        # Si la reclamación se creó hoy, no escalar inmediatamente
                        if fecha_radicacion == fecha_actual:
                            return {
                                "accion": "mantener", 
                                "razon": f"Escalamiento nivel 3 VITAL recién completado hoy ({fecha_actual}). Esperando plazo."
                            }
                            
                    except Exception as e:
                        # Si hay error con fechas, continuar con lógica normal
                        logger.warning(f"Error verificando fecha de escalamiento VITAL: {e}")
            
            return _evaluar_escalamiento_vital(nivel_actual, tipo_actual)
        
        return {"accion": "error", "razon": "Categoría de riesgo no reconocida"}
        
    except Exception as e:
        logger.error(f"Error determinando escalamiento automático: {e}")
        return {"accion": "error", "razon": f"Error técnico: {str(e)}"}


def _generar_accion_inicial(categoria_riesgo: str) -> Dict[str, Any]:
    """Genera la primera acción según la categoría de riesgo."""
    if categoria_riesgo == "vital":
        plazo = 1  # 24 horas
    else:  # simple y priorizado
        plazo = 5  # 5 días
        
    return {
        "accion": "generar",
        "tipo": "reclamacion_eps", 
        "nivel_escalamiento": 1,
        "plazo_dias": plazo,
        "razon": f"Escalamiento inicial EPS - {categoria_riesgo} (nivel 1)"
    }


def _evaluar_escalamiento_simple(nivel_actual: int, tipo_actual: str) -> Dict[str, Any]:
    """
    Escalamiento para riesgo SIMPLE:
    Nivel 1: EPS → Nivel 2: Supersalud → Nivel 3: EPS + Supersalud → Nivel 4: Tutela → Nivel 5: Desacato (repite)
    """
    if nivel_actual == 1 and tipo_actual == "reclamacion_eps":
        return {
            "accion": "generar",
            "tipo": "reclamacion_supersalud",
            "nivel_escalamiento": 2,
            "plazo_dias": 20,
            "razon": "Simple: EPS sin respuesta → Supersalud nivel 2"
        }
    elif nivel_actual == 2 and tipo_actual == "reclamacion_supersalud":
        return {
            "accion": "generar_multiple",
            "tipos": ["reclamacion_eps", "reclamacion_supersalud"],
            "nivel_escalamiento": 3,
            "plazo_dias": 20,
            "razon": "Simple: Supersalud sin respuesta → EPS+Supersalud nivel 3"
        }
    elif nivel_actual == 3 and tipo_actual in ["reclamacion_eps", "reclamacion_supersalud"]:
        return {
            "accion": "generar",
            "tipo": "tutela",
            "nivel_escalamiento": 4,
            "plazo_dias": 15,
            "razon": "Simple: EPS+Supersalud sin respuesta → Tutela nivel 4"
        }
    elif nivel_actual == 4 and tipo_actual == "tutela":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": 5,
            "plazo_dias": 10,
            "razon": "Simple: Tutela incumplida → Desacato nivel 5"
        }
    elif nivel_actual >= 5 and tipo_actual == "desacato":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": nivel_actual + 1,
            "plazo_dias": 10,
            "razon": f"Simple: Desacato previo incumplido → Desacato nivel {nivel_actual + 1}"
        }
    return {"accion": "mantener", "razon": "Simple: Situación no contemplada"}


def _evaluar_escalamiento_priorizado(nivel_actual: int, tipo_actual: str) -> Dict[str, Any]:
    """
    Escalamiento para riesgo PRIORIZADO:
    Nivel 1: EPS → Nivel 2: Supersalud → Nivel 3: EPS+Supersalud → Nivel 4: Tutela → Nivel 5: Desacato (repite)
    """
    if nivel_actual == 1 and tipo_actual == "reclamacion_eps":
        return {
            "accion": "generar",
            "tipo": "reclamacion_supersalud",
            "nivel_escalamiento": 2,
            "plazo_dias": 20,
            "razon": "Priorizado: EPS sin respuesta → Supersalud nivel 2"
        }
    elif nivel_actual == 2 and tipo_actual == "reclamacion_supersalud":
        return {
            "accion": "generar_multiple",
            "tipos": ["reclamacion_eps", "reclamacion_supersalud"],
            "nivel_escalamiento": 3,
            "plazo_dias": 20,
            "razon": "Priorizado: Supersalud sin respuesta → EPS+Supersalud nivel 3"
        }
    elif nivel_actual == 3 and tipo_actual in ["reclamacion_eps", "reclamacion_supersalud"]:
        return {
            "accion": "generar",
            "tipo": "tutela",
            "nivel_escalamiento": 4,
            "plazo_dias": 15,
            "razon": "Priorizado: EPS+Supersalud sin respuesta → Tutela nivel 4"
        }
    elif nivel_actual == 4 and tipo_actual == "tutela":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": 5,
            "plazo_dias": 10,
            "razon": "Priorizado: Tutela incumplida → Desacato nivel 5"
        }
    elif nivel_actual >= 5 and tipo_actual == "desacato":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": nivel_actual + 1,
            "plazo_dias": 10,
            "razon": f"Priorizado: Desacato previo incumplido → Desacato nivel {nivel_actual + 1}"
        }
    return {"accion": "mantener", "razon": "Priorizado: Situación no contemplada"}



def _evaluar_escalamiento_vital(nivel_actual: int, tipo_actual: str) -> Dict[str, Any]:
    """
    Escalamiento para riesgo VITAL:
    Nivel 1: EPS → Nivel 2: Supersalud → Nivel 3: Tutela → Nivel 4: Desacato (repite)
    """
    if nivel_actual == 1 and tipo_actual == "reclamacion_eps":
        return {
            "accion": "generar",
            "tipo": "reclamacion_supersalud",
            "nivel_escalamiento": 2,
            "plazo_dias": 1,
            "razon": "Vital: EPS sin respuesta (24h) → Supersalud nivel 2"
        }
    elif nivel_actual == 2 and tipo_actual == "reclamacion_supersalud":
        return {
            "accion": "generar",
            "tipo": "tutela",
            "nivel_escalamiento": 3,
            "plazo_dias": 15,
            "razon": "Vital: Supersalud sin respuesta (24h) → Tutela nivel 3"
        }
    elif nivel_actual == 3 and tipo_actual == "tutela":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": 4,
            "plazo_dias": 5,
            "razon": "Vital: Tutela incumplida → Desacato nivel 4"
        }
    elif nivel_actual >= 4 and tipo_actual == "desacato":
        return {
            "accion": "generar",
            "tipo": "desacato",
            "nivel_escalamiento": nivel_actual + 1,
            "plazo_dias": 5,
            "razon": f"Vital: Desacato previo incumplido → Desacato nivel {nivel_actual + 1}"
        }
    return {"accion": "mantener", "razon": "Vital: Situación no contemplada"}



def _ejecutar_escalamiento_especifico(patient_key: str, tipo: str) -> Dict[str, Any]:
    """
    Ejecuta un solo tipo de escalamiento usando las funciones existentes.
    ✅ MODIFICADO: Verifica datos de tutela antes de generar desacato.
    """
    try:
        if tipo == "reclamacion_eps":
            return generar_reclamacion_eps(patient_key)
        elif tipo == "reclamacion_supersalud":
            return generar_reclamacion_supersalud(patient_key)
        elif tipo == "tutela":
            return generar_tutela(patient_key)
        elif tipo == "desacato":
            # ✅ MEJORAR: Verificar datos completos antes de generar desacato
            validacion = validar_datos_tutela_completos(patient_key)
            
            if not validacion["completo"]:
                return {
                    "success": False,
                    "error": "Datos de tutela incompletos para generar desacato",
                    "requiere_recoleccion_tutela": True,
                    "campos_faltantes": validacion["campos_faltantes"]
                }
            
            return generar_desacato(patient_key, validacion["datos_existentes"])
        else:
            return {"success": False, "error": f"Tipo de escalamiento no reconocido: {tipo}"}
        
    except Exception as e:
        logger.error(f"Error ejecutando escalamiento {tipo} para {patient_key}: {e}")
        return {"success": False, "error": str(e)}


def _ejecutar_escalamiento_multiple(patient_key: str, tipos: List[str]) -> Dict[str, Any]:
    """Ejecuta múltiples tipos de escalamiento."""
    resultados = []
    exitos = 0
    
    for tipo in tipos:
        resultado = _ejecutar_escalamiento_especifico(patient_key, tipo)
        resultados.append({
            "tipo": tipo,
            "resultado": resultado
        })
        
        if resultado.get("success"):
            exitos += 1
    
    # Retornar el formato esperado por el escalamiento múltiple
    if exitos > 0:
        tipos_exitosos = [r["tipo"] for r in resultados if r["resultado"].get("success")]
        return {
            "success": True,
            "tipo": f"multiple_{'+'.join(tipos_exitosos)}",
            "total_generados": exitos,
            "total_intentados": len(tipos),
            "resultados": resultados
        }
    else:
        return {
            "success": False,
            "error": "Ningún escalamiento fue exitoso",
            "resultados": resultados
        }
    
def _verificar_datos_completos_desacato(patient_key: str) -> Dict[str, Any]:
    """
    Verifica si el paciente tiene todos los datos necesarios para desacato.
    ✅ MODIFICADO: Usa tabla tutelas simplificada.
    """
    try:
        # ✅ VERIFICACIÓN SIMPLIFICADA usando nueva función
        datos_tutela = _obtener_datos_tutela_para_desacato(patient_key)
        
        if not datos_tutela:
            return {
                "datos_completos": False,
                "campos_faltantes": ["numero_sentencia", "fecha_sentencia", "juzgado", "ciudad"],
                "error": "No se encontraron datos de tutela para desacato"
            }
        
        # ✅ Verificar que tenga los campos esenciales
        campos_requeridos = ["numero_sentencia", "juzgado", "fecha_sentencia"]
        campos_faltantes = []
        
        for campo in campos_requeridos:
            if not datos_tutela.get(campo):
                campos_faltantes.append(campo)
        
        return {
            "datos_completos": len(campos_faltantes) == 0,
            "campos_faltantes": campos_faltantes,
            "tutela_data": datos_tutela
        }
        
    except Exception as e:
        logger.error(f"Error verificando datos para desacato: {e}")
        return {
            "datos_completos": False,
            "error": f"Error técnico: {str(e)}"
        }
    
def _guardar_escalamiento_en_bd(patient_key: str, resultado: Dict, nivel_escalamiento: int, current_session_id: str) -> bool:
    """
    Guarda el resultado del escalamiento en BigQuery.
    Maneja tanto escalamientos simples como múltiples.
    """
    try:
        client = get_bigquery_client()

        # Si es escalamiento múltiple, guardar cada resultado por separado
        if resultado.get("tipo", "").startswith("multiple_") and "resultados" in resultado:
            for item in resultado["resultados"]:
                if item["resultado"].get("success"):
                    _guardar_escalamiento_individual( 
                        client, patient_key, item["resultado"], nivel_escalamiento, current_session_id
                    ) 
            return True 
        else:
            # Escalamiento simple
            return _guardar_escalamiento_individual(client, patient_key, resultado, nivel_escalamiento,current_session_id)

    except Exception as e:
        logger.error(f"Error guardando escalamiento para {patient_key}: {e}", exc_info=True) 
        return False


def _guardar_escalamiento_individual(client, patient_key: str, resultado: Dict, nivel: int, current_session_id) -> bool:
    """
    Guarda un escalamiento individual en BigQuery.
    ✅ MODIFICADO: Incluye URL del PDF automáticamente.
    """
    try:

        pdf_url = resultado.get("pdf_url", "")
        if pdf_url:
            logger.info(f"📎 Guardando escalamiento con PDF: {pdf_url}")
        else:
            logger.warning(f"⚠️ Escalamiento sin PDF para tipo: {resultado.get('tipo', 'unknown')}")

        # Escapar texto para SQL
        texto_escaped = resultado["texto_reclamacion"].replace("'", "''")
        
        # Calcular próxima fecha de revisión según el tipo
        tipo = resultado.get("tipo", resultado.get("tipo_reclamacion", ""))
        
        dias_revision = 5
        if "supersalud" in tipo:
            dias_revision = 20
        elif "tutela" in tipo:
            dias_revision = 15
        elif "desacato" in tipo:
            dias_revision = 10
        
        if "tutela" in tipo or "desacato" in tipo:
            fecha_radic_expr = "DATE_ADD(CURRENT_DATE(), INTERVAL 2 DAY)"
        else:
            fecha_radic_expr = "NULL"

        estado_update_logic = f"""
            CASE
                -- Cuando se genera desacato (nivel 5), marcar tutela (nivel 4) como escalada
                WHEN {nivel} = 5 AND r.tipo_accion = 'tutela' AND r.nivel_escalamiento = 4
                     AND r.estado_reclamacion NOT IN ('resuelto', 'escalado')
                THEN 'escalado'
                
                -- Cuando se genera tutela (nivel 4), marcar supersalud/eps previas como escaladas
                WHEN {nivel} = 4 AND r.nivel_escalamiento < 4
                     AND r.estado_reclamacion NOT IN ('resuelto', 'escalado')
                THEN 'escalado'
                
                -- Lógica general: niveles menores no resueltos pasan a escalados
                WHEN r.nivel_escalamiento < {nivel} 
                     AND r.estado_reclamacion NOT IN ('resuelto', 'escalado')
                THEN 'escalado'
                
                ELSE r.estado_reclamacion
            END
        """

        sql = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS t
            SET reclamaciones = ARRAY_CONCAT(
                ARRAY(
                    SELECT AS STRUCT
                        r.med_no_entregados,
                        r.tipo_accion,
                        r.texto_reclamacion,
                        {estado_update_logic} AS estado_reclamacion,
                        r.nivel_escalamiento,
                        r.url_documento,
                        r.numero_radicado,
                        r.fecha_radicacion,
                        r.fecha_revision,
                        r.id_session
                    FROM UNNEST(t.reclamaciones) AS r
                ),
                ARRAY(
                    SELECT AS STRUCT
                        CAST('{resultado.get("medicamentos_afectados", "")}' AS STRING) AS med_no_entregados,
                        CAST('{tipo}' AS STRING) AS tipo_accion,
                        CAST('''{texto_escaped}''' AS STRING) AS texto_reclamacion,
                        CAST('pendiente_radicacion' AS STRING) AS estado_reclamacion,
                        CAST({nivel} AS INT64) AS nivel_escalamiento,
                        CAST('{pdf_url}' AS STRING) AS url_documento,
                        CAST('' AS STRING) AS numero_radicado,
                        CAST({fecha_radic_expr} AS DATE) AS fecha_radicacion,
                        DATE_ADD(CAST({fecha_radic_expr} AS DATE), INTERVAL {dias_revision} DAY) AS fecha_revision,
                        CAST('{current_session_id}' AS STRING) AS id_session
                )
            )
            WHERE paciente_clave = '{patient_key}'
        """


        logger.info(f"fecha_radicacion {fecha_radic_expr}")
        client.query(sql).result()
        logger.info(f"✅ Escalamiento {tipo} guardado para {patient_key} en nivel {nivel} con PDF: {pdf_url}")
        return True

    except Exception as e:
        logger.error(f"❌ Error guardando escalamiento individual: {e}")
        return False

    
def _format_array_to_string(array_field) -> str:
    """Convierte arrays de BigQuery a string para uso en prompts."""
    if isinstance(array_field, list):
        return ", ".join(str(item) for item in array_field if item)
    return str(array_field) if array_field else ""

def _obtener_datos_tutela_para_desacato(patient_key: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene datos de tutela desde la tabla tutelas simplificada para generar desacato.
    
    Args:
        patient_key: Clave del paciente
        
    Returns:
        Dict con datos de tutela si existen, None si no hay datos
    """
    try:
        client = get_bigquery_client()
        
        # Consultar tabla tutelas simplificada
        query = f"""
        SELECT 
            numero_sentencia,
            fecha_sentencia,
            fecha_radicacion_tutela,
            juzgado,
            ciudad
        FROM `{PROJECT_ID}.{DATASET_ID}.tutelas`
        WHERE paciente_clave = @patient_key
        ORDER BY created_at DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("patient_key", "STRING", patient_key)
            ]
        )
        
        results = client.query(query, job_config=job_config).result()
        
        for row in results:
            return {
                "numero_sentencia": row.numero_sentencia or "", 
                "fecha_sentencia": row.fecha_sentencia.strftime("%d/%m/%Y") if row.fecha_sentencia else "",
                "fecha_radicacion_tutela": row.fecha_radicacion_tutela.strftime("%d/%m/%Y") if row.fecha_radicacion_tutela else "",
                "juzgado": row.juzgado or "",
                "ciudad": row.ciudad or ""
            }
        
        logger.info(f"No se encontraron datos de tutela para desacato: {patient_key}")
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo datos de tutela para desacato {patient_key}: {e}")
        return None

def validar_datos_tutela_completos(patient_key: str) -> Dict[str, Any]:
    """
    Valida que todos los campos de tutela estén completos para desacato.
    
    Args:
        patient_key: Clave del paciente
        
    Returns:
        Dict con información sobre campos faltantes
    """
    try:
        
        datos_tutela = _obtener_datos_tutela_para_desacato(patient_key)
        
        if not datos_tutela:
            return {
                "completo": False,
                "campos_faltantes": REQUIRED_TUTELA_FIELDS,
                "mensaje": "No se encontraron datos de tutela. Es necesario recopilar todos los campos."
            }
        
        campos_faltantes = []
        for campo in REQUIRED_TUTELA_FIELDS:
            valor = datos_tutela.get(campo)
            if not valor or (isinstance(valor, str) and not valor.strip()):
                campos_faltantes.append(campo)
        
        return {
            "completo": len(campos_faltantes) == 0,
            "campos_faltantes": campos_faltantes,
            "datos_existentes": datos_tutela,
            "mensaje": f"Faltan {len(campos_faltantes)} campos por completar" if campos_faltantes else "Todos los datos están completos"
        }
        
    except Exception as e:
        logger.error(f"Error validando datos de tutela: {e}")
        return {
            "completo": False,
            "error": str(e),
            "campos_faltantes": REQUIRED_TUTELA_FIELDS
        }
    
def determinar_tipo_reclamacion_siguiente(session_id: str) -> str:
    """
    Determina qué tipo de reclamación seguiría si el paciente acepta escalar.
    
    """
    try:
        # Usar funciones existentes sin modificarlas
        patient_key = _obtener_patient_key_por_session_id(session_id)
        if not patient_key:
            return "una nueva reclamación"
        
        datos_paciente = _obtener_datos_paciente_para_escalamiento(patient_key)
        if not datos_paciente:
            return "una nueva reclamación"
        
        decision_escalamiento = _determinar_siguiente_escalamiento_automatico(datos_paciente)
        
        if decision_escalamiento["accion"] == "generar":
            tipo = decision_escalamiento["tipo"]
            
            # Mapear tipos técnicos a texto legible
            if tipo == "reclamacion_eps":
                return "una reclamación ante tu EPS"
            elif tipo == "reclamacion_supersalud":
                return "una reclamación ante Supersalud"
            elif tipo == "tutela":
                return "una acción de tutela"
            elif tipo == "desacato":
                return "un incidente de desacato"
            
        elif decision_escalamiento["accion"] == "generar_multiple":
            tipos = decision_escalamiento.get("tipos", [])
            if "reclamacion_eps" in tipos and "reclamacion_supersalud" in tipos:
                return "reclamaciones ante tu EPS y Supersalud"
            else:
                return "múltiples reclamaciones"
        
        return "una nueva reclamación"
        
    except Exception as e:
        logger.error(f"Error determinando tipo de reclamación siguiente para session {session_id}: {e}")
        return "una nueva reclamación"