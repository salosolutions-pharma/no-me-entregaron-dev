import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import pytz
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER

from .cloud_storage_pip import upload_image_to_bucket, CloudStorageServiceError

logger = logging.getLogger(__name__)

COLOMBIA_TZ = pytz.timezone('America/Bogota')


class PDFGeneratorError(Exception):
    """Excepción para errores en la generación de PDFs."""
    pass


class PDFGenerator:
    """Generador de documentos PDF para tutelas."""
    
    def __init__(self, bucket_name: Optional[str] = None):
        """
        Inicializa el generador de PDFs.
        
        Args:
            bucket_name: Nombre del bucket para documentos generados
        """
        # Usar bucket de documentos generados o el de prescripciones como fallback
        self.bucket_name = bucket_name or os.getenv("BUCKET_DOCUMENTOS_GENERADOS")
        if not self.bucket_name:
            self.bucket_name = os.getenv("BUCKET_PRESCRIPCIONES")
            logger.warning("BUCKET_DOCUMENTOS_GENERADOS no configurado. Usando bucket de prescripciones.")
        
        logger.info(f"PDFGenerator inicializado con bucket: {self.bucket_name}")

        try:
            from .cloud_storage_pip import get_cloud_storage_client
            client = get_cloud_storage_client()
            bucket_id = self.bucket_name.replace("gs://", "").split("/", maxsplit=1)[0]
            bucket = client.bucket(bucket_id)
            if bucket.exists():
                logger.info(f"✅ Bucket {bucket_id} verificado exitosamente")
            else:
                logger.error(f"❌ Bucket {bucket_id} no existe o no es accesible")
        except Exception as e:
            logger.error(f"❌ Error verificando bucket: {e}")

    def _create_pdf_styles(self):
        """Crea los estilos para el documento PDF."""
        styles = getSampleStyleSheet()
        
        # Estilo para título
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=20,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        # Estilo para texto normal
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=11,
            spaceAfter=12,
            alignment=TA_JUSTIFY,
            fontName='Helvetica'
        )
        
        # Estilo para encabezados de sección
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=12,
            spaceAfter=10,
            fontName='Helvetica-Bold'
        )
        
        return {
            'title': title_style,
            'normal': normal_style,
            'heading': heading_style
        }

    def generate_tutela_pdf(self, claim_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Genera un PDF de tutela y lo sube a Cloud Storage.
        ✅ CORREGIDO: Tutela NO requiere tutela_id.
        
        Args:
            claim_data: Datos de la reclamación generada por ClaimGenerator
            
        Returns:
            Dict con información del PDF generado y su URL
        """
        try:
            if not claim_data.get("success") or claim_data.get("tipo_reclamacion") != "tutela":
                raise PDFGeneratorError("Los datos proporcionados no corresponden a una tutela válida")
            
            patient_key = claim_data.get("patient_key")
            texto_tutela = claim_data.get("texto_reclamacion", "")
            
            if not patient_key or not texto_tutela:
                raise PDFGeneratorError("Faltan datos esenciales para generar el PDF")
            
            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_path = Path(temp_file.name)
            
            try:
                # Crear documento PDF
                doc = SimpleDocTemplate(
                    str(temp_path),
                    pagesize=letter,
                    rightMargin=0.75*inch,
                    leftMargin=0.75*inch,
                    topMargin=1*inch,
                    bottomMargin=1*inch
                )
                
                # Construir contenido
                story = []
                styles = self._create_pdf_styles()
                
                # Título
                title = Paragraph("ACCIÓN DE TUTELA", styles['title'])
                story.append(title)
                story.append(Spacer(1, 20))
                
                # Contenido principal
                # Dividir el texto en párrafos
                paragraphs = texto_tutela.split('\n\n')
                for para in paragraphs:
                    if para.strip():
                        # Detectar si es un encabezado (líneas cortas en mayúsculas)
                        if (len(para.strip()) < 50 and 
                            para.strip().isupper() and 
                            not para.strip().startswith('Art')):
                            p = Paragraph(para.strip(), styles['heading'])
                        else:
                            p = Paragraph(para.strip(), styles['normal'])
                        story.append(p)
                        story.append(Spacer(1, 6))
                
                # Generar PDF
                doc.build(story)
                
                # ✅ CORREGIDO: Nombre de archivo SIN tutela_id (no es necesario para tutela)
                timestamp = datetime.now(COLOMBIA_TZ).strftime("%Y%m%d_%H%M%S")
                pdf_filename = f"tutela_{patient_key}_{timestamp}.pdf"
                logger.info(f"📎 PDF tutela generado (escalamiento automático): {pdf_filename}")
                
                try:
                    pdf_url = upload_image_to_bucket(
                        bucket_name=self.bucket_name,
                        image_path=temp_path,
                        patient_key=patient_key,
                        prefix="documentos_generados/tutelas"
                    )
                    
                    logger.info(f"PDF de tutela generado y subido: {pdf_url}")
                    logger.info(f"📎 PDF tutela listo para envío automático")
                    
                    return {
                        "success": True,
                        "pdf_url": pdf_url,
                        "pdf_filename": pdf_filename,
                        "document_type": "tutela",
                        "patient_key": patient_key,
                        "generated_at": datetime.now(COLOMBIA_TZ).isoformat(),
                        "file_size_bytes": temp_path.stat().st_size
                        # ✅ CORREGIDO: NO incluir tutela_id para tutela
                    }
                    
                except CloudStorageServiceError as e:
                    logger.error(f"Error subiendo PDF a Cloud Storage: {e}")
                    raise PDFGeneratorError(f"Error subiendo PDF: {e}")
            
            finally:
                # Limpiar archivo temporal
                if temp_path.exists():
                    temp_path.unlink()
                    
        except Exception as e:
            logger.error(f"Error generando PDF de tutela: {e}")
            return {
                "success": False,
                "error": f"Error generando PDF: {str(e)}",
                "document_type": "tutela",
                "patient_key": claim_data.get("patient_key", "unknown")
                # ✅ CORREGIDO: NO incluir tutela_id para tutela
            }
    
    def generate_desacato_pdf(self, claim_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Genera un PDF de incidente de desacato y lo sube a Cloud Storage.
        ✅ CORREGIDO: Desacato SÍ requiere tutela_id obligatorio.
        
        Args:
            claim_data: Datos del desacato generado por ClaimGenerator
            
        Returns:
            Dict con información del PDF generado y su URL
        """
        try:
            if not claim_data.get("success") or claim_data.get("tipo_reclamacion") != "desacato":
                raise PDFGeneratorError("Los datos proporcionados no corresponden a un desacato válido")
            
            patient_key = claim_data.get("patient_key")
            texto_desacato = claim_data.get("texto_reclamacion", "")
            tutela_id = claim_data.get("tutela_id")
            
            if not patient_key or not texto_desacato:
                raise PDFGeneratorError("Faltan datos esenciales para generar el PDF de desacato")
            
            # ✅ MANTENER: tutela_id es OBLIGATORIO para desacato
            if not tutela_id or not str(tutela_id).strip():
                logger.error(f"❌ tutela_id OBLIGATORIO faltante para PDF de desacato del paciente {patient_key}")
                return {
                    "success": False,
                    "error": "tutela_id es obligatorio para generar PDF de desacato",
                    "document_type": "desacato",
                    "patient_key": patient_key,
                    "tutela_id": ""
                }
            
            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_path = Path(temp_file.name)
            
            try:
                # Crear documento PDF
                doc = SimpleDocTemplate(
                    str(temp_path),
                    pagesize=letter,
                    rightMargin=0.75*inch,
                    leftMargin=0.75*inch,
                    topMargin=1*inch,
                    bottomMargin=1*inch
                )
                
                # Construir contenido
                story = []
                styles = self._create_pdf_styles()
                
                # Título
                numero_sentencia = claim_data.get("numero_sentencia_referencia", "")
                title_text = f"INCIDENTE DE DESACATO"
                if numero_sentencia:
                    title_text += f"\nACCIÓN DE TUTELA No. {numero_sentencia}"
                
                title = Paragraph(title_text, styles['title'])
                story.append(title)
                story.append(Spacer(1, 20))
                
                # Contenido principal
                # Dividir el texto en párrafos
                paragraphs = texto_desacato.split('\n\n')
                for para in paragraphs:
                    if para.strip():
                        # Detectar si es un encabezado (líneas cortas en mayúsculas o que empiecen con "Señor")
                        if (len(para.strip()) < 50 and 
                            (para.strip().isupper() or para.strip().startswith('Señor')) and 
                            not para.strip().startswith('Art')):
                            p = Paragraph(para.strip(), styles['heading'])
                        else:
                            p = Paragraph(para.strip(), styles['normal'])
                        story.append(p)
                        story.append(Spacer(1, 6))
                
                # Generar PDF
                doc.build(story)
                
                # ✅ MANTENER: Incluir tutela_id en el nombre del archivo (obligatorio para desacato)
                timestamp = datetime.now(COLOMBIA_TZ).strftime("%Y%m%d_%H%M%S")
                pdf_filename = f"desacato_{patient_key}_{tutela_id}_{timestamp}.pdf"
                logger.info(f"📎 PDF desacato con tutela_id: {tutela_id}")
                
                try:
                    pdf_url = upload_image_to_bucket(
                        bucket_name=self.bucket_name,
                        image_path=temp_path,
                        patient_key=patient_key,
                        prefix="documentos_generados/desacatos"
                    )
                    
                    logger.info(f"PDF de desacato generado y subido: {pdf_url}")
                    logger.info(f"📎 PDF desacato listo para envío automático")
                    
                    return {
                        "success": True,
                        "pdf_url": pdf_url,
                        "pdf_filename": pdf_filename,
                        "document_type": "desacato",
                        "patient_key": patient_key,
                        "tutela_id": tutela_id,  # ✅ MANTENER: Incluir tutela_id para desacato
                        "numero_sentencia_referencia": claim_data.get("numero_sentencia_referencia", ""),
                        "juzgado": claim_data.get("juzgado", ""),
                        "generated_at": datetime.now(COLOMBIA_TZ).isoformat(),
                        "file_size_bytes": temp_path.stat().st_size
                    }
                    
                except CloudStorageServiceError as e:
                    logger.error(f"Error subiendo PDF de desacato a Cloud Storage: {e}")
                    raise PDFGeneratorError(f"Error subiendo PDF: {e}")
            
            finally:
                # Limpiar archivo temporal
                if temp_path.exists():
                    temp_path.unlink()
                    
        except Exception as e:
            logger.error(f"Error generando PDF de desacato: {e}")
            return {
                "success": False,
                "error": f"Error generando PDF: {str(e)}",
                "document_type": "desacato",
                "patient_key": claim_data.get("patient_key", "unknown"),
                "tutela_id": claim_data.get("tutela_id", "")
            }

def create_pdf_generator() -> PDFGenerator:
    """Factory function para crear instancia del generador de PDFs."""
    try:
        return PDFGenerator()
    except Exception as e:
        logger.error(f"Error creando PDFGenerator: {e}")
        raise PDFGeneratorError(f"Error inicializando PDFGenerator: {e}")


# Función de conveniencia
def generar_pdf_tutela(claim_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Función de conveniencia para generar PDF de tutela.
    ✅ CORREGIDO: No maneja tutela_id para tutela.
    """
    try:
        pdf_generator = create_pdf_generator()
        result = pdf_generator.generate_tutela_pdf(claim_data)
        
        # ✅ CORREGIDO: Log sin tutela_id para tutela
        if result.get("success"):
            logger.info(f"✅ PDF tutela generado exitosamente")
        
        return result
    except Exception as e:
        logger.error(f"Error en función de conveniencia para PDF tutela: {e}")
        return {
            "success": False,
            "error": f"Error generando PDF: {str(e)}",
            "document_type": "tutela"
            # ✅ CORREGIDO: NO incluir tutela_id para tutela
        }
    
# Función de conveniencia para desacato
def generar_pdf_desacato(claim_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Función de conveniencia para generar PDF de desacato.
    ✅ MANTENER: Maneja tutela_id obligatorio para desacato.
    """
    try:
        pdf_generator = create_pdf_generator()
        result = pdf_generator.generate_desacato_pdf(claim_data)
        
        # ✅ MANTENER: Log con tutela_id para desacato
        if result.get("success") and result.get("tutela_id"):
            logger.info(f"✅ PDF desacato generado con tutela_id: {result['tutela_id']}")
        
        return result
    except Exception as e:
        logger.error(f"Error en función de conveniencia para PDF desacato: {e}")
        return {
            "success": False,
            "error": f"Error generando PDF: {str(e)}",
            "document_type": "desacato",
            "tutela_id": claim_data.get("tutela_id", "") if claim_data else ""
        }