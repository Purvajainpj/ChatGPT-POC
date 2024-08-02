import io
from typing import Optional
from azure.ai.documentintelligence import DocumentIntelligenceClient
import PyPDF2
from langchain_community.document_loaders import AzureAIDocumentIntelligenceLoader
from langchain.text_splitter import MarkdownHeaderTextSplitter
import os
# load logging
from utils.ml_logging import get_logger
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import DocumentAnalysisFeature
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

logger = get_logger()

doc_intell_key=os.getenv("DOC_INTELL_KEY")
doc_intell_endpoint = os.getenv("DOC_INTELL_ENDPOINT")



def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> Optional[str]:
    """
    Extracts text from a PDF file provided as a bytes object.

    :param pdf_bytes: Bytes object containing the PDF file data.
    :return: Extracted text from the PDF as a string, or None if extraction fails.
    """
    try:
        document_analysis_client = DocumentAnalysisClient(endpoint='https://sharepoint-doc.cognitiveservices.azure.com/', credential=AzureKeyCredential('08f959b49e79475994dae17bed649037'))
        text = []
        poller = document_analysis_client.begin_analyze_document(
        "prebuilt-document", document=pdf_bytes
    )
        result=poller.result()
        for page in result.pages:
            for line in page.lines:
                text.append(line.content)
        extracted_text = "\n".join(text)        
        return extracted_text
    except Exception as e:
        logger.error(f"An unexpected error occurred during PDF text extraction: {e}")

    return None


def extract_text_from_docx_bytes(docx_bytes: bytes) -> Optional[str]:
    try:
        document_analysis_client = DocumentAnalysisClient(
            endpoint='https://sharepoint-doc.cognitiveservices.azure.com/',
            credential=AzureKeyCredential('08f959b49e79475994dae17bed649037')
        )
        
        poller = document_analysis_client.begin_analyze_document(
            "prebuilt-document", document=docx_bytes
        )
        result = poller.result()
        
        text = []
        for page in result.pages:
            for line in page.lines:
                text.append(line.content)
        
        extracted_text = "\n".join(text)
        
        return extracted_text
    except Exception as e:
        logger.error(f"An unexpected error occurred during Word document text extraction: {e}")
        return None