"""Pipeline didático do trabalho avaliativo de dados do CNPJ."""

from .pipeline import normalize_text, run_pipeline
from .security import mask_document

__all__ = ["mask_document", "normalize_text", "run_pipeline"]
