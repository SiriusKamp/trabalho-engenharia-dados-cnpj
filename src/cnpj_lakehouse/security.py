import re


def mask_document(value: str | None) -> str:
    """Oculta um CPF/CNPJ, preservando somente os quatro últimos dígitos."""

    digits = re.sub(r"\D", "", value or "")
    if not digits:
        return ""
    if len(digits) <= 4:
        return "*" * len(digits)
    return "*" * (len(digits) - 4) + digits[-4:]
