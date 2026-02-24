"""
Módulo de Extração de Texto de Documentos PDF.

Suporta extração direta de texto via pdfplumber e fallback para OCR
(pytesseract) quando o PDF contém imagens escaneadas.
"""

import os
from pathlib import Path

import pdfplumber
import pandas as pd

try:
    import pytesseract
    from PIL import Image
    OCR_DISPONIVEL = True
except ImportError:
    OCR_DISPONIVEL = False


def extrair_texto_pagina(pagina):
    """Extrai texto de uma página do pdfplumber. Retorna string."""
    texto = pagina.extract_text()
    return texto if texto and texto.strip() else None


def extrair_texto_ocr(pagina):
    """Converte página em imagem e aplica OCR com pytesseract."""
    if not OCR_DISPONIVEL:
        return None
    imagem = pagina.to_image(resolution=300).original
    texto = pytesseract.image_to_string(imagem, lang="por")
    return texto if texto and texto.strip() else None


def extrair_texto_pdf(caminho_pdf):
    """
    Extrai texto de um arquivo PDF.

    Tenta extração direta primeiro. Se a página não contiver texto
    selecionável, faz fallback para OCR.

    Args:
        caminho_pdf: Caminho para o arquivo PDF.

    Returns:
        Lista de dicionários com 'pagina' (int) e 'texto' (str).
    """
    caminho = Path(caminho_pdf)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_pdf}")

    resultados = []

    with pdfplumber.open(caminho) as pdf:
        for i, pagina in enumerate(pdf.pages, start=1):
            texto = extrair_texto_pagina(pagina)

            if texto is None:
                texto = extrair_texto_ocr(pagina)

            if texto is None:
                texto = ""

            resultados.append({
                "pagina": i,
                "texto": texto.strip(),
                "metodo": "texto_direto" if extrair_texto_pagina(pagina) else "ocr",
            })

    return resultados


def extrair_para_dataframe(caminho_pdf):
    """
    Extrai texto do PDF e retorna como DataFrame do Pandas.

    Args:
        caminho_pdf: Caminho para o arquivo PDF.

    Returns:
        pandas.DataFrame com colunas: pagina, texto, metodo, arquivo.
    """
    resultados = extrair_texto_pdf(caminho_pdf)
    df = pd.DataFrame(resultados)
    df["arquivo"] = os.path.basename(caminho_pdf)
    return df


def extrair_tabelas_pdf(caminho_pdf):
    """
    Extrai tabelas encontradas no PDF.

    Args:
        caminho_pdf: Caminho para o arquivo PDF.

    Returns:
        Lista de DataFrames, um por tabela encontrada.
    """
    caminho = Path(caminho_pdf)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_pdf}")

    tabelas = []
    with pdfplumber.open(caminho) as pdf:
        for i, pagina in enumerate(pdf.pages, start=1):
            tabelas_pagina = pagina.extract_tables()
            for j, tabela in enumerate(tabelas_pagina):
                if tabela:
                    df = pd.DataFrame(tabela[1:], columns=tabela[0])
                    df["pagina_origem"] = i
                    df["tabela_num"] = j + 1
                    tabelas.append(df)

    return tabelas


def texto_completo(caminho_pdf):
    """Retorna todo o texto do PDF como uma única string."""
    resultados = extrair_texto_pdf(caminho_pdf)
    return "\n\n".join(r["texto"] for r in resultados if r["texto"])
