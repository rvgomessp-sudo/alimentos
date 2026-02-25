"""
Módulo de Reconhecimento de Entidades Nomeadas (NER) para textos jurídicos.

Identifica entidades como:
- Partes do processo (exequente, executado, requerente, requerido)
- Juízes e desembargadores
- Advogados (OAB)
- Valores monetários
- Números de processo
- Datas
- Varas e tribunais
"""

import re
from dataclasses import dataclass, field

try:
    import spacy
    SPACY_DISPONIVEL = True
except ImportError:
    SPACY_DISPONIVEL = False


@dataclass
class EntidadeJuridica:
    """Representa uma entidade extraída do texto jurídico."""
    tipo: str
    valor: str
    posicao_inicio: int = 0
    posicao_fim: int = 0
    contexto: str = ""


# Padrões regex para entidades jurídicas brasileiras
PADROES = {
    "numero_processo": re.compile(
        r"\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}"
    ),
    "valor_monetario": re.compile(
        r"R\$\s*[\d.,]+(?:\s*(?:mil|milhão|milhões|bilhão|bilhões))?"
    ),
    "data": re.compile(
        r"\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}"
    ),
    "oab": re.compile(
        r"OAB[/\s]*(?:[A-Z]{2})[/\s]*[\d.]+",
        re.IGNORECASE,
    ),
    "cpf": re.compile(
        r"\d{3}\.\d{3}\.\d{3}-\d{2}"
    ),
    "cnpj": re.compile(
        r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"
    ),
}

# Termos que indicam papéis processuais
PAPEIS_PROCESSUAIS = {
    "exequente": [
        "exequente", "exeqüente", "credor", "credora",
        "requerente", "autor", "autora", "alimentando",
        "alimentanda", "representante legal",
    ],
    "executado": [
        "executado", "executada", "devedor", "devedora",
        "requerido", "requerida", "réu", "ré",
        "alimentante",
    ],
    "juiz": [
        "juiz", "juíza", "juiz de direito", "juíza de direito",
        "desembargador", "desembargadora", "magistrado", "magistrada",
        "meritíssimo", "meritíssima", "mm. juiz", "mm. juíza",
    ],
    "advogado": [
        "advogado", "advogada", "patrono", "patrona",
        "procurador", "procuradora", "defensor", "defensora",
    ],
    "promotor": [
        "promotor", "promotora", "ministério público",
        "mp", "parquet",
    ],
}

# Tipos de atos processuais
ATOS_PROCESSUAIS = {
    "decisao": [
        "decisão", "decisão interlocutória", "despacho decisório",
        "tutela", "tutela de urgência", "liminar", "antecipação de tutela",
    ],
    "sentenca": [
        "sentença", "julgamento", "dispositivo", "julgo procedente",
        "julgo improcedente", "julgo parcialmente procedente",
    ],
    "despacho": [
        "despacho", "despacho de mero expediente", "cite-se",
        "intime-se", "manifeste-se", "dê-se vista",
    ],
    "peticao": [
        "petição", "petição inicial", "contestação", "réplica",
        "impugnação", "embargos", "recurso", "agravo", "apelação",
    ],
    "intimacao": [
        "intimação", "notificação", "citação", "mandado",
    ],
    "certidao": [
        "certidão", "certidão negativa", "certidão positiva",
    ],
}


def extrair_entidades_regex(texto):
    """
    Extrai entidades usando padrões regex.

    Args:
        texto: Texto do documento jurídico.

    Returns:
        Lista de EntidadeJuridica encontradas.
    """
    entidades = []

    for tipo, padrao in PADROES.items():
        for match in padrao.finditer(texto):
            inicio = max(0, match.start() - 50)
            fim = min(len(texto), match.end() + 50)
            entidades.append(EntidadeJuridica(
                tipo=tipo,
                valor=match.group().strip(),
                posicao_inicio=match.start(),
                posicao_fim=match.end(),
                contexto=texto[inicio:fim].strip(),
            ))

    return entidades


def identificar_papeis(texto):
    """
    Identifica partes processuais mencionadas no texto.

    Busca nomes próprios próximos a termos indicadores de papel.

    Args:
        texto: Texto do documento jurídico.

    Returns:
        Dicionário com papel -> lista de menções encontradas.
    """
    texto_lower = texto.lower()
    papeis_encontrados = {}

    for papel, termos in PAPEIS_PROCESSUAIS.items():
        mencoes = []
        for termo in termos:
            pos = 0
            while True:
                idx = texto_lower.find(termo, pos)
                if idx == -1:
                    break
                # Captura contexto ao redor (100 chars após o termo)
                contexto_inicio = max(0, idx - 20)
                contexto_fim = min(len(texto), idx + len(termo) + 100)
                contexto = texto[contexto_inicio:contexto_fim].strip()
                mencoes.append({
                    "termo": termo,
                    "posicao": idx,
                    "contexto": contexto,
                })
                pos = idx + 1

        if mencoes:
            papeis_encontrados[papel] = mencoes

    return papeis_encontrados


def identificar_atos(texto):
    """
    Identifica tipos de atos processuais no texto.

    Args:
        texto: Texto do documento jurídico.

    Returns:
        Dicionário com tipo_ato -> lista de ocorrências.
    """
    texto_lower = texto.lower()
    atos_encontrados = {}

    for tipo_ato, termos in ATOS_PROCESSUAIS.items():
        ocorrencias = []
        for termo in termos:
            pos = 0
            while True:
                idx = texto_lower.find(termo, pos)
                if idx == -1:
                    break
                contexto_inicio = max(0, idx - 30)
                contexto_fim = min(len(texto), idx + len(termo) + 80)
                ocorrencias.append({
                    "termo": termo,
                    "posicao": idx,
                    "contexto": texto[contexto_inicio:contexto_fim].strip(),
                })
                pos = idx + 1

        if ocorrencias:
            atos_encontrados[tipo_ato] = ocorrencias

    return atos_encontrados


def extrair_com_spacy(texto, modelo="pt_core_news_lg"):
    """
    Extrai entidades usando modelo spaCy.

    Args:
        texto: Texto do documento jurídico.
        modelo: Nome do modelo spaCy a utilizar.

    Returns:
        Lista de EntidadeJuridica ou None se spaCy não estiver disponível.
    """
    if not SPACY_DISPONIVEL:
        return None

    try:
        nlp = spacy.load(modelo)
    except OSError:
        return None

    doc = nlp(texto)
    entidades = []

    for ent in doc.ents:
        entidades.append(EntidadeJuridica(
            tipo=ent.label_,
            valor=ent.text,
            posicao_inicio=ent.start_char,
            posicao_fim=ent.end_char,
            contexto=texto[max(0, ent.start_char - 30):ent.end_char + 30].strip(),
        ))

    return entidades


def analisar_documento(texto):
    """
    Análise completa de um documento jurídico.

    Combina regex, identificação de papéis, atos e spaCy (se disponível).

    Args:
        texto: Texto completo do documento.

    Returns:
        Dicionário com todas as entidades e informações extraídas.
    """
    resultado = {
        "entidades_regex": extrair_entidades_regex(texto),
        "papeis_processuais": identificar_papeis(texto),
        "atos_processuais": identificar_atos(texto),
        "entidades_spacy": extrair_com_spacy(texto),
    }

    return resultado
