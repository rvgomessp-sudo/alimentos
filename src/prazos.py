"""
Módulo de Extração e Cálculo de Prazos Processuais.

Identifica expressões de prazo em textos jurídicos e calcula
datas-limite considerando dias úteis e feriados forenses.
"""

import re
from datetime import datetime, timedelta
from dataclasses import dataclass

from dateutil.parser import parse as parse_date


@dataclass
class PrazoProcessual:
    """Representa um prazo processual extraído do texto."""
    tipo: str
    dias: int
    unidade: str  # "dias_uteis", "dias_corridos", "horas"
    texto_original: str
    data_inicio: datetime = None
    data_limite: datetime = None
    contexto: str = ""


# Padrões para identificar expressões de prazo
PADROES_PRAZO = [
    # "prazo de X dias"
    re.compile(
        r"prazo\s+de\s+(\d+)\s*\(?dias?\)?\s*(úteis|corridos|úte is)?",
        re.IGNORECASE,
    ),
    # "no prazo de X dias"
    re.compile(
        r"no\s+prazo\s+de\s+(\d+)\s*\(?dias?\)?\s*(úteis|corridos)?",
        re.IGNORECASE,
    ),
    # "em X dias"
    re.compile(
        r"em\s+(\d+)\s*\(?dias?\)?\s*(úteis|corridos)?",
        re.IGNORECASE,
    ),
    # "X dias para"
    re.compile(
        r"(\d+)\s*\(?dias?\)?\s*(úteis|corridos)?\s+para",
        re.IGNORECASE,
    ),
    # "prazo de X horas"
    re.compile(
        r"prazo\s+de\s+(\d+)\s*horas?",
        re.IGNORECASE,
    ),
    # "48 horas" ou "24 horas"
    re.compile(
        r"(\d+)\s*horas?\s+(?:para|após|a\s+contar)",
        re.IGNORECASE,
    ),
]

# Prazos legais comuns em processos de alimentos
PRAZOS_LEGAIS = {
    "contestacao": {"dias": 15, "unidade": "dias_uteis"},
    "recurso_apelacao": {"dias": 15, "unidade": "dias_uteis"},
    "recurso_agravo": {"dias": 15, "unidade": "dias_uteis"},
    "embargos_declaracao": {"dias": 5, "unidade": "dias_uteis"},
    "impugnacao_cumprimento": {"dias": 15, "unidade": "dias_uteis"},
    "pagamento_voluntario": {"dias": 3, "unidade": "dias_uteis"},
    "prisao_civil_justificativa": {"dias": 3, "unidade": "dias_uteis"},
    "manifestacao_generica": {"dias": 5, "unidade": "dias_uteis"},
    "cumprimento_ordem": {"dias": 48, "unidade": "horas"},
}

# Feriados nacionais brasileiros (base, sem considerar feriados estaduais)
FERIADOS_NACIONAIS = [
    (1, 1),    # Confraternização Universal
    (4, 21),   # Tiradentes
    (5, 1),    # Dia do Trabalho
    (9, 7),    # Independência do Brasil
    (10, 12),  # Nossa Senhora Aparecida
    (11, 2),   # Finados
    (11, 15),  # Proclamação da República
    (12, 25),  # Natal
]


def _eh_feriado(data, feriados_extras=None):
    """Verifica se uma data é feriado nacional."""
    if (data.month, data.day) in FERIADOS_NACIONAIS:
        return True
    if feriados_extras:
        return data.date() in feriados_extras
    return False


def _eh_dia_util(data, feriados_extras=None):
    """Verifica se uma data é dia útil (não é fim de semana nem feriado)."""
    if data.weekday() >= 5:  # Sábado(5) ou Domingo(6)
        return False
    return not _eh_feriado(data, feriados_extras)


def calcular_prazo_dias_uteis(data_inicio, dias, feriados_extras=None):
    """
    Calcula a data limite considerando dias úteis.

    Args:
        data_inicio: Data de início da contagem.
        dias: Número de dias úteis.
        feriados_extras: Lista opcional de datas adicionais de feriados.

    Returns:
        Data limite (datetime).
    """
    # Primeiro dia útil subsequente (início da contagem conforme CPC)
    data_atual = data_inicio + timedelta(days=1)
    while not _eh_dia_util(data_atual, feriados_extras):
        data_atual += timedelta(days=1)

    dias_contados = 0
    while dias_contados < dias:
        if _eh_dia_util(data_atual, feriados_extras):
            dias_contados += 1
            if dias_contados == dias:
                break
        data_atual += timedelta(days=1)

    return data_atual


def calcular_prazo_horas(data_inicio, horas):
    """
    Calcula a data limite em horas.

    Args:
        data_inicio: Data e hora de início.
        horas: Número de horas.

    Returns:
        Data e hora limite (datetime).
    """
    return data_inicio + timedelta(hours=horas)


def extrair_prazos(texto):
    """
    Extrai menções a prazos do texto jurídico.

    Args:
        texto: Texto do documento.

    Returns:
        Lista de PrazoProcessual encontrados.
    """
    prazos = []

    for padrao in PADROES_PRAZO:
        for match in padrao.finditer(texto):
            grupos = match.groups()
            dias = int(grupos[0])

            # Determinar unidade
            if "hora" in match.group().lower():
                unidade = "horas"
            elif len(grupos) > 1 and grupos[1] and "corrido" in grupos[1].lower():
                unidade = "dias_corridos"
            else:
                unidade = "dias_uteis"

            # Contexto ao redor
            inicio = max(0, match.start() - 60)
            fim = min(len(texto), match.end() + 60)

            prazos.append(PrazoProcessual(
                tipo=_inferir_tipo_prazo(texto[inicio:fim]),
                dias=dias,
                unidade=unidade,
                texto_original=match.group().strip(),
                contexto=texto[inicio:fim].strip(),
            ))

    return prazos


def _inferir_tipo_prazo(contexto):
    """Tenta inferir o tipo de prazo pelo contexto."""
    contexto_lower = contexto.lower()

    mapeamento = {
        "contestar": "contestacao",
        "contestação": "contestacao",
        "apelar": "recurso_apelacao",
        "apelação": "recurso_apelacao",
        "agravo": "recurso_agravo",
        "embargos": "embargos_declaracao",
        "impugnar": "impugnacao_cumprimento",
        "impugnação": "impugnacao_cumprimento",
        "pagar": "pagamento_voluntario",
        "pagamento": "pagamento_voluntario",
        "manifestar": "manifestacao_generica",
        "manifestação": "manifestacao_generica",
        "justificar": "prisao_civil_justificativa",
        "justificativa": "prisao_civil_justificativa",
        "cumprir": "cumprimento_ordem",
        "cumprimento": "cumprimento_ordem",
    }

    for chave, tipo in mapeamento.items():
        if chave in contexto_lower:
            return tipo

    return "generico"


def extrair_datas(texto):
    """
    Extrai datas mencionadas no texto.

    Args:
        texto: Texto do documento.

    Returns:
        Lista de dicionários com data parseada e contexto.
    """
    padrao_data = re.compile(r"\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}")
    datas = []

    for match in padrao_data.finditer(texto):
        try:
            data = parse_date(match.group(), dayfirst=True)
            inicio = max(0, match.start() - 40)
            fim = min(len(texto), match.end() + 40)
            datas.append({
                "data_texto": match.group(),
                "data_parseada": data,
                "contexto": texto[inicio:fim].strip(),
            })
        except (ValueError, OverflowError):
            continue

    return datas


def gerar_alertas_prazo(prazos, data_referencia=None):
    """
    Gera alertas para prazos próximos do vencimento.

    Args:
        prazos: Lista de PrazoProcessual com datas calculadas.
        data_referencia: Data de referência (default: hoje).

    Returns:
        Lista de alertas com nível de urgência.
    """
    if data_referencia is None:
        data_referencia = datetime.now()

    alertas = []
    for prazo in prazos:
        if prazo.data_limite is None:
            continue

        dias_restantes = (prazo.data_limite - data_referencia).days

        if dias_restantes < 0:
            nivel = "VENCIDO"
        elif dias_restantes == 0:
            nivel = "VENCE_HOJE"
        elif dias_restantes <= 2:
            nivel = "CRITICO"
        elif dias_restantes <= 5:
            nivel = "URGENTE"
        else:
            nivel = "NORMAL"

        alertas.append({
            "prazo": prazo,
            "dias_restantes": dias_restantes,
            "nivel": nivel,
            "data_limite": prazo.data_limite.strftime("%d/%m/%Y"),
        })

    return sorted(alertas, key=lambda x: x["dias_restantes"])
