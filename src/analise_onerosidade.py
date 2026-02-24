"""
Módulo de Análise de Onerosidade Processual.

Classifica atos processuais por autoria, calcula métricas de
protelação, efetividade e comportamento de cada parte ao longo
do processo.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dateutil.parser import parse as parse_date


# Partes do caso com identificadores
PARTES = {
    "exequente": {
        "nome": "Nelson Wilson Fonseca Costa (Jairo)",
        "cpf": "447.726.258-28",
        "identificadores": [
            "exequente", "exeqüente", "credor", "credora",
            "nelson", "fonseca costa", "alimentando",
            "447.726.258-28", "requerente",
        ],
    },
    "executado": {
        "nome": "Leandro Lopes da Costa",
        "cpf": "259.171.178-02",
        "identificadores": [
            "executado", "executada", "devedor", "devedora",
            "leandro", "lopes da costa", "alimentante",
            "259.171.178-02", "requerido",
        ],
    },
    "terceiro_jair": {
        "nome": "Jair Ribeiro da Costa",
        "cpf": "614.786.528-53",
        "identificadores": [
            "jair", "ribeiro da costa", "614.786.528-53",
            "terceiro", "sócio retirante",
        ],
    },
    "juizo": {
        "nome": "Juízo da 5a Vara de Família - N. Sra. do Ó",
        "cpf": None,
        "identificadores": [
            "juiz", "juíza", "magistrado", "magistrada",
            "decisão", "despacho", "sentença", "determino",
            "defiro", "indefiro", "cite-se", "intime-se",
            "manifeste-se", "dê-se vista", "cumpra-se",
        ],
    },
    "ministerio_publico": {
        "nome": "Ministério Público",
        "cpf": None,
        "identificadores": [
            "promotor", "promotora", "ministério público", "mp", "parquet",
        ],
    },
}

# Classificação de atos por natureza
ATOS_PROTELATARIOS = [
    "embargos de declaração", "embargos", "pedido de prazo",
    "pedido de dilação", "pedido de reconsideração",
    "agravo", "exceção de incompetência", "exceção de suspeição",
    "impugnação ao valor", "pedido de vista",
    "pedido de substituição", "pedido de parcelamento",
]

ATOS_CONSTRICAO = [
    "penhora", "bloqueio", "sisbajud", "bacenjud", "renajud",
    "infojud", "arresto", "sequestro", "busca e apreensão",
    "prisão civil",
]

ATOS_RECURSO = [
    "agravo de instrumento", "agravo", "apelação", "recurso",
    "embargos de declaração", "embargos", "recurso especial",
    "recurso extraordinário",
]


class AnaliseOnerosidade:
    """Analisa comportamento processual e onerosidade de cada parte."""

    def __init__(self, caminhos_json):
        """
        Args:
            caminhos_json: Lista de caminhos para JSONs de análise.
        """
        self.dados = {}
        for caminho in caminhos_json:
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)
            chave = Path(caminho).stem.replace("_analise", "")
            self.dados[chave] = dados

        self.eventos = self._construir_eventos_unificados()

    def _construir_eventos_unificados(self):
        """Monta lista unificada de eventos com autoria atribuída."""
        eventos = []
        for chave, dados in self.dados.items():
            for d in dados.get("datas", []):
                try:
                    dt = parse_date(d["data_parseada"])
                    if not (1990 <= dt.year <= 2030):
                        continue
                except (ValueError, OverflowError):
                    continue

                contexto = d["contexto"]
                autor = self._atribuir_autoria(contexto)
                tipo_ato = self._classificar_ato(contexto)

                eventos.append({
                    "data": dt,
                    "data_texto": d["data_texto"],
                    "contexto": contexto,
                    "processo": chave,
                    "autor": autor,
                    "tipo_ato": tipo_ato,
                })

        eventos.sort(key=lambda e: e["data"])
        return eventos

    def _atribuir_autoria(self, contexto):
        """Atribui autoria de um ato pelo contexto textual."""
        ctx = contexto.lower()

        # Prioriza juízo (decisões/despachos costumam ter termos específicos)
        termos_juizo = ["decisão", "despacho", "sentença", "determino", "defiro", "indefiro",
                        "cite-se", "intime-se", "cumpra-se", "julgo"]
        if any(t in ctx for t in termos_juizo):
            return "juizo"

        # Verifica cada parte
        scores = {}
        for parte, info in PARTES.items():
            if parte == "juizo":
                continue
            score = sum(1 for ident in info["identificadores"] if ident in ctx)
            if score > 0:
                scores[parte] = score

        if scores:
            return max(scores, key=scores.get)

        return "indefinido"

    def _classificar_ato(self, contexto):
        """Classifica o tipo de ato processual."""
        ctx = contexto.lower()
        if any(t in ctx for t in ["petição", "peticiona", "requer"]):
            return "peticao"
        if any(t in ctx for t in ["decisão", "decido", "defiro", "indefiro"]):
            return "decisao"
        if any(t in ctx for t in ["sentença", "julgo"]):
            return "sentenca"
        if any(t in ctx for t in ["certidão", "certifico"]):
            return "certidao"
        if any(t in ctx for t in ["intimação", "intimado"]):
            return "intimacao"
        if any(t in ctx for t in ATOS_RECURSO):
            return "recurso"
        if any(t in ctx for t in ATOS_CONSTRICAO):
            return "constricao"
        if any(t in ctx for t in ["despacho"]):
            return "despacho"
        return "outros"

    def calcular_metricas_por_parte(self):
        """
        Calcula métricas processuais por parte.

        Métricas:
        - Total de petições
        - Tempo médio entre intimação e resposta
        - Recursos interpostos
        - Períodos de omissão (>60 dias sem manifestação)
        """
        metricas = {}

        for parte in PARTES:
            eventos_parte = [e for e in self.eventos if e["autor"] == parte]
            if not eventos_parte:
                metricas[parte] = {
                    "nome": PARTES[parte]["nome"],
                    "total_atos": 0,
                    "peticoes": 0,
                    "recursos": 0,
                    "outros": 0,
                    "periodos_omissao_60d": [],
                    "tempo_medio_entre_atos_dias": None,
                    "primeiro_ato": None,
                    "ultimo_ato": None,
                }
                continue

            peticoes = [e for e in eventos_parte if e["tipo_ato"] == "peticao"]
            recursos = [e for e in eventos_parte if e["tipo_ato"] == "recurso"]

            # Calcular gaps
            gaps = []
            for i in range(1, len(eventos_parte)):
                delta = (eventos_parte[i]["data"] - eventos_parte[i - 1]["data"]).days
                if delta > 0:
                    gaps.append({
                        "de": eventos_parte[i - 1]["data"].strftime("%d/%m/%Y"),
                        "ate": eventos_parte[i]["data"].strftime("%d/%m/%Y"),
                        "dias": delta,
                        "contexto_antes": eventos_parte[i - 1]["contexto"][:80],
                        "contexto_depois": eventos_parte[i]["contexto"][:80],
                    })

            omissoes = [g for g in gaps if g["dias"] > 60]
            tempo_medio = sum(g["dias"] for g in gaps) / len(gaps) if gaps else None

            metricas[parte] = {
                "nome": PARTES[parte]["nome"],
                "total_atos": len(eventos_parte),
                "peticoes": len(peticoes),
                "recursos": len(recursos),
                "outros": len(eventos_parte) - len(peticoes) - len(recursos),
                "periodos_omissao_60d": sorted(omissoes, key=lambda x: x["dias"], reverse=True)[:10],
                "total_omissoes_60d": len(omissoes),
                "tempo_medio_entre_atos_dias": round(tempo_medio, 1) if tempo_medio else None,
                "primeiro_ato": eventos_parte[0]["data"].strftime("%d/%m/%Y"),
                "ultimo_ato": eventos_parte[-1]["data"].strftime("%d/%m/%Y"),
            }

        return metricas

    def calcular_indice_protelacao(self):
        """
        Calcula o índice de protelação do executado.

        Fórmula:
            (recursos protelatórios + pedidos de prazo + certidões negativas) / total de atos

        Score de 0 a 1, onde 1 indica máxima protelação.
        """
        eventos_executado = [e for e in self.eventos if e["autor"] == "executado"]
        if not eventos_executado:
            return {
                "indice": 0.0,
                "classificacao": "SEM_DADOS",
                "detalhes": {},
            }

        total_atos = len(eventos_executado)

        # Contar atos protelatórios
        atos_prot = 0
        detalhes_prot = []
        for ev in eventos_executado:
            ctx = ev["contexto"].lower()
            for termo in ATOS_PROTELATARIOS:
                if termo in ctx:
                    atos_prot += 1
                    detalhes_prot.append({
                        "data": ev["data"].strftime("%d/%m/%Y"),
                        "tipo": termo,
                        "contexto": ev["contexto"][:100],
                    })
                    break

        # Contar certidões negativas (bloqueios sem resultado)
        cert_negativas = sum(
            1 for ev in self.eventos
            if "negativ" in ev["contexto"].lower()
            and ev["autor"] in ("executado", "indefinido")
        )

        numerador = atos_prot + cert_negativas
        indice = numerador / total_atos if total_atos > 0 else 0.0

        return {
            "indice": round(indice, 4),
            "classificacao": (
                "ALTISSIMO" if indice >= 0.5
                else "ALTO" if indice >= 0.3
                else "MODERADO" if indice >= 0.15
                else "BAIXO" if indice >= 0.05
                else "MINIMO"
            ),
            "detalhes": {
                "total_atos_executado": total_atos,
                "atos_protelatarios": atos_prot,
                "certidoes_negativas": cert_negativas,
                "exemplos_protelacao": detalhes_prot[:15],
            },
        }

    def calcular_indice_efetividade_exequente(self):
        """
        Calcula o índice de efetividade do exequente.

        Fórmula:
            bloqueios exitosos / total de tentativas de constrição

        Score de 0 a 1, onde 1 indica máxima efetividade.
        """
        # Buscar tentativas de constrição e resultados
        tentativas = []
        exitosos = []

        for ev in self.eventos:
            ctx = ev["contexto"].lower()
            if any(t in ctx for t in ATOS_CONSTRICAO):
                tentativas.append(ev)
                # Verificar se teve resultado positivo
                if any(t in ctx for t in [
                    "valor bloqueado", "penhora realizada", "encontrado",
                    "bloqueio realizado", "transferido", "apreendido",
                    "deferido", "r$",
                ]):
                    exitosos.append(ev)

        total_tentativas = len(tentativas)
        total_exitosos = len(exitosos)

        if total_tentativas == 0:
            indice = 0.0
        else:
            indice = total_exitosos / total_tentativas

        return {
            "indice": round(indice, 4),
            "classificacao": (
                "ALTA" if indice >= 0.5
                else "MODERADA" if indice >= 0.25
                else "BAIXA" if indice >= 0.1
                else "MUITO_BAIXA"
            ),
            "detalhes": {
                "total_tentativas_constricao": total_tentativas,
                "constricoes_exitosas": total_exitosos,
                "constricoes_frustradas": total_tentativas - total_exitosos,
                "exemplos_tentativas": [
                    {
                        "data": e["data"].strftime("%d/%m/%Y"),
                        "contexto": e["contexto"][:100],
                    }
                    for e in tentativas[:15]
                ],
            },
        }

    def calcular_tempo_resposta(self):
        """
        Calcula tempo médio entre intimações e respostas do executado.

        Busca pares intimação->manifestação do executado.
        """
        intimacoes = [
            e for e in self.eventos
            if e["tipo_ato"] == "intimacao"
            and "executado" in e["contexto"].lower()
        ]
        respostas_executado = [
            e for e in self.eventos
            if e["autor"] == "executado"
            and e["tipo_ato"] in ("peticao", "recurso")
        ]

        pares = []
        for intim in intimacoes:
            # Buscar próxima resposta do executado após a intimação
            for resp in respostas_executado:
                delta = (resp["data"] - intim["data"]).days
                if 0 < delta <= 180:  # Aceitar até 180 dias
                    pares.append({
                        "intimacao_data": intim["data"].strftime("%d/%m/%Y"),
                        "resposta_data": resp["data"].strftime("%d/%m/%Y"),
                        "dias": delta,
                        "intimacao_contexto": intim["contexto"][:80],
                        "resposta_contexto": resp["contexto"][:80],
                    })
                    break  # Pegar apenas a primeira resposta

        if not pares:
            return {
                "tempo_medio_dias": None,
                "mediana_dias": None,
                "total_pares": 0,
                "sem_resposta": len(intimacoes),
            }

        tempos = sorted([p["dias"] for p in pares])
        tempo_medio = sum(tempos) / len(tempos)
        mediana = tempos[len(tempos) // 2]

        return {
            "tempo_medio_dias": round(tempo_medio, 1),
            "mediana_dias": mediana,
            "total_pares": len(pares),
            "sem_resposta": len(intimacoes) - len(pares),
            "mais_rapida_dias": tempos[0],
            "mais_lenta_dias": tempos[-1],
            "detalhes": pares[:10],
        }

    def gerar_dados_graficos(self):
        """Gera dados estruturados para visualização gráfica."""
        # Atos por parte
        atos_por_parte = defaultdict(int)
        for ev in self.eventos:
            atos_por_parte[ev["autor"]] += 1

        # Atos por mês por parte
        mensal = defaultdict(lambda: defaultdict(int))
        for ev in self.eventos:
            mes = ev["data"].strftime("%Y-%m")
            mensal[mes][ev["autor"]] += 1

        # Tipos de ato por parte
        tipos_por_parte = defaultdict(lambda: defaultdict(int))
        for ev in self.eventos:
            tipos_por_parte[ev["autor"]][ev["tipo_ato"]] += 1

        return {
            "atos_por_parte": dict(atos_por_parte),
            "atividade_mensal": {
                mes: dict(partes) for mes, partes in sorted(mensal.items())
            },
            "tipos_por_parte": {
                parte: dict(tipos) for parte, tipos in tipos_por_parte.items()
            },
        }

    def executar(self):
        """Executa análise completa de onerosidade."""
        resultado = {
            "data_analise": datetime.now().isoformat(),
            "processos_analisados": list(self.dados.keys()),
            "metricas_por_parte": self.calcular_metricas_por_parte(),
            "indice_protelacao": self.calcular_indice_protelacao(),
            "indice_efetividade_exequente": self.calcular_indice_efetividade_exequente(),
            "tempo_resposta_executado": self.calcular_tempo_resposta(),
            "dados_graficos": self.gerar_dados_graficos(),
        }
        return resultado
