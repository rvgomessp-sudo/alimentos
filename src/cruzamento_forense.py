"""
Módulo de Cruzamento Forense entre Processos.

Cruza dados extraídos de múltiplos processos para identificar
entidades comuns, construir timeline unificada e detectar
padrões suspeitos de comportamento processual.
"""

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from dateutil.parser import parse as parse_date


# Mapeamento de partes conhecidas do caso
PARTES_CONHECIDAS = {
    "447.726.258-28": {
        "nome": "Nelson Wilson Fonseca Costa",
        "papel": "exequente",
        "alias": ["nelson", "fonseca costa", "exequente", "credor"],
    },
    "259.171.178-02": {
        "nome": "Leandro Lopes da Costa",
        "papel": "executado",
        "alias": ["leandro", "lopes da costa", "executado", "devedor", "alimentante"],
    },
    "614.786.528-53": {
        "nome": "Jair Ribeiro da Costa",
        "papel": "terceiro",
        "alias": ["jair", "ribeiro da costa", "terceiro"],
    },
}

CNPJ_JRCIA = "01.950.077/0001-95"

# Termos que indicam atos/reações do executado
TERMOS_REACAO_EXECUTADO = [
    "impugnação", "embargos", "agravo", "recurso", "contestação",
    "pedido de prazo", "juntada de documentos", "alteração contratual",
    "transferência", "cessão de cotas", "alteração societária",
    "retirada de sócio", "inclusão de sócio",
]

# Termos que indicam decisões desfavoráveis ao executado
TERMOS_DECISAO_DESFAVORAVEL = [
    "penhora", "bloqueio", "sisbajud", "bacenjud", "renajud",
    "prisão civil", "inadimplemento", "débito", "execução deferida",
    "tutela deferida", "determino o bloqueio", "defiro a penhora",
    "julgo procedente", "condeno", "desconsideração deferida",
]

# Termos de movimentação societária
TERMOS_MOVIMENTACAO_SOCIETARIA = [
    "alteração contratual", "alteração societária", "cessão de cotas",
    "transferência de cotas", "retirada de sócio", "inclusão de sócio",
    "dissolução", "arquivamento", "junta comercial", "nire",
    "contrato social", "distrato",
]


class CruzamentoForense:
    """Cruza dados entre múltiplos processos para análise forense."""

    def __init__(self, caminhos_json):
        """
        Args:
            caminhos_json: Lista de caminhos para JSONs de análise gerados pelo main.py.
        """
        self.dados = {}
        for caminho in caminhos_json:
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)
            chave = Path(caminho).stem.replace("_analise", "")
            self.dados[chave] = dados

    def _extrair_entidades_por_tipo(self, chave_processo, tipo):
        """Extrai valores únicos de entidades de um tipo específico."""
        return set(
            e["valor"]
            for e in self.dados[chave_processo]["entidades"]["regex"]
            if e["tipo"] == tipo
        )

    def identificar_entidades_comuns(self):
        """
        Identifica CPFs, CNPJs, valores e números de processo comuns
        entre os processos analisados.
        """
        chaves = list(self.dados.keys())
        if len(chaves) < 2:
            return {"erro": "Necessário pelo menos 2 processos para cruzamento"}

        resultado = {}
        for tipo in ["cpf", "cnpj", "numero_processo", "valor_monetario"]:
            conjuntos = {}
            for chave in chaves:
                conjuntos[chave] = self._extrair_entidades_por_tipo(chave, tipo)

            comuns = set.intersection(*conjuntos.values()) if conjuntos else set()
            exclusivos = {}
            for chave in chaves:
                exclusivos[chave] = sorted(conjuntos[chave] - comuns)

            resultado[tipo] = {
                "comuns": sorted(comuns),
                "total_comuns": len(comuns),
                "exclusivos_por_processo": exclusivos,
            }

            # Anotar quem é cada CPF/CNPJ comum
            if tipo == "cpf":
                anotacoes = []
                for cpf in sorted(comuns):
                    info = PARTES_CONHECIDAS.get(cpf, {})
                    anotacoes.append({
                        "cpf": cpf,
                        "nome": info.get("nome", "Desconhecido"),
                        "papel": info.get("papel", "indefinido"),
                    })
                resultado[tipo]["identificados"] = anotacoes

            if tipo == "cnpj":
                anotacoes = []
                for cnpj in sorted(comuns):
                    nome = "JRCIA Consultoria e Tecnologia Ltda" if cnpj == CNPJ_JRCIA else "Desconhecido"
                    anotacoes.append({"cnpj": cnpj, "nome": nome})
                resultado[tipo]["identificados"] = anotacoes

        return resultado

    def _parsear_datas_validas(self, chave_processo):
        """Filtra e parseia datas válidas (descartando artefatos de OCR)."""
        datas = []
        for d in self.dados[chave_processo].get("datas", []):
            try:
                dt = parse_date(d["data_parseada"])
                # Filtrar datas absurdas (antes de 1990 ou depois de 2030)
                if 1990 <= dt.year <= 2030:
                    datas.append({
                        "data": dt,
                        "data_texto": d["data_texto"],
                        "contexto": d["contexto"],
                        "processo": chave_processo,
                    })
            except (ValueError, OverflowError):
                continue
        return datas

    def construir_timeline_unificada(self):
        """
        Constrói timeline cronológica unificada de todos os processos.

        Cada evento contém data, processo de origem, tipo inferido e contexto.
        """
        todos_eventos = []

        for chave in self.dados:
            datas = self._parsear_datas_validas(chave)
            for d in datas:
                tipo_evento = self._classificar_evento(d["contexto"])
                autor = self._inferir_autor_evento(d["contexto"])
                todos_eventos.append({
                    "data": d["data"].isoformat(),
                    "data_formatada": d["data"].strftime("%d/%m/%Y"),
                    "processo": chave,
                    "tipo_evento": tipo_evento,
                    "autor_inferido": autor,
                    "contexto": d["contexto"],
                })

        # Ordenar cronologicamente
        todos_eventos.sort(key=lambda e: e["data"])

        # Remover duplicatas muito próximas (mesma data + contexto similar)
        eventos_filtrados = []
        vistos = set()
        for ev in todos_eventos:
            chave_dedup = (ev["data"][:10], ev["contexto"][:50])
            if chave_dedup not in vistos:
                vistos.add(chave_dedup)
                eventos_filtrados.append(ev)

        return {
            "total_eventos": len(eventos_filtrados),
            "periodo": {
                "inicio": eventos_filtrados[0]["data_formatada"] if eventos_filtrados else None,
                "fim": eventos_filtrados[-1]["data_formatada"] if eventos_filtrados else None,
            },
            "eventos": eventos_filtrados,
        }

    def _classificar_evento(self, contexto):
        """Classifica tipo de evento pelo contexto."""
        ctx = contexto.lower()
        if any(t in ctx for t in ["decisão", "decido", "defiro", "indefiro", "determino"]):
            return "decisao"
        if any(t in ctx for t in ["sentença", "julgo", "condeno"]):
            return "sentenca"
        if any(t in ctx for t in ["petição", "requer", "pede"]):
            return "peticao"
        if any(t in ctx for t in ["intimação", "intimado", "cite-se"]):
            return "intimacao"
        if any(t in ctx for t in ["certidão", "certifico"]):
            return "certidao"
        if any(t in ctx for t in ["penhora", "bloqueio", "sisbajud", "bacenjud"]):
            return "constricao"
        if any(t in ctx for t in ["recurso", "agravo", "apelação", "embargos"]):
            return "recurso"
        if any(t in ctx for t in TERMOS_MOVIMENTACAO_SOCIETARIA):
            return "movimentacao_societaria"
        return "outros"

    def _inferir_autor_evento(self, contexto):
        """Infere autor do evento pelo contexto."""
        ctx = contexto.lower()
        for cpf, info in PARTES_CONHECIDAS.items():
            for alias in info["alias"]:
                if alias in ctx:
                    return info["papel"]
        if any(t in ctx for t in ["juiz", "juíza", "decisão", "despacho", "sentença", "determino"]):
            return "juizo"
        if any(t in ctx for t in ["promotor", "ministério público", "mp"]):
            return "ministerio_publico"
        return "indefinido"

    def detectar_padroes_suspeitos(self):
        """
        Detecta padrões suspeitos cruzando eventos entre processos.

        Padrões analisados:
        1. Reações do executado após decisões desfavoráveis (janela 30 dias)
        2. Movimentações societárias coincidentes com execuções
        3. Gaps de inércia vs períodos de alta atividade
        """
        timeline = self.construir_timeline_unificada()
        eventos = timeline["eventos"]

        padroes = {
            "reacoes_pos_decisao": self._detectar_reacoes_pos_decisao(eventos),
            "movimentacoes_societarias": self._detectar_movimentacoes_coincidentes(eventos),
            "padroes_atividade": self._analisar_padroes_atividade(eventos),
        }

        return padroes

    def _detectar_reacoes_pos_decisao(self, eventos, janela_dias=30):
        """Encontra atos do executado que ocorrem logo após decisões desfavoráveis."""
        decisoes_desfavoraveis = []
        reacoes_executado = []

        for ev in eventos:
            ctx = ev["contexto"].lower()
            if any(t in ctx for t in TERMOS_DECISAO_DESFAVORAVEL):
                decisoes_desfavoraveis.append(ev)
            if ev["autor_inferido"] == "executado" and any(t in ctx for t in TERMOS_REACAO_EXECUTADO):
                reacoes_executado.append(ev)

        correlacoes = []
        for decisao in decisoes_desfavoraveis:
            try:
                dt_decisao = datetime.fromisoformat(decisao["data"])
            except (ValueError, TypeError):
                continue

            for reacao in reacoes_executado:
                try:
                    dt_reacao = datetime.fromisoformat(reacao["data"])
                except (ValueError, TypeError):
                    continue

                delta = (dt_reacao - dt_decisao).days
                if 0 < delta <= janela_dias:
                    correlacoes.append({
                        "decisao": {
                            "data": decisao["data_formatada"],
                            "processo": decisao["processo"],
                            "contexto": decisao["contexto"],
                        },
                        "reacao": {
                            "data": reacao["data_formatada"],
                            "processo": reacao["processo"],
                            "contexto": reacao["contexto"],
                        },
                        "dias_entre": delta,
                    })

        return {
            "total_decisoes_desfavoraveis": len(decisoes_desfavoraveis),
            "total_reacoes_executado": len(reacoes_executado),
            "correlacoes_encontradas": len(correlacoes),
            "janela_dias": janela_dias,
            "detalhes": correlacoes[:50],  # Limitar a 50 mais relevantes
        }

    def _detectar_movimentacoes_coincidentes(self, eventos):
        """Detecta movimentações societárias da JRCIA coincidentes com execuções."""
        mov_societarias = []
        atos_execucao = []

        for ev in eventos:
            ctx = ev["contexto"].lower()
            if any(t in ctx for t in TERMOS_MOVIMENTACAO_SOCIETARIA) or CNPJ_JRCIA in ev["contexto"]:
                mov_societarias.append(ev)
            if any(t in ctx for t in ["penhora", "bloqueio", "execução", "prisão", "sisbajud", "bacenjud"]):
                atos_execucao.append(ev)

        coincidencias = []
        for mov in mov_societarias:
            try:
                dt_mov = datetime.fromisoformat(mov["data"])
            except (ValueError, TypeError):
                continue

            for execucao in atos_execucao:
                try:
                    dt_exec = datetime.fromisoformat(execucao["data"])
                except (ValueError, TypeError):
                    continue

                delta = abs((dt_mov - dt_exec).days)
                if delta <= 90:  # Janela de 90 dias
                    coincidencias.append({
                        "movimentacao": {
                            "data": mov["data_formatada"],
                            "contexto": mov["contexto"],
                        },
                        "execucao": {
                            "data": execucao["data_formatada"],
                            "contexto": execucao["contexto"],
                        },
                        "dias_entre": delta,
                        "direcao": "mov_antes_exec" if dt_mov < dt_exec else "mov_apos_exec",
                    })

        return {
            "total_movimentacoes_societarias": len(mov_societarias),
            "total_atos_execucao": len(atos_execucao),
            "coincidencias_90_dias": len(coincidencias),
            "detalhes": coincidencias[:30],
        }

    def _analisar_padroes_atividade(self, eventos):
        """Analisa gaps de inércia vs períodos de alta atividade do executado."""
        eventos_executado = [
            ev for ev in eventos if ev["autor_inferido"] == "executado"
        ]
        eventos_exequente = [
            ev for ev in eventos if ev["autor_inferido"] == "exequente"
        ]

        def _calcular_gaps(lista_eventos):
            if len(lista_eventos) < 2:
                return []
            gaps = []
            for i in range(1, len(lista_eventos)):
                try:
                    dt_atual = datetime.fromisoformat(lista_eventos[i]["data"])
                    dt_anterior = datetime.fromisoformat(lista_eventos[i - 1]["data"])
                    delta = (dt_atual - dt_anterior).days
                    if delta > 0:
                        gaps.append({
                            "de": lista_eventos[i - 1]["data_formatada"],
                            "ate": lista_eventos[i]["data_formatada"],
                            "dias": delta,
                        })
                except (ValueError, TypeError):
                    continue
            return gaps

        gaps_executado = _calcular_gaps(eventos_executado)
        gaps_exequente = _calcular_gaps(eventos_exequente)

        # Identificar períodos de omissão (>60 dias sem ato)
        omissoes_executado = [g for g in gaps_executado if g["dias"] > 60]

        # Identificar períodos de alta atividade (<7 dias entre atos)
        alta_atividade_executado = [g for g in gaps_executado if g["dias"] <= 7]

        # Atividade por mês
        atividade_mensal = defaultdict(lambda: {"executado": 0, "exequente": 0, "juizo": 0})
        for ev in eventos:
            try:
                dt = datetime.fromisoformat(ev["data"])
                chave_mes = dt.strftime("%Y-%m")
                autor = ev["autor_inferido"]
                if autor in ("executado", "exequente", "juizo"):
                    atividade_mensal[chave_mes][autor] += 1
            except (ValueError, TypeError):
                continue

        return {
            "executado": {
                "total_eventos": len(eventos_executado),
                "periodos_omissao_60d": len(omissoes_executado),
                "detalhes_omissao": sorted(omissoes_executado, key=lambda x: x["dias"], reverse=True)[:10],
                "periodos_alta_atividade": len(alta_atividade_executado),
            },
            "exequente": {
                "total_eventos": len(eventos_exequente),
            },
            "atividade_mensal": dict(sorted(atividade_mensal.items())),
        }

    def calcular_score_correlacao(self):
        """
        Gera score de correlação temporal entre eventos dos processos.

        Score 0-100 indicando o grau de interdependência temporal.
        """
        timeline = self.construir_timeline_unificada()
        eventos = timeline["eventos"]

        if not eventos:
            return {"score": 0, "componentes": {}}

        chaves = list(self.dados.keys())
        if len(chaves) < 2:
            return {"score": 0, "componentes": {}}

        # Componente 1: Entidades comuns (peso 30%)
        entidades = self.identificar_entidades_comuns()
        cpfs_comuns = entidades.get("cpf", {}).get("total_comuns", 0)
        cnpjs_comuns = entidades.get("cnpj", {}).get("total_comuns", 0)
        score_entidades = min(100, (cpfs_comuns * 15 + cnpjs_comuns * 20))

        # Componente 2: Proximidade temporal de eventos (peso 40%)
        eventos_por_proc = defaultdict(list)
        for ev in eventos:
            try:
                dt = datetime.fromisoformat(ev["data"])
                eventos_por_proc[ev["processo"]].append(dt)
            except (ValueError, TypeError):
                continue

        pares_proximos = 0
        total_comparacoes = 0
        procs = list(eventos_por_proc.keys())
        if len(procs) >= 2:
            for dt1 in eventos_por_proc[procs[0]]:
                for dt2 in eventos_por_proc[procs[1]]:
                    total_comparacoes += 1
                    if abs((dt1 - dt2).days) <= 30:
                        pares_proximos += 1

        score_temporal = min(100, (pares_proximos / max(1, total_comparacoes)) * 200)

        # Componente 3: Padrões suspeitos (peso 30%)
        padroes = self.detectar_padroes_suspeitos()
        n_correlacoes = padroes["reacoes_pos_decisao"]["correlacoes_encontradas"]
        n_coincidencias = padroes["movimentacoes_societarias"]["coincidencias_90_dias"]
        n_omissoes = padroes["padroes_atividade"]["executado"]["periodos_omissao_60d"]
        score_padroes = min(100, n_correlacoes * 10 + n_coincidencias * 15 + n_omissoes * 5)

        score_final = (score_entidades * 0.30 + score_temporal * 0.40 + score_padroes * 0.30)

        return {
            "score": round(score_final, 1),
            "classificacao": (
                "MUITO_ALTO" if score_final >= 80
                else "ALTO" if score_final >= 60
                else "MODERADO" if score_final >= 40
                else "BAIXO" if score_final >= 20
                else "MINIMO"
            ),
            "componentes": {
                "entidades_comuns": {"score": round(score_entidades, 1), "peso": "30%"},
                "proximidade_temporal": {"score": round(score_temporal, 1), "peso": "40%"},
                "padroes_suspeitos": {"score": round(score_padroes, 1), "peso": "30%"},
            },
        }

    def executar(self):
        """Executa análise completa de cruzamento forense."""
        resultado = {
            "data_analise": datetime.now().isoformat(),
            "processos_analisados": list(self.dados.keys()),
            "entidades_comuns": self.identificar_entidades_comuns(),
            "timeline_unificada": self.construir_timeline_unificada(),
            "padroes_suspeitos": self.detectar_padroes_suspeitos(),
            "score_correlacao": self.calcular_score_correlacao(),
        }
        return resultado
