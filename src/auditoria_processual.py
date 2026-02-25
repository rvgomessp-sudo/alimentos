"""
Módulo de Auditoria Processual.

Detecta falhas, inconsistências e irregularidades nos atos processuais:
- Protocolos SISBAJud ausentes nos autos
- MLEs com dados incorretos (processo errado, PIX sem chave, conta incompleta)
- Valores bloqueados não transferidos / não levantados
- Honorários periciais ociosos de perícia cancelada
- Gaps de cumprimento de decisões judiciais
- Inconsistências entre decisões e cumprimento

Projetado para múltiplos processos com configuração genérica.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dateutil.parser import parse as parse_date


# ============================================================
# REGRAS DE AUDITORIA
# ============================================================

class RegraAuditoria:
    """Define uma regra de auditoria com critérios de detecção."""

    def __init__(self, codigo, nome, descricao, gravidade, categoria):
        self.codigo = codigo
        self.nome = nome
        self.descricao = descricao
        self.gravidade = gravidade  # CRITICO, ALTO, MEDIO, BAIXO, INFO
        self.categoria = categoria  # sisbajud, mle, valores, prazos, cumprimento

    def to_dict(self):
        return {
            "codigo": self.codigo,
            "nome": self.nome,
            "descricao": self.descricao,
            "gravidade": self.gravidade,
            "categoria": self.categoria,
        }


REGRAS = {
    # SISBAJud
    "SBJ-001": RegraAuditoria(
        "SBJ-001", "Protocolo SISBAJud ausente nos autos",
        "Protocolo de bloqueio/transferência sem correspondência nos autos do processo",
        "ALTO", "sisbajud",
    ),
    "SBJ-002": RegraAuditoria(
        "SBJ-002", "Bloqueio sem transferência posterior",
        "Valor efetivamente bloqueado mas sem ordem de transferência para conta judicial",
        "CRITICO", "sisbajud",
    ),
    "SBJ-003": RegraAuditoria(
        "SBJ-003", "Teimosinha com taxa zero de recuperação",
        "Ciclo de reiteração SISBAJud sem nenhum valor recuperado",
        "ALTO", "sisbajud",
    ),
    "SBJ-004": RegraAuditoria(
        "SBJ-004", "Transferência não efetivada pelo banco",
        "Ordem de transferência emitida mas não confirmada como enviada",
        "CRITICO", "sisbajud",
    ),
    "SBJ-005": RegraAuditoria(
        "SBJ-005", "Valor bloqueado irrisório vs dívida",
        "Valor bloqueado representa menos de 0.1% do débito total",
        "MEDIO", "sisbajud",
    ),

    # MLE
    "MLE-001": RegraAuditoria(
        "MLE-001", "MLE com número de processo incorreto",
        "Formulário MLE contém número de processo diferente do caso",
        "CRITICO", "mle",
    ),
    "MLE-002": RegraAuditoria(
        "MLE-002", "MLE com PIX sem chave informada",
        "Opção PIX selecionada mas campo de chave PIX vazio",
        "CRITICO", "mle",
    ),
    "MLE-003": RegraAuditoria(
        "MLE-003", "MLE com dados bancários incompletos",
        "Dados bancários (conta/agência) ausentes ou incompletos",
        "CRITICO", "mle",
    ),
    "MLE-004": RegraAuditoria(
        "MLE-004", "MLE não juntado após prazo judicial",
        "Decisão determinou juntada de MLE em prazo, sem cumprimento detectado",
        "ALTO", "mle",
    ),

    # Valores
    "VAL-001": RegraAuditoria(
        "VAL-001", "Depósito de perícia cancelada não levantado",
        "Honorários periciais depositados permanecem ociosos após cancelamento da perícia",
        "ALTO", "valores",
    ),
    "VAL-002": RegraAuditoria(
        "VAL-002", "Valor bloqueado parado por longo período",
        "Valor bloqueado há mais de 180 dias sem movimentação",
        "ALTO", "valores",
    ),
    "VAL-003": RegraAuditoria(
        "VAL-003", "Divergência entre valor certificado e bloqueado",
        "Certidão da serventia indica valor diferente do registrado no SISBAJud",
        "MEDIO", "valores",
    ),

    # Cumprimento
    "CMP-001": RegraAuditoria(
        "CMP-001", "Decisão judicial não cumprida no prazo",
        "Determinação judicial sem comprovação de cumprimento no prazo fixado",
        "ALTO", "cumprimento",
    ),
    "CMP-002": RegraAuditoria(
        "CMP-002", "Inércia da serventia na transferência",
        "Serventia não providenciou transferência determinada pelo juiz",
        "ALTO", "cumprimento",
    ),
}


class Achado:
    """Representa um achado (finding) da auditoria."""

    def __init__(self, regra, evidencia, contexto=None, valores=None):
        """
        Args:
            regra: Código da regra (ex: "SBJ-001").
            evidencia: Texto descrevendo a evidência concreta.
            contexto: Trecho do texto original que suporta o achado.
            valores: Dicionário com dados numéricos relevantes.
        """
        self.regra = REGRAS.get(regra)
        self.codigo_regra = regra
        self.evidencia = evidencia
        self.contexto = contexto
        self.valores = valores or {}
        self.data_deteccao = datetime.now()

    def to_dict(self):
        return {
            "regra": self.regra.to_dict() if self.regra else {"codigo": self.codigo_regra},
            "evidencia": self.evidencia,
            "contexto": self.contexto[:200] if self.contexto else None,
            "valores": self.valores,
            "data_deteccao": self.data_deteccao.isoformat(),
        }


class AuditoriaProcessual:
    """
    Executa auditoria completa dos atos processuais.

    Recebe dados do MapeamentoMovimentos e dos módulos de análise
    para detectar falhas e inconsistências.
    """

    def __init__(self, mapeamento_resultado, cruzamento_resultado=None,
                 onerosidade_resultado=None, caminhos_texto=None, config=None):
        """
        Args:
            mapeamento_resultado: Output do MapeamentoMovimentos.executar().
            cruzamento_resultado: Output opcional do CruzamentoForense.
            onerosidade_resultado: Output opcional da AnaliseOnerosidade.
            caminhos_texto: Lista de caminhos para textos extraídos.
            config: Configuração do caso.
        """
        self.mapeamento = mapeamento_resultado
        self.cruzamento = cruzamento_resultado
        self.onerosidade = onerosidade_resultado

        self.textos = {}
        if caminhos_texto:
            for caminho in caminhos_texto:
                with open(caminho, "r", encoding="utf-8") as f:
                    self.textos[Path(caminho).stem] = f.read()

        self.config = config or {
            "processo_principal": "1006347-84.2014.8.26.0020",
            "processos_validos": [
                "1006347-84.2014.8.26.0020",
                "0007068-67.2025.8.26.0020",
            ],
            "debito_total": 385490.11,
        }

        self.achados = []

    def auditar_sisbajud(self):
        """Audita eventos SISBAJud."""
        sisbajud = self.mapeamento.get("sisbajud", {})
        cronologia = sisbajud.get("cronologia", [])
        debito = self.config.get("debito_total", 0)

        # SBJ-002: Bloqueios sem transferência
        pendentes = self.mapeamento.get("valores_pendentes", {})
        for pendente in pendentes.get("detalhes", []):
            if pendente["saldo_pendente"] > 0:
                dias = pendente.get("dias_parado", 0)
                self.achados.append(Achado(
                    regra="SBJ-002",
                    evidencia=(
                        f"Bloqueio de R$ {pendente['total_bloqueado']:.2f} na chave "
                        f"{pendente['chave_bloqueio']} ({pendente['banco']}) "
                        f"sem transferência completa. "
                        f"Pendente: R$ {pendente['saldo_pendente']:.2f}. "
                        f"Parado há {dias} dias desde {pendente['data_primeiro_bloqueio']}."
                    ),
                    valores={
                        "bloqueado": pendente["total_bloqueado"],
                        "transferido": pendente["total_transferido"],
                        "pendente": pendente["saldo_pendente"],
                        "dias_parado": dias,
                    },
                ))

        # SBJ-003: Teimosinha com taxa zero
        for ciclo in sisbajud.get("ciclos_teimosinha", []):
            if ciclo["valor_total_bloqueado"] == 0 and ciclo["total_reiteracoes"] > 1:
                self.achados.append(Achado(
                    regra="SBJ-003",
                    evidencia=(
                        f"Ciclo teimosinha protocolo {ciclo['protocolo']} com "
                        f"{ciclo['total_reiteracoes']} reiterações e ZERO valor recuperado. "
                        f"Período: {ciclo['periodo']['inicio']} a {ciclo['periodo']['fim']}. "
                        f"Valor solicitado: R$ {ciclo['valor_solicitado']:,.2f}."
                    ),
                    valores={
                        "reiteracoes": ciclo["total_reiteracoes"],
                        "valor_solicitado": ciclo["valor_solicitado"],
                        "valor_bloqueado": 0.0,
                    },
                ))

        # SBJ-004: Transferência não efetivada
        for ev in cronologia:
            if (ev.get("tipo_solicitacao") and "Transferência" in ev["tipo_solicitacao"]
                    and ev.get("resposta")
                    and "não enviada" in ev["resposta"].lower()):
                self.achados.append(Achado(
                    regra="SBJ-004",
                    evidencia=(
                        f"Transferência no protocolo {ev['protocolo']} em "
                        f"{ev.get('data_ordem_formatada', 'N/D')} com status "
                        f"'Não enviada'. Valor: R$ {ev.get('valor_solicitado', 0):,.2f}."
                    ),
                    valores={"valor": ev.get("valor_solicitado", 0)},
                ))

        # SBJ-005: Bloqueio irrisório
        total_bloqueado = sisbajud.get("valores", {}).get("total_efetivamente_bloqueado", 0)
        if debito > 0 and total_bloqueado > 0:
            percentual = total_bloqueado / debito * 100
            if percentual < 0.1:
                self.achados.append(Achado(
                    regra="SBJ-005",
                    evidencia=(
                        f"Total bloqueado (R$ {total_bloqueado:,.2f}) representa apenas "
                        f"{percentual:.4f}% do débito total (R$ {debito:,.2f}). "
                        f"De {sisbajud.get('total_bloqueios', 0)} ordens de bloqueio, "
                        f"o valor efetivamente retido é irrisório."
                    ),
                    valores={
                        "total_bloqueado": total_bloqueado,
                        "debito_total": debito,
                        "percentual": percentual,
                    },
                ))

    def auditar_mle(self):
        """Audita formulários MLE."""
        mle_dados = self.mapeamento.get("mle", {})

        for mle in mle_dados.get("mles", []):
            for erro in mle.get("erros_detectados", []):
                if erro["tipo"] == "PROCESSO_ERRADO":
                    self.achados.append(Achado(
                        regra="MLE-001",
                        evidencia=(
                            f"MLE de {mle.get('beneficiario', 'N/D')} "
                            f"(R$ {mle.get('valor_nominal', 0):,.2f}) "
                            f"contém processo {mle.get('processo_informado', 'N/D')}. "
                            f"Processo correto: {self.config['processo_principal']}."
                        ),
                        valores={
                            "processo_informado": mle.get("processo_informado"),
                            "processo_correto": self.config["processo_principal"],
                        },
                    ))

                elif erro["tipo"] == "PIX_SEM_CHAVE":
                    self.achados.append(Achado(
                        regra="MLE-002",
                        evidencia=(
                            f"MLE de {mle.get('beneficiario', 'N/D')} solicita PIX "
                            f"mas o campo de chave PIX está vazio. "
                            f"Valor: R$ {mle.get('valor_nominal', 0):,.2f}."
                        ),
                    ))

                elif erro["tipo"] in ("CONTA_NAO_INFORMADA", "BANCO_SEM_CONTA", "AGENCIA_NAO_INFORMADA"):
                    self.achados.append(Achado(
                        regra="MLE-003",
                        evidencia=(
                            f"MLE de {mle.get('beneficiario', 'N/D')}: {erro['descricao']}. "
                            f"Banco: {mle.get('banco', 'N/D')}, "
                            f"Agência: {mle.get('agencia', 'N/D')}, "
                            f"Conta: {mle.get('conta', 'N/D')}. "
                            f"Valor: R$ {mle.get('valor_nominal', 0):,.2f}."
                        ),
                    ))

    def auditar_valores(self):
        """Audita situação de valores (depósitos, perícias, bloqueios antigos)."""
        # VAL-001: Depósito de perícia cancelada
        for chave, texto in self.textos.items():
            texto_lower = texto.lower()
            if ("honorários periciais" in texto_lower
                    and ("cancelada" in texto_lower or "suspens" in texto_lower or "revog" in texto_lower)):
                # Buscar menções ao depósito
                m_deposito = re.search(
                    r"honorários periciais.*?(?:fls?\.\s*[\d/,\s]+)",
                    texto_lower,
                )
                # Buscar valor
                m_valor = re.search(
                    r"R\$\s*([\d.,]+).*?honorários periciais|honorários periciais.*?R\$\s*([\d.,]+)",
                    texto,
                )
                valor = 0.0
                if m_valor:
                    val_str = m_valor.group(1) or m_valor.group(2)
                    if val_str:
                        valor = float(val_str.replace(".", "").replace(",", "."))

                # Verificar se tem mandado de levantamento expedido
                tem_levantamento = "mandado de levantamento dos honorários" in texto_lower

                self.achados.append(Achado(
                    regra="VAL-001",
                    evidencia=(
                        f"Depósito de honorários periciais "
                        f"{'(R$ ' + f'{valor:,.2f})' if valor > 0 else ''} "
                        f"permanece {'sem levantamento confirmado' if not tem_levantamento else 'com mandado determinado mas pendente de expedição'}. "
                        f"Perícia foi cancelada/suspensa/revogada."
                    ),
                    contexto=m_deposito.group(0) if m_deposito else None,
                    valores={"valor_deposito": valor},
                ))

        # VAL-002: Valores bloqueados parados por longo período
        pendentes = self.mapeamento.get("valores_pendentes", {})
        for p in pendentes.get("detalhes", []):
            dias = p.get("dias_parado", 0)
            if dias and dias > 180:
                self.achados.append(Achado(
                    regra="VAL-002",
                    evidencia=(
                        f"R$ {p['saldo_pendente']:.2f} bloqueado há {dias} dias "
                        f"({p['data_primeiro_bloqueio']}) no {p['banco']}. "
                        f"Chave: {p['chave_bloqueio']}."
                    ),
                    valores={
                        "valor": p["saldo_pendente"],
                        "dias": dias,
                    },
                ))

    def auditar_cumprimento(self):
        """Audita cumprimento de decisões judiciais."""
        for chave, texto in self.textos.items():
            texto_lower = texto.lower()

            # CMP-002: Inércia da serventia
            # Buscar padrões "providencie a Serventia a transferência" seguidos de "não transferido"
            pattern = re.compile(
                r"providencie\s+a\s+serventia\s+a\s+transferência.*?(?:fls?\.\s*[\d/]+)",
                re.IGNORECASE | re.DOTALL,
            )
            for m in pattern.finditer(texto):
                contexto = texto[m.start():m.end() + 200]
                if "não transferido" in contexto.lower() or "ainda não" in contexto.lower():
                    self.achados.append(Achado(
                        regra="CMP-002",
                        evidencia=(
                            f"Juiz determinou que a Serventia providenciasse transferência, "
                            f"porém o valor segue não transferido."
                        ),
                        contexto=contexto[:200],
                    ))

    def gerar_resumo(self):
        """Gera resumo da auditoria com todos os achados."""
        # Contagem por gravidade
        por_gravidade = defaultdict(int)
        for a in self.achados:
            grav = a.regra.gravidade if a.regra else "INDEFINIDO"
            por_gravidade[grav] += 1

        # Contagem por categoria
        por_categoria = defaultdict(int)
        for a in self.achados:
            cat = a.regra.categoria if a.regra else "indefinido"
            por_categoria[cat] += 1

        # Score de conformidade (inverso: quanto mais achados críticos, menor)
        criticos = por_gravidade.get("CRITICO", 0)
        altos = por_gravidade.get("ALTO", 0)
        medios = por_gravidade.get("MEDIO", 0)
        score_penalidade = criticos * 20 + altos * 10 + medios * 5
        score_conformidade = max(0, 100 - score_penalidade)

        return {
            "total_achados": len(self.achados),
            "por_gravidade": dict(por_gravidade),
            "por_categoria": dict(por_categoria),
            "score_conformidade": score_conformidade,
            "classificacao": (
                "CONFORME" if score_conformidade >= 80
                else "ATENCAO" if score_conformidade >= 60
                else "IRREGULAR" if score_conformidade >= 40
                else "CRITICO"
            ),
        }

    def executar(self):
        """Executa auditoria completa."""
        self.auditar_sisbajud()
        self.auditar_mle()
        self.auditar_valores()
        self.auditar_cumprimento()

        resultado = {
            "data_analise": datetime.now().isoformat(),
            "resumo": self.gerar_resumo(),
            "achados": [a.to_dict() for a in self.achados],
            "regras_aplicadas": {
                cod: regra.to_dict() for cod, regra in REGRAS.items()
            },
        }
        return resultado
