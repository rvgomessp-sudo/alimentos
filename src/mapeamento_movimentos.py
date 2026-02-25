"""
Módulo de Mapeamento Granular de Movimentos Processuais.

Rastreia cada ato processual individual com profundidade máxima:
- Eventos SISBAJud (bloqueios, desbloqueios, transferências, teimosinha)
- MLEs (Mandados de Levantamento Eletrônico) e alvarás
- Protocolos e seus status no processo
- Valores bloqueados vs solicitados vs transferidos vs levantados
- Petições com atribuição de autoria e conteúdo

Projetado para múltiplos processos com configuração genérica.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dateutil.parser import parse as parse_date


# ============================================================
# PARSERS SISBAJUD
# ============================================================

# Regex para protocolo SISBAJud (14 dígitos, prefixo ano)
RE_PROTOCOLO = re.compile(r"Protocolo:\s*(\d{14})")

# Regex para campos de ordens judiciais SISBAJud
RE_DATA_ORDEM = re.compile(
    r"Data Ordem:\s*(\d{2}/\d{2}/\d{4})\s+"
    r"Seq\.Bloq\.:\s*(\d+)\s+"
    r"Ret\.Bloq\.:\s*(\d+)\s+"
    r"Seq\.Solic\.:\s*(\d+)\s+"
    r"Ret\.Solic\.:\s*(\d+)"
)
RE_TIPO_SOLICITACAO = re.compile(r"Tipo Solicitação:\s*(.+?)\s+Processo:")
RE_PROCESSO_SISBAJUD = re.compile(r"Processo:\s*(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})")
RE_SOLICITANTE = re.compile(r"Solicitante:\s*(.+?)\s+Prazo")
RE_VALOR_SOLICITADO = re.compile(r"Valor Solicitado:\s*([\d.,]+)")
RE_EMPRESA = re.compile(r"Empresa:\s*(.+?)\s+Status:")
RE_STATUS = re.compile(r"Status:\s*(.+?)(?:\s+NOSSA|\s*$)")
RE_CHAVE_BLOQUEIO = re.compile(r"Chave Bloqueio:\s*(\d+)")
RE_DATA_BLOQUEIO = re.compile(r"Data Bloqueio:\s*(\d{2}/\d{2}/\d{4})")
RE_RESPOSTA = re.compile(r"Resposta:\s*(.+?)(?:\n|$)")
RE_DESBLOQUEIO_REMANESCENTE = re.compile(r"Desbloqueio Saldo Remanescente:\s*(\S+)")

# Regex para operações detalhadas (valores por conta)
RE_OPERACAO = re.compile(
    r"(\d{3}-[\w\s.]+?)\s+(\d{5})\s+(CC|RF|PP)\s+(\S+)\s+"
    r"([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,\-]+)"
)
RE_OPERACAO_TRANSF = re.compile(
    r"(\d{3}-[\w\s.]+?)\s+(\d{5})\s+(CC|RF|PP)\s+(\S+)\s+"
    r"([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,\-]+)\s+(\w+)"
)

# Regex para campos de MLE
RE_MLE = re.compile(r"(?:formulário|FORMULÁRIO)\s*\(?MLE\)?", re.IGNORECASE)
RE_MLE_PROCESSO = re.compile(r"Número do processo.*?:\s*(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})")
RE_MLE_BENEFICIARIO = re.compile(r"Nome do Credor.*?:\s*(.+?)(?:\n|$)")
RE_MLE_CPF_BENEFICIARIO = re.compile(r"CPF/CNPJ do Credor.*?:\s*([\d./-]+)")
RE_MLE_FORMA = re.compile(r"\(X\)\s*(I+V?|V|VI|Pix|PIX)[\s–-]+(.+?)(?:\[|;|\n|\*)")
RE_MLE_BANCO = re.compile(r"Banco:\s*(\S+)")
RE_MLE_AGENCIA = re.compile(r"Agência:\s*(\S+)")
RE_MLE_CONTA = re.compile(r"Conta\s*n[º°]:\s*(\S*)")
RE_MLE_CHAVE_PIX = re.compile(r"chave\s+Pix.*?:\s*(\S*)", re.IGNORECASE)
RE_MLE_VALOR = re.compile(r"Valor nominal.*?:\s*R\$\s*([\d.,]+)")
RE_MLE_PAGINAS = re.compile(r"comprovante do depósito:\s*(.+?)(?:\n|$)")

# Regex para teimosinha
RE_TEIMOSINHA = re.compile(
    r"teimosinha.*?(\d+)\s*dias",
    re.IGNORECASE,
)
RE_TEIMOSINHA_VALOR = re.compile(
    r"teimosinha.*?R\$\s*([\d.,]+)",
    re.IGNORECASE,
)

# Regex para petições (protocolo TJSP: 15+ digits no padrão WNSO)
RE_PETICAO_PROTOCOLO = re.compile(r"(\d{15,})OSNW")
RE_FLS = re.compile(r"fls?\.\s*(\d+(?:/\d+)?(?:,\s*\d+(?:/\d+))*)")


def _parse_valor(s):
    """Converte string de valor BR para float."""
    if not s:
        return 0.0
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_data(s, dayfirst=True):
    """Tenta parsear uma data, retorna None se falhar."""
    if not s:
        return None
    try:
        return parse_date(s.strip(), dayfirst=dayfirst)
    except (ValueError, OverflowError):
        return None


class EventoSISBAJUD:
    """Representa um evento individual do SISBAJud."""

    def __init__(self):
        self.protocolo = None
        self.data_ordem = None
        self.seq_bloqueio = None
        self.tipo_solicitacao = None  # "Bloqueio de Valor", "Transferência de Valor"
        self.processo = None
        self.solicitante = None
        self.cpf_cnpj_alvo = None
        self.nome_alvo = None
        self.valor_solicitado = 0.0
        self.empresa_banco = None
        self.status = None
        self.chave_bloqueio = None
        self.data_bloqueio = None
        self.resposta = None
        self.codigo_resposta = None
        self.desbloqueio_remanescente = None
        self.operacoes = []  # [{agencia, sistema, operacao, saldo_antes, bloqueado, desbloqueado, transferido, saldo_depois}]
        self.valor_total_bloqueado = 0.0
        self.valor_total_transferido = 0.0
        self.eh_teimosinha = False
        self.fonte = None  # arquivo de origem

    def to_dict(self):
        return {
            "protocolo": self.protocolo,
            "data_ordem": self.data_ordem.isoformat() if self.data_ordem else None,
            "data_ordem_formatada": self.data_ordem.strftime("%d/%m/%Y") if self.data_ordem else None,
            "seq_bloqueio": self.seq_bloqueio,
            "tipo_solicitacao": self.tipo_solicitacao,
            "processo": self.processo,
            "solicitante": self.solicitante,
            "cpf_cnpj_alvo": self.cpf_cnpj_alvo,
            "nome_alvo": self.nome_alvo,
            "valor_solicitado": self.valor_solicitado,
            "empresa_banco": self.empresa_banco,
            "status": self.status,
            "chave_bloqueio": self.chave_bloqueio,
            "data_bloqueio": self.data_bloqueio.isoformat() if self.data_bloqueio else None,
            "resposta": self.resposta,
            "codigo_resposta": self.codigo_resposta,
            "desbloqueio_remanescente": self.desbloqueio_remanescente,
            "operacoes": self.operacoes,
            "valor_total_bloqueado": self.valor_total_bloqueado,
            "valor_total_transferido": self.valor_total_transferido,
            "eh_teimosinha": self.eh_teimosinha,
            "resultado": self._classificar_resultado(),
            "fonte": self.fonte,
        }

    def _classificar_resultado(self):
        """Classifica resultado do evento."""
        if not self.codigo_resposta:
            return "indefinido"
        cod = self.codigo_resposta.strip()
        if cod.startswith("01"):
            return "cumprida_integralmente"
        if cod.startswith("02"):
            return "sem_saldo"
        if cod.startswith("13"):
            return "cumprida_parcialmente"
        if cod.startswith("00"):
            return "nao_cliente"
        if cod.startswith("98"):
            return "sem_resposta"
        return "outro"


class EventoMLE:
    """Representa um formulário MLE encontrado."""

    def __init__(self):
        self.processo_informado = None
        self.beneficiario = None
        self.cpf_beneficiario = None
        self.forma_recebimento = None
        self.banco = None
        self.agencia = None
        self.conta = None
        self.tipo_conta = None
        self.chave_pix = None
        self.valor_nominal = 0.0
        self.paginas_comprovante = None
        self.erros = []  # lista de erros detectados
        self.fonte = None

    def to_dict(self):
        return {
            "processo_informado": self.processo_informado,
            "beneficiario": self.beneficiario,
            "cpf_beneficiario": self.cpf_beneficiario,
            "forma_recebimento": self.forma_recebimento,
            "banco": self.banco,
            "agencia": self.agencia,
            "conta": self.conta,
            "tipo_conta": self.tipo_conta,
            "chave_pix": self.chave_pix,
            "valor_nominal": self.valor_nominal,
            "paginas_comprovante": self.paginas_comprovante,
            "erros_detectados": self.erros,
            "fonte": self.fonte,
        }


class MapeamentoMovimentos:
    """
    Mapeia todos os movimentos processuais em nível granular.

    Parseia textos extraídos para identificar cada evento SISBAJud,
    cada MLE, cada protocolo, e reconstrói o fluxo financeiro completo.
    """

    def __init__(self, caminhos_texto, caminhos_json=None, config=None):
        """
        Args:
            caminhos_texto: Lista de caminhos para arquivos de texto extraídos.
            caminhos_json: Lista opcional de JSONs de análise (para metadados).
            config: Dicionário opcional de configuração do caso.
        """
        self.textos = {}
        for caminho in caminhos_texto:
            with open(caminho, "r", encoding="utf-8") as f:
                self.textos[Path(caminho).stem] = f.read()

        self.dados_json = {}
        if caminhos_json:
            for caminho in caminhos_json:
                with open(caminho, "r", encoding="utf-8") as f:
                    self.dados_json[Path(caminho).stem.replace("_analise", "")] = json.load(f)

        # Config do caso (pode ser sobrescrita por caso)
        self.config = config or {
            "processo_principal": "1006347-84.2014.8.26.0020",
            "processos_validos": [
                "1006347-84.2014.8.26.0020",
                "0007068-67.2025.8.26.0020",
            ],
        }

        self.eventos_sisbajud = []
        self.eventos_mle = []
        self.protocolos_peticao = []

    def parsear_sisbajud(self):
        """
        Parseia todas as ordens judiciais SISBAJud dos textos extraídos.

        Identifica cada protocolo, operação de bloqueio/transferência,
        valores por conta, e classifica resultados.
        """
        for chave, texto in self.textos.items():
            linhas = texto.split("\n")
            protocolo_atual = None
            i = 0
            while i < len(linhas):
                linha = linhas[i]

                # Detectar início de protocolo
                m_prot = RE_PROTOCOLO.search(linha)
                if m_prot:
                    protocolo_atual = m_prot.group(1)

                # Detectar linha "Data Ordem" (início de evento)
                m_ordem = RE_DATA_ORDEM.search(linha)
                if m_ordem:
                    evento = EventoSISBAJUD()
                    evento.fonte = chave
                    evento.protocolo = protocolo_atual
                    evento.data_ordem = _parse_data(m_ordem.group(1))
                    evento.seq_bloqueio = m_ordem.group(2)

                    # Coletar bloco de texto deste evento (~15 linhas)
                    bloco_inicio = i
                    bloco_fim = min(i + 25, len(linhas))
                    bloco = "\n".join(linhas[bloco_inicio:bloco_fim])

                    # Parsear campos
                    m = RE_TIPO_SOLICITACAO.search(bloco)
                    if m:
                        evento.tipo_solicitacao = m.group(1).strip()

                    m = RE_PROCESSO_SISBAJUD.search(bloco)
                    if m:
                        evento.processo = m.group(1)

                    m = RE_SOLICITANTE.search(bloco)
                    if m:
                        evento.solicitante = m.group(1).strip()

                    m = RE_VALOR_SOLICITADO.search(bloco)
                    if m:
                        evento.valor_solicitado = _parse_valor(m.group(1))

                    m = RE_EMPRESA.search(bloco)
                    if m:
                        evento.empresa_banco = m.group(1).strip()

                    m = RE_STATUS.search(bloco)
                    if m:
                        evento.status = m.group(1).strip()

                    m = RE_CHAVE_BLOQUEIO.search(bloco)
                    if m:
                        evento.chave_bloqueio = m.group(1)

                    m = RE_DATA_BLOQUEIO.search(bloco)
                    if m:
                        evento.data_bloqueio = _parse_data(m.group(1))

                    m = RE_RESPOSTA.search(bloco)
                    if m:
                        evento.resposta = m.group(1).strip()
                        # Extrair código
                        cod_match = re.match(r"(\d+)", evento.resposta)
                        if cod_match:
                            evento.codigo_resposta = cod_match.group(1)

                    m = RE_DESBLOQUEIO_REMANESCENTE.search(bloco)
                    if m:
                        evento.desbloqueio_remanescente = m.group(1).strip()

                    # CPF alvo
                    m_cpf = re.search(r"CPF/CNPJ:\s*([\d./-]+)", bloco)
                    if m_cpf:
                        evento.cpf_cnpj_alvo = m_cpf.group(1)

                    m_nome = re.search(r"Nome:\s*(.+?)(?:\s+Desbloqueio|\s*$)", bloco)
                    if m_nome:
                        evento.nome_alvo = m_nome.group(1).strip()

                    # Parsear operações financeiras detalhadas
                    for j in range(bloco_inicio, bloco_fim):
                        if j >= len(linhas):
                            break
                        m_op = RE_OPERACAO.search(linhas[j])
                        if m_op:
                            op = {
                                "empresa": m_op.group(1).strip(),
                                "agencia": m_op.group(2),
                                "sistema": m_op.group(3),
                                "operacao": m_op.group(4),
                                "saldo_antes": _parse_valor(m_op.group(5)),
                                "valor_bloqueado": _parse_valor(m_op.group(6)),
                                "valor_desbloqueado": _parse_valor(m_op.group(7)),
                                "valor_transferido": _parse_valor(m_op.group(8)),
                                "saldo_depois": _parse_valor(m_op.group(9)),
                            }
                            evento.operacoes.append(op)
                            evento.valor_total_bloqueado += op["valor_bloqueado"]
                            evento.valor_total_transferido += op["valor_transferido"]

                    self.eventos_sisbajud.append(evento)

                i += 1

        # Marcar eventos teimosinha (mesma chave de bloqueio, múltiplas datas)
        chaves = defaultdict(list)
        for ev in self.eventos_sisbajud:
            if ev.chave_bloqueio:
                chaves[ev.chave_bloqueio].append(ev)

        for chave_bloq, evs in chaves.items():
            if len(evs) > 1:
                for ev in evs:
                    ev.eh_teimosinha = True

        # Classificar protocolo por ciclo
        protocolos_vistos = set()
        for ev in self.eventos_sisbajud:
            if ev.protocolo:
                protocolos_vistos.add(ev.protocolo)

        return self.eventos_sisbajud

    def parsear_mle(self):
        """
        Parseia formulários MLE dos textos.

        Identifica dados bancários, processo informado, beneficiário,
        e detecta erros (processo errado, dados incompletos, etc.).
        """
        for chave, texto in self.textos.items():
            # Buscar blocos de formulário MLE
            indices_mle = [m.start() for m in RE_MLE.finditer(texto)]

            for idx in indices_mle:
                # Pegar bloco de ~2000 chars a partir do MLE
                bloco = texto[idx:idx + 2000]

                # Verificar se é um formulário (não apenas menção)
                if "FORMULÁRIO MLE" in bloco or "formulário (MLE)" in bloco:
                    # Se é só menção ("providencie a juntada do formulário (MLE)"), registrar referência
                    if "providencie" in bloco[max(0, bloco.find("MLE") - 100):bloco.find("MLE")].lower():
                        continue

                mle = EventoMLE()
                mle.fonte = chave

                # Processo informado
                m = RE_MLE_PROCESSO.search(bloco)
                if m:
                    mle.processo_informado = m.group(1)

                # Beneficiário
                m = RE_MLE_BENEFICIARIO.search(bloco)
                if m:
                    mle.beneficiario = m.group(1).strip()

                m = RE_MLE_CPF_BENEFICIARIO.search(bloco)
                if m:
                    mle.cpf_beneficiario = m.group(1).strip()

                # Forma de recebimento
                m_formas = re.findall(r"\(X\)\s*(.*?)(?:\n|\[)", bloco)
                if m_formas:
                    mle.forma_recebimento = m_formas[0].strip()

                # Dados bancários
                m = RE_MLE_BANCO.search(bloco)
                if m:
                    mle.banco = m.group(1).strip()

                m = RE_MLE_AGENCIA.search(bloco)
                if m:
                    mle.agencia = m.group(1).strip()

                m = RE_MLE_CONTA.search(bloco)
                if m:
                    mle.conta = m.group(1).strip()

                # Tipo conta
                m_tipo = re.search(r"\(X\)\s*Conta\s+(Corrente|Poupança)", bloco)
                if m_tipo:
                    mle.tipo_conta = m_tipo.group(1)

                # Chave Pix
                m = RE_MLE_CHAVE_PIX.search(bloco)
                if m:
                    mle.chave_pix = m.group(1).strip()

                # Valor
                m = RE_MLE_VALOR.search(bloco)
                if m:
                    mle.valor_nominal = _parse_valor(m.group(1))

                # Páginas comprovante
                m = RE_MLE_PAGINAS.search(bloco)
                if m:
                    mle.paginas_comprovante = m.group(1).strip()

                # Detectar erros
                self._validar_mle(mle)

                if mle.processo_informado or mle.beneficiario or mle.valor_nominal > 0:
                    self.eventos_mle.append(mle)

        return self.eventos_mle

    def _validar_mle(self, mle):
        """Valida um MLE e detecta erros."""
        proc_principal = self.config.get("processo_principal", "")
        procs_validos = self.config.get("processos_validos", [])

        # Erro: Processo errado
        if mle.processo_informado and mle.processo_informado not in procs_validos:
            mle.erros.append({
                "tipo": "PROCESSO_ERRADO",
                "descricao": (
                    f"Processo informado no MLE ({mle.processo_informado}) "
                    f"não corresponde a nenhum processo do caso. "
                    f"Esperado: {proc_principal}"
                ),
                "gravidade": "CRITICO",
            })

        # Erro: PIX sem chave
        if mle.forma_recebimento and "pix" in mle.forma_recebimento.lower():
            if not mle.chave_pix:
                mle.erros.append({
                    "tipo": "PIX_SEM_CHAVE",
                    "descricao": "Forma de recebimento PIX selecionada mas chave PIX não informada",
                    "gravidade": "CRITICO",
                })

        # Erro: Dados bancários incompletos
        if mle.forma_recebimento and ("crédito em conta" in mle.forma_recebimento.lower()
                                       or "conta" in str(mle.tipo_conta or "").lower()):
            if not mle.conta:
                mle.erros.append({
                    "tipo": "CONTA_NAO_INFORMADA",
                    "descricao": "Número da conta bancária não informado no formulário MLE",
                    "gravidade": "CRITICO",
                })
            if not mle.agencia:
                mle.erros.append({
                    "tipo": "AGENCIA_NAO_INFORMADA",
                    "descricao": "Agência bancária não informada",
                    "gravidade": "ALTO",
                })

        # Erro: Banco sem conta (incoerência)
        if mle.banco and not mle.conta:
            mle.erros.append({
                "tipo": "BANCO_SEM_CONTA",
                "descricao": f"Banco informado ({mle.banco}) mas sem número de conta",
                "gravidade": "ALTO",
            })

    def gerar_resumo_sisbajud(self):
        """
        Gera resumo analítico de todos os eventos SISBAJud.

        Inclui:
        - Cronologia completa de bloqueios
        - Análise da teimosinha (ciclo reiterado)
        - Valores totais bloqueados vs solicitados
        - Status por instituição financeira
        """
        if not self.eventos_sisbajud:
            self.parsear_sisbajud()

        # Agrupar por protocolo
        por_protocolo = defaultdict(list)
        for ev in self.eventos_sisbajud:
            if ev.protocolo:
                por_protocolo[ev.protocolo].append(ev)

        # Separar bloqueios vs transferências
        bloqueios = [ev for ev in self.eventos_sisbajud if ev.tipo_solicitacao and "Bloqueio" in ev.tipo_solicitacao]
        transferencias = [ev for ev in self.eventos_sisbajud if ev.tipo_solicitacao and "Transferência" in ev.tipo_solicitacao]

        # Calcular totais
        total_solicitado_unico = 0.0
        valores_solicitados = set()
        for ev in bloqueios:
            if ev.protocolo and ev.protocolo not in valores_solicitados:
                total_solicitado_unico = max(total_solicitado_unico, ev.valor_solicitado)
                valores_solicitados.add(ev.protocolo)

        total_bloqueado = sum(ev.valor_total_bloqueado for ev in bloqueios)
        total_transferido = sum(ev.valor_total_transferido for ev in transferencias)

        # Resultados por banco
        por_banco = defaultdict(lambda: {
            "total_ordens": 0, "cumpridas": 0, "sem_saldo": 0,
            "parciais": 0, "nao_cliente": 0, "sem_resposta": 0,
            "valor_bloqueado": 0.0,
        })
        for ev in bloqueios:
            banco = ev.empresa_banco or "Desconhecido"
            por_banco[banco]["total_ordens"] += 1
            resultado = ev._classificar_resultado()
            if resultado == "cumprida_integralmente":
                por_banco[banco]["cumpridas"] += 1
            elif resultado == "sem_saldo":
                por_banco[banco]["sem_saldo"] += 1
            elif resultado == "cumprida_parcialmente":
                por_banco[banco]["parciais"] += 1
            elif resultado == "nao_cliente":
                por_banco[banco]["nao_cliente"] += 1
            elif resultado == "sem_resposta":
                por_banco[banco]["sem_resposta"] += 1
            por_banco[banco]["valor_bloqueado"] += ev.valor_total_bloqueado

        # Ciclos teimosinha
        ciclos_teimosinha = []
        protocolos_teimosinha = set()
        for ev in self.eventos_sisbajud:
            if ev.eh_teimosinha and ev.protocolo:
                protocolos_teimosinha.add(ev.protocolo)

        for prot in sorted(protocolos_teimosinha):
            evs = sorted(por_protocolo.get(prot, []), key=lambda e: e.data_ordem or datetime.min)
            bloqs = [e for e in evs if e.tipo_solicitacao and "Bloqueio" in e.tipo_solicitacao]
            transfs = [e for e in evs if e.tipo_solicitacao and "Transferência" in e.tipo_solicitacao]
            ciclos_teimosinha.append({
                "protocolo": prot,
                "total_reiteracoes": len(bloqs),
                "periodo": {
                    "inicio": bloqs[0].data_ordem.strftime("%d/%m/%Y") if bloqs and bloqs[0].data_ordem else None,
                    "fim": bloqs[-1].data_ordem.strftime("%d/%m/%Y") if bloqs and bloqs[-1].data_ordem else None,
                },
                "valor_solicitado": bloqs[0].valor_solicitado if bloqs else 0,
                "valor_total_bloqueado": sum(e.valor_total_bloqueado for e in bloqs),
                "valor_total_transferido": sum(e.valor_total_transferido for e in transfs),
                "resultados": [e._classificar_resultado() for e in bloqs],
                "transferencia_realizada": len(transfs) > 0,
            })

        # Cronologia completa
        cronologia = []
        for ev in sorted(self.eventos_sisbajud, key=lambda e: e.data_ordem or datetime.min):
            cronologia.append(ev.to_dict())

        return {
            "total_eventos": len(self.eventos_sisbajud),
            "total_bloqueios": len(bloqueios),
            "total_transferencias": len(transferencias),
            "total_protocolos": len(por_protocolo),
            "valores": {
                "maior_valor_solicitado": total_solicitado_unico,
                "total_efetivamente_bloqueado": round(total_bloqueado, 2),
                "total_transferido": round(total_transferido, 2),
                "taxa_recuperacao": (
                    f"{(total_bloqueado / total_solicitado_unico * 100):.4f}%"
                    if total_solicitado_unico > 0 else "0%"
                ),
            },
            "por_banco": dict(por_banco),
            "ciclos_teimosinha": ciclos_teimosinha,
            "cronologia": cronologia,
        }

    def gerar_resumo_mle(self):
        """Gera resumo analítico dos MLEs encontrados."""
        if not self.eventos_mle:
            self.parsear_mle()

        erros_totais = []
        for mle in self.eventos_mle:
            for erro in mle.erros:
                erros_totais.append({
                    **erro,
                    "mle_processo": mle.processo_informado,
                    "mle_beneficiario": mle.beneficiario,
                    "mle_valor": mle.valor_nominal,
                })

        return {
            "total_mles": len(self.eventos_mle),
            "total_erros": len(erros_totais),
            "erros_criticos": len([e for e in erros_totais if e["gravidade"] == "CRITICO"]),
            "mles": [mle.to_dict() for mle in self.eventos_mle],
            "erros": erros_totais,
        }

    def mapear_valores_pendentes(self):
        """
        Identifica valores bloqueados que não foram transferidos ou levantados.

        Cruza bloqueios com transferências pela chave de bloqueio.
        """
        if not self.eventos_sisbajud:
            self.parsear_sisbajud()

        pendentes = []
        bloqueios_por_chave = defaultdict(list)
        transferencias_por_chave = defaultdict(list)

        for ev in self.eventos_sisbajud:
            if not ev.chave_bloqueio:
                continue
            if ev.tipo_solicitacao and "Bloqueio" in ev.tipo_solicitacao:
                bloqueios_por_chave[ev.chave_bloqueio].append(ev)
            elif ev.tipo_solicitacao and "Transferência" in ev.tipo_solicitacao:
                transferencias_por_chave[ev.chave_bloqueio].append(ev)

        for chave, bloqs in bloqueios_por_chave.items():
            transfs = transferencias_por_chave.get(chave, [])
            total_bloqueado = sum(ev.valor_total_bloqueado for ev in bloqs)
            total_transferido = sum(ev.valor_total_transferido for ev in transfs)
            saldo_pendente = round(total_bloqueado - total_transferido, 2)

            if saldo_pendente > 0.01:
                pendentes.append({
                    "chave_bloqueio": chave,
                    "protocolo": bloqs[0].protocolo,
                    "banco": bloqs[0].empresa_banco,
                    "data_primeiro_bloqueio": (
                        bloqs[0].data_ordem.strftime("%d/%m/%Y")
                        if bloqs[0].data_ordem else None
                    ),
                    "total_bloqueado": total_bloqueado,
                    "total_transferido": total_transferido,
                    "saldo_pendente": saldo_pendente,
                    "tem_transferencia": len(transfs) > 0,
                    "dias_parado": (
                        (datetime.now() - bloqs[0].data_ordem).days
                        if bloqs[0].data_ordem else None
                    ),
                })

        return {
            "total_chaves_pendentes": len(pendentes),
            "valor_total_pendente": round(sum(p["saldo_pendente"] for p in pendentes), 2),
            "detalhes": sorted(pendentes, key=lambda p: p["saldo_pendente"], reverse=True),
        }

    def executar(self):
        """Executa mapeamento completo."""
        self.parsear_sisbajud()
        self.parsear_mle()

        resultado = {
            "data_analise": datetime.now().isoformat(),
            "arquivos_analisados": list(self.textos.keys()),
            "sisbajud": self.gerar_resumo_sisbajud(),
            "mle": self.gerar_resumo_mle(),
            "valores_pendentes": self.mapear_valores_pendentes(),
        }
        return resultado
