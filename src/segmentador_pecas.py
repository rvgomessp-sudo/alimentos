"""
Segmentador de Peças Processuais

Segmenta o texto extraído de processos judiciais em peças individuais,
identificando tipo, autoria, data e folhas de cada peça.

Padrões detectados:
- Petições do exequente (Dr. Marcello Damianovich)
- Petições do executado (Dra. Tricya Pranstretter Arthuzo)
- Decisões e despachos judiciais (Juiz André Menezes Del Mastro)
- Certidões da serventia (Certidão de Remessa, Publicação)
- Relatórios do SISBAJud (Pesquisas Judiciais)
- Documentos JUCESP (Certidão de Inteiro Teor)
- Intimações/Comunicações (DJE)
"""

import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional


@dataclass
class PecaProcessual:
    """Representa uma peça individual dentro do processo."""
    numero: int = 0
    tipo: str = ""                      # decisao, despacho, peticao, certidao, relatorio, intimacao, documento
    subtipo: str = ""                   # remessa_relacao, publicacao, inteiro_teor, sisbajud, jucesp, etc.
    autor: str = ""                     # Nome de quem assinou/protocolou
    parte: str = ""                     # exequente, executado, juiz, serventia, sistema, terceiro
    data: Optional[str] = None          # Data da peça (DD/MM/YYYY)
    data_protocolo: Optional[str] = None  # Data do protocolo digital
    hora_protocolo: Optional[str] = None  # Hora do protocolo
    fls_inicio: Optional[int] = None    # Folha inicial
    fls_fim: Optional[int] = None       # Folha final
    linha_inicio: int = 0               # Linha inicial no texto
    linha_fim: int = 0                  # Linha final no texto
    processo: Optional[str] = None      # Número do processo referenciado
    conteudo: str = ""                  # Texto integral da peça
    resumo: str = ""                    # Resumo (primeiros 300 chars de conteúdo relevante)
    codigo_verificacao: Optional[str] = None  # Código de verificação digital
    numero_protocolo: Optional[str] = None    # Número WSNP do protocolo


# ---------- Padrões de INÍCIO de peça ----------

# Petição do exequente (Marcello Damianovich)
RE_INICIO_MARCELLO = re.compile(
    r"^MARCELLO DAMIANOVICH SOCIEDADE DE ADVOCACIA$", re.MULTILINE
)

# Petição do executado (Tricya Pranstretter Arthuzo)
RE_INICIO_TRICYA = re.compile(
    r"^TRICYA PRANSTRETTER ARTHUZO$", re.MULTILINE
)

# Petição anterior (Nelson Wilians)
RE_INICIO_NELSON = re.compile(
    r"^AO JUÍZO DE DIREITO DA 5ª VARA", re.MULTILINE
)

# Decisão/Despacho judicial
RE_INICIO_TRIBUNAL = re.compile(
    r"^TRIBUNAL DE JUSTIÇA DO ESTADO DE SÃO PAULO$", re.MULTILINE
)

# Certidão de Remessa/Publicação (emitida pela serventia)
RE_INICIO_CERTIDAO_SERVENTIA = re.compile(
    r"^Foro Regional XII\s*-\s*Nossa Senhora do Ó\s+Emitido em:", re.MULTILINE
)

# Relatório SISBAJud (Pesquisas Judiciais)
RE_INICIO_PESQUISAS = re.compile(
    r"^PESQUISAS JUDICIAIS\s+Página\s+\d+\s+de\s+\d+", re.MULTILINE
)

# Documento JUCESP
RE_INICIO_JUCESP = re.compile(
    r"^GOVERNO DO ESTADO DE SÃO PAULO$", re.MULTILINE
)

# Intimação DJE
RE_INICIO_INTIMACAO = re.compile(
    r"^Destinatários\(as\):", re.MULTILINE
)


# ---------- Padrões de ASSINATURA DIGITAL (texto invertido) ----------

# Detecta o bloco invertido que termina cada peça digital
# O bloco começa com "." seguido de linhas invertidas e termina com "araP"
RE_BLOCO_ASSINATURA = re.compile(
    r"^\.$\n"                     # Começa com "."
    r"(?:.*\n)*?"                 # Linhas intermediárias
    r"^araP$",                    # Termina com "araP" (Para invertido)
    re.MULTILINE
)

# Nome invertido: MARCELLO DAMIANOVICH
RE_NOME_MARCELLO_INV = re.compile(r"HCIVONAIMAD")
RE_NOME_MARCELLO2_INV = re.compile(r"OLLECRAM")

# Nome invertido: TRICYA PRANSTRETTER ARTHUZO
RE_NOME_TRICYA_INV = re.compile(r"AYCIRT")
RE_NOME_TRICYA2_INV = re.compile(r"OZUHTRA")

# Nome invertido: ANDRÉ MENEZES DEL MASTRO
RE_NOME_ANDRE_INV = re.compile(r"ERDNA")
RE_NOME_MASTRO_INV = re.compile(r",?ORTSAM")

# Data invertida: ex. "5202/11/71" = "17/11/2025"
RE_DATA_INVERTIDA = re.compile(r"^(\d{4}/\d{2}/\d{2})$", re.MULTILINE)

# Hora invertida: ex. "00:71" = "17:00"
RE_HORA_INVERTIDA = re.compile(r"^(\d{2}:\d{2})$", re.MULTILINE)

# Número protocolo WSNP invertido: ex. "01761810752OSNW"
RE_PROTOCOLO_INV = re.compile(r"^(\d+OSNW)$", re.MULTILINE)

# Código de verificação: ex. ".dzukxZrK"
RE_CODIGO_VERIF = re.compile(r"^\.([a-zA-Z0-9]+)$", re.MULTILINE)

# Processo invertido: ex. "0200.62.8.4102.48-7436001"
RE_PROCESSO_INV = re.compile(r"^(\d{4}\.\d{2}\.\d\.\d{4}\.\d{2}-\d{7})$", re.MULTILINE)


# ---------- Padrões auxiliares ----------

RE_FLS = re.compile(r"^fls\.\s*(\d[\d.]*)", re.MULTILINE)
RE_DATA_TEXTO = re.compile(
    r"São Paulo,\s*(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})"
)
RE_TIPO_JUDICIAL = re.compile(r"^(DECISÃO|DESPACHO)$", re.MULTILINE)
RE_PROCESSO_DIGITAL = re.compile(r"Processo (?:Digital )?n[ºo]\.?:?\s*([\d.-]+)")
RE_CERTIDAO_TIPO = re.compile(
    r"(CERTIDÃO DE REMESSA DE RELAÇÃO|"
    r"CERTIDÃO DE INTEIRO TEOR|"
    r"Certidão de Publicação|"
    r"CERTIDÃO)", re.IGNORECASE
)

MESES = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12",
}


def inverter_texto(texto: str) -> str:
    """Inverte uma string."""
    return texto[::-1]


def parse_data_invertida(data_inv: str) -> Optional[str]:
    """Converte data invertida ex '5202/11/71' → '17/11/2025'."""
    invertida = inverter_texto(data_inv)
    # Validar formato DD/MM/YYYY
    if re.match(r"\d{2}/\d{2}/\d{4}", invertida):
        return invertida
    return None


def parse_hora_invertida(hora_inv: str) -> Optional[str]:
    """Converte hora invertida ex '00:71' → '17:00'."""
    invertida = inverter_texto(hora_inv)
    if re.match(r"\d{2}:\d{2}", invertida):
        return invertida
    return None


def parse_protocolo_invertido(prot_inv: str) -> Optional[str]:
    """Converte protocolo invertido ex '01761810752OSNW' → 'WNSO25701816170'."""
    return inverter_texto(prot_inv)


def parse_processo_invertido(proc_inv: str) -> Optional[str]:
    """Converte processo invertido ex '0200.62.8.4102.48-7436001' → '1006347-84.2014.8.26.0020'."""
    return inverter_texto(proc_inv)


def parse_data_texto(texto: str) -> Optional[str]:
    """Extrai data de 'São Paulo, DD de MES de AAAA'."""
    m = RE_DATA_TEXTO.search(texto)
    if m:
        dia = m.group(1).zfill(2)
        mes = MESES.get(m.group(2).lower(), "00")
        ano = m.group(3)
        if mes != "00":
            return f"{dia}/{mes}/{ano}"
    return None


def extrair_fls(texto: str) -> list:
    """Extrai todas as referências a 'fls. XXXX' do texto."""
    resultados = []
    for m in RE_FLS.finditer(texto):
        try:
            num = int(m.group(1).replace(".", ""))
            resultados.append(num)
        except ValueError:
            pass
    return resultados


def identificar_autor_assinatura(bloco_invertido: str) -> tuple:
    """
    Identifica o autor a partir do bloco de assinatura digital invertida.
    Retorna (autor, parte, data_protocolo, hora_protocolo, protocolo, codigo, processo).
    """
    autor = "Desconhecido"
    parte = "desconhecido"

    if RE_NOME_MARCELLO_INV.search(bloco_invertido):
        autor = "Marcello Damianovich"
        parte = "exequente"
    elif RE_NOME_TRICYA_INV.search(bloco_invertido):
        autor = "Tricya Pranstretter Arthuzo"
        parte = "executado"
    elif RE_NOME_ANDRE_INV.search(bloco_invertido) or RE_NOME_MASTRO_INV.search(bloco_invertido):
        autor = "André Menezes Del Mastro"
        parte = "juiz"

    # Data do protocolo
    data_protocolo = None
    m_data = RE_DATA_INVERTIDA.search(bloco_invertido)
    if m_data:
        data_protocolo = parse_data_invertida(m_data.group(1))

    # Hora do protocolo
    hora_protocolo = None
    m_hora = RE_HORA_INVERTIDA.search(bloco_invertido)
    if m_hora:
        hora_protocolo = parse_hora_invertida(m_hora.group(1))

    # Número protocolo WSNP
    protocolo = None
    m_prot = RE_PROTOCOLO_INV.search(bloco_invertido)
    if m_prot:
        protocolo = parse_protocolo_invertido(m_prot.group(1))

    # Código de verificação
    codigo = None
    m_cod = RE_CODIGO_VERIF.search(bloco_invertido)
    if m_cod:
        codigo = inverter_texto(m_cod.group(1))

    # Processo
    processo = None
    m_proc = RE_PROCESSO_INV.search(bloco_invertido)
    if m_proc:
        processo = parse_processo_invertido(m_proc.group(1))

    return autor, parte, data_protocolo, hora_protocolo, protocolo, codigo, processo


class SegmentadorPecas:
    """
    Segmenta texto processual em peças individuais com autoria e classificação.
    """

    def __init__(self, caminhos_texto: list):
        """
        Args:
            caminhos_texto: Lista de caminhos para arquivos de texto extraídos.
        """
        self.caminhos_texto = [str(p) for p in caminhos_texto]
        self.pecas_por_arquivo = {}

    def executar(self) -> dict:
        """Executa a segmentação de todos os arquivos."""
        resultado = {
            "arquivos_processados": len(self.caminhos_texto),
            "processos": {},
        }

        for caminho in self.caminhos_texto:
            nome = Path(caminho).stem
            print(f"\n  Segmentando: {Path(caminho).name}")

            with open(caminho, "r", encoding="utf-8") as f:
                texto = f.read()

            linhas = texto.split("\n")
            pecas = self._segmentar(linhas, caminho)
            self.pecas_por_arquivo[caminho] = pecas

            # Estatísticas
            stats = self._calcular_estatisticas(pecas)

            resultado["processos"][nome] = {
                "arquivo": Path(caminho).name,
                "total_linhas": len(linhas),
                "total_pecas": len(pecas),
                "estatisticas": stats,
                "pecas": [asdict(p) for p in pecas],
            }

            # Resumo no console
            print(f"    · Total de peças: {len(pecas)}")
            print(f"    · Por tipo:")
            for tipo, qtd in sorted(stats["por_tipo"].items(), key=lambda x: -x[1]):
                print(f"      - {tipo}: {qtd}")
            print(f"    · Por parte:")
            for parte, qtd in sorted(stats["por_parte"].items(), key=lambda x: -x[1]):
                print(f"      - {parte}: {qtd}")

        return resultado

    def _segmentar(self, linhas: list, caminho: str) -> list:
        """
        Segmenta as linhas do texto em peças individuais.
        Estratégia: detecta marcadores de INÍCIO e fecha peças com assinatura invertida.
        """
        pecas = []
        peca_atual = None
        i = 0
        total_linhas = len(linhas)

        while i < total_linhas:
            linha = linhas[i]
            linha_strip = linha.strip()

            # --- Detectar INÍCIO de peça ---
            nova_peca = self._detectar_inicio(linhas, i)

            if nova_peca is not None:
                # Fechar peça anterior se existir
                if peca_atual is not None:
                    peca_atual.linha_fim = i - 1
                    self._finalizar_peca(peca_atual, linhas)
                    pecas.append(peca_atual)

                peca_atual = nova_peca
                peca_atual.linha_inicio = i
                i += 1
                continue

            # --- Detectar marcadores de fls. (apenas linhas standalone "fls. XXXX") ---
            m_fls = RE_FLS.match(linha_strip)
            if m_fls and peca_atual is not None:
                # Apenas contar como marcador de página se a linha é standalone
                # (apenas "fls. XXXX" sem outro texto), para não confundir com
                # referências a páginas no corpo do texto
                texto_apos_fls = linha_strip[m_fls.end():].strip()
                if not texto_apos_fls:
                    try:
                        num_fls = int(m_fls.group(1).replace(".", ""))
                        if peca_atual.fls_inicio is None:
                            peca_atual.fls_inicio = num_fls
                        peca_atual.fls_fim = num_fls
                    except ValueError:
                        pass

            # --- Detectar bloco de assinatura invertida (termina com "araP") ---
            if linha_strip == "araP" and peca_atual is not None:
                # Voltar até encontrar "." que marca início do bloco invertido
                bloco_inicio = i
                for j in range(i - 1, max(i - 60, peca_atual.linha_inicio) - 1, -1):
                    if linhas[j].strip() == ".":
                        bloco_inicio = j
                        break

                bloco_texto = "\n".join(linhas[bloco_inicio:i + 1])
                autor_sig, parte_sig, data_prot, hora_prot, prot, cod, proc = \
                    identificar_autor_assinatura(bloco_texto)

                # Atualizar metadados da peça com dados da assinatura
                if peca_atual.autor in ("Desconhecido", ""):
                    peca_atual.autor = autor_sig
                if peca_atual.parte in ("desconhecido", ""):
                    peca_atual.parte = parte_sig
                if data_prot:
                    peca_atual.data_protocolo = data_prot
                if hora_prot:
                    peca_atual.hora_protocolo = hora_prot
                if prot:
                    peca_atual.numero_protocolo = prot
                if cod:
                    peca_atual.codigo_verificacao = cod
                if proc and not peca_atual.processo:
                    peca_atual.processo = proc

                # O fls. seguinte (se existir) marca o início de uma nova seção
                # mas não fechamos a peça aqui pois pode continuar na próxima página

            i += 1

        # Fechar última peça
        if peca_atual is not None:
            peca_atual.linha_fim = total_linhas - 1
            self._finalizar_peca(peca_atual, linhas)
            pecas.append(peca_atual)

        # Numerar peças
        for idx, p in enumerate(pecas, 1):
            p.numero = idx

        return pecas

    def _detectar_inicio(self, linhas: list, i: int) -> Optional[PecaProcessual]:
        """Detecta se a linha i marca o início de uma nova peça."""
        if i >= len(linhas):
            return None

        linha = linhas[i].strip()
        total = len(linhas)

        # 1. Petição do exequente (Marcello Damianovich)
        if linha == "MARCELLO DAMIANOVICH SOCIEDADE DE ADVOCACIA":
            peca = PecaProcessual(
                tipo="peticao",
                subtipo="peticao_exequente",
                autor="Marcello Damianovich",
                parte="exequente",
            )
            # Verificar se próximas linhas contêm "EXCELENTÍSSIMO" (petição formal)
            for j in range(i + 1, min(i + 5, total)):
                if "EXCELENTÍSSIMO" in linhas[j]:
                    peca.subtipo = "peticao_exequente"
                    break
            # Procurar processo
            for j in range(i, min(i + 10, total)):
                m = RE_PROCESSO_DIGITAL.search(linhas[j])
                if m:
                    peca.processo = m.group(1)
                    break
            return peca

        # 2. Petição do executado (Tricya Pranstretter Arthuzo)
        if linha == "TRICYA PRANSTRETTER ARTHUZO":
            # Verificar contexto: se é header de petição (próxima linha com EXCELENTÍSSIMO)
            # ou se é citação dentro de certidão
            for j in range(i + 1, min(i + 5, total)):
                if "EXCELENTÍSSIMO" in linhas[j] or "SENHOR" in linhas[j]:
                    peca = PecaProcessual(
                        tipo="peticao",
                        subtipo="peticao_executado",
                        autor="Tricya Pranstretter Arthuzo",
                        parte="executado",
                    )
                    for k in range(i, min(i + 10, total)):
                        m = RE_PROCESSO_DIGITAL.search(linhas[k])
                        if m:
                            peca.processo = m.group(1)
                            break
                    return peca
            # Se não é seguida de "EXCELENTÍSSIMO", pode ser continuação da petição
            # (peça de múltiplas páginas). Verificar se a linha anterior era fls.
            if i > 0 and RE_FLS.match(linhas[i - 1].strip()):
                return None  # Continuação, não nova peça
            # Ainda pode ser início de nova peça se seguido de conteúdo substantivo
            peca = PecaProcessual(
                tipo="peticao",
                subtipo="peticao_executado",
                autor="Tricya Pranstretter Arthuzo",
                parte="executado",
            )
            return peca

        # 3. Petição anterior (Nelson Wilians)
        if linha.startswith("AO JUÍZO DE DIREITO DA 5ª VARA"):
            # Verificar se não é uma peça de Marcello que veio antes
            peca = PecaProcessual(
                tipo="peticao",
                subtipo="peticao_exequente",
                autor="Nelson Wilians Fratoni Rodrigues",
                parte="exequente",
            )
            for j in range(i, min(i + 15, total)):
                if "NELSON WILIANS" in linhas[j].upper():
                    peca.autor = "Nelson Wilians Fratoni Rodrigues"
                    break
                elif "MARCELLO DAMIANOVICH" in linhas[j].upper():
                    peca.autor = "Marcello Damianovich"
                    break
                m = RE_PROCESSO_DIGITAL.search(linhas[j])
                if m:
                    peca.processo = m.group(1)
            return peca

        # 4. Decisão/Despacho judicial (TRIBUNAL DE JUSTIÇA)
        if linha == "TRIBUNAL DE JUSTIÇA DO ESTADO DE SÃO PAULO":
            # Verificar se é seguido por DECISÃO ou DESPACHO (pode ter linhas de endereço entre)
            tipo_judicial = None
            for j in range(i + 1, min(i + 15, total)):
                lj = linhas[j].strip()
                if lj == "DECISÃO":
                    tipo_judicial = "decisao"
                    break
                elif lj == "DESPACHO":
                    tipo_judicial = "despacho"
                    break

            if tipo_judicial:
                peca = PecaProcessual(
                    tipo=tipo_judicial,
                    subtipo=tipo_judicial,
                    autor="André Menezes Del Mastro",
                    parte="juiz",
                )
                for j in range(i, min(i + 15, total)):
                    m = RE_PROCESSO_DIGITAL.search(linhas[j])
                    if m:
                        peca.processo = m.group(1)
                        break
                # Procurar data no corpo
                for j in range(i, min(i + 50, total)):
                    data = parse_data_texto(linhas[j])
                    if data:
                        peca.data = data
                        break
                return peca
            else:
                # TRIBUNAL sem DECISÃO/DESPACHO - pode ser outro tipo
                # Verificar se é Certidão
                return None

        # 5. Certidão da Serventia
        if RE_INICIO_CERTIDAO_SERVENTIA.match(linha):
            peca = PecaProcessual(
                tipo="certidao",
                subtipo="certidao_serventia",
                autor="Serventia Judicial",
                parte="serventia",
            )
            # Extrair data do "Emitido em:"
            m_data = re.search(r"Emitido em:\s*(\d{2}/\d{2}/\d{4})", linha)
            if m_data:
                peca.data = m_data.group(1)
            # Verificar subtipo
            for j in range(i, min(i + 5, total)):
                lj = linhas[j].strip()
                if "CERTIDÃO DE REMESSA DE RELAÇÃO" in lj:
                    peca.subtipo = "certidao_remessa"
                    break
                elif "Certidão de Publicação" in lj or "CERTIDÃO DE PUBLICAÇÃO" in lj:
                    peca.subtipo = "certidao_publicacao"
                    break
                elif "CERTIDÃO DE INTEIRO TEOR" in lj:
                    peca.subtipo = "certidao_inteiro_teor"
                    break
            # Procurar processo
            for j in range(i, min(i + 5, total)):
                m = RE_PROCESSO_DIGITAL.search(linhas[j])
                if m:
                    peca.processo = m.group(1)
                    break
            return peca

        # 6. Relatório SISBAJud
        if RE_INICIO_PESQUISAS.match(linha):
            peca = PecaProcessual(
                tipo="relatorio",
                subtipo="sisbajud",
                autor="Sistema SISBAJud",
                parte="sistema",
            )
            # Extrair data do relatório
            m_data = re.search(r"Data:\s*(\d{2}/\d{2}/\d{4})", linhas[i + 1] if i + 1 < total else "")
            if m_data:
                peca.data = m_data.group(1)
            return peca

        # 7. Documento JUCESP
        if linha == "GOVERNO DO ESTADO DE SÃO PAULO":
            # Verificar se é JUNTA COMERCIAL
            for j in range(i + 1, min(i + 5, total)):
                if "JUNTA COMERCIAL" in linhas[j]:
                    peca = PecaProcessual(
                        tipo="documento",
                        subtipo="jucesp",
                        autor="JUCESP",
                        parte="sistema",
                    )
                    for k in range(j, min(j + 5, total)):
                        if "CERTIDÃO DE INTEIRO TEOR" in linhas[k]:
                            peca.subtipo = "certidao_inteiro_teor_jucesp"
                            break
                    return peca
            return None

        # 8. Intimação DJE
        if RE_INICIO_INTIMACAO.match(linha):
            # Este é um bloco de comunicação/intimação
            peca = PecaProcessual(
                tipo="intimacao",
                subtipo="comunicacao_dje",
                autor="Sistema DJE",
                parte="sistema",
            )
            # Extrair destinatário
            dest = linha.replace("Destinatários(as):", "").strip()
            if dest:
                peca.resumo = f"Destinatário: {dest}"
            # Verificar se tem data de disponibilização (nas linhas anteriores)
            for j in range(max(0, i - 5), i):
                if "Disponibilizado em:" in linhas[j]:
                    m_data = re.search(r"Disponibilizado em:(\d{2}/\d{2}/\d{4})", linhas[j])
                    if m_data:
                        peca.data = m_data.group(1)
                    break
            return peca

        return None

    def _finalizar_peca(self, peca: PecaProcessual, linhas: list):
        """Finaliza uma peça, extraindo conteúdo e resumo."""
        inicio = peca.linha_inicio
        fim = min(peca.linha_fim, len(linhas) - 1)

        # Extrair conteúdo completo
        conteudo_linhas = linhas[inicio:fim + 1]
        peca.conteudo = "\n".join(conteudo_linhas)

        # Filtrar linhas de assinatura invertida para o resumo
        linhas_limpas = []
        em_bloco_invertido = False
        for ln in conteudo_linhas:
            ls = ln.strip()
            # Detectar início de bloco invertido (linha com apenas ".")
            if ls == "." and len(ls) == 1:
                em_bloco_invertido = True
                continue
            if em_bloco_invertido:
                if ls == "araP":
                    em_bloco_invertido = False
                continue
            # Pular linhas de fls.
            if RE_FLS.match(ls):
                continue
            # Pular linhas vazias consecutivas
            if ls:
                linhas_limpas.append(ls)

        texto_limpo = " ".join(linhas_limpas)

        # Resumo: primeiros 300 caracteres de conteúdo relevante
        # Pular headers (MARCELLO DAMIANOVICH..., EXCELENTÍSSIMO..., TRIBUNAL...)
        texto_resumo = texto_limpo
        for prefixo in [
            "MARCELLO DAMIANOVICH SOCIEDADE DE ADVOCACIA",
            "TRICYA PRANSTRETTER ARTHUZO",
            "TRIBUNAL DE JUSTIÇA DO ESTADO DE SÃO PAULO",
            "AO JUÍZO DE DIREITO",
        ]:
            idx = texto_resumo.find(prefixo)
            if idx == 0:
                # Pular até depois do header
                pos_vistos = texto_resumo.find("Vistos.")
                pos_requer = texto_resumo.find("requer")
                pos_vem = texto_resumo.find("vem,")
                # Encontrar o primeiro ponto de conteúdo real
                marcadores = [p for p in [pos_vistos, pos_requer, pos_vem] if p > 0]
                if marcadores:
                    texto_resumo = texto_resumo[min(marcadores):]
                break

        if not peca.resumo:
            peca.resumo = texto_resumo[:300].strip()

        # Extrair data do corpo se não tem
        if not peca.data:
            peca.data = parse_data_texto(peca.conteudo)

        # Extrair fls. se não tem (apenas de linhas standalone "fls. XXXX")
        if peca.fls_inicio is None or peca.fls_fim is None:
            for ln in conteudo_linhas:
                ls = ln.strip()
                m = RE_FLS.match(ls)
                if m and not ls[m.end():].strip():  # linha standalone
                    try:
                        num = int(m.group(1).replace(".", ""))
                        if peca.fls_inicio is None:
                            peca.fls_inicio = num
                        peca.fls_fim = num
                    except ValueError:
                        pass

        # Extrair processo se não tem
        if not peca.processo:
            m = RE_PROCESSO_DIGITAL.search(peca.conteudo)
            if m:
                peca.processo = m.group(1)

        # Não incluir conteúdo completo no JSON final (muito grande)
        # Apenas manter resumo e metadados

    def _calcular_estatisticas(self, pecas: list) -> dict:
        """Calcula estatísticas das peças segmentadas."""
        stats = {
            "por_tipo": {},
            "por_parte": {},
            "por_autor": {},
            "por_subtipo": {},
            "com_data": 0,
            "com_fls": 0,
            "com_protocolo": 0,
        }

        for p in pecas:
            stats["por_tipo"][p.tipo] = stats["por_tipo"].get(p.tipo, 0) + 1
            stats["por_parte"][p.parte] = stats["por_parte"].get(p.parte, 0) + 1
            stats["por_autor"][p.autor] = stats["por_autor"].get(p.autor, 0) + 1
            stats["por_subtipo"][p.subtipo] = stats["por_subtipo"].get(p.subtipo, 0) + 1
            if p.data or p.data_protocolo:
                stats["com_data"] += 1
            if p.fls_inicio is not None:
                stats["com_fls"] += 1
            if p.numero_protocolo:
                stats["com_protocolo"] += 1

        return stats

    def obter_pecas(self, caminho: str = None) -> list:
        """Retorna as peças segmentadas de um arquivo específico ou de todos."""
        if caminho:
            return self.pecas_por_arquivo.get(caminho, [])
        todas = []
        for pecas in self.pecas_por_arquivo.values():
            todas.extend(pecas)
        return todas

    def filtrar_pecas(
        self,
        tipo: str = None,
        parte: str = None,
        autor: str = None,
        caminho: str = None,
    ) -> list:
        """Filtra peças por tipo, parte ou autor."""
        pecas = self.obter_pecas(caminho)
        resultado = []
        for p in pecas:
            if tipo and p.tipo != tipo:
                continue
            if parte and p.parte != parte:
                continue
            if autor and autor.lower() not in p.autor.lower():
                continue
            resultado.append(p)
        return resultado

    def timeline(self, caminho: str = None) -> list:
        """Retorna peças ordenadas cronologicamente pela data mais confiável."""
        pecas = self.obter_pecas(caminho)
        datadas = []
        for p in pecas:
            data_str = p.data or p.data_protocolo
            if data_str:
                try:
                    partes = data_str.split("/")
                    data_sort = f"{partes[2]}{partes[1]}{partes[0]}"
                    datadas.append((data_sort, p))
                except (IndexError, ValueError):
                    datadas.append(("99999999", p))
            else:
                datadas.append(("99999999", p))

        datadas.sort(key=lambda x: x[0])
        return [p for _, p in datadas]
