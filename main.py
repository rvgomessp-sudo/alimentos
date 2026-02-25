"""
Analisador de Documentos Jurídicos - Script Principal

Orquestra a extração de texto, reconhecimento de entidades,
classificação e análise de prazos de documentos PDF jurídicos.
Inclui módulos forenses de cruzamento, onerosidade, relatório,
mapeamento granular de movimentos e auditoria processual.

Uso:
    python main.py <caminho_pdf> [--output <diretorio_saida>]
    python main.py --todos                  # Analisa todos os PDFs no diretório
    python main.py --forense --output output/   # Executa pipeline forense completo
    python main.py --cruzamento --output output/ # Apenas cruzamento forense
    python main.py --onerosidade --output output/ # Apenas análise de onerosidade
    python main.py --relatorio --output output/   # Gera relatório DOCX consolidado
    python main.py --mapeamento --output output/  # Mapeamento granular SISBAJud/MLE
    python main.py --auditoria --output output/   # Auditoria de falhas e inconsistências
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.extrator_texto import extrair_texto_pdf, extrair_para_dataframe, extrair_tabelas_pdf, texto_completo
from src.ner_juridico import analisar_documento, extrair_entidades_regex, identificar_papeis, identificar_atos
from src.classificador import classificar_por_keywords, detectar_urgencia, classificar_paginas
from src.prazos import extrair_prazos, extrair_datas, gerar_alertas_prazo


def analisar_pdf(caminho_pdf, diretorio_saida=None):
    """
    Executa a análise completa de um documento PDF jurídico.

    Args:
        caminho_pdf: Caminho para o arquivo PDF.
        diretorio_saida: Diretório para salvar os resultados.

    Returns:
        Dicionário com todos os resultados da análise.
    """
    caminho = Path(caminho_pdf)
    nome_base = caminho.stem

    print(f"\n{'='*60}")
    print(f"  ANÁLISE: {caminho.name}")
    print(f"{'='*60}")

    # 1. Extração de texto
    print("\n[1/5] Extraindo texto do PDF...")
    paginas = extrair_texto_pdf(caminho_pdf)
    texto = "\n\n".join(p["texto"] for p in paginas if p["texto"])
    total_paginas = len(paginas)
    paginas_com_texto = sum(1 for p in paginas if p["texto"])

    print(f"  - Total de páginas: {total_paginas}")
    print(f"  - Páginas com texto: {paginas_com_texto}")
    print(f"  - Caracteres extraídos: {len(texto):,}")

    # 2. Extração de tabelas
    print("\n[2/5] Extraindo tabelas...")
    tabelas = extrair_tabelas_pdf(caminho_pdf)
    print(f"  - Tabelas encontradas: {len(tabelas)}")

    # 3. Reconhecimento de entidades
    print("\n[3/5] Identificando entidades jurídicas...")
    entidades = extrair_entidades_regex(texto)
    papeis = identificar_papeis(texto)
    atos = identificar_atos(texto)

    print(f"  - Entidades encontradas (regex): {len(entidades)}")
    for tipo in set(e.tipo for e in entidades):
        qtd = sum(1 for e in entidades if e.tipo == tipo)
        print(f"    · {tipo}: {qtd}")

    print(f"  - Papéis processuais identificados:")
    for papel, mencoes in papeis.items():
        print(f"    · {papel}: {len(mencoes)} menção(ões)")

    print(f"  - Atos processuais identificados:")
    for ato, ocorrencias in atos.items():
        print(f"    · {ato}: {len(ocorrencias)} ocorrência(s)")

    # 4. Classificação
    print("\n[4/5] Classificando documento...")
    classificacao = classificar_por_keywords(texto)
    urgencia = detectar_urgencia(texto)

    print(f"  - Tipo: {classificacao.tipo_documento}")
    print(f"  - Confiança: {classificacao.confianca:.1%}")
    if classificacao.categorias_secundarias:
        print(f"  - Categorias secundárias: {', '.join(classificacao.categorias_secundarias)}")
    print(f"  - Urgência: {urgencia['nivel_urgencia']} ({urgencia['total_ocorrencias']} indicadores)")

    # 5. Prazos
    print("\n[5/5] Extraindo prazos...")
    prazos = extrair_prazos(texto)
    datas = extrair_datas(texto)

    print(f"  - Prazos encontrados: {len(prazos)}")
    for prazo in prazos:
        print(f"    · {prazo.tipo}: {prazo.dias} {prazo.unidade} - \"{prazo.texto_original}\"")

    print(f"  - Datas encontradas: {len(datas)}")

    # Montar resultado
    resultado = {
        "arquivo": caminho.name,
        "data_analise": datetime.now().isoformat(),
        "extracao": {
            "total_paginas": total_paginas,
            "paginas_com_texto": paginas_com_texto,
            "total_caracteres": len(texto),
            "tabelas_encontradas": len(tabelas),
        },
        "entidades": {
            "regex": [
                {"tipo": e.tipo, "valor": e.valor, "contexto": e.contexto}
                for e in entidades
            ],
            "papeis_processuais": {
                papel: [m["contexto"] for m in mencoes[:5]]  # Limita a 5 exemplos
                for papel, mencoes in papeis.items()
            },
            "atos_processuais": {
                ato: [o["contexto"] for o in ocorrencias[:5]]
                for ato, ocorrencias in atos.items()
            },
        },
        "classificacao": {
            "tipo_documento": classificacao.tipo_documento,
            "confianca": classificacao.confianca,
            "categorias_secundarias": classificacao.categorias_secundarias,
            "urgencia": urgencia,
        },
        "prazos": [
            {
                "tipo": p.tipo,
                "dias": p.dias,
                "unidade": p.unidade,
                "texto_original": p.texto_original,
                "contexto": p.contexto,
            }
            for p in prazos
        ],
        "datas": [
            {
                "data_texto": d["data_texto"],
                "data_parseada": d["data_parseada"].isoformat(),
                "contexto": d["contexto"],
            }
            for d in datas
        ],
    }

    # Salvar resultados
    if diretorio_saida:
        dir_saida = Path(diretorio_saida)
        dir_saida.mkdir(parents=True, exist_ok=True)

        # Salvar JSON
        caminho_json = dir_saida / f"{nome_base}_analise.json"
        with open(caminho_json, "w", encoding="utf-8") as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        print(f"\n  Resultados salvos em: {caminho_json}")

        # Salvar texto extraído
        caminho_txt = dir_saida / f"{nome_base}_texto.txt"
        with open(caminho_txt, "w", encoding="utf-8") as f:
            f.write(texto)
        print(f"  Texto extraído salvo em: {caminho_txt}")

        # Salvar DataFrame das páginas
        df = extrair_para_dataframe(caminho_pdf)
        caminho_csv = dir_saida / f"{nome_base}_paginas.csv"
        df.to_csv(caminho_csv, index=False, encoding="utf-8")
        print(f"  Dados das páginas salvos em: {caminho_csv}")

    return resultado


def encontrar_pdfs(diretorio="."):
    """Encontra todos os PDFs no diretório."""
    return list(Path(diretorio).glob("*.pdf"))


def encontrar_jsons_analise(diretorio):
    """Encontra JSONs de análise existentes no diretório de saída."""
    dir_saida = Path(diretorio)
    jsons = sorted(dir_saida.glob("*_analise.json"))
    return [str(j) for j in jsons]


def executar_cruzamento(diretorio_saida):
    """Executa o módulo de cruzamento forense."""
    from src.cruzamento_forense import CruzamentoForense

    jsons = encontrar_jsons_analise(diretorio_saida)
    if len(jsons) < 2:
        print("  ERRO: Necessário pelo menos 2 JSONs de análise para cruzamento.")
        print(f"  Encontrados: {len(jsons)} em {diretorio_saida}/")
        print("  Execute primeiro: python main.py --todos --output output/")
        return None

    print(f"\n{'='*60}")
    print("  CRUZAMENTO FORENSE")
    print(f"{'='*60}")
    print(f"\n  Arquivos de entrada: {len(jsons)}")
    for j in jsons:
        print(f"    · {Path(j).name}")

    cruzamento = CruzamentoForense(jsons)
    resultado = cruzamento.executar()

    # Resumo
    entidades = resultado.get("entidades_comuns", {})
    score = resultado.get("score_correlacao", {})
    timeline = resultado.get("timeline_unificada", {})
    padroes = resultado.get("padroes_suspeitos", {})

    print(f"\n  Entidades comuns:")
    print(f"    · CPFs: {entidades.get('cpf', {}).get('total_comuns', 0)}")
    print(f"    · CNPJs: {entidades.get('cnpj', {}).get('total_comuns', 0)}")
    print(f"    · Nº Processos: {entidades.get('numero_processo', {}).get('total_comuns', 0)}")

    print(f"\n  Timeline unificada: {timeline.get('total_eventos', 0)} eventos")
    periodo = timeline.get("periodo", {})
    print(f"    · Período: {periodo.get('inicio', 'N/D')} a {periodo.get('fim', 'N/D')}")

    print(f"\n  Padrões suspeitos:")
    reacoes = padroes.get("reacoes_pos_decisao", {})
    print(f"    · Reações pós-decisão (30 dias): {reacoes.get('correlacoes_encontradas', 0)}")
    mov = padroes.get("movimentacoes_societarias", {})
    print(f"    · Movimentações societárias coincidentes: {mov.get('coincidencias_90_dias', 0)}")
    ativ = padroes.get("padroes_atividade", {}).get("executado", {})
    print(f"    · Omissões do executado (>60d): {ativ.get('periodos_omissao_60d', 0)}")

    print(f"\n  Score de correlação: {score.get('score', 0)}/100 ({score.get('classificacao', '')})")

    # Salvar
    caminho_json = Path(diretorio_saida) / "cruzamento_forense.json"
    with open(caminho_json, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    print(f"\n  Salvo em: {caminho_json}")

    return resultado


def executar_onerosidade(diretorio_saida):
    """Executa o módulo de análise de onerosidade."""
    from src.analise_onerosidade import AnaliseOnerosidade

    jsons = encontrar_jsons_analise(diretorio_saida)
    if not jsons:
        print("  ERRO: Nenhum JSON de análise encontrado.")
        print(f"  Execute primeiro: python main.py --todos --output output/")
        return None

    print(f"\n{'='*60}")
    print("  ANÁLISE DE ONEROSIDADE")
    print(f"{'='*60}")

    analise = AnaliseOnerosidade(jsons)
    resultado = analise.executar()

    # Resumo
    metricas = resultado.get("metricas_por_parte", {})
    protelacao = resultado.get("indice_protelacao", {})
    efetividade = resultado.get("indice_efetividade_exequente", {})
    tempo_resp = resultado.get("tempo_resposta_executado", {})

    print(f"\n  Métricas por parte:")
    for parte, m in metricas.items():
        if m.get("total_atos", 0) > 0:
            print(f"    · {m['nome'][:40]}: {m['total_atos']} atos, "
                  f"{m['peticoes']} petições, {m['recursos']} recursos")

    print(f"\n  Índice de protelação (executado): "
          f"{protelacao.get('indice', 0):.2%} ({protelacao.get('classificacao', '')})")
    det = protelacao.get("detalhes", {})
    print(f"    · Atos protelatórios: {det.get('atos_protelatarios', 0)}")
    print(f"    · Certidões negativas: {det.get('certidoes_negativas', 0)}")

    print(f"\n  Índice de efetividade (exequente): "
          f"{efetividade.get('indice', 0):.2%} ({efetividade.get('classificacao', '')})")
    det_e = efetividade.get("detalhes", {})
    print(f"    · Tentativas: {det_e.get('total_tentativas_constricao', 0)}")
    print(f"    · Exitosas: {det_e.get('constricoes_exitosas', 0)}")

    if tempo_resp.get("tempo_medio_dias"):
        print(f"\n  Tempo de resposta do executado:")
        print(f"    · Média: {tempo_resp['tempo_medio_dias']} dias")
        print(f"    · Mediana: {tempo_resp.get('mediana_dias', 'N/D')} dias")

    # Salvar
    caminho_json = Path(diretorio_saida) / "onerosidade.json"
    with open(caminho_json, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    print(f"\n  Salvo em: {caminho_json}")

    return resultado


def executar_relatorio(diretorio_saida, cruzamento=None, onerosidade=None):
    """Gera relatório DOCX consolidado."""
    from src.gerador_relatorio import GeradorRelatorioForense

    # Carregar dados se não fornecidos
    dir_saida = Path(diretorio_saida)

    if cruzamento is None:
        caminho_cruz = dir_saida / "cruzamento_forense.json"
        if not caminho_cruz.exists():
            print("  ERRO: cruzamento_forense.json não encontrado.")
            print("  Execute primeiro: python main.py --cruzamento --output output/")
            return None
        with open(caminho_cruz, "r", encoding="utf-8") as f:
            cruzamento = json.load(f)

    if onerosidade is None:
        caminho_oner = dir_saida / "onerosidade.json"
        if not caminho_oner.exists():
            print("  ERRO: onerosidade.json não encontrado.")
            print("  Execute primeiro: python main.py --onerosidade --output output/")
            return None
        with open(caminho_oner, "r", encoding="utf-8") as f:
            onerosidade = json.load(f)

    print(f"\n{'='*60}")
    print("  GERAÇÃO DE RELATÓRIO FORENSE")
    print(f"{'='*60}")

    gerador = GeradorRelatorioForense(cruzamento, onerosidade)
    caminho_docx = dir_saida / "relatorio_forense.docx"
    resultado = gerador.gerar(str(caminho_docx))

    print(f"\n  Relatório gerado: {resultado}")
    print(f"  Seções:")
    print(f"    · Capa")
    print(f"    · Identificação do caso")
    print(f"    · 1. Resumo executivo")
    print(f"    · 2. Timeline unificada")
    print(f"    · 3. Análise de onerosidade")
    print(f"    · 4. Correlações entre processos")
    print(f"    · 5. Conclusões técnicas")

    return resultado


def encontrar_textos(diretorio):
    """Encontra arquivos de texto extraídos no diretório de saída."""
    dir_saida = Path(diretorio)
    txts = sorted(dir_saida.glob("*_texto.txt"))
    return [str(t) for t in txts]


def executar_mapeamento(diretorio_saida):
    """Executa mapeamento granular de movimentos processuais."""
    from src.mapeamento_movimentos import MapeamentoMovimentos

    textos = encontrar_textos(diretorio_saida)
    jsons = encontrar_jsons_analise(diretorio_saida)

    if not textos:
        print("  ERRO: Nenhum arquivo de texto extraído encontrado.")
        print("  Execute primeiro: python main.py --todos --output output/")
        return None

    print(f"\n{'='*60}")
    print("  MAPEAMENTO GRANULAR DE MOVIMENTOS")
    print(f"{'='*60}")
    print(f"\n  Textos de entrada: {len(textos)}")
    for t in textos:
        print(f"    · {Path(t).name}")

    mapeamento = MapeamentoMovimentos(textos, jsons)
    resultado = mapeamento.executar()

    # Resumo SISBAJud
    sbj = resultado.get("sisbajud", {})
    print(f"\n  SISBAJud:")
    print(f"    · Eventos parseados: {sbj.get('total_eventos', 0)}")
    print(f"    · Bloqueios: {sbj.get('total_bloqueios', 0)}")
    print(f"    · Transferências: {sbj.get('total_transferencias', 0)}")
    print(f"    · Protocolos únicos: {sbj.get('total_protocolos', 0)}")

    valores = sbj.get("valores", {})
    print(f"    · Maior valor solicitado: R$ {valores.get('maior_valor_solicitado', 0):,.2f}")
    print(f"    · Total bloqueado: R$ {valores.get('total_efetivamente_bloqueado', 0):,.2f}")
    print(f"    · Total transferido: R$ {valores.get('total_transferido', 0):,.2f}")
    print(f"    · Taxa recuperação: {valores.get('taxa_recuperacao', '0%')}")

    ciclos = sbj.get("ciclos_teimosinha", [])
    if ciclos:
        print(f"\n  Teimosinha:")
        for c in ciclos:
            print(f"    · Protocolo {c['protocolo']}: "
                  f"{c['total_reiteracoes']} reiterações, "
                  f"R$ {c['valor_total_bloqueado']:,.2f} bloqueado")

    # Resumo MLE
    mle = resultado.get("mle", {})
    print(f"\n  MLEs:")
    print(f"    · Formulários encontrados: {mle.get('total_mles', 0)}")
    print(f"    · Erros detectados: {mle.get('total_erros', 0)}")
    print(f"    · Erros críticos: {mle.get('erros_criticos', 0)}")

    for erro in mle.get("erros", []):
        print(f"    · [{erro.get('gravidade', '')}] {erro.get('tipo', '')}: "
              f"{erro.get('descricao', '')[:80]}")

    # Valores pendentes
    pend = resultado.get("valores_pendentes", {})
    print(f"\n  Valores pendentes:")
    print(f"    · Chaves com saldo pendente: {pend.get('total_chaves_pendentes', 0)}")
    print(f"    · Valor total pendente: R$ {pend.get('valor_total_pendente', 0):,.2f}")

    # Salvar
    caminho_json = Path(diretorio_saida) / "mapeamento_movimentos.json"
    with open(caminho_json, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    print(f"\n  Salvo em: {caminho_json}")

    return resultado


def executar_auditoria(diretorio_saida, mapeamento=None):
    """Executa auditoria processual de falhas e inconsistências."""
    from src.auditoria_processual import AuditoriaProcessual

    textos = encontrar_textos(diretorio_saida)
    dir_saida = Path(diretorio_saida)

    # Carregar mapeamento se não fornecido
    if mapeamento is None:
        caminho_map = dir_saida / "mapeamento_movimentos.json"
        if not caminho_map.exists():
            print("  ERRO: mapeamento_movimentos.json não encontrado.")
            print("  Execute primeiro: python main.py --mapeamento --output output/")
            return None
        with open(caminho_map, "r", encoding="utf-8") as f:
            mapeamento = json.load(f)

    # Carregar cruzamento e onerosidade se existirem
    cruzamento = None
    caminho_cruz = dir_saida / "cruzamento_forense.json"
    if caminho_cruz.exists():
        with open(caminho_cruz, "r", encoding="utf-8") as f:
            cruzamento = json.load(f)

    onerosidade = None
    caminho_oner = dir_saida / "onerosidade.json"
    if caminho_oner.exists():
        with open(caminho_oner, "r", encoding="utf-8") as f:
            onerosidade = json.load(f)

    print(f"\n{'='*60}")
    print("  AUDITORIA PROCESSUAL")
    print(f"{'='*60}")

    auditoria = AuditoriaProcessual(
        mapeamento, cruzamento, onerosidade, textos,
    )
    resultado = auditoria.executar()

    # Resumo
    resumo = resultado.get("resumo", {})
    print(f"\n  Score de conformidade: {resumo.get('score_conformidade', 0)}/100 "
          f"({resumo.get('classificacao', '')})")
    print(f"  Total de achados: {resumo.get('total_achados', 0)}")

    por_grav = resumo.get("por_gravidade", {})
    for grav in ["CRITICO", "ALTO", "MEDIO", "BAIXO", "INFO"]:
        qtd = por_grav.get(grav, 0)
        if qtd > 0:
            print(f"    · {grav}: {qtd}")

    print(f"\n  Achados detalhados:")
    for achado in resultado.get("achados", []):
        regra = achado.get("regra", {})
        print(f"    [{regra.get('gravidade', '?')}] {regra.get('codigo', '?')}: "
              f"{achado.get('evidencia', '')[:100]}")

    # Salvar
    caminho_json = dir_saida / "auditoria_processual.json"
    with open(caminho_json, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    print(f"\n  Salvo em: {caminho_json}")

    return resultado


def main():
    parser = argparse.ArgumentParser(
        description="Analisador de Documentos Jurídicos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py documento.pdf
  python main.py documento.pdf --output resultados/
  python main.py --todos --output resultados/
  python main.py --cruzamento --output output/
  python main.py --onerosidade --output output/
  python main.py --relatorio --output output/
  python main.py --mapeamento --output output/
  python main.py --auditoria --output output/
  python main.py --forense --output output/
        """,
    )
    parser.add_argument(
        "arquivo",
        nargs="?",
        help="Caminho para o arquivo PDF a analisar",
    )
    parser.add_argument(
        "--todos",
        action="store_true",
        help="Analisar todos os PDFs no diretório atual",
    )
    parser.add_argument(
        "--output", "-o",
        default="output",
        help="Diretório de saída para os resultados (padrão: output/)",
    )
    parser.add_argument(
        "--cruzamento",
        action="store_true",
        help="Executar cruzamento forense nos JSONs existentes",
    )
    parser.add_argument(
        "--onerosidade",
        action="store_true",
        help="Executar análise de onerosidade",
    )
    parser.add_argument(
        "--relatorio",
        action="store_true",
        help="Gerar relatório DOCX consolidado",
    )
    parser.add_argument(
        "--mapeamento",
        action="store_true",
        help="Mapeamento granular de movimentos (SISBAJud, MLE, protocolos)",
    )
    parser.add_argument(
        "--auditoria",
        action="store_true",
        help="Auditoria de falhas e inconsistências processuais",
    )
    parser.add_argument(
        "--forense",
        action="store_true",
        help="Pipeline forense completo (cruzamento + onerosidade + mapeamento + auditoria + relatório)",
    )

    args = parser.parse_args()

    modo_forense = (args.cruzamento or args.onerosidade or args.relatorio
                    or args.mapeamento or args.auditoria or args.forense)
    modo_extracao = args.arquivo or args.todos

    if not modo_forense and not modo_extracao:
        parser.print_help()
        sys.exit(1)

    print("=" * 60)
    print("  ANALISADOR DE DOCUMENTOS JURÍDICOS")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    # Fase 1: Extração (se solicitada)
    if modo_extracao:
        if args.todos:
            pdfs = encontrar_pdfs()
            if not pdfs:
                print("Nenhum arquivo PDF encontrado no diretório atual.")
                sys.exit(1)
            print(f"\nEncontrados {len(pdfs)} arquivo(s) PDF.")
            for pdf in pdfs:
                analisar_pdf(str(pdf), args.output)
        else:
            if not Path(args.arquivo).exists():
                print(f"Erro: Arquivo não encontrado: {args.arquivo}")
                sys.exit(1)
            analisar_pdf(args.arquivo, args.output)

    # Fase 2: Análise forense
    if args.forense:
        cruzamento = executar_cruzamento(args.output)
        onerosidade = executar_onerosidade(args.output)
        mapeamento = executar_mapeamento(args.output)
        auditoria_result = executar_auditoria(args.output, mapeamento)
        if cruzamento and onerosidade:
            executar_relatorio(args.output, cruzamento, onerosidade)
    else:
        cruzamento = None
        onerosidade = None
        mapeamento = None

        if args.cruzamento:
            cruzamento = executar_cruzamento(args.output)

        if args.onerosidade:
            onerosidade = executar_onerosidade(args.output)

        if args.mapeamento:
            mapeamento = executar_mapeamento(args.output)

        if args.auditoria:
            executar_auditoria(args.output, mapeamento)

        if args.relatorio:
            executar_relatorio(args.output, cruzamento, onerosidade)

    print(f"\n{'='*60}")
    print("  ANÁLISE CONCLUÍDA")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
