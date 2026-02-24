"""
Analisador de Documentos Jurídicos - Script Principal

Orquestra a extração de texto, reconhecimento de entidades,
classificação e análise de prazos de documentos PDF jurídicos.

Uso:
    python main.py <caminho_pdf> [--output <diretorio_saida>]
    python main.py --todos                  # Analisa todos os PDFs no diretório
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


def main():
    parser = argparse.ArgumentParser(
        description="Analisador de Documentos Jurídicos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py documento.pdf
  python main.py documento.pdf --output resultados/
  python main.py --todos --output resultados/
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

    args = parser.parse_args()

    if not args.arquivo and not args.todos:
        parser.print_help()
        sys.exit(1)

    print("=" * 60)
    print("  ANALISADOR DE DOCUMENTOS JURÍDICOS")
    print(f"  Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

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

    print(f"\n{'='*60}")
    print("  ANÁLISE CONCLUÍDA")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
