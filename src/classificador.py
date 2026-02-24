"""
Módulo de Classificação de Documentos Jurídicos.

Classifica documentos por tipo (petição, decisão, sentença, etc.)
usando abordagem baseada em regras e keywords, com suporte para
classificação via scikit-learn quando treinado.
"""

import re
from collections import Counter
from dataclasses import dataclass

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.naive_bayes import MultinomialNB
    from sklearn.pipeline import Pipeline
    import pickle
    SKLEARN_DISPONIVEL = True
except ImportError:
    SKLEARN_DISPONIVEL = False


@dataclass
class ResultadoClassificacao:
    """Resultado da classificação de um documento."""
    tipo_documento: str
    confianca: float
    categorias_secundarias: list
    palavras_chave_encontradas: dict


# Palavras-chave por tipo de documento jurídico
KEYWORDS_TIPO_DOCUMENTO = {
    "peticao_inicial": {
        "peso": 1.0,
        "termos": [
            "petição inicial", "vem respeitosamente", "requer a citação",
            "dos fatos", "do direito", "dos pedidos", "requer a v. exa",
            "ante o exposto", "pede deferimento", "valor da causa",
        ],
    },
    "contestacao": {
        "peso": 1.0,
        "termos": [
            "contestação", "preliminarmente", "no mérito",
            "requer a improcedência", "pela improcedência",
            "em sede de contestação",
        ],
    },
    "decisao_interlocutoria": {
        "peso": 1.2,
        "termos": [
            "decisão interlocutória", "decido", "defiro", "indefiro",
            "tutela de urgência", "tutela antecipada",
            "determino", "fixo alimentos provisórios",
            "alimentos provisórios", "alimentos gravídicos",
        ],
    },
    "sentenca": {
        "peso": 1.3,
        "termos": [
            "sentença", "julgo procedente", "julgo improcedente",
            "julgo parcialmente procedente", "dispositivo",
            "custas pelo", "honorários advocatícios",
            "trânsito em julgado", "p.r.i.", "publique-se",
        ],
    },
    "despacho": {
        "peso": 0.8,
        "termos": [
            "despacho", "cite-se", "intime-se", "manifeste-se",
            "dê-se vista", "cumpra-se", "junte-se",
            "aguarde-se", "diligencie-se",
        ],
    },
    "certidao": {
        "peso": 0.9,
        "termos": [
            "certidão", "certifico e dou fé", "certifico",
            "prazo decorrido", "sem manifestação",
        ],
    },
    "acordao": {
        "peso": 1.3,
        "termos": [
            "acórdão", "acordam os desembargadores",
            "ementa", "voto do relator", "câmara cível",
            "turma recursal", "deram provimento",
            "negaram provimento",
        ],
    },
    "cumprimento_sentenca": {
        "peso": 1.1,
        "termos": [
            "cumprimento de sentença", "execução de alimentos",
            "penhora", "bloqueio", "bacenjud", "sisbajud",
            "renajud", "infojud", "prisão civil",
            "inadimplemento", "débito alimentar",
        ],
    },
    "impugnacao": {
        "peso": 1.0,
        "termos": [
            "impugnação", "impugnação ao cumprimento",
            "excesso de execução", "impugna",
        ],
    },
    "idpj": {
        "peso": 1.2,
        "termos": [
            "incidente de desconsideração da personalidade jurídica",
            "idpj", "desconsideração da personalidade",
            "confusão patrimonial", "desvio de finalidade",
            "grupo econômico", "sócio",
        ],
    },
}

# Indicadores de urgência
INDICADORES_URGENCIA = [
    "urgente", "urgência", "risco", "perigo",
    "irreversível", "irreparável", "imediato",
    "prisão", "inadimplemento", "descumprimento",
]


def classificar_por_keywords(texto):
    """
    Classifica o documento por análise de palavras-chave.

    Args:
        texto: Texto do documento.

    Returns:
        ResultadoClassificacao com tipo e confiança.
    """
    texto_lower = texto.lower()
    pontuacoes = {}

    for tipo, config in KEYWORDS_TIPO_DOCUMENTO.items():
        termos_encontrados = []
        for termo in config["termos"]:
            contagem = texto_lower.count(termo)
            if contagem > 0:
                termos_encontrados.append((termo, contagem))

        if termos_encontrados:
            # Pontuação = soma das contagens * peso do tipo
            pontuacao = sum(c for _, c in termos_encontrados) * config["peso"]
            pontuacoes[tipo] = {
                "pontuacao": pontuacao,
                "termos": termos_encontrados,
            }

    if not pontuacoes:
        return ResultadoClassificacao(
            tipo_documento="indefinido",
            confianca=0.0,
            categorias_secundarias=[],
            palavras_chave_encontradas={},
        )

    # Ordena por pontuação
    ranking = sorted(pontuacoes.items(), key=lambda x: x[1]["pontuacao"], reverse=True)
    melhor = ranking[0]
    total = sum(v["pontuacao"] for v in pontuacoes.values())
    confianca = melhor[1]["pontuacao"] / total if total > 0 else 0.0

    return ResultadoClassificacao(
        tipo_documento=melhor[0],
        confianca=round(confianca, 3),
        categorias_secundarias=[r[0] for r in ranking[1:4]],
        palavras_chave_encontradas={
            tipo: info["termos"] for tipo, info in ranking[:4]
        },
    )


def detectar_urgencia(texto):
    """
    Detecta indicadores de urgência no documento.

    Args:
        texto: Texto do documento.

    Returns:
        Dicionário com nível de urgência e indicadores encontrados.
    """
    texto_lower = texto.lower()
    encontrados = []

    for indicador in INDICADORES_URGENCIA:
        contagem = texto_lower.count(indicador)
        if contagem > 0:
            encontrados.append({"termo": indicador, "contagem": contagem})

    total = sum(i["contagem"] for i in encontrados)

    if total == 0:
        nivel = "normal"
    elif total <= 3:
        nivel = "moderado"
    elif total <= 8:
        nivel = "alto"
    else:
        nivel = "critico"

    return {
        "nivel_urgencia": nivel,
        "indicadores": encontrados,
        "total_ocorrencias": total,
    }


def classificar_paginas(paginas):
    """
    Classifica cada página individualmente.

    Args:
        paginas: Lista de dicionários com 'pagina' e 'texto'.

    Returns:
        Lista de resultados de classificação por página.
    """
    resultados = []
    for pagina in paginas:
        classificacao = classificar_por_keywords(pagina["texto"])
        resultados.append({
            "pagina": pagina["pagina"],
            "classificacao": classificacao,
        })
    return resultados


class ClassificadorML:
    """Classificador baseado em machine learning (scikit-learn)."""

    def __init__(self):
        if not SKLEARN_DISPONIVEL:
            raise ImportError("scikit-learn não está instalado")

        self.pipeline = Pipeline([
            ("tfidf", TfidfVectorizer(
                max_features=10000,
                ngram_range=(1, 3),
                strip_accents="unicode",
            )),
            ("clf", MultinomialNB(alpha=0.1)),
        ])
        self._treinado = False

    def treinar(self, textos, labels):
        """
        Treina o classificador com dados rotulados.

        Args:
            textos: Lista de textos de treinamento.
            labels: Lista de labels correspondentes.
        """
        self.pipeline.fit(textos, labels)
        self._treinado = True

    def classificar(self, texto):
        """
        Classifica um texto usando o modelo treinado.

        Args:
            texto: Texto a classificar.

        Returns:
            Dicionário com classe predita e probabilidades.
        """
        if not self._treinado:
            raise RuntimeError("Modelo não foi treinado ainda")

        predicao = self.pipeline.predict([texto])[0]
        probabilidades = self.pipeline.predict_proba([texto])[0]
        classes = self.pipeline.classes_

        probs_dict = {
            classe: round(float(prob), 4)
            for classe, prob in zip(classes, probabilidades)
        }

        return {
            "classe": predicao,
            "probabilidades": probs_dict,
        }

    def salvar(self, caminho):
        """Salva o modelo treinado em disco."""
        with open(caminho, "wb") as f:
            pickle.dump(self.pipeline, f)

    def carregar(self, caminho):
        """Carrega um modelo previamente salvo."""
        with open(caminho, "rb") as f:
            self.pipeline = pickle.load(f)
        self._treinado = True
