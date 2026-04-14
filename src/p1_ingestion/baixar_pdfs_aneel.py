"""
baixar_pdfs_aneel.py
====================
Pessoa 1 — Data Engineer · Sprint Dia 1 (tarde)

Baixa os PDFs referenciados nos JSONs limpos da ANEEL.

ATENÇÃO: São ~27.000 PDFs com estimativa de ~5GB no total.
Recomendamos começar só com texto_integral (17.637 PDFs)
e adicionar voto/nota_tecnica se o tempo e espaço permitirem.

Uso — rodar da RAIZ do projeto (pasta RAG-ANEEL-CEIA):

  Só texto integral (recomendado para começar):
    python src/p1_ingestion/baixar_pdfs_aneel.py

  Mais tipos:
    python src/p1_ingestion/baixar_pdfs_aneel.py --categorias texto_integral voto nota_tecnica

  Só um ano (para testar):
    python src/p1_ingestion/baixar_pdfs_aneel.py --ano 2016

  Retomar download interrompido (pula arquivos já existentes):
    python src/p1_ingestion/baixar_pdfs_aneel.py

Estrutura de saída:
  pdfs/
    2016/texto_integral/dsp20163386ti.pdf
    2021/texto_integral/...
    2022/texto_integral/...
"""

import asyncio
import aiohttp
import aiofiles
import json
import argparse
from pathlib import Path
from collections import defaultdict

try:
    from tqdm.asyncio import tqdm
    USA_TQDM = True
except ImportError:
    USA_TQDM = False
    print("Dica: instale tqdm para barra de progresso  →  pip install tqdm")


# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

# Raiz do projeto: sobe de src/p1_ingestion/ até RAG-ANEEL-CEIA/
RAIZ_PROJETO = Path(__file__).resolve().parent.parent.parent

ARQUIVO_JSON        = RAIZ_PROJETO / "data" / "aneel_vigentes_completo.json"
PASTA_PDFS          = RAIZ_PROJETO / "pdfs"

CONEXOES_SIMULTANEAS = 3
TIMEOUT_SEGUNDOS     = 30
MAX_TENTATIVAS       = 3
PAUSA_ENTRE_RETRY    = 2

TODAS_CATEGORIAS = ["texto_integral", "voto", "nota_tecnica", "decisao", "anexo", "outro"]


# ---------------------------------------------------------------------------
# Funções
# ---------------------------------------------------------------------------

def coletar_downloads(json_path: Path, categorias: list, anos: list) -> list:
    """Lê o JSON e retorna lista de dicts com url, destino, categoria, ano_fonte."""
    with open(json_path, encoding="utf-8") as f:
        registros = json.load(f)

    downloads = []
    for reg in registros:
        ano_fonte = reg.get("ano_fonte", "")
        if anos and ano_fonte not in anos:
            continue
        for pdf in reg.get("pdfs") or []:
            cat = pdf.get("categoria", "")
            if cat not in categorias:
                continue
            url     = pdf.get("url")
            arquivo = pdf.get("arquivo")
            if not url or not arquivo:
                continue
            destino = PASTA_PDFS / ano_fonte / cat / arquivo
            downloads.append({
                "url":         url,
                "destino":     destino,
                "categoria":   cat,
                "ano_fonte":   ano_fonte,
                "registro_id": reg.get("id", ""),
            })
    return downloads


async def baixar_um(session: aiohttp.ClientSession, item: dict, semaforo: asyncio.Semaphore) -> dict:
    """Baixa um único PDF com retry."""
    url     = item["url"]
    destino = item["destino"]

    if destino.exists() and destino.stat().st_size > 0:
        return {"status": "pulado", "arquivo": destino.name}

    destino.parent.mkdir(parents=True, exist_ok=True)

    async with semaforo:
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=TIMEOUT_SEGUNDOS)
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        async with aiofiles.open(destino, "wb") as f:
                            await f.write(await resp.read())
                        return {"status": "ok", "arquivo": destino.name}
                    else:
                        if tentativa == MAX_TENTATIVAS:
                            return {"status": f"erro_http_{resp.status}", "arquivo": destino.name, "url": url}
            except Exception as e:
                if tentativa == MAX_TENTATIVAS:
                    return {"status": "erro_timeout", "arquivo": destino.name, "url": url, "erro": str(e)}
                await asyncio.sleep(PAUSA_ENTRE_RETRY)

    return {"status": "erro_desconhecido", "arquivo": destino.name}


async def baixar_todos(downloads: list, n_conexoes: int) -> tuple:
    """Executa todos os downloads em paralelo."""
    semaforo   = asyncio.Semaphore(n_conexoes)
    resultados = defaultdict(int)
    falhas     = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://www2.aneel.gov.br/",
    }
    connector = aiohttp.TCPConnector(limit=n_conexoes, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tarefas = [baixar_um(session, item, semaforo) for item in downloads]

        if USA_TQDM:
            lista = []
            for coro in tqdm.as_completed(tarefas, total=len(tarefas), desc="Baixando PDFs"):
                r = await coro
                lista.append(r)
                resultados[r["status"]] += 1
        else:
            lista = await asyncio.gather(*tarefas)
            for r in lista:
                resultados[r["status"]] += 1

        falhas = [r for r in lista if r["status"].startswith("erro")]

    return dict(resultados), falhas


def imprimir_plano(downloads: list, categorias: list) -> None:
    por_cat  = defaultdict(int)
    por_ano  = defaultdict(int)
    ja_existem = sum(1 for d in downloads if Path(d["destino"]).exists())

    for d in downloads:
        por_cat[d["categoria"]] += 1
        por_ano[d["ano_fonte"]] += 1

    print("\n" + "=" * 55)
    print("PLANO DE DOWNLOAD")
    print("=" * 55)
    print(f"  Total de PDFs     : {len(downloads)}")
    print(f"  Já existem (pular): {ja_existem}")
    print(f"  A baixar          : {len(downloads) - ja_existem}")
    print(f"  Estimativa espaço : ~{(len(downloads) - ja_existem) * 0.2 / 1024:.1f} GB")
    print(f"\n  Por categoria:")
    for cat in categorias:
        if cat in por_cat:
            print(f"    {cat}: {por_cat[cat]}")
    print(f"\n  Por ano:")
    for ano in sorted(por_ano):
        print(f"    {ano}: {por_ano[ano]}")
    print("=" * 55)
    print("\nIniciando downloads...\n")


def salvar_falhas(falhas: list) -> None:
    if not falhas:
        return
    log = PASTA_PDFS / "falhas_download.json"
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "w", encoding="utf-8") as f:
        json.dump(falhas, f, ensure_ascii=False, indent=2)
    print(f"\nLog de falhas: {log}")
    print("Rode o script novamente para tentar os arquivos que falharam.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Baixa PDFs da ANEEL referenciados no JSON limpo"
    )
    parser.add_argument("--json", default=str(ARQUIVO_JSON))
    parser.add_argument(
        "--categorias", nargs="+", default=["texto_integral"],
        choices=TODAS_CATEGORIAS,
    )
    parser.add_argument(
        "--ano", nargs="+", default=[],
        choices=["2016", "2021", "2022"],
    )
    parser.add_argument("--conexoes", type=int, default=CONEXOES_SIMULTANEAS)
    args = parser.parse_args()

    json_path = Path(args.json)
    if not json_path.exists():
        print(f"ERRO: JSON não encontrado → {json_path}")
        print("Certifique-se de rodar limpar_json_aneel.py primeiro.")
        return

    print(f"Lendo JSON: {json_path}")
    downloads = coletar_downloads(json_path, args.categorias, args.ano)

    if not downloads:
        print("Nenhum PDF encontrado com os filtros informados.")
        return

    imprimir_plano(downloads, args.categorias)

    resultados, falhas = asyncio.run(baixar_todos(downloads, args.conexoes))

    print("\n" + "=" * 55)
    print("RESULTADO")
    print("=" * 55)
    print(f"  Baixados com sucesso : {resultados.get('ok', 0)}")
    print(f"  Pulados (já existiam): {resultados.get('pulado', 0)}")
    print(f"  Erros               : {len(falhas)}")
    print("=" * 55)

    salvar_falhas(falhas)

    if not falhas:
        print("\nTodos os PDFs baixados com sucesso!")
    else:
        print(f"\n{len(falhas)} arquivos falharam. Rode novamente para tentar de novo.")


if __name__ == "__main__":
    main()
