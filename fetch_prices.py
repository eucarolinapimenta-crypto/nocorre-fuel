"""
NoCorre — Coletor de Preços ANP
Roda toda segunda-feira via GitHub Actions.
Gera prices.json com preços médios de combustível por estado.
"""

import json
import urllib.request
import zipfile
import io
import csv
import datetime
import os

# ── Configuração ──────────────────────────────────────────────────
ANP_URL = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/levantamento-de-precos/arquivos-e-planilhas-2024/lpc_2024.zip"
ANP_URL_2025 = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/levantamento-de-precos/arquivos-e-planilhas-2025/lpc_2025.zip"
ANP_URL_2026 = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/levantamento-de-precos/arquivos-e-planilhas-2026/lpc_2026.zip"

REGIOES = {
    'AC':'Norte','AM':'Norte','AP':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
    'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste',
    'PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
    'DF':'Centro-Oeste','GO':'Centro-Oeste','MS':'Centro-Oeste','MT':'Centro-Oeste',
    'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
    'PR':'Sul','RS':'Sul','SC':'Sul',
}

COMBUSTIVEIS_ALVO = {
    'GASOLINA COMUM': 'gasolina',
    'ETANOL HIDRATADO': 'etanol',
    'ÓLEO DIESEL': 'diesel',
    'OLEO DIESEL': 'diesel',
}

# ── Fallback com dados embutidos (usados se ANP estiver fora do ar) ──
FALLBACK_DATA = {
    "ultima_coleta_anp": "2026-04-25",
    "gerado_em": "2026-04-25T00:00:00",
    "fonte": "fallback_embutido",
    "regioes": [
        {"nome":"Norte","media_gasolina":6.21,"media_etanol":4.11,"media_diesel":5.78,"estados":[
            {"id_estado":"AM","media_gasolina":6.34,"tendencia_gasolina":"subida","media_etanol":4.20,"tendencia_etanol":"estavel","media_diesel":5.91,"tendencia_diesel":"subida"},
            {"id_estado":"PA","media_gasolina":6.08,"tendencia_gasolina":"estavel","media_etanol":4.02,"tendencia_etanol":"descida","media_diesel":5.65,"tendencia_diesel":"estavel"},
            {"id_estado":"AC","media_gasolina":6.45,"tendencia_gasolina":"subida","media_etanol":4.30,"tendencia_etanol":"estavel","media_diesel":5.99,"tendencia_diesel":"subida"},
            {"id_estado":"RO","media_gasolina":6.10,"tendencia_gasolina":"estavel","media_etanol":4.05,"tendencia_etanol":"estavel","media_diesel":5.72,"tendencia_diesel":"estavel"},
            {"id_estado":"RR","media_gasolina":6.38,"tendencia_gasolina":"subida","media_etanol":4.22,"tendencia_etanol":"estavel","media_diesel":5.95,"tendencia_diesel":"subida"},
            {"id_estado":"AP","media_gasolina":6.28,"tendencia_gasolina":"estavel","media_etanol":4.15,"tendencia_etanol":"estavel","media_diesel":5.80,"tendencia_diesel":"estavel"},
            {"id_estado":"TO","media_gasolina":5.98,"tendencia_gasolina":"descida","media_etanol":3.95,"tendencia_etanol":"descida","media_diesel":5.60,"tendencia_diesel":"estavel"}
        ]},
        {"nome":"Nordeste","media_gasolina":5.98,"media_etanol":3.89,"media_diesel":5.54,"estados":[
            {"id_estado":"BA","media_gasolina":6.01,"tendencia_gasolina":"subida","media_etanol":3.95,"tendencia_etanol":"estavel","media_diesel":5.58,"tendencia_diesel":"estavel"},
            {"id_estado":"CE","media_gasolina":5.92,"tendencia_gasolina":"estavel","media_etanol":3.80,"tendencia_etanol":"descida","media_diesel":5.49,"tendencia_diesel":"estavel"},
            {"id_estado":"PE","media_gasolina":5.95,"tendencia_gasolina":"estavel","media_etanol":3.85,"tendencia_etanol":"estavel","media_diesel":5.52,"tendencia_diesel":"estavel"},
            {"id_estado":"MA","media_gasolina":6.05,"tendencia_gasolina":"subida","media_etanol":3.98,"tendencia_etanol":"estavel","media_diesel":5.65,"tendencia_diesel":"subida"},
            {"id_estado":"PB","media_gasolina":5.88,"tendencia_gasolina":"estavel","media_etanol":3.75,"tendencia_etanol":"estavel","media_diesel":5.44,"tendencia_diesel":"estavel"},
            {"id_estado":"RN","media_gasolina":5.90,"tendencia_gasolina":"estavel","media_etanol":3.78,"tendencia_etanol":"estavel","media_diesel":5.47,"tendencia_diesel":"estavel"},
            {"id_estado":"AL","media_gasolina":5.97,"tendencia_gasolina":"subida","media_etanol":3.88,"tendencia_etanol":"estavel","media_diesel":5.55,"tendencia_diesel":"estavel"},
            {"id_estado":"PI","media_gasolina":6.02,"tendencia_gasolina":"estavel","media_etanol":3.92,"tendencia_etanol":"estavel","media_diesel":5.62,"tendencia_diesel":"estavel"},
            {"id_estado":"SE","media_gasolina":5.94,"tendencia_gasolina":"estavel","media_etanol":3.82,"tendencia_etanol":"estavel","media_diesel":5.50,"tendencia_diesel":"estavel"}
        ]},
        {"nome":"Centro-Oeste","media_gasolina":5.77,"media_etanol":3.72,"media_diesel":5.42,"estados":[
            {"id_estado":"DF","media_gasolina":5.81,"tendencia_gasolina":"estavel","media_etanol":3.78,"tendencia_etanol":"estavel","media_diesel":5.45,"tendencia_diesel":"estavel"},
            {"id_estado":"GO","media_gasolina":5.73,"tendencia_gasolina":"descida","media_etanol":3.66,"tendencia_etanol":"estavel","media_diesel":5.39,"tendencia_diesel":"estavel"},
            {"id_estado":"MT","media_gasolina":5.79,"tendencia_gasolina":"estavel","media_etanol":3.72,"tendencia_etanol":"descida","media_diesel":5.44,"tendencia_diesel":"estavel"},
            {"id_estado":"MS","media_gasolina":5.75,"tendencia_gasolina":"estavel","media_etanol":3.72,"tendencia_etanol":"estavel","media_diesel":5.40,"tendencia_diesel":"estavel"}
        ]},
        {"nome":"Sudeste","media_gasolina":5.89,"media_etanol":3.68,"media_diesel":5.41,"estados":[
            {"id_estado":"RJ","media_gasolina":6.05,"tendencia_gasolina":"estavel","media_etanol":3.81,"tendencia_etanol":"estavel","media_diesel":5.62,"tendencia_diesel":"subida"},
            {"id_estado":"SP","media_gasolina":5.74,"tendencia_gasolina":"descida","media_etanol":3.55,"tendencia_etanol":"descida","media_diesel":5.28,"tendencia_diesel":"estavel"},
            {"id_estado":"MG","media_gasolina":5.88,"tendencia_gasolina":"estavel","media_etanol":3.62,"tendencia_etanol":"estavel","media_diesel":5.44,"tendencia_diesel":"estavel"},
            {"id_estado":"ES","media_gasolina":5.89,"tendencia_gasolina":"subida","media_etanol":3.74,"tendencia_etanol":"estavel","media_diesel":5.30,"tendencia_diesel":"estavel"}
        ]},
        {"nome":"Sul","media_gasolina":5.71,"media_etanol":3.61,"media_diesel":5.35,"estados":[
            {"id_estado":"PR","media_gasolina":5.68,"tendencia_gasolina":"estavel","media_etanol":3.57,"tendencia_etanol":"descida","media_diesel":5.31,"tendencia_diesel":"estavel"},
            {"id_estado":"RS","media_gasolina":5.75,"tendencia_gasolina":"estavel","media_etanol":3.66,"tendencia_etanol":"estavel","media_diesel":5.39,"tendencia_diesel":"estavel"},
            {"id_estado":"SC","media_gasolina":5.70,"tendencia_gasolina":"estavel","media_etanol":3.60,"tendencia_etanol":"estavel","media_diesel":5.35,"tendencia_diesel":"estavel"}
        ]}
    ]
}


def baixar_csv_anp(url):
    """Baixa o ZIP da ANP e retorna o conteúdo do CSV mais recente."""
    print(f"Baixando dados ANP: {url}")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    z = zipfile.ZipFile(io.BytesIO(data))
    # Pega o arquivo CSV/xlsx mais recente dentro do ZIP
    csvs = [n for n in z.namelist() if n.lower().endswith('.csv')]
    if not csvs:
        raise ValueError("Nenhum CSV encontrado no ZIP")
    csvs.sort(reverse=True)  # mais recente primeiro
    with z.open(csvs[0]) as f:
        return f.read().decode('latin-1')


def parsear_csv(conteudo):
    """Processa o CSV da ANP e retorna médias por estado e combustível."""
    reader = csv.DictReader(io.StringIO(conteudo), delimiter=';')
    
    # Acumula: {estado: {combustivel: [preços]}}
    dados = {}
    ultima_data = None
    
    for row in reader:
        estado = row.get('Estado - Sigla', '').strip().upper()
        produto = row.get('Produto', '').strip().upper()
        preco_str = row.get('Preço Médio Revenda', '').replace(',', '.').strip()
        data_coleta = row.get('Data da Coleta', '').strip()
        
        if not estado or not produto or not preco_str:
            continue
        
        combustivel = COMBUSTIVEIS_ALVO.get(produto)
        if not combustivel:
            continue
        
        try:
            preco = float(preco_str)
        except ValueError:
            continue
        
        if estado not in dados:
            dados[estado] = {}
        if combustivel not in dados[estado]:
            dados[estado][combustivel] = []
        dados[estado][combustivel].append(preco)
        
        # Captura a data mais recente encontrada
        if data_coleta:
            ultima_data = data_coleta
    
    return dados, ultima_data


def calcular_media(lista):
    if not lista:
        return None
    return round(sum(lista) / len(lista), 2)


def inferir_tendencia(estado, combustivel, precos_novos, dados_anteriores):
    """
    Compara com coleta anterior se disponível.
    Sem histórico, retorna 'estavel'.
    """
    if not dados_anteriores:
        return 'estavel'
    media_nova = calcular_media(precos_novos)
    media_ant = dados_anteriores.get(estado, {}).get(combustivel)
    if media_ant is None or media_nova is None:
        return 'estavel'
    diff = media_nova - media_ant
    if diff > 0.05:
        return 'subida'
    if diff < -0.05:
        return 'descida'
    return 'estavel'


def montar_json(dados_estados, ultima_data, dados_anteriores=None):
    """Monta o JSON final no mesmo formato que o app espera."""
    regioes_dict = {}
    
    for estado, combustiveis in dados_estados.items():
        regiao = REGIOES.get(estado)
        if not regiao:
            continue
        
        if regiao not in regioes_dict:
            regioes_dict[regiao] = {'nome': regiao, 'estados': []}
        
        entry = {'id_estado': estado}
        for comb in ['gasolina', 'etanol', 'diesel']:
            precos = combustiveis.get(comb, [])
            media = calcular_media(precos)
            tendencia = inferir_tendencia(estado, comb, precos, dados_anteriores)
            entry[f'media_{comb}'] = media if media else 0.0
            entry[f'tendencia_{comb}'] = tendencia
        
        regioes_dict[regiao]['estados'].append(entry)
    
    # Calcula médias regionais
    ordem = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
    regioes_lista = []
    for nome in ordem:
        if nome not in regioes_dict:
            continue
        reg = regioes_dict[nome]
        estados = reg['estados']
        for comb in ['gasolina', 'etanol', 'diesel']:
            valores = [e[f'media_{comb}'] for e in estados if e[f'media_{comb}'] > 0]
            reg[f'media_{comb}'] = calcular_media(valores) or 0.0
        regioes_lista.append(reg)
    
    # Formata data de coleta
    try:
        dt = datetime.datetime.strptime(ultima_data, '%d/%m/%Y')
        data_fmt = dt.strftime('%Y-%m-%d')
    except Exception:
        data_fmt = datetime.date.today().isoformat()
    
    return {
        'ultima_coleta_anp': data_fmt,
        'gerado_em': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'fonte': 'ANP - Agência Nacional do Petróleo',
        'regioes': regioes_lista
    }


def main():
    output_path = os.path.join(os.path.dirname(__file__), 'prices.json')
    
    # Tenta carregar dados anteriores para calcular tendências
    dados_anteriores = None
    if os.path.exists(output_path):
        try:
            with open(output_path) as f:
                old = json.load(f)
            dados_anteriores = {}
            for reg in old.get('regioes', []):
                for est in reg.get('estados', []):
                    sid = est['id_estado']
                    dados_anteriores[sid] = {
                        'gasolina': est.get('media_gasolina'),
                        'etanol':   est.get('media_etanol'),
                        'diesel':   est.get('media_diesel'),
                    }
            print("Dados anteriores carregados para cálculo de tendências.")
        except Exception as e:
            print(f"Aviso: não foi possível carregar dados anteriores: {e}")
    
    # Tenta buscar da ANP (ano atual primeiro, depois anos anteriores)
    resultado = None
    for url in [ANP_URL_2026, ANP_URL_2025, ANP_URL]:
        try:
            conteudo = baixar_csv_anp(url)
            dados, ultima_data = parsear_csv(conteudo)
            if dados:
                resultado = montar_json(dados, ultima_data or '', dados_anteriores)
                print(f"Sucesso! {len(dados)} estados processados. Data: {ultima_data}")
                break
        except Exception as e:
            print(f"Falha em {url}: {e}")
            continue
    
    # Fallback se ANP estiver fora do ar
    if not resultado:
        print("AVISO: ANP indisponível. Usando dados do fallback embutido.")
        resultado = FALLBACK_DATA
        resultado['gerado_em'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        resultado['fonte'] = 'fallback_embutido'
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)
    
    print(f"prices.json gerado em: {output_path}")
    print(f"Coleta: {resultado['ultima_coleta_anp']} | Fonte: {resultado['fonte']}")


if __name__ == '__main__':
    main()
