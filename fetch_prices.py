"""
NoCorre — Coletor de Preços ANP
Roda toda segunda-feira via GitHub Actions.
Gera prices.json com preços médios de combustível por estado.

Estratégia de fontes (em cascata):
1. ANP dados abertos — CSV direto das últimas 4 semanas (automotivos)
2. ANP dados abertos — CSV mensal do ano atual
3. ANP dados abertos — CSV do mês anterior
4. Fallback embutido
"""

import json, urllib.request, io, csv, datetime, os, time

REGIOES = {
    'AC':'Norte','AM':'Norte','AP':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
    'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste',
    'PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
    'DF':'Centro-Oeste','GO':'Centro-Oeste','MS':'Centro-Oeste','MT':'Centro-Oeste',
    'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
    'PR':'Sul','RS':'Sul','SC':'Sul',
}
ORDEM = ['Norte','Nordeste','Centro-Oeste','Sudeste','Sul']
PRODUTOS = {
    'GASOLINA COMUM':'gasolina','ETANOL HIDRATADO':'etanol',
    'ÓLEO DIESEL':'diesel','OLEO DIESEL':'diesel',
    'ÓLEO DIESEL S10':'diesel','OLEO DIESEL S10':'diesel',
}
HEADERS = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept':'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language':'pt-BR,pt;q=0.9',
    'Referer':'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos',
    'Cache-Control':'no-cache',
}

def urls():
    a, m = datetime.date.today().year, datetime.date.today().month
    base = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan'
    ma, aa = (m-1 or 12), (a if m>1 else a-1)
    return [
        f'{base}/ultimas-4-semanas-automotivos.csv',
        f'{base}/{a}/ca-{a}-{m:02d}.csv',
        f'{base}/{aa}/ca-{aa}-{ma:02d}.csv',
    ]

def baixar(url):
    for i in range(3):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
            for enc in ('latin-1','utf-8','cp1252'):
                try: return raw.decode(enc)
                except: pass
        except Exception as e:
            print(f'  [{i+1}/3] {type(e).__name__}: {e}')
            if i < 2: time.sleep(3)
    return None

def parsear(txt):
    sep = ';' if txt[:500].count(';') > txt[:500].count(',') else ','
    reader = csv.DictReader(io.StringIO(txt), delimiter=sep)
    dados, ultima = {}, None
    for row in reader:
        row = {k.strip().lstrip('\ufeff'):v for k,v in row.items()}
        uf = (row.get('Estado - Sigla') or row.get('ESTADO') or row.get('UF') or '').strip().upper()
        prod = (row.get('Produto') or row.get('PRODUTO') or '').strip().upper()
        ps = (row.get('Preço Médio Revenda') or row.get('PREÇO MÉDIO REVENDA') or
              row.get('Valor de Venda') or '').replace(',','.').strip()
        dt = (row.get('Data da Coleta') or row.get('DATA DA COLETA') or
              row.get('Data Inicial') or '').strip()
        if not uf or not prod or not ps or uf not in REGIOES: continue
        comb = PRODUTOS.get(prod)
        if not comb: continue
        try:
            p = float(ps)
            if not (0 < p < 20): continue
        except: continue
        dados.setdefault(uf, {}).setdefault(comb, []).append(p)
        if dt: ultima = dt
    return dados, ultima

def med(lst): return round(sum(lst)/len(lst),2) if lst else 0.0

def tend(novo, ant):
    if ant is None or novo == 0: return 'estavel'
    return 'subida' if novo-ant > 0.05 else 'descida' if ant-novo > 0.05 else 'estavel'

def fmtdata(s):
    for f in ('%d/%m/%Y','%Y-%m-%d','%d-%m-%Y'):
        try: return datetime.datetime.strptime(s,f).strftime('%Y-%m-%d')
        except: pass
    return datetime.date.today().isoformat()

def montar(dados, ultima, ant=None):
    reg = {}
    for uf, combs in dados.items():
        r = REGIOES.get(uf)
        if not r: continue
        reg.setdefault(r, {'nome':r,'estados':[]})
        e = {'id_estado':uf}
        for c in ['gasolina','etanol','diesel']:
            m = med(combs.get(c,[]))
            e[f'media_{c}'] = m
            e[f'tendencia_{c}'] = tend(m, (ant or {}).get(uf,{}).get(c))
        reg[r]['estados'].append(e)
    regioes = []
    for nome in ORDEM:
        if nome not in reg: continue
        r = reg[nome]
        for c in ['gasolina','etanol','diesel']:
            vals = [e[f'media_{c}'] for e in r['estados'] if e[f'media_{c}']>0]
            r[f'media_{c}'] = med(vals)
        regioes.append(r)
    return {
        'ultima_coleta_anp': fmtdata(ultima) if ultima else datetime.date.today().isoformat(),
        'gerado_em': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'fonte': 'ANP - Agência Nacional do Petróleo',
        'regioes': regioes,
    }

FALLBACK = {"ultima_coleta_anp":"2026-04-25","gerado_em":"2026-04-25T00:00:00","fonte":"fallback_embutido","regioes":[{"nome":"Norte","media_gasolina":6.21,"media_etanol":4.11,"media_diesel":5.78,"estados":[{"id_estado":"AM","media_gasolina":6.34,"tendencia_gasolina":"subida","media_etanol":4.20,"tendencia_etanol":"estavel","media_diesel":5.91,"tendencia_diesel":"subida"},{"id_estado":"PA","media_gasolina":6.08,"tendencia_gasolina":"estavel","media_etanol":4.02,"tendencia_etanol":"descida","media_diesel":5.65,"tendencia_diesel":"estavel"},{"id_estado":"AC","media_gasolina":6.45,"tendencia_gasolina":"subida","media_etanol":4.30,"tendencia_etanol":"estavel","media_diesel":5.99,"tendencia_diesel":"subida"},{"id_estado":"RO","media_gasolina":6.10,"tendencia_gasolina":"estavel","media_etanol":4.05,"tendencia_etanol":"estavel","media_diesel":5.72,"tendencia_diesel":"estavel"},{"id_estado":"RR","media_gasolina":6.38,"tendencia_gasolina":"subida","media_etanol":4.22,"tendencia_etanol":"estavel","media_diesel":5.95,"tendencia_diesel":"subida"},{"id_estado":"AP","media_gasolina":6.28,"tendencia_gasolina":"estavel","media_etanol":4.15,"tendencia_etanol":"estavel","media_diesel":5.80,"tendencia_diesel":"estavel"},{"id_estado":"TO","media_gasolina":5.98,"tendencia_gasolina":"descida","media_etanol":3.95,"tendencia_etanol":"descida","media_diesel":5.60,"tendencia_diesel":"estavel"}]},{"nome":"Nordeste","media_gasolina":5.98,"media_etanol":3.89,"media_diesel":5.54,"estados":[{"id_estado":"BA","media_gasolina":6.01,"tendencia_gasolina":"subida","media_etanol":3.95,"tendencia_etanol":"estavel","media_diesel":5.58,"tendencia_diesel":"estavel"},{"id_estado":"CE","media_gasolina":5.92,"tendencia_gasolina":"estavel","media_etanol":3.80,"tendencia_etanol":"descida","media_diesel":5.49,"tendencia_diesel":"estavel"},{"id_estado":"PE","media_gasolina":5.95,"tendencia_gasolina":"estavel","media_etanol":3.85,"tendencia_etanol":"estavel","media_diesel":5.52,"tendencia_diesel":"estavel"},{"id_estado":"MA","media_gasolina":6.05,"tendencia_gasolina":"subida","media_etanol":3.98,"tendencia_etanol":"estavel","media_diesel":5.65,"tendencia_diesel":"subida"},{"id_estado":"PB","media_gasolina":5.88,"tendencia_gasolina":"estavel","media_etanol":3.75,"tendencia_etanol":"estavel","media_diesel":5.44,"tendencia_diesel":"estavel"},{"id_estado":"RN","media_gasolina":5.90,"tendencia_gasolina":"estavel","media_etanol":3.78,"tendencia_etanol":"estavel","media_diesel":5.47,"tendencia_diesel":"estavel"},{"id_estado":"AL","media_gasolina":5.97,"tendencia_gasolina":"subida","media_etanol":3.88,"tendencia_etanol":"estavel","media_diesel":5.55,"tendencia_diesel":"estavel"},{"id_estado":"PI","media_gasolina":6.02,"tendencia_gasolina":"estavel","media_etanol":3.92,"tendencia_etanol":"estavel","media_diesel":5.62,"tendencia_diesel":"estavel"},{"id_estado":"SE","media_gasolina":5.94,"tendencia_gasolina":"estavel","media_etanol":3.82,"tendencia_etanol":"estavel","media_diesel":5.50,"tendencia_diesel":"estavel"}]},{"nome":"Centro-Oeste","media_gasolina":5.77,"media_etanol":3.72,"media_diesel":5.42,"estados":[{"id_estado":"DF","media_gasolina":5.81,"tendencia_gasolina":"estavel","media_etanol":3.78,"tendencia_etanol":"estavel","media_diesel":5.45,"tendencia_diesel":"estavel"},{"id_estado":"GO","media_gasolina":5.73,"tendencia_gasolina":"descida","media_etanol":3.66,"tendencia_etanol":"estavel","media_diesel":5.39,"tendencia_diesel":"estavel"},{"id_estado":"MT","media_gasolina":5.79,"tendencia_gasolina":"estavel","media_etanol":3.72,"tendencia_etanol":"descida","media_diesel":5.44,"tendencia_diesel":"estavel"},{"id_estado":"MS","media_gasolina":5.75,"tendencia_gasolina":"estavel","media_etanol":3.72,"tendencia_etanol":"estavel","media_diesel":5.40,"tendencia_diesel":"estavel"}]},{"nome":"Sudeste","media_gasolina":5.89,"media_etanol":3.68,"media_diesel":5.41,"estados":[{"id_estado":"RJ","media_gasolina":6.05,"tendencia_gasolina":"estavel","media_etanol":3.81,"tendencia_etanol":"estavel","media_diesel":5.62,"tendencia_diesel":"subida"},{"id_estado":"SP","media_gasolina":5.74,"tendencia_gasolina":"descida","media_etanol":3.55,"tendencia_etanol":"descida","media_diesel":5.28,"tendencia_diesel":"estavel"},{"id_estado":"MG","media_gasolina":5.88,"tendencia_gasolina":"estavel","media_etanol":3.62,"tendencia_etanol":"estavel","media_diesel":5.44,"tendencia_diesel":"estavel"},{"id_estado":"ES","media_gasolina":5.89,"tendencia_gasolina":"subida","media_etanol":3.74,"tendencia_etanol":"estavel","media_diesel":5.30,"tendencia_diesel":"estavel"}]},{"nome":"Sul","media_gasolina":5.71,"media_etanol":3.61,"media_diesel":5.35,"estados":[{"id_estado":"PR","media_gasolina":5.68,"tendencia_gasolina":"estavel","media_etanol":3.57,"tendencia_etanol":"descida","media_diesel":5.31,"tendencia_diesel":"estavel"},{"id_estado":"RS","media_gasolina":5.75,"tendencia_gasolina":"estavel","media_etanol":3.66,"tendencia_etanol":"estavel","media_diesel":5.39,"tendencia_diesel":"estavel"},{"id_estado":"SC","media_gasolina":5.70,"tendencia_gasolina":"estavel","media_etanol":3.60,"tendencia_etanol":"estavel","media_diesel":5.35,"tendencia_diesel":"estavel"}]}]}

def main():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'prices.json')
    ant = None
    if os.path.exists(path):
        try:
            with open(path) as f: old = json.load(f)
            if old.get('fonte') != 'fallback_embutido':
                ant = {e['id_estado']:{c:e[f'media_{c}'] for c in ['gasolina','etanol','diesel']}
                       for r in old.get('regioes',[]) for e in r.get('estados',[])}
                print(f'Histórico carregado: {len(ant)} estados.')
        except Exception as e: print(f'Aviso histórico: {e}')

    resultado = None
    for url in urls():
        print(f'\n→ {url}')
        txt = baixar(url)
        if not txt: continue
        dados, ultima = parsear(txt)
        n = sum(1 for v in dados.values() if any(len(p)>0 for p in v.values()))
        print(f'  {n} estados com dados')
        if n >= 20:
            resultado = montar(dados, ultima, ant)
            print(f'  OK — coleta: {resultado["ultima_coleta_anp"]}')
            break
        print(f'  Insuficiente, tentando próxima...')

    if not resultado:
        print('\nFALLBACK: todas as fontes falharam.')
        resultado = dict(FALLBACK)
        resultado['gerado_em'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    with open(path,'w',encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f'\n✓ prices.json salvo')
    print(f'  fonte:  {resultado["fonte"]}')
    print(f'  coleta: {resultado["ultima_coleta_anp"]}')
    print(f'  gerado: {resultado["gerado_em"]}')

if __name__ == '__main__':
    main()
