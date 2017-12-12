#Importamos las bibliotecas necesarias

import requests as rq
import json
import pandas as pd
import time
import numpy as np
import tables as tb

def preciosBitcoin(url):
    url = 'https://blockchain.info/es/ticker'
    r = rq.get(url)

    if (r.status_code == rq.codes.ok):
        datos_bitcoin = json.loads(r.text)
        if ('JPY' and 'USD' and 'EUR' in datos_bitcoin.keys()):
            monedas_fiat = ['JPY', 'USD', 'EUR']
            precio_venta_btc= list()
            precio_compra_btc = list()

            for k1 in monedas_fiat:
                if ('sell' and 'buy' in datos_bitcoin[k1].keys()):
                    tipo_precio_btc = ['sell', 'buy']
                    precios_btc= list()
                    for k2 in tipo_precio_btc:
                        precios_btc.append(datos_bitcoin[k1][k2])
                    
                precio_venta_btc.append(precios_btc[0])
                precio_compra_btc.append(precios_btc[1])

            if ((len(precio_venta_btc) == len(monedas_fiat)) and (len(precio_compra_btc) == len(monedas_fiat)) ):
                d = {'Precio Venta Bitcoin': precio_venta_btc,
                      'Precio Compra Bitcoin': precio_compra_btc}
                df = pd.DataFrame(d, index = monedas_fiat)
                
                return df
				
def cambioMoneda(url, base, moneda1, moneda2):
    
    r = rq.get(url)
         
    if (r.status_code == rq.codes.ok):
        datos_tipo_cambio = json.loads(r.text)
            
        if ('base' and 'rates' in datos_tipo_cambio):
            if ((datos_tipo_cambio['base']==base) and (moneda1 and moneda2 in datos_tipo_cambio['rates'])):
                return datos_tipo_cambio['rates'][moneda1], datos_tipo_cambio['rates'][moneda2]
				
def oportunidadesBitcoin():
    
    with tb.open_file('oportunidadesBitcoin.h5', mode = 'w', title = 'Datos de oportunidades bitcoin') as f:
        f.create_group("/", "seriesTemporales", title = "Series temporales de oportunidades bitcoin")
    
    store = pd.HDFStore('oportunidadesBitcoin.h5',mode='w')
    
    mostrados_max = 120
    mostrados = 0
    
    while (mostrados < mostrados_max):
        exec_time = time.time()
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(exec_time))
        
        url1 = 'https://blockchain.info/es/ticker'
        precios_bitcoin_df = preciosBitcoin(url1)
        
        url2 = 'http://api.fixer.io/latest?symbols=USD,JPY'
        url3 = 'http://api.fixer.io/latest?base=USD'
        url4 = 'http://api.fixer.io/latest?base=JPY'
        cambio_euro_yen, cambio_euro_dolar = cambioMoneda(url2, 'EUR', 'JPY', 'USD')
        cambio_dolar_yen, cambio_dolar_euro = cambioMoneda(url3, 'USD', 'JPY', 'EUR')
        cambio_yen_dolar, cambio_yen_euro = cambioMoneda(url4, 'JPY', 'USD', 'EUR')
    
        oportunidad_dolar_euro = int(precios_bitcoin_df.loc['USD']['Precio Venta Bitcoin']*cambio_dolar_euro \
        - precios_bitcoin_df.loc['EUR']['Precio Compra Bitcoin'])

        oportunidad_dolar_yen = int(precios_bitcoin_df.loc['USD']['Precio Venta Bitcoin']*cambio_dolar_yen \
        - precios_bitcoin_df.loc['JPY']['Precio Compra Bitcoin'])

        oportunidad_euro_dolar = int(precios_bitcoin_df.loc['EUR']['Precio Venta Bitcoin']*cambio_euro_dolar \
        - precios_bitcoin_df.loc['USD']['Precio Compra Bitcoin'])

        oportunidad_euro_yen = int(precios_bitcoin_df.loc['EUR']['Precio Venta Bitcoin']*cambio_euro_yen \
        - precios_bitcoin_df.loc['JPY']['Precio Compra Bitcoin'])

        oportunidad_yen_euro = int(precios_bitcoin_df.loc['JPY']['Precio Venta Bitcoin']*cambio_yen_euro \
        - precios_bitcoin_df.loc['EUR']['Precio Compra Bitcoin'])

        oportunidad_yen_dolar = int(precios_bitcoin_df.loc['JPY']['Precio Venta Bitcoin']*cambio_yen_dolar \
        - precios_bitcoin_df.loc['USD']['Precio Compra Bitcoin'])
        
        oportunidades = {'Oportunidad Dolar a Euro': oportunidad_dolar_euro,
                        'Oportunidad Dolar a Yen': oportunidad_dolar_yen,
                        'Oportunidad Euro a Dolar': oportunidad_euro_dolar,
                        'Oportunidad Euro a Yen': oportunidad_euro_yen,
                        'Oportunidad Yen a Euro': oportunidad_yen_euro,
                        'Oportunidad Yen a Dolar': oportunidad_yen_dolar,
                        'Tiempo': current_time}
        if(mostrados == 0):
            oportunidades_df = pd.DataFrame(oportunidades, index = np.arange(1))
            
        else:
            oportunidades_df = oportunidades_df.append(oportunidades, ignore_index = True)
        
        
        mostrados = mostrados + 1
        
        time.sleep(60.0)
    
    store.append("seriesTemporales", oportunidades_df)
    store.close()

oportunidadesBitcoin()