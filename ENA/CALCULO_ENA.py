# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 09:06:45 2022
@author: alex.lourenco
"""
import pendulum
import pandas as pd
import pyodbc
import os
import datetime
import time
from pathlib import Path 
from datetime import timedelta
from sqlalchemy.engine import URL
import schedule
from sqlalchemy import create_engine
def engine():
    connection_string = "DRIVER={SQL Server};SERVER=CL01VTF02ENVSQL;DATABASE=RPACOMERCIALIZADORA;UID=UGPL01T02F58;PWD=P.GaX1Y2zP"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    return engine
def atualizar_dados(data):
  m_ti = time.ctime(data) 
  t_obj = time.strptime(m_ti) 
  T_stamp = time.strftime("%d/%m/%Y %H:%M:%S", t_obj) 
  return T_stamp
def hora ():
  now = datetime.datetime.now() 
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%d/%m/%Y %H:%M')
  now_ = datetime.datetime.now()
  #date = now - datetime.timedelta(days=1)
  agora_ = now_.strftime('%d/%m/%Y %H:%M')      
  return agora, agora_
def data ():
  now = datetime.datetime.now()
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%m/%d/%Y %H:%M')       
  return agora
def data_banco ():
   eng = engine()
   query = "SELECT TOP 1 * FROM  ENA.ENA_BACIA ORDER BY Data desc"
   df = pd.read_sql(query, eng)
   data = list(df['Data'])
   return data[0]
def banco ():
   eng = engine()
   query = "SELECT TOP 1 * FROM  ENA.ENA_BACIA ORDER BY Data desc"
   df = pd.read_sql(query, eng)
   data = list(df['Data'])
   return data[0]
''' A partir daqui o código atualiza a ENA com o arquivo ACOMPH '''
def inserir_bdo_ena(data, indicador, seco, s, ne, n, sin):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into dbo.BASE_ENA(
                                                 data,
                                                 INDICADOR,
                                                 SECO,
                                                 S,
                                                 NE,
                                                 N, SIN) values(?, ?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n, sin) )
    conn.commit()
    conn.close()
def caminho_acomph():
    ##  Essa funcao retorna o caminho do arquivo acomph  e data de atualizacao no formato de string 
    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\07.ACOMPH")
    caminho_acomp = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), data_requisitada.format('YYYY') + '-' + data_requisitada.format('MM'))
    lista_arquivos_acomp = os.listdir(caminho_acomp)
    lista_datas_acomp = []
    for arquivo_acomp in lista_arquivos_acomp:
        if ".xlsx" or ".xls" or ".xlsm" in arquivo_acomp:
                data_acomp = os.path.getmtime(f"{caminho_acomp}/{arquivo_acomp}")
                lista_datas_acomp.append((data_acomp, arquivo_acomp))
    lista_datas_acomp.sort(reverse=True)
    ultimo_arquivo_acomp = lista_datas_acomp[0]
    data_acomp = lista_datas_acomp[0]
    ultima_data_acomp = atualizar_dados(data_acomp[0]) #.strftime('%d/%m/%Y')
    ultima_data_acomph = datetime.datetime.strptime(ultima_data_acomp, '%d/%m/%Y %H:%M:%S').date() - timedelta(days= 1)
    ultima_data_acomp = ultima_data_acomph.strftime("%Y-%m-%d") 
    data_hora = ultima_data_acomph.strftime("%Y-%m-%d")
    path = r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\07.ACOMPH\2023\2023-07\ACOMPH_18.07.2023.xls'
    ultima_data_acomp = '2023-01-14'
    data_hora = ultima_data_acomp
    ultimo_arquivo_acomp = data_hora
    #return (Path.joinpath(caminho_acomp,ultimo_arquivo_acomp[1]), ultima_data_acomp, data_hora)
    return(path, ultima_data_acomp, data_hora, data_hora)
def prod() -> list:    
    eng = engine()
    query = f"SELECT * FROM ENA.PRODUTIBILIDADE_POSTOS;"
    df = pd.read_sql(query, eng)
    return df
# print(prod()[prod()['BACIA']=='Grande'])
def acomph() -> list:
    acomph = caminho_acomph()[0]
    lista_bacias = ['Grande', 'Paranaíba', 'Tietê', 'Paranapanema', 'Paraná', 'Iguaçu', 'Uruguai', 'Jacui', 'Outras Sul', 'Paraguai', 'Paraíba do Sul', 'Doce', 'Outras Sudeste', 'São Francisco',  'Outras Nordeste', 'Tocantins','Amazonas', 'Araguari']                                                
    #colunas = ['I, Q, Y, AG, AO, AW, BE, BM, BU, CC, CK, CS, DA, DI'] #['I','Q','Y','AG','AO','AW','BE','BM','BU','CC','CK','CS','DA','DI']
    # Grande
    df_bacia = pd.read_excel(io = acomph, sheet_name = lista_bacias[0] , header = None,  usecols= 'I, Q, Y, AG, AO, AW, BE, BM, BU, CC, CK, CS, DA, DI', skiprows=(34), nrows = 1 )
    lista_grande = df_bacia.values
    # Paranaiba
    df_bacia_paranaiba = pd.read_excel(io = acomph, sheet_name = lista_bacias[1] , header = None,  usecols= 'I, Q, Y, AG, AO, AW, BE, BM, BU, CC, CK, CS, DA, DI, DQ, DY, EG, EO, EW', skiprows=(34), nrows = 1)
    lista_paranaiba = df_bacia_paranaiba.values
    # Alto tiete
    df_bacia_tiete = pd.read_excel(io = acomph, sheet_name = lista_bacias[2] , header = None,  usecols= ' AG, AO, AW, BE, BM, BU', skiprows=(34), nrows = 1 )
    lista_tiete = df_bacia_tiete.values
    #Paranapanema
    df_bacia_paranapanema = pd.read_excel(io = acomph, sheet_name = lista_bacias[3] , header = None,  usecols= 'I, Q, Y, AO, AW, BE, BU, CC, CK', skiprows=(34), nrows = 1 )
    lista_paranapanema = df_bacia_paranapanema.values
    #Paranapanema SUL
    df_bacia_paranapanema_s = pd.read_excel(io = acomph, sheet_name = lista_bacias[3] , header = None,  usecols= 'BM', skiprows=(34), nrows = 1 )
    lista_paranapanema_s = df_bacia_paranapanema_s.values
    # Alto Parana
    df_bacia_alto_parana = pd.read_excel(io = acomph, sheet_name = lista_bacias[4] , header = None,  usecols= 'I, Q', skiprows=(34), nrows = 1 )
    lista_alto_parana = df_bacia_alto_parana.values
    # Baixo Parana
    df_bacia_baixo_parana = pd.read_excel(io = acomph, sheet_name = lista_bacias[4] , header = None,  usecols= 'Y, AG, AO', skiprows=(34), nrows = 1 )
    lista_baixo_parana = df_bacia_baixo_parana.values
    #Iguaçu     
    df_bacia_iguacu = pd.read_excel(io = acomph, sheet_name = lista_bacias[5] , header = None,  usecols= 'I, Q, Y, AG, AO, AW, BE, BU', skiprows=(34), nrows = 1 )
    lista_iguacu = df_bacia_iguacu.values
    #Uruguai
    df_bacia_uruguai = pd.read_excel(io = acomph, sheet_name = lista_bacias[6] , header = None,  usecols= 'I, Q, Y, AG, AO, AW, BE, BM, BU, CC, CK', skiprows=(34), nrows = 1 )
    lista_uruguai = df_bacia_uruguai.values
    #Jacui
    df_bacia_jacui = pd.read_excel(io = acomph, sheet_name = lista_bacias[7] , header = None,  usecols= 'Q, Y, AG, AO, AW, BE, BM', skiprows=(34), nrows = 1 )
    lista_jacui = df_bacia_jacui.values      
    # PARAGAUI
    df_bacia_paraguai = pd.read_excel(io = acomph, sheet_name = lista_bacias[9] , header = None,  usecols= 'I, Q, Y, AG', skiprows=(34), nrows = 1 )
    lista_paraguai = df_bacia_paraguai.values  
    # Paraiba do Sul 
    df_bacia_paraiba_do_sul = pd.read_excel(io = acomph, sheet_name = lista_bacias[10] , header = None,  usecols= 'I, Q, Y, AG, AW, BE, BU', skiprows=(34), nrows = 1 )
    lista_paraiba_do_sul = df_bacia_paraiba_do_sul.values  
    # Doce
    df_bacia_doce = pd.read_excel(io = acomph, sheet_name = lista_bacias[11] , header = None,  usecols= 'I, Q, Y, AG, AO, AW, BE, BM', skiprows=(34), nrows = 1 )
    lista_doce = df_bacia_doce.values
    # Sao Francisco
    df_bacia_sao_francisco_se = pd.read_excel(io = acomph, sheet_name = lista_bacias[13] , header = None,  usecols= 'I, Q, Y', skiprows=(34), nrows = 1 )
    lista_sao_francisco_se = df_bacia_sao_francisco_se.values
    # Amazonas - Sudeste
    df_bacia_amazonas_se = pd.read_excel(io = acomph, sheet_name = lista_bacias[16] , header = None,  usecols= 'I, Y, AG, AO, AW, BE, CC, CK, CS, DA', skiprows=(34), nrows = 1 )
    lista_amazonas_se = df_bacia_amazonas_se.values
    # Amazonas - Norte
    df_bacia_amazonas_n = pd.read_excel(io = acomph, sheet_name = lista_bacias[16] , header = None,  usecols= 'Q, BM, BU', skiprows=(34), nrows = 1 )
    lista_amazonas_n = df_bacia_amazonas_n.values
    #Araguari
    df_bacia_araguari = pd.read_excel(io = acomph, sheet_name = lista_bacias[17] , header = None,  usecols= 'I, Q, Y', skiprows=(34), nrows = 1 )
    lista_araguari = df_bacia_araguari.values
    #Mucuri - Outras Sudeste
    df_bacia_mucuri = pd.read_excel(io = acomph, sheet_name = lista_bacias[12] , header = None,  usecols= 'Q', skiprows=(34), nrows = 1 )
    lista_mucuri = df_bacia_mucuri.values
    # Itabapoana - Outras Sudeste
    df_bacia_itabapoana = pd.read_excel(io = acomph, sheet_name = lista_bacias[12] , header = None,  usecols= 'I', skiprows=(34), nrows = 1 )
    lista_itabapoana = df_bacia_itabapoana.values
    #Jequitinhonha SE - Outras Nordeste
    df_bacia_jequitinhonha_se = pd.read_excel(io = acomph, sheet_name = lista_bacias[14] , header = None,  usecols= 'Q', skiprows=(34), nrows = 1 )
    lista_jequitinhonha_se = df_bacia_jequitinhonha_se.values
    #Jequitinhonha NE - Outras Nordeste
    df_bacia_jequitinhonha_ne = pd.read_excel(io = acomph, sheet_name = lista_bacias[14] , header = None,  usecols= 'Y', skiprows=(34), nrows = 1 )
    lista_jequitinhonha_ne = df_bacia_jequitinhonha_ne.values
    #Tocantins_ne
    df_bacia_tocantins_se = pd.read_excel(io = acomph, sheet_name = lista_bacias[15] , header = None,  usecols= 'I, Q, Y, AG, AO ', skiprows=(34), nrows = 1 )
    lista_tocantins_se = df_bacia_tocantins_se.values
    #Tocantins_Norte
    df_bacia_tocantins_n = pd.read_excel(io = acomph, sheet_name = lista_bacias[15] , header = None,  usecols= 'AW, BE', skiprows=(34), nrows = 1 )
    lista_tocantins_n = df_bacia_tocantins_n.values
    # São Francisco - NE 
    df_bacia_sao_francisco_ne = pd.read_excel(io = acomph, sheet_name = lista_bacias[13] , header = None,  usecols= 'AG, AO, AW, BE ', skiprows=(34), nrows = 1 )
    lista_sao_francisco_ne = df_bacia_sao_francisco_ne.values
    # ARAGUARI
    df_bacia_araguari = pd.read_excel(io = acomph, sheet_name = 'Araguari' , header = None,  usecols= 'Q, Y', skiprows=(34), nrows = 1 )
    lista_araguari = df_bacia_araguari.values
    # Capivari
    df_bacia_capivari = pd.read_excel(io = acomph, sheet_name = 'Outras Sul' , header = None,  usecols= 'I', skiprows=(34), nrows = 1 )
    lista_capivari = df_bacia_capivari.values
    # Itajai Acu
    df_bacia_itajai = pd.read_excel(io = acomph, sheet_name = 'Outras Sul' , header = None,  usecols= 'Q', skiprows=(34), nrows = 1 )
    lista_itajai = df_bacia_capivari.values
    # Xingu
    df_bacia_xingu = pd.read_excel(io = acomph, sheet_name = 'Amazonas' , header = None,  usecols= 'DI', skiprows=(34), nrows = 1 )
    lista_xingu = df_bacia_xingu.values
    # Parnaiba
    df_bacia_parnaiba = pd.read_excel(io = acomph, sheet_name = 'Outras Nordeste' , header = None,  usecols= 'I', skiprows=(34), nrows = 1 )
    lista_parnaiba = df_bacia_parnaiba.values
    # Paraguaçu
    df_bacia_paraguacu = pd.read_excel(io = acomph, sheet_name = 'Outras Nordeste' , header = None,  usecols= 'AG', skiprows=(34), nrows = 1 )
    lista_paraguacu = df_bacia_paraguacu.values
    
    return (lista_grande, lista_paranaiba, lista_tiete, lista_paranapanema, lista_alto_parana, lista_baixo_parana,
            lista_iguacu, lista_uruguai, lista_jacui, lista_paraguai, lista_paraiba_do_sul, lista_doce,
            lista_sao_francisco_se, lista_araguari,  lista_mucuri, lista_itabapoana, lista_jequitinhonha_se,
            lista_jequitinhonha_ne, lista_tocantins_n, lista_tocantins_se, lista_amazonas_se, lista_amazonas_n,
            lista_paranapanema_s, lista_sao_francisco_ne, lista_araguari, lista_capivari, lista_itajai, lista_xingu,
            lista_parnaiba, lista_paraguacu)
print(acomph()[7][0])
def produtibilidade():
    # lista_bacias = ['GRANDE', 'PARANAIBA', 'TIETÊ', 'PARANAPANEMA ', 'Paraná', 'IGUAÇU', 'URUGUAI', 'JACUÍ', 'OUTRAS_SUL', 'PARAGUAI', 'PARAIBA DO SUL', 'DOCE', 'ITABAPOANA', 'SÃO FRANCISCO (SE)', 'SÃO FRANCISCO (NE)',  'JEQUITINHONHA (NE)', 'TOCANTINS (SE)','AMAZONAS (SE)', 'ARAGUARI', 'MUCURI', 'PARANAPANEMA (S)', 'SÃO FRANCISCO (NE)', 'JEQUITINHONHA (SE)', 'TOCANTINS (N)', 'ARAGUARI']
    lista_bacias = ['GRANDE', 'PARANAIBA', 'TIETÊ', 'PARANAPANEMA', 'Paraná', 'IGUAÇU', 'URUGUAI', 'JACUÍ',
                    'OUTRAS_SUL', 'PARAGUAI', 'PARAIBA DO SUL', 'DOCE', 'ITABAPOANA', 'SÃO FRANCISCO (SE)',
                    'SÃO FRANCISCO (NE)', 'JEQUITINHONHA (NE)', 'TOCANTINS (SE)', 'AMAZONAS (SE)', 'ARAGUARI', 'MUCURI',
                    'PARANAPANEMA (S)', 'SÃO FRANCISCO (NE)', 'JEQUITINHONHA (SE)', 'TOCANTINS (N)', 'ARAGUARI']
    df = prod()
    lista_grande = df[df['BACIA'] == lista_bacias[0]]['PRODUTIBILIDADE'].values
    lista_paranaiba = df[df['BACIA'] == lista_bacias[1]]['PRODUTIBILIDADE'].values
    lista_tiete = df[df['BACIA'] == lista_bacias[2]]['PRODUTIBILIDADE'].values
    lista_paranapanema = df[df['BACIA'] == lista_bacias[3]]['PRODUTIBILIDADE'].values
    lista_paranapanema_s = df[df['BACIA'] == lista_bacias[20]]['PRODUTIBILIDADE'].values
    lista_parana = df[df['BACIA'] == lista_bacias[4]]['PRODUTIBILIDADE'].values
    lista_iguacu = df[df['BACIA'] == lista_bacias[5]]['PRODUTIBILIDADE'].values
    lista_uruguai = df[df['BACIA'] == lista_bacias[6]]['PRODUTIBILIDADE'].values
    lista_jacui = df[df['BACIA'] == lista_bacias[7]]['PRODUTIBILIDADE'].values
    lista_outras_sul = df[df['BACIA'] == lista_bacias[8]]['PRODUTIBILIDADE'].values
    lista_paraguai = df[df['BACIA'] == lista_bacias[9]]['PRODUTIBILIDADE'].values
    lista_paraiba_do_sul = df[df['BACIA'] == lista_bacias[10]]['PRODUTIBILIDADE'].values
    lista_doce = df[df['BACIA'] == lista_bacias[11]]['PRODUTIBILIDADE'].values
    lista_itabapoana = df[df['BACIA'] == lista_bacias[12]]['PRODUTIBILIDADE'].values
    lista_sao_francisco_se = df[df['BACIA'] == lista_bacias[13]]['PRODUTIBILIDADE'].values
    lista_jequitinhonha_ne = df[df['BACIA'] == 'JEQUITINHONHA (NE)']['PRODUTIBILIDADE'].values
    lista_tocantins_se = df[df['BACIA'] == lista_bacias[15]]['PRODUTIBILIDADE'].values 
    lista_amazonas_se = df[df['BACIA'] == 'AMAZONAS (SE)']['PRODUTIBILIDADE'].values
    lista_araguari = df[df['BACIA'] == lista_bacias[17]]['PRODUTIBILIDADE'].values
    lista_mucuri = df[df['BACIA'] == lista_bacias[18]]['PRODUTIBILIDADE'].values
    lista_sao_francisco_ne = df[df['BACIA'] == lista_bacias[19]]['PRODUTIBILIDADE'].values
    lista_jequitinhonha_se = df[df['BACIA'] == lista_bacias[22]]['PRODUTIBILIDADE'].values
    lista_tocantins_n = df[df['BACIA'] == lista_bacias[23]]['PRODUTIBILIDADE'].values
    lista_sao_francisco_ne = df[df['BACIA'] == lista_bacias[14]]['PRODUTIBILIDADE'].values
    lista_araguari = df[df['BACIA'] == lista_bacias[24]]['PRODUTIBILIDADE'].values
    lista_capivari = df[df['BACIA'] == 'CAPIVARI']['PRODUTIBILIDADE'].values
    lista_itajai = df[df['BACIA'] == 'ITAJAÍ-AÇU']['PRODUTIBILIDADE'].values
    lista_amazonas_n = df[df['BACIA'] == 'AMAZONAS (N)']['PRODUTIBILIDADE'].values
    lista_xingu = df[df['BACIA'] == 'XINGU']['PRODUTIBILIDADE'].values
    lista_parnaiba = df[df['BACIA'] == 'PARNAÍBA']['PRODUTIBILIDADE'].values
    lista_paraguacu = df[df['BACIA'] == 'PARAGUAÇU']['PRODUTIBILIDADE'].values
    return (lista_grande, lista_paranaiba, lista_tiete, lista_paranapanema, lista_parana, lista_iguacu, lista_uruguai,
           lista_jacui, lista_outras_sul, lista_paraguai, lista_paraiba_do_sul, lista_doce, lista_itabapoana,
           lista_sao_francisco_se, lista_jequitinhonha_ne, lista_tocantins_se, lista_amazonas_se, lista_araguari,
           lista_mucuri, lista_sao_francisco_ne, lista_jequitinhonha_se, lista_tocantins_n, lista_paranapanema_s,
           lista_araguari, lista_capivari, lista_itajai, lista_amazonas_n, lista_xingu, lista_parnaiba, lista_paraguacu)
print(produtibilidade()[6])
def calculo():
    lista_acomph = acomph()
    prod = produtibilidade()
    grande = list()
    for i in range(1, 15):
                    grande.append(prod[0][i-1]* lista_acomph[0][0][i-1])
    ena_grande = 0
    for i in grande: 
        ena_grande += i
    # Paranaiba
    paranaiba = list()
    for i in range(1, 20):
                paranaiba.append(lista_acomph[1][0][i-1] * prod[1][i-1])
                #print(li)
    ena_paranaiba = 0
    for i in paranaiba: 
         ena_paranaiba += i     
        # TIETE
    tiete = list()
    for i in range(1, 7):
                tiete.append(lista_acomph[2][0][i-1] * prod[2][i-1])
                #print(li)
    ena_tiete = 0
    for i in tiete: 
         ena_tiete += i
    #Paranapanema
    paranapanema = list()
    for i in range(1, 10):
                paranapanema.append(lista_acomph[3][0][i-1] * prod[3][i-1])
    ena_paranapanema_se = 0
    for i in paranapanema: 
         ena_paranapanema_se += i   
    #Paranapanema sul
    paranapanema_s = list()
    for i in range(1, 2):
                paranapanema_s.append(lista_acomph[22][0][i-1] * prod[22][i-1])
    ena_paranapanema_s = 0
    for i in paranapanema: 
         ena_paranapanema_s += i  
    # ENA_SUL + ENA_SE     
    ena_paranapanema =  ena_paranapanema_se + ena_paranapanema_s   
    # Alto Parana
    alto_parana = list()
    for i in range(1, 3):
                alto_parana.append(lista_acomph[4][0][i-1] * prod[4][i-1])
                #print(li)
    ena_parana_alto = 0
    for i in alto_parana: 
         ena_parana_alto += i  
    # Baixo Parana
    baixo_parana = list()
    for i in range(1, 4):
                baixo_parana.append(lista_acomph[5][0][i-1] * prod[4][i+1])
                #print(li)
    ena_parana_baixo = 0
    for i in baixo_parana: 
         ena_parana_baixo += i 
    ## ENA PARANÁ TOTAL 
    ena_parana = ena_parana_baixo + ena_parana_alto
     # Iguacu
    iguacu = list()
    for i in range(1, 9):
                iguacu.append(lista_acomph[6][0][i-1] * prod[5][i-1])
                #print(li)
    ena_iguacu = 0
    for i in iguacu: 
         ena_iguacu += i   
    # Uruguai      
    uruguai = list()
    for i in range(1, 12):
                uruguai.append(lista_acomph[7][0][i-1] * prod[6][i-1])
                print((lista_acomph[7][0][i-1] * prod[6][i-1]))
    ena_uruguai = 0
    for i in uruguai: 
         ena_uruguai += i     
    #Jacui 
    jacui = list()
    for i in range(1, 8):
                jacui.append(lista_acomph[8][0][i-1] * prod[7][i-1])
                #print(li)
    ena_jacui = 0
    for i in jacui: 
         ena_jacui += i 
    # Paraguai
    paraguai = list()
    for i in range(1, 5):
                paraguai.append(lista_acomph[9][0][i-1] * prod[9][i-1])
                #print(li)
    ena_paraguai = 0
    for i in paraguai: 
         ena_paraguai += i   
    # Paraiba do Sul 
    paraiba_do_sul = list()
    for i in range(1, 8):
                paraiba_do_sul.append(lista_acomph[10][0][i-1] * prod[10][i-1])
                #print(li)
    ena_paraiba_do_sul = 0
    for i in paraiba_do_sul: 
         ena_paraiba_do_sul += i  
    # Rever o Paraiba do sul     
    # Doce
    doce = list()
    for i in range(1, 9):
                doce.append(lista_acomph[11][0][i-1] * prod[11][i-1])
                #print(li)
    ena_doce = 0
    for i in doce: 
         ena_doce += i
    # Mucuri - Outras Sudeste
    mucuri = list()
    for i in range(1, 2):
                mucuri.append(lista_acomph[14][0][i-1] * prod[18][i-1])
                #print(li)
    ena_mucuri  = 0
    for i in mucuri : 
         ena_mucuri += i
    # Itabapoana - Outras Sudeste
    itabapoana = list()
    for i in range(1, 2):
                itabapoana.append(lista_acomph[16][0][i-1] * prod[12][i-1])
                #print(li)
    ena_itabapoana  = 0
    for i in itabapoana : 
         ena_itabapoana += i     
    
    # Sao Francisco - SE 
    sao_francisco_se = list()
    for i in range(1, 4):
                sao_francisco_se.append(lista_acomph[12][0][i-1] * prod[13][i-1])
                
    ena_sao_francisco_se = 0
    for i in sao_francisco_se: 
         ena_sao_francisco_se += i
    # Sao Francisco - NE 
    sao_francisco_ne = list()
    for i in range(1, 5):
                sao_francisco_ne.append(lista_acomph[23][0][i-1] * prod[19][i-1])
                
    ena_sao_francisco_ne = 0
    for i in sao_francisco_ne: 
         ena_sao_francisco_ne += i
    # Ena São Francisco SE + NE
    ena_sf =  ena_sao_francisco_se + ena_sao_francisco_ne        
    # Tocatins
    tocantins_se = list()
    for i in range(1, 6):
                tocantins_se.append(lista_acomph[19][0][i-1] * prod[16][i-1])
              
    ena_tocantins_se = 0
    for i in tocantins_se: 
         ena_tocantins_se += i
    # Tocatins - N
    tocantins_n = list()
    for i in range(1, 3):
                tocantins_n.append(lista_acomph[18][0][i-1] * prod[21][i-1])
              
    ena_tocantins_n = 0
    for i in tocantins_n: 
         ena_tocantins_n += i     
    # Soma total da ENA TOCANTINS
    ena_tocantin_total =  ena_tocantins_n +  ena_tocantins_se    
    # Araguari
    araguari = list()
    for i in range(1, 3):
                araguari.append(lista_acomph[24][0][i-1] * prod[23][i-1])              
    ena_araguari = 0
    for i in araguari: 
         ena_araguari += i
    #Jequtinhonha SE 
    jequitinhonhase = list()
    for i in range(1, 2):
                jequitinhonhase.append(lista_acomph[16][0][i-1] * prod[20][i-1])
              
    ena_jequitinhonhase = 0
    for i in jequitinhonhase: 
         ena_jequitinhonhase += i      
    # AMAZONAS SE 
    amazonasse = list()
    for i in range(1, 11):
                amazonasse.append(lista_acomph[20][0][i-1] * prod[16][i-1])              
    ena_amazonasse = 0
    for i in amazonasse: 
         ena_amazonasse += i 
   # AMAZONAS N 
    amazonasn = list()
    for i in range(1, 4):
                amazonasn.append(lista_acomph[21][0][i-1] * prod[26][i-1])              
    ena_amazonasn = 0
    for i in amazonasn: 
         ena_amazonasn += i      
    # Capivari 
    capivari = list()
    for i in range(1, 2):
                capivari.append(lista_acomph[25][0][i-1] * prod[24][i-1])              
    ena_capivari = 0
    for i in capivari: 
         ena_capivari += i 
    # Itajai Acu
    itajai = list()
    for i in range(1, 2):
                itajai.append(lista_acomph[26][0][i-1] * prod[25][i-1])              
    ena_itajai = 0
    for i in itajai: 
         ena_itajai += i
    # Xingu
    xingu = list()
    for i in range(1, 2):
                xingu.append(lista_acomph[27][0][i-1] * prod[27][i-1])              
    ena_xingu = 0
    for i in xingu: 
         ena_xingu += i    
    # Jequitinhonha NE
    jequitinhonhane = list()
    for i in range(1, 2):
                jequitinhonhane.append(lista_acomph[17][0][i-1] * prod[14][i-1])
              
    ena_jequitinhonhane = 0
    for i in jequitinhonhane: 
         ena_jequitinhonhane += i   
    # Parnaíba
    parnaiba = list()
    for i in range(1, 2):
                parnaiba.append(lista_acomph[28][0][i-1] * prod[28][i-1])
              
    ena_parnaiba = 0
    for i in parnaiba: 
         ena_parnaiba += i      
    # Paraguacu
    paraguacu = list()
    for i in range(1, 2):
                paraguacu.append(lista_acomph[29][0][i-1] * prod[29][i-1])
              
    ena_paraguacu = 0
    for i in paraguacu: 
         ena_paraguacu += i
    sudeste = [ena_grande, ena_paranaiba, ena_tiete, ena_paranapanema_se,  ena_parana, ena_paraguai, ena_paraiba_do_sul, ena_doce, ena_mucuri, ena_itabapoana, ena_sao_francisco_se, ena_tocantins_se, ena_jequitinhonhase, ena_amazonasse]
    sul = [ena_iguacu, ena_uruguai, ena_paranapanema_s, ena_jacui, ena_capivari, ena_itajai]
    norte = [ena_tocantins_n, ena_araguari, ena_amazonasn, ena_xingu]
    nordeste = [ena_sao_francisco_ne, ena_jequitinhonhane, ena_parnaiba, ena_paraguacu]
    # return sudeste, sul, norte, nordeste
    return sul

def calculo_submercado():
    ''' Calculo por submercado'''
    submercado = calculo()
    nordeste = submercado[3]
    norte = submercado[2]
    sul = submercado[1]
    se = submercado[0]
    ena_se = 0
    for i in se:
        ena_se += i
    ena_ne = 0
    for i in nordeste:
        ena_ne += i    
    ena_s = 0
    for i in sul:
        ena_s += i 
    ena_n = 0
    for i in norte:
        ena_n += i   
    ena_n    
        
    return ena_se, ena_s, ena_ne, ena_n   
 
# print(calculo_submercado())
print(calculo())

def SQL_ena_bacia(data, sudeste, sul, norte, nordeste):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into ENA.ENA_BACIA(
                                                  Data,
                                                  GRANDE, 
                                                  PARANAIBA, 
                                                  TIETE, 
                                                  PARANAPANEMA_SE, 
                                                  PARANA, 
                                                  PARAGUAI, 
                                                  PARAIBA_DO_SUL,
                                                  DOCE, 
                                                  MUCURI, 
                                                  ITABAPOANA, 
                                                  SAO_FRANCISCO_SE, 
                                                  TOCANTINS_SE, 
                                                  JEQUITINHONHA_SE, 
                                                  AMAZONAS_SE, 
                                                  ITAJAI, 
                                                  CAPIVARI, 
                                                  JACUI,
                                                  PARANAPANEMA_S, 
                                                  URUGUAI, 
                                                  IGUACU, 
                                                  XINGU, 
                                                  AMAZONAS_N, 
                                                  ARAGUARI, 
                                                  TOCANTINS_N, 
                                                  SAO_FRANCISCO_NE, 
                                                  PARNAIBA, 
                                                  PARAGUACU
                                                 ) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",(data, sudeste[0],sudeste[1],sudeste[2] , sudeste[3], sudeste[4] ,sudeste[5], sudeste[6], sudeste[7], sudeste[8], sudeste[9], sudeste[10], sudeste[11], sudeste[12], sudeste[13], sul[0], sul[1], sul[2], sul[3], sul[4], sul[5], norte[0],norte[1], norte[2], norte[3], nordeste[0], nordeste[1], nordeste[2]) )
    conn.commit()
    conn.close()
    
def ENA_BACIA():
    data = caminho_acomph()[1]
    sudeste = calculo()[0]
    sul = calculo()[1]
    norte = calculo()[2]
    nordeste = calculo()[3]
    SQL_ena_bacia(data = data, sudeste = sudeste , sul = sul, norte = norte, nordeste = nordeste)

def SQL_base_ENA(data, indicador, seco, s, ne, n, sin):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into ENA.BASE_ENA(
                                                 data,
                                                 INDICADOR,
                                                 SECO,
                                                 S,
                                                 NE,
                                                 N, 
                                                 SIN) values(?, ?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n, sin) )
    conn.commit()
    conn.close()
def inserir_ena():      
    data = caminho_acomph()[1]
    print(caminho_acomph()[1])
    indicador = 'MWmed'
    seco = calculo_submercado()[0]
    sul = calculo_submercado()[1]
    ne = calculo_submercado()[2]
    n = calculo_submercado()[3]
    sin = calculo_submercado()[0] + calculo_submercado()[1] + calculo_submercado()[2] + calculo_submercado()[3]
    SQL_base_ENA(data = data, indicador = indicador, seco = seco, s = sul, ne = ne, n=n, sin = sin)

# inserir_ena()
# ENA_BACIA()

# def atualizar_banco():
#     data = data_banco ()
#     data_atual =  hora ()[0]
#     data_acomph = caminho_acomph()[2]
#     if data_acomph > data:
#             #inserir_ena()
#             #ENA_BACIA()
#             print('Atualizado na hora', data_atual)
#     else:
#             print('Não Atualizado', data_atual)
       
#print(data_banco ())
#print(caminho_acomph()[2])
#def teste_1():
#    print('foi')

#teste_1()   
# atualizar_banco()
# schedule.every(1).minutes.do(atualizar_banco)
# while True:
#     schedule.run_pending()
#     time.sleep(60)
