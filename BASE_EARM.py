"""
Created on Tue May 10 13:55:53 2022

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
#import acomph
import numpy as np
import schedule
import BASE_EARM_MWM

def inserir_bdo_earm(data, indicador, seco, s, ne, n):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into ENA.BASE_EARM(
                                                 data,
                                                 INDICADOR,
                                                 SECO,
                                                 S,
                                                 NE,
                                                 N) values(?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n) )
    conn.commit()
    conn.close() 

def atualizar_dados(data):

  m_ti = time.ctime(data) 
  t_obj = time.strptime(m_ti) 
  T_stamp = time.strftime("%d/%m/%Y %H:%M:%S", t_obj)
  #print(T_stamp) 
  return T_stamp

def caminho_rdh():
        
    ##  Chamando os nomes das variaveis 

    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH")
    #caminho_rdh =Path(f'{caminho_raiz}', data_requisitada.format('YYYY'), data_requisitada.format('MM'))
    caminho_rdh = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), data_requisitada.format('YYYY') + '-' + data_requisitada.format('MM'))
    #caminho_rdh =Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH\2022\2022-04")   #J:\SEDE\Comercializadora de Energia\6. MIDDLE\12.ANALISES
    lista_arquivos_rdh = os.listdir(caminho_rdh)

    lista_datas_rdh = []
    for arquivo_rdh in lista_arquivos_rdh:
        # descobrir a data desse arquivo
        if ".xlsx" or ".xls" or ".xlsm" in arquivo_rdh:
                data_rdh = os.path.getmtime(f"{caminho_rdh}/{arquivo_rdh}")
                lista_datas_rdh.append((data_rdh, arquivo_rdh))
    lista_datas_rdh.sort(reverse=True)
    ultimo_arquivo_rdh = lista_datas_rdh[0]
    data_rdh = lista_datas_rdh[0]
    ultima_data_rdh = atualizar_dados(data_rdh[0]) #.strftime('%d/%m/%Y')
    ultima_data_rdh = datetime.datetime.strptime(ultima_data_rdh, '%d/%m/%Y %H:%M:%S')
    #ultima = atualizar_dados(data_rdh[0])
    ultima_data_rdh = ultima_data_rdh.strftime("%d/%m/%Y %H:%M")
    path = r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH\2022\2022-11\RDH-04.11.2022.xlsx'
    data = '2022-11-04'
    #return (Path.joinpath(caminho_rdh,ultimo_arquivo_rdh[1]), ultima_data_rdh)
    return (path, data)



def lista():
    rdh = caminho_rdh()
    df = pd.read_excel(io = rdh[0], sheet_name = 'Hidroenergética-Subsistemas', header = None,  usecols= 'L', skiprows=(6), nrows = 31)
    lista = list() 
    lista_indice = [0, 8, 16, 24]
    for i in lista_indice:
        lista.append(df.iloc[i].astype(float, errors = 'raise').to_list())
        #lista = (df.iloc[i].astype(float, errors = 'raise').to_list())
    
    #lista = df.iloc[8].astype(float, errors = 'raise').to_list()
    #lista = df.iloc[16].astype(float, errors = 'raise').to_list()
    #lista = df.iloc[24].astype(float, errors = 'raise').to_list()
    return lista

def hora():
  now = datetime.datetime.now() 
  date_antes = now - datetime.timedelta(minutes = 2)
  date_depois = now - datetime.timedelta(minutes= 2)
  agora_antes = date_antes.strftime('%d/%m/%Y %H:%M') 
  agora_depois = date_depois.strftime('%d/%m/%Y %H:%M')
  agora = now.strftime('%d/%m/%Y %H')      
  return (agora, agora_antes, agora_depois)

def data():
  now = datetime.datetime.now() - timedelta(days= 1)
  return now

def atualizar():
#    if data_rdh[1] >= atualizacao[1] or data_rdh[1] == atualizacao[2] :
        print('foi')
        data_ = caminho_rdh()[1]#data()
        indicador = '%ArmMax'
        lista_ = lista()
        seco = lista_[0]
        sul = lista_[1]
        ne = lista_[2]
        n = lista_[3]
        # inserir_bdo_earm(data = data_, indicador = indicador, seco = seco[0], s = sul[0], ne = ne[0], n = n[0])
        BASE_EARM_MWM.atualizar_base_earm_mwm(data=data_)
        print('Houve atualização da Base_EARM em', data_)
  #  else: 
  #     print('Não houve atualização do Banco de dados ENA até', atualizacao[1])


atualizar()    
#schedule.every(2).minutes.do(atualizar)
#while True:
#    schedule.run_pending()
#    time.sleep(1)


#inserir_bdo_earm(data = data, indicador = indicador, seco = seco[0], s = sul[0], ne = ne[0], n = n[0])

## A partir daqui o código atualiza a ENA com o arquivo acomph 
#def inserir_bdo_ena(data, indicador, seco, s, ne, n, sin):
#    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
#    cursor = conn.cursor()
#    cursor.execute(""" insert into dbo.BASE_ENA(
#                                                 data,
#                                                 INDICADOR,
#                                                 SECO,
#                                                 S,
#                                                 NE,
#                                                 N, SIN) values(?, ?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n, sin) )
#     conn.commit()
#     conn.close()
#prod = list()
#prod = ['1', 0.2076,'2', 0.2437,'6' , 0.8130]
#print(prod)
# def caminho_acomph():
#
#     ##  Chamando os nomes das variaveis
#
#     data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
#     caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\07.ACOMPH")
#     #caminho_rdh =Path(f'{caminho_raiz}', data_requisitada.format('YYYY'), data_requisitada.format('MM'))
#     caminho_acomp = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), data_requisitada.format('YYYY') + '-' + data_requisitada.format('MM'))
#     #caminho_rdh =Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH\2022\2022-04")   #J:\SEDE\Comercializadora de Energia\6. MIDDLE\12.ANALISES
#     lista_arquivos_acomp = os.listdir(caminho_acomp)
#
#     lista_datas_acomp = []
#     for arquivo_acomp in lista_arquivos_acomp:
#         # descobrir a data desse arquivo
#         if ".xlsx" or ".xls" or ".xlsm" in arquivo_acomp:
#                 data_acomp = os.path.getmtime(f"{caminho_acomp}/{arquivo_acomp}")
#                 lista_datas_acomp.append((data_acomp, arquivo_acomp))
#
#                 # data inicial = 01/01/2021
#                 # data1 = 02/01/2021 -> 10.000
#                 # data2 = 15/02/2021 -> 150.000
#
#     lista_datas_acomp.sort(reverse=True)
#     ultimo_arquivo_acomp = lista_datas_acomp[0]
#     data_acomp = lista_datas_acomp[0]
#     ultima_data_acomp = atualizar_dados(data_acomp[0]) #.strftime('%d/%m/%Y')
#     ultima_data_acomp = datetime.datetime.strptime(ultima_data_acomp, '%d/%m/%Y %H:%M:%S')
#     #ultima = atualizar_dados(data_rdh[0])
#     ultima_data_acomp = ultima_data_acomp.strftime("%d/%m/%Y")
#     return Path.joinpath(caminho_acomp,ultimo_arquivo_acomp[1])
#
#acomph_caminho = caminho_acomph()
#df = pd.read_excel(io = acomph, sheet_name = 'Hidroenergética-Subsistemas', header = None,  usecols= 'L', skiprows=(6), nrows = 31)
#print(df) 

#print(acomph.Acomph(file_path=acomph_caminho).write_to_database())
#values = list()
#values.append((acomph.Acomph(file_path=acomph_caminho).write_to_database()))
#print(values)
#df = pd.DataFrame(values)
#print(df)