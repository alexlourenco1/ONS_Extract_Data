# -*- coding: utf-8 -*-
"""
Created on Tue May 17 17:27:24 2022

@author: alex.lourenco
"""
#Importação das bibliotecas necessarias
import pendulum
import pandas as pd
import pymssql
import os
import datetime
import time
from pathlib import Path 
from datetime import timedelta

def str_to_float(value):
        """
        Auxiliary function to convert many formats of string (with comma, dot, etc) to float type
        :param value: String value
        :return: Float value
        """

        if isinstance(value, str):
            if value.replace('-', '').strip() == '' or value == 'ND':
                return
            elif ',' in value and '.' in value:
                return float(value.replace('.', '').replace(',', '.'))
            elif ',' in value:
                return float(value.replace(',', '.'))
            #elif '' in value:
            #    return float(value.replace(' ', '0'))
            elif '.' in value:
                return float(value)
            else:
                raise Exception('Invalid str value')
        else:
            return value

def inserir_rdh(postoid, data,vazaonat, nivelres, volumearm,  vazaotur, vazaover, vazaootr,  vazaodfl, vazaotra, vazaoafl, vazaoinc, vazaousocon, vazaoevp, vazaoart):
    conn = pymssql.connect(server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    
    cursor.execute(""" insert into RDH.rdh(     
                                                 posto_id,
                                                 data,
                                                 vazao_nat,
                                                 nivel_res,
                                                 volume_arm,
                                                 vazao_tur,
                                                 vazao_ver,
                                                 vazao_otr,
                                                 vazao_dfl,
                                                 vazao_tra,
                                                 vazao_afl,
                                                 vazao_inc,
                                                 vazao_uso_con,
                                                 vazao_evp,
                                                 vazao_art) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",(postoid, data,vazaonat, nivelres, volumearm,  vazaotur, vazaover, vazaootr,  vazaodfl, vazaotra, vazaoafl, vazaoinc, vazaousocon, vazaoevp, vazaoart) )
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
                
                # data inicial = 01/01/2021
                # data1 = 02/01/2021 -> 10.000
                # data2 = 15/02/2021 -> 150.000
    
    lista_datas_rdh.sort(reverse=True)
    ultimo_arquivo_rdh = lista_datas_rdh[0]
    data_rdh = lista_datas_rdh[0]
    ultima_data_rdh = atualizar_dados(data_rdh[0]) #.strftime('%d/%m/%Y')
    ultima_data_rdh = datetime.datetime.strptime(ultima_data_rdh, '%d/%m/%Y %H:%M:%S')
    #ultima = atualizar_dados(data_rdh[0])
    ultima_data_rdh = ultima_data_rdh.strftime("%d/%m/%Y %H:%M")
    return (Path.joinpath(caminho_rdh,ultimo_arquivo_rdh[1]),  ultima_data_rdh)

def hora():
  now = datetime.datetime.now() 
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%d/%m/%Y %H:%M')      
  return agora

def data():  
  now = datetime.datetime.now() - timedelta(days= 1) 
  now = datetime.datetime.date(now)    
  return now

def data_atual():
    now = datetime.datetime.now()- timedelta(minutes= 3)
    antes_1 = now.strftime('%d/%m/%Y %H:%M')  
    antes_2 = datetime.datetime.now()- timedelta(minutes = 1)
    antes_2 = antes_2.strftime('%d/%m/%Y')
                               #%H:%M')
    return antes_1, antes_2 
    
def lista():
    filename =  r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH\2022\2022-07\RDH_18JUL2022.xlsx'
    #caminho_rdh()
    df = pd.read_excel(io = filename, sheet_name= 'Hidráulico-Hidrológica', header = None, usecols = 'E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S,T, U, V, W, X, Y, Z, AA', skiprows=(7), nrows=(165))
    lista = list()
    df = df.applymap(lambda x:str_to_float(x))
    df = df.fillna(value = 0)
    lista = list(df[df[4] !=0].values)
    return lista

def atualizar_rdh():
    rdh = lista()
    data_rdh =  datetime.datetime.strptime('2022-07-18', '%Y-%m-%d')
    print(data_rdh)
    #caminho_rdh()
    data_hoje = data_atual()
    #if ( data_hoje[0] <= data_rdh[1]) or (data_hoje[1] <= data_rdh[1]):
    for i in range(1,165):
            inserir_rdh(postoid = rdh[i-1][0], data = data_rdh ,vazaonat = rdh[i-1][1], nivelres = rdh[i-1][2], volumearm = rdh[i-1][3],  vazaotur = rdh[i-1][4], vazaover = rdh[i-1][5], vazaootr = rdh[i-1][6],  vazaodfl = rdh[i-1][7], vazaotra = rdh[i-1][8], vazaoafl = rdh[i-1][9], vazaoinc = rdh[i-1][10], vazaousocon = rdh[i-1][11], vazaoevp = rdh[i-1][12], vazaoart = rdh[i-1][13])

atualizar_rdh()
