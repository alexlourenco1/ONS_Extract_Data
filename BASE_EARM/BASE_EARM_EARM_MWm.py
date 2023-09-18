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
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

''' Acesso ao banco de dados RPAComercializadora'''
def engine():
    connection_string = "DRIVER={SQL Server};SERVER=CL01VTF02ENVSQL;DATABASE=RPACOMERCIALIZADORA;UID=UGPL01T02F58;PWD=P.GaX1Y2zP"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    return engine
'''Selecionar a úl'''
def data_banco ():
   eng = engine()
   query = "SELECT TOP 1 * FROM  ENA.BASE_EARM ORDER BY data desc"
   df = pd.read_sql(query, eng)
   data = list(df['data'])
   return data[0]
#print(data_banco ())
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
    ultima_data_rdh = datetime.datetime.strptime(ultima_data_rdh, '%d/%m/%Y %H:%M:%S') - timedelta(days= 1)
    #ultima = atualizar_dados(data_rdh[0])
    ultima_data_rdh = ultima_data_rdh.strftime("%Y-%m-%d")
    filename =  r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\06.RDH\2022\2022-08\RDH_12AGO2022.xlsx'
    data = '2022-08-12'    
    return (Path.joinpath(caminho_rdh,ultimo_arquivo_rdh[1]), ultima_data_rdh)
    #return (filename, data)
#print(caminho_rdh()[1])

def lista():
    rdh = caminho_rdh()
    df = pd.read_excel(io = rdh[0], sheet_name = 'Hidroenergética-Subsistemas', header = None,  usecols= 'L', skiprows=(6), nrows = 31)
    lista = list() 
    lista_indice = [0, 8, 16, 24]
    for i in lista_indice:
        lista.append(df.iloc[i].astype(float, errors = 'raise').to_list())   
    return lista

def hora():
  now = datetime.datetime.now() 
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%d/%m/%Y')      
  return agora

def data():
  now = datetime.datetime.now() - timedelta(days= 1)
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%m/%d/%Y')       
  return agora

def inserir_base_earm(data, indicador, seco, s, ne, n, sin):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into ENA.BASE_EARM_MWm(
                                                 data,
                                                 INDICADOR,
                                                 SECO,
                                                 S,
                                                 NE,
                                                 N, 
                                                 SIN) values(?, ?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n, sin) )
    conn.commit()
    conn.close() 
 
def calculo_earm_MWm ():
    eng = engine()
    # Dataframe da tabela armazenamento max do Banco de dados
    query_arm_max = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N], [SIN] FROM ENA.BASE_EARM_Arm_max;"
    df_arm_max = pd.read_sql(query_arm_max, eng)
    # Dataframe da tabela armazenamento MWmed do Banco de dados
    query_arm_mwm = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N], [SIN] FROM ENA.BASE_EARM_MWm;"
    df_arm_mwm = pd.read_sql(query_arm_mwm, eng)
    # Dataframe da tabela armazenamento em percentual do Banco de dados   
    query_earm = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N] FROM ENA.BASE_EARM;"
    df_earm = pd.read_sql(query_earm, eng)  
    lista_arm_mwm = list()
    lista_arm_max = list()  
    ## Separando  por submercado  
    lista_arm_mwm_ne = sorted(list(df_arm_mwm['NE']))
    lista_arm_max_ne = sorted(list(df_arm_max[ 'NE']))   
    ## Variaveis Earm
    lista_earm_data = list()
    lista_earm_data = sorted(list(df_earm['data']), reverse = True)
    lista_earm = list()
    lista_earm = list((df_earm.loc[df_earm['data']== lista_earm_data[0]]).values) 
    se_earm =   (lista_earm[0][2])/100
    sul_earm = (lista_earm[0][3])/100
    ne_earm = (lista_earm[0][4])/100
    n_earm = (lista_earm[0][5])/100      
    # Variaveis Arm_Max
    lista_arm_max_data = list()
    lista_arm_max_data = sorted(list(df_arm_max['data']), reverse = True) 
    lista_arm_max = list()
    lista_arm_max = list((df_arm_max.loc[df_arm_max['data']== lista_earm_data[0]]).values) 
    se_arm_max =   (lista_arm_max[0][2])
    sul_arm_max = (lista_arm_max[0][3])
    ne_arm_max = (lista_arm_max[0][4])
    n_arm_max = (lista_arm_max[0][5])    
    # Multiplicando 
    SE = se_arm_max * se_earm
    SUL = sul_arm_max * sul_earm
    NORTE = n_arm_max * n_earm
    NORDESTE = ne_arm_max * ne_earm
    SIN = SE + SUL + NORTE + NORDESTE
    SE = round(SE, 2)
    SUL = round(SUL, 2)
    NORTE = round(NORTE, 2)
    NORDESTE = round(NORDESTE, 2)
    SIN = round(SIN, 2)  
    return SE, SUL, NORTE, NORDESTE, SIN 

def atualizar_base_earm_mwm():
    submercados = list()
    submercados = calculo_earm_MWm ()
    SE = submercados[0]
    SUL = submercados[1]
    NORTE = submercados[2]
    NORDESTE = submercados[3]
    SIN = submercados[4]
    date = data()
    indicador = 'Mwmed'
    inserir_base_earm(data = date, indicador = indicador, seco = SE, s = SUL, ne = NORDESTE, n = NORTE, sin = SIN)
def atualizar():
    data_rdh = caminho_rdh()
    atualizacao = hora()    
    
    if data_rdh[1] > data_banco (): 
        print('foi')
        data_ = data_rdh[1]
        indicador = '%ArmMax'
        lista_ = lista()
        seco = lista_[0]
        sul = lista_[1]
        ne = lista_[2]
        n = lista_[3]
        inserir_bdo_earm(data = data_, indicador = indicador, seco = seco[0], s = sul[0], ne = ne[0], n = n[0])
        atualizar_base_earm_mwm()
        print('Houve atualização em', data()) 
    else: 
       print('Não houve atualização até', data())

#atualizar()      
#schedule.every(2).minutes.do(atualizar)
#while True:
#    schedule.run_pending()
#    time.sleep(1)
