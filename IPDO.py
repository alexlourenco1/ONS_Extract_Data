'''
Author:Alex
'''
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
  agora_ = now_.strftime('%d/%m/%Y %H:%M')
  return agora, agora_
def data ():
  now = datetime.datetime.now()
  #date = now - datetime.timedelta(days=1)
  agora = now.strftime('%m/%d/%Y %H:%M')
  return agora
def ultima_data():
   eng = engine()
   query = "SELECT TOP 1 * FROM CARGA.carga_diaria ORDER BY Data desc"
   df = pd.read_sql(query, eng)
   data = list(df['Data'])
   return data[0]
# print(ultima_data())
''''IPDO Analysis'''
def ipdo_path():
    ##  Essa funcao retorna o caminho do arquivo acomph  e data de atualizacao no formato de string
    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path(r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\05.IPDO")
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
    path = r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\07.ACOMPH\2023\2023-01\ACOMPH_15.01.2023.xls'
    ultima_data_acomp = '14-04-2023'
    data_hora = ultima_data_acomp
    #ultimo_arquivo_acomp = data_hora
    return (Path.joinpath(caminho_acomp,ultimo_arquivo_acomp[1]), ultima_data_acomp, data_hora)
    #return(path, ultima_data_acomp, data_hora, data_hora)
ipdo=ipdo_path()[0]
df = pd.read_excel(io=ipdo, header=None, sheet_name='IPDO')
print(df.loc[[15],[14]])