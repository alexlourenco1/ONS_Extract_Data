# -*- coding: utf-8 -*-
"""
Created on Wed May 11 15:54:54 2022

@author: alex.lourenco
"""
import pyodbc
import datetime
import pandas as pd
from datetime import timedelta
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port

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
    
    cnxn = pyodbc.connect(DRIVER='{SQL Server}',server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = cnxn.cursor()
    
    # Dataframe da tabela armazenamento max do Banco de dados
    query_arm_max = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N], [SIN] FROM ENA.BASE_EARM_Arm_max;"
    df_arm_max = pd.read_sql(query_arm_max, cnxn)
    
    # Dataframe da tabela armazenamento MWmed do Banco de dados
    
    query_arm_mwm = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N], [SIN] FROM ENA.BASE_EARM_MWm;"
    df_arm_mwm = pd.read_sql(query_arm_mwm, cnxn)
    
    # Dataframe da tabela armazenamento em percentual do Banco de dados
    
    query_earm = "SELECT [data], [INDICADOR], [SECO], [S], [NE], [N] FROM ENA.BASE_EARM;"
    df_earm = pd.read_sql(query_earm, cnxn)
    
    lista_arm_mwm = list()
    lista_arm_max = list()
    
    ## Separando  por submercado 
    
    lista_arm_mwm_ne = sorted(list(df_arm_mwm['NE']))
    #lista_arm_max.append(df_arm_max.loc[ '2022-05-04'].to_list())
    #lista_earm.append(df_earm[df_earm['data'] == '2022-05-04'].to_list())
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
    
    
    # Variaveis EARM_mwm
    #lista_arm_mw = list()
    #lista_arm_mw_data = sorted(list(df_arm_mwm['data']), reverse = True)
        
    #lista_arm_mw = list()
    #lista_arm_mw = list((df_arm_mwm.loc[df_arm_mwm['data']== lista_earm_data[0]]).values) 
    #print(lista_earm_data[0])
    #se_arm_mw =   (lista_arm_mw[0][2])
    #sul_arm_mw = (lista_arm_mw[0][3])
    #ne_arm_mw = (lista_arm_mw[0][4])
    #n_arm_mw = (lista_arm_mw[0][5]) 
    
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

def atualizar_base_earm_mwm(data):
    submercados = list()
    submercados = calculo_earm_MWm ()
    SE = submercados[0]
    SUL = submercados[1]
    NORTE = submercados[2]
    NORDESTE = submercados[3]
    SIN = submercados[4]
    date = data
    indicador = 'Mwmed'
    inserir_base_earm(data = date, indicador = indicador, seco = SE, s = SUL, ne = NORDESTE, n = NORTE, sin = SIN)

# atualizar_base_earm_mwm()



