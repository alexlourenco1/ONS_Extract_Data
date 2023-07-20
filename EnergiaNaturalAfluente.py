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
from Infrastructure.Postgres.Postgres import Postgres
from datetime import date, timedelta as delta
from Config import incremental_stations, calculated_stations_acomph, stations_code
import numpy as np
from dateutil.relativedelta import relativedelta as delta, FR

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

'''' This code is dedicated to calculate the Affluent Natural Energy - 
Energia Natural Afluente é calculada a partir dos dados de vazão natural '''

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

def SQL_base_ENA(data, indicador, seco, s, ne, n, sin):
    conn = pyodbc.connect(DRIVER='{SQL Server}',server="CL01VTF02ENVSQL", user="UGPL01T02F58 ", password="P.GaX1Y2zP", database="RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into ENA.BASE_ENA(
                                                 data,
                                                 INDICADOR,
                                                 SECO,
                                                 S,
                                                 NE,
                                                 N, 
                                                 SIN) values(?, ?, ?, ?, ?, ?, ?)""",(data, indicador, seco, s, ne, n, sin))
    conn.commit()
    conn.close()
# def inserir_ena():
#     data = caminho_acomph()[1]
#     print(caminho_acomph()[1])
#     indicador = 'MWmed'
#     seco = calculo_submercado()[0]
#     sul = calculo_submercado()[1]
#     ne = calculo_submercado()[2]
#     n = calculo_submercado()[3]
#     sin = calculo_submercado()[0] + calculo_submercado()[1] + calculo_submercado()[2] + calculo_submercado()[3]
#     SQL_base_ENA(data = data, indicador = indicador, seco = seco, s = sul, ne = ne, n=n, sin = sin)

# inserir_ena()
# ENA_BACIA()
# print(calculo_submercado())
# print(calculo())
db_postos = Postgres(database='Postos')
db_ons = Postgres(database='SeriesTemporaisOns')

def next_friday(ref_date: date) -> date:
    output = ref_date + delta(weekday=FR)
    return output

def get_hydro_productivity() -> pd.DataFrame:
        """
        Read productivity by powerplant from database
        :return: Pandas Dataframe containing productivity data by powerplant id
        """
        data_base = Postgres(database='Postos')
        query = f'''SELECT
                        id_usina as id_usina,
                        initcap(nome_submercado) as nome_submercado,
                        initcap(nome_ree) as nome_ree,
                        coalesce(initcap(nome_bacia_acomph), initcap(nome_bacia_rdh)) as nome_bacia,
                        produtibilidade as produtibiliade
                    FROM ons.vw_usina_posto_bacia_ree
                    where produtibilidade is not null
                    ORDER BY id_usina asc; '''
        values = data_base.read(query=query)
        df = pd.DataFrame(data=values, columns=['Usina', 'Submercado', 'REE', 'Bacia', 'Produtibilidade'])
        return df

def get_historical_inflow_diary(ref_date) -> pd.DataFrame:
    init_date = date.today() - timedelta(days=6)
    query_statement = f"""select 
                                posto_id, 
                                coalesce(vazao_nat_acomph, vazao_nat_rdh) as vazao_nat
                            from series_diarias.vw_vazoes_rdh_acomph
                            where data between  %s and %s
                            order by posto_id asc, data asc;"""

    aux = db_ons.read(query=query_statement, params=(ref_date, ref_date), to_dict=True)
    df = pd.DataFrame(data=aux).set_index('posto_id')
    #stations = sorted(list(set([i['posto_id'] for i in aux])))
    # dates = sorted(list(set([i['data'] for i in aux])))
    # values = np.zeros((len(dates), len(stations)))
    # cont_date = 0
    # for i in dates:
    #     cont_station = 0
    #     for j in stations:
    #         vazao_nat = [x['vazao_nat'] for x in aux if x['posto_id'] == j and x['data'] == i][0]
    #         if j in list(incremental_stations.keys()):
    #             values[cont_date, cont_station] = vazao_nat
    #
    #         cont_station += 1
    #
    #     cont_date += 1
    #
    # df = pd.DataFrame(data=values, columns=stations, index=dates)
    #
    # df = df.sort_index()
    return df

def get_historical_inflow_diary_inc(ref_date) -> pd.DataFrame:
    init_date = date.today() - timedelta(days=6)
    query_statement = f"""select 
                                posto_id, 
                                coalesce(vazao_inc_acomph, vazao_inc_rdh) as vazao_inc
                            from series_diarias.vw_vazoes_rdh_acomph
                            where data between  %s and %s
                            order by posto_id asc, data asc;"""

    aux = db_ons.read(query=query_statement, params=(ref_date, ref_date), to_dict=True)
    df = pd.DataFrame(data=aux).set_index('posto_id')
    #stations = sorted(list(set([i['posto_id'] for i in aux])))
    # dates = sorted(list(set([i['data'] for i in aux])))
    # values = np.zeros((len(dates), len(stations)))
    # cont_date = 0
    # for i in dates:
    #     cont_station = 0
    #     for j in stations:
    #         vazao_nat = [x['vazao_nat'] for x in aux if x['posto_id'] == j and x['data'] == i][0]
    #         if j in list(incremental_stations.keys()):
    #             values[cont_date, cont_station] = vazao_nat
    #
    #         cont_station += 1
    #
    #     cont_date += 1
    #
    # df = pd.DataFrame(data=values, columns=stations, index=dates)
    #
    # df = df.sort_index()
    return df

def calculo_ena_acomph(inflow)-> pd.DataFrame:
 produtib_all = get_hydro_productivity()
 aux = list()
 index = list()
 for hydro, config in stations_code.items():
    if hydro in produtib_all['Usina'].values:
        produtib = produtib_all[produtib_all['Usina'] == hydro]['Produtibilidade'].values[0]
        # print(produtib)
        region = produtib_all[produtib_all['Usina'] == hydro]['Submercado'].values[0]
        ree = produtib_all[produtib_all['Usina'] == hydro]['REE'].values[0]
        basin = produtib_all[produtib_all['Usina'] == hydro]['Bacia'].values[0]
    else:
        produtib = 0
        region = None
        ree = None
        basin = None
    for station_nat in config['id_nat']:
        if station_nat in inflow.index:
            ena = produtib * inflow.loc[station_nat, :].values
            aux.append([region, ree, basin] + ena.tolist())
            index.append(station_nat)
#
#     for station_art in config['id_art']:
#         if station_art in inflow.index:
#             ena = produtib * inflow.loc[station_art, :].values
#             aux.append([region, ree, basin] + ena.tolist())
#             index.append(station_art)
# print(aux)
 df = pd.DataFrame(data=aux, columns=['region', 'ree', 'basin'] + list(inflow.columns), index=index)
 df = df.sort_index()
 df = df.round(decimals=2)
 df=df.rename(columns={'vazao_nat': 'ENA'})
 return df

def calculo_ena_acomph_inc(inflow)-> pd.DataFrame:
 produtib_all = get_hydro_productivity()
 aux = list()
 index = list()
 for hydro, config in stations_code.items():
    if hydro in produtib_all['Usina'].values:
        produtib = produtib_all[produtib_all['Usina'] == hydro]['Produtibilidade'].values[0]
        region = produtib_all[produtib_all['Usina'] == hydro]['Submercado'].values[0]
        ree = produtib_all[produtib_all['Usina'] == hydro]['REE'].values[0]
        basin = produtib_all[produtib_all['Usina'] == hydro]['Bacia'].values[0]
    else:
        produtib = 0
        region = None
        ree = None
        basin = None
    for station_nat in config['id_nat']:
        if station_nat in inflow.index:
            ena = produtib * inflow.loc[station_nat, :].values
            aux.append([region, ree, basin] + ena.tolist())
            index.append(station_nat)
 df = pd.DataFrame(data=aux, columns=['region', 'ree', 'basin'] + list(inflow.columns), index=index)
 df = df.sort_index()
 df = df.round(decimals=2)
 df=df.rename(columns={'vazao_inc': 'ENA'})
 return df

def inserir_ena(ref_date):
    data=str(ref_date)
    indicador = 'MWmed'
    '''incremental'''
    inflow_inc = get_historical_inflow_diary_inc(ref_date)
    df_inc = calculo_ena_acomph_inc(inflow_inc)
    '''natural'''
    inflow = get_historical_inflow_diary(ref_date)
    df = calculo_ena_acomph(inflow)
    df_ena_submercado_nat = df.groupby("region")["ENA"].sum().reset_index()
    df_ena_submercado_inc = df_inc[(df_inc['region'] == 'Nordeste') | (df_inc['region'] == 'Norte')].groupby("region")[
        "ENA"].sum().reset_index()
    df_concatenado = pd.concat([df_ena_submercado_nat, df_ena_submercado_inc])
    ena = df_concatenado.groupby('region')['ENA'].sum().reset_index()
    sin = ena[ena['region']=='Sudeste']['ENA'].values + ena[ena['region']=='Sul']['ENA'].values + ena[ena['region']=='Nordeste']['ENA'].values + ena[ena['region']=='Norte']['ENA'].values
    seco=ena[ena['region']=='Sudeste']['ENA'].values
    sul=ena[ena['region']=='Sul']['ENA'].values
    norte=ena[ena['region']=='Norte']['ENA'].values
    nordeste=ena[ena['region']=='Nordeste']['ENA'].values
    SQL_base_ENA(data=data, indicador=indicador, seco=float(seco[0]), s=float(sul[0]), ne=float(nordeste[0]), n=float(norte[0]), sin=float(sin[0]))

ref_date=date.today() - timedelta(days=1) #até o 6
print('A data do Acomph é',ref_date)
inserir_ena(ref_date)