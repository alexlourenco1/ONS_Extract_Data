# -*- coding: utf-8 -*-
"""
Created on Tue May 17 11:05:21 2022

@author: alex.lourenco
"""
import pendulum
import pandas as pd
import os
import time
from pathlib import Path 
#from datetime import timedelta
from datetime import datetime
import pymssql
import win32com.client as win32



#### ATUALIZAR BANCO DE DADOS 

""" A partir daqui o codigo converte o arquivo excel 
        e atualiza o banco de dados"""

        
def atualizar_dados(data):

  m_ti = time.ctime(data) 
  t_obj = time.strptime(m_ti) 
  T_stamp = time.strftime("%d/%m/%Y %H:%M:%S", t_obj)
  #print(T_stamp) 
  return T_stamp

def inserir_DESPACHO(nomeusina, codigoons, potinstalada, ordemmerito, inflexnumeric, restricaoeletrica, substordemmerito, gerforamerito, energiareposicao, garantiaenergetica, export, reservapotencia, unitcommitment, verificadonumeric, data):
    conn = pymssql.connect(server = "CL01VTF02ENVSQL", user = "UGPL01T02F58 ", password = "P.GaX1Y2zP", database = "RPACOMERCIALIZADORA")
    cursor = conn.cursor()
    cursor.execute(""" insert into BDO.DESPACHOS(
                                                 nome_usina,
                                                 codigo_ons,
                                                 pot_instalada,
                                                 ordem_merito,
                                                 inflex,
                                                 restricao_eletrica,
                                                 subst_ordem_merito,
                                                 ger_fora_merito,
                                                 energia_reposicao,
                                                 garantia_energetica,
                                                 export,
                                                 reserva_potencia,
                                                 unit_commitment,
                                                 verificado,
                                                 data )  values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (nomeusina, codigoons, potinstalada, ordemmerito, inflexnumeric, restricaoeletrica, substordemmerito, gerforamerito, energiareposicao, garantiaenergetica, export, reservapotencia, unitcommitment, verificadonumeric, data) 
    )
    conn.commit()
    conn.close() 

def caminho_temporario(caminho):
    file = caminho
    file = str(file)
    print(file[66:-1])
    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\08.BDO")
    caminho_bdo = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), 'temp')
    caminho_temp = Path.joinpath(caminho_bdo, Path(str(file[66:-1])))
    #print(caminho_temp)
    return caminho_temp
    
def tranformar_arquivo_excel(caminho):
    #caminho = r'J:\SEDE\Comercializadora de Energia\6. MIDDLE\08.BDO\2022\2022-05\DIARIO_16-05-2022.xlsx'
    fname = caminho
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(fname)
    name = caminho_temporario(fname)
    #print(fname)
    wb.SaveAs(str(name), FileFormat = 56)    #FileFormat = 51 is for .xlsx extension
    wb.Close()                               #FileFormat = 56 is for .xls extension
    excel.Application.Quit()
    return name

def caminho_bdo():
    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\08.BDO")
    caminho_bdo = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), data_requisitada.format('YYYY') + '-' + data_requisitada.format('MM'))
    lista_arquivos_bdo = os.listdir(caminho_bdo)
    lista_datas_bdo = []
    for arquivo_bdo in lista_arquivos_bdo:
        # descobrir a data desse arquivo
        if ".xlsx" in arquivo_bdo:
                data_bdo = os.path.getmtime(f"{caminho_bdo}/{arquivo_bdo}")
                lista_datas_bdo.append((data_bdo, arquivo_bdo))

    lista_datas_bdo.sort(reverse=True)
    ultimo_arquivo_bdo = lista_datas_bdo[0]
    data_bdo = lista_datas_bdo[0]
    ultima_data_bdo = atualizar_dados(data_bdo[0]) #.strftime('%d/%m/%Y')
    ultima_data_bdo = datetime.strptime(ultima_data_bdo, '%d/%m/%Y %H:%M:%S')
    #ultima = atualizar_dados(data_rdh[0])
    ultima_data_bdo = ultima_data_bdo.strftime("%d/%m/%Y")
    #ultimo = ultimo_arquivo_bdo[1].replace('xlsx','xls')
    caminho = Path.joinpath(caminho_bdo,ultimo_arquivo_bdo[1])
    #print(caminho)
    #caminho_bdo = tranformar_arquivo_excel(caminho)
    return(caminho, ultimo_arquivo_bdo[1][7:-5])
#a = caminho_bdo()
#print(a[1])

def ultima_data_bdo():
    data_requisitada = pendulum.now('America/Sao_Paulo').subtract(days = 1)
    caminho_raiz = Path( r"J:\SEDE\Comercializadora de Energia\6. MIDDLE\08.BDO")
    caminho_bdo = Path.joinpath(Path(f'{caminho_raiz}', data_requisitada.format('YYYY')), 'temp')
    lista_arquivos_bdo = os.listdir(caminho_bdo)
    lista_datas_bdo = []
    for arquivo_bdo in lista_arquivos_bdo:
        # descobrir a data desse arquivo
        if ".xls" in arquivo_bdo:
                data_bdo = os.path.getmtime(f"{caminho_bdo}/{arquivo_bdo}")
                lista_datas_bdo.append((data_bdo, arquivo_bdo))

    lista_datas_bdo.sort(reverse=True)
    ultimo_arquivo_bdo = lista_datas_bdo[0]
    data_bdo = lista_datas_bdo[0]
    ultima_data_bdo = atualizar_dados(data_bdo[0]) #.strftime('%d/%m/%Y')
    ultima_data_bdo = datetime.strptime(ultima_data_bdo, '%d/%m/%Y %H:%M:%S')
    #ultima = atualizar_dados(data_rdh[0])
    ultima_data_bdo = ultima_data_bdo.strftime("%d/%m/%Y")
    #ultimo = ultimo_arquivo_bdo[1].replace('xlsx','xls')
    caminho = Path.joinpath(caminho_bdo,ultimo_arquivo_bdo[1])
    return(ultimo_arquivo_bdo[1][7:-4])
#print(ultima_data_bdo())

#def hora(arg):
#  data = arg  
#  now = pd.datetime.date(data) 
  #now = datetime.datetime.date(now)  
#  return now

def despacho_values(caminho):
    filename = caminho
    df = pd.read_excel(io = filename, sheet_name= '12-Motivo do Despacho Térmico', header = None,
                       usecols = 'A, B, C, D,E, F, G, H, I, J, K, L, M, N', skiprows=(6), nrows = 104)    
    lista = list()
    lista = (list(df.values))
    #print(lista[0][13])
    return lista        
                                                                                                                          
#lista = despacho_values()
#lista = [float(x) if type(x) is int else x for x in lista]
#print(lista[1][0])

def atualiza_bdo_despacho():
    data_xls = ultima_data_bdo()
    [caminho, data] = caminho_bdo()
    #atualizar = hora().strftime("%d/%m/%Y")
    #data_banco = datetime.strptime(data_xls, '%d-%m-%Y').date()
    
    ############## Condicao de atualizacao ###############################
    while data > data_xls:
            #caminho = caminho_bdo()
            data_banco = datetime.strptime(data, '%d-%m-%Y').date()
            caminho_novo = tranformar_arquivo_excel(caminho)
            #print(caminho)
            print(data_xls)
            lista = despacho_values(caminho_novo)
            lista = [float(x) if type(x) is int else x for x in lista]
            #for i in range(1, 103):
            #    inserir_DESPACHO(nomeusina = lista[i - 1][0], codigoons = lista[i- 1][1], potinstalada = lista[i - 1][2], ordemmerito = lista[i- 1][3], inflexnumeric = lista[i- 1][4], restricaoeletrica = lista[i- 1][5], substordemmerito = lista[i- 1][6], gerforamerito = lista[i- 1][7], energiareposicao = lista[i- 1][8], garantiaenergetica = lista[i- 1][9], export = lista[i- 1][10], reservapotencia = lista[i- 1][11], unitcommitment = lista[i- 1][12], verificadonumeric = lista[i- 1][13], data = data_banco)  
    else: 
        print("Não atualizou")
        #print(data_banco) 
    return data    

print(atualiza_bdo_despacho())                               


