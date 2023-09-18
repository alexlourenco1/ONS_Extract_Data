#Atualizacao do Banco
import datetime
import BASE_EARM 
import BASE_EARM_MWM
#import bdo_despacho
from rdh import atualizar_rdh
import time
import schedule
from BASE_EARM_EARM_MWm import atualizar
from CALCULO_ENA import atualizar_banco
from ipdo import atualiza_ipdo

def agendador():
    now = datetime.datetime.now()
    hora_inicio = now.replace(hour=8, minute=40)
    hora_fim = now.replace(hour=20, minute=30)
    
    while (now > hora_inicio and now < hora_fim):
            lista = [atualizar(), atualizar_banco(), atualizar_rdh(), atualiza_ipdo()] 
            for i in lista:
                time.sleep(10)
                i                
            #atualizar()
            #atualizar_banco()
            #atualizar_rdh()
            #funcoes.download_bdo_automatico()
            #bdo_despacho.atualiza_bdo_despacho() 
            #print(now)
           
        
agendador ()           
#atualizar_banco() 
schedule.every(3).minutes.do(agendador)
while True:
    schedule.run_pending()
    time.sleep(60)    

