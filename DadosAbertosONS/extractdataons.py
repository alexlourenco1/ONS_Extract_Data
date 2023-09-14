import pandas as pd



year=2023

ENA_df = pd.read_csv(
        "https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/ena_subsistema_di/ENA_DIARIO_SUBSISTEMA_" + str(
            year) + ".csv", sep=";")
#ENA_df_sub = ENA_df.drop(columns=['nom_subsistema']), 'id_subsistema', 'ena_data')
# print(ENA_df)

#Balanco Energetico

dfBE=pd.read_csv(
        "https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/balanco_programacao_diaria_tm/BALANCO_PROGRAMACAO_DIARIA_" + str(
            year) + ".csv", sep=";")

print(dfBE[(dfBE['id_subsistema']=='SE')&(dfBE['din_instante']>'30/08/2023  23:00:00')])