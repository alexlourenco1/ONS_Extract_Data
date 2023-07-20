from datetime import date, timedelta as delta
from Previvaz import Previvaz
from Utils.AuxFunctions import next_friday

file_path = r'D:\Documents\Rotinas_Alexandre\Decks_ChuvaVazao\Arquivos\20200901\prevs_ons\prevs_202009.txt'
ref_date = next_friday(date.today()) + delta(days=7)

prevs_path, df_previvaz = Previvaz(path=r'D:\Documents\Rotinas_Alexandre\Previvaz', ref_date=ref_date).run(files_path=file_path, num_threads=5,
                                                                                                           output_path=r'D:\Documents\Rotinas_Alexandre\Decks_ChuvaVazao\Arquivos\20200901\prevs_ons')

print('teste')
