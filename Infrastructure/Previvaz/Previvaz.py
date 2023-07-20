import pandas as pd
from datetime import date, timedelta as delta, datetime as dt
import numpy as np
from .Infrastructure.Postgre.Postgre import Postgre
from .Utils.AuxFunctions import next_friday, operative_week_month, operative_week, day_by_operative_week
from .Utils.common import run_async
from Config import incremental_stations
from .Modules.Regressed.Regressed import Regressed#
from .Modules.AuxiliarFunctions.AuxiliarFunctions import calc_special_stations, calc_inc_nat_stations, generate_prevs, get_historical_inflow_weekly, get_historical_inflow_weekly_nat, get_ve_inflow_stations
from .Modules.InputModify.InputModify import copy_folder, week_prediction_change
import zipfile as zp
import os
import logging as log
import shutil
import subprocess
import time
import re
from random import choice
from string import digits, ascii_letters as letters
from google.cloud import storage
from pathlib import Path

log.basicConfig(level=log.INFO)


class Previvaz(object):

    __db_postos: Postgre = Postgre(database='Postos')
    __db_stons: Postgre = Postgre(database='SeriesTemporaisOns')

    def __init__(self, path: str, ref_date: date, db_postos: Postgre = __db_postos, db_stons: Postgre = __db_stons):

        self.db_postos = db_postos
        self.db_stons = db_stons
        self.path = path
        self.ref_date = ref_date

    @staticmethod
    def read_prevs(files_path: str, concatenate_type: str) -> pd.DataFrame:
        """
        Read txt prevs file structuring it on Pandas Dataframe
        :param files_path: Path of prevs file
        :param concatenate_type: type of concatenate prevs
        :return: Pandas Dataframe
        """

        if os.path.isfile(files_path):
            pattern = r'\d{6}\.'
            ref_date = dt.strptime(re.findall(pattern, files_path)[0][:-1], '%Y%m').date()
            aux = pd.read_csv(filepath_or_buffer=files_path,
                              header=None, sep=r'\s+',
                              usecols=[1, 2, 3, 4, 5, 6, 7],
                              index_col=0)
            df = aux.transpose()
            init_date = next_friday(ref_date)
            index = [int(f'{(init_date + delta(days=7 * i)).year}{operative_week(init_date + delta(days=7 * i)):02}')
                     for i in range(df.shape[0])]
            df.index = index

            return df

        else:

            files = os.listdir(files_path)

            prevs = dict()
            for file in files:

                pattern = r'\d{6}\.'
                ref_date = dt.strptime(re.findall(pattern, os.path.join(files_path, file))[0][:-1], '%Y%m').date()
                aux = pd.read_csv(filepath_or_buffer=os.path.join(files_path, file),
                                  header=None, sep=r'\s+',
                                  usecols=[1, 2, 3, 4, 5, 6, 7],
                                  index_col=0)
                df = aux.sort_index().transpose()
                init_date = next_friday(ref_date)
                index = [int(f'{(init_date + delta(days=7 * i)).year}{operative_week(init_date + delta(days=7 * i)):02}') for i in range(df.shape[0])]
                df.index = index
                prevs[ref_date] = df

            prevs_asc = {k: prevs[k] for k in sorted(prevs)}
            prevs_desc = {k: prevs[k] for k in sorted(prevs, reverse=True)}
            if concatenate_type == 'overlap_last':
                for idx, k in enumerate(prevs_desc.keys()):
                    if idx == 0:
                        result = prevs_desc[k]
                    else:
                        result = result.combine_first(prevs_desc[k])
            elif concatenate_type == 'overlap_first':
                for idx, k in enumerate(prevs_asc.keys()):
                    if idx == 0:
                        result = prevs_asc[k]
                    else:
                        result = result.combine_first(prevs_asc[k])

            else:

                msg = 'Please enter with correct concatenate_type: overlap_last (default) or overlap_first)!'
                return msg

            return result

    @staticmethod
    def offset_traveltime_verify_week(value: float) -> list:
        week = int(value / 168) + 1
        hours = 168 * week - value

        aux = [{'hours': hours, 'offset': -week + 1}, {'hours': 168 - hours, 'offset': -week}]

        return aux

    @staticmethod
    def incremental_treat(data: pd.DataFrame) -> pd.DataFrame:

        weeks = data.index

        columns = list()
        all_values = list()
        for station, config in incremental_stations.items():
            try:
                aux = list()
                for idx, dep_station in enumerate(config['dep_stations']):

                    # offset_list = self.offset_traveltime_verify_week(value=config['traveltime_reserv'][idx])

                    # aux2 = np.zeros(len(weeks))
                    # aux2[0] = data.loc[weeks[0], dep_station].astype(float)
                    # for idx_week in range(1, len(weeks)):
                    #     aux2[idx_week] = (1/168) * (offset_list[0]['hours'] * data.loc[weeks[idx_week + offset_list[0]['offset']], dep_station] +
                    #                                 offset_list[1]['hours'] * data.loc[weeks[idx_week + offset_list[1]['offset']], dep_station])
                    aux2 = data.loc[:, dep_station]
                    aux.append(pd.Series(aux2))

                values = config['formula_nat_inc'](aux).values
                values = np.maximum(values, 1)
                all_values.append(values)
                columns.append(station)

            except:
                pass

        all_values = np.asarray(all_values).transpose()
        df = pd.DataFrame(data=all_values, index=weeks, columns=columns)

        return df

    def input_previvaz_download(self):

        if os.path.exists(os.path.join(self.path, 'Arq_Entrada')):
            shutil.rmtree(os.path.join(self.path, 'Arq_Entrada'))

        ref_date = next_friday(self.ref_date)
        year = ref_date.year
        month = ref_date.month
        rvx = operative_week_month(ref_date) - 1
        if rvx == 0:
            file_name = f'Arq_Entrada_e_Saida_PREVIVAZ_{year}{month:02}_PMO.zip'
        else:
            file_name = f'Arq_Entrada_e_Saida_PREVIVAZ_{year}{month:02}_REV{rvx}.zip'

        storage_client = storage.Client()
        bucket_name = os.environ['GCS_BUCKET']
        gcs_path = os.environ['GCS_PATH_PREVIVAZ']
        gcs_path = f'{gcs_path}/'
        blobs = storage_client.list_blobs(bucket_name, prefix=gcs_path)
        for blob in blobs:
            if blob.name.split('/')[-1] == file_name:
                zip_file_path = Path(self.path, file_name)
                blob.download_to_filename(zip_file_path)
                break

        zip_file = zp.ZipFile(zip_file_path)
        file = [x for x in zip_file.namelist() if 'Arq_Entrada.zip' in x]
        path = Path(self.path, 'Auxiliar')
        zip_file.extract(file[0], path)
        zip_file.close()

        zip_file = zp.ZipFile(Path(path, file[0]))
        file = [x for x in zip_file.namelist()]
        for j in file:
            zip_file.extract(j, self.path)
        zip_file.close()

        shutil.rmtree(Path(self.path, 'Auxiliar'))
        os.remove(Path(self.path, [x for x in os.listdir(self.path) if '.zip' in x][0]))

    def create_postos_file(self, arq_postos: str) -> None:

        if os.path.exists(arq_postos):
            os.remove(arq_postos)

        postos = sorted(set([int(j.split('_')[0]) for j in [i.split('.')[0] for i in os.listdir(os.path.join(self.path, 'Arq_Entrada'))]]))

        file = open(arq_postos, 'w')
        for i in postos:
            file.write(f' {i} \n')
        file.close()

    @staticmethod
    def create_inflow_file(data_base: pd.DataFrame, arq_entrada: str) -> None:

        if os.path.exists(arq_entrada):
            os.remove(arq_entrada)

        file = open(arq_entrada, 'w')
        for i in data_base.index:
            for j in data_base.columns:
                if not np.isnan(data_base.loc[i, j]):
                    file.write(f'{j} {str(i)[:4]} {int(str(i)[4:])} {int(round(data_base.loc[i, j], 0))}\n')
        file.close()

    @staticmethod
    def get_output_previvaz(file: str, num_threads) -> pd.DataFrame:

        aux = dict()
        for thread in range(num_threads):

            df_saida = pd.read_csv(f'{file}{thread+1}',
                                   names=['cenario', 'posto', 'ano', 'mes', 'semana', 'prev1', 'prev2', 'prev3', 'prev4', 'prev5', 'prev6'],
                                   delim_whitespace=True)

            for i in df_saida.index.values:

                if df_saida.loc[i, 'cenario'] == 'VE':

                    year = int(df_saida.loc[i, "ano"])
                    week = int(f'{df_saida.loc[i, "semana"]:02}')

                    ref_date = next_friday(date(year + 1, 1, 1)) - delta(days=7)
                    last_oper_week = operative_week(ref_date)

                    aux_2 = dict()
                    for j in range(6):
                        if j > 0:
                            if week == last_oper_week:
                                year += 1
                                week = 1
                            else:
                                week += 1

                        aux_2.update({int(f'{year}{week:02}'): df_saida.loc[i, f"prev{j + 1}"]})

                    aux.update({df_saida.loc[i, 'posto']: aux_2})

        df = pd.DataFrame(aux).sort_index()

        # Remove vazoes saida temporary files
        for thread in range(num_threads):
            os.remove(f'{file}{thread+1}')

        return df

    def get_ref_date_arq_entrada(self, id_posto: int) -> date:

        file_path = os.path.join(self.path, 'Arq_Entrada', f'{id_posto}.inp')
        file = open(file_path, 'r+').readlines()
        week = int(file[8])
        year = int(file[9])

        ref_date = day_by_operative_week(week=week, year=year)['data_fim']

        return ref_date

    def run_previvaz(self, data_base: pd.DataFrame, ahead_weeks: int = 1, read: int = 0, num_threads: int = 3,
                     modify_arq_entrada: dict = None) -> str:

        id_string = ''.join(choice(letters + digits) for _ in range(5))

        arq_postos = os.environ['TEMP'] + os.sep + f"vazoes_postos_smap_{id_string}_"
        arq_entrada = os.environ['TEMP'] + os.sep + f"vazoes_entrada_smap_{id_string}_"
        arq_saida = os.environ['TEMP'] + os.sep + f"vazoes_saida_smap_{id_string}_"

        if modify_arq_entrada:

            dir_entrada_path = os.path.join(self.path, 'Arq_Entrada_Modify')
            if os.path.exists(dir_entrada_path):
                shutil.rmtree(dir_entrada_path)
            copy_folder(input_path_folder=os.path.join(self.path, 'Arq_Entrada'), output_path_folder=dir_entrada_path)

            for station, weeks_add in modify_arq_entrada.items():
                if station in data_base.columns:
                    week_prediction_change(station=station, path=dir_entrada_path, weeks_add=weeks_add)

        else:

            dir_entrada_path = os.path.join(self.path, 'Arq_Entrada')

        ### TESTE
        dir_saida_path = os.path.join(self.path, f'Arq_Saida_{id_string}')
        os.mkdir(dir_saida_path)
        for posto in data_base.columns:
            os.mkdir(os.path.join(dir_saida_path, f'{posto:03}'))
        ###

        # dir_saida_path = os.path.join(self.path, 'Arq_Saida')

        # region Generate input files to previvaz process

        # POSTOS
        self.create_postos_file(arq_postos=arq_postos)

        # VAZOES ENTRADA
        self.create_inflow_file(data_base=data_base, arq_entrada=arq_entrada)

        # endregion

        # region Run Previvaz
        previvaz_p_list: list = []
        files = [i for i in os.listdir(f"{os.sep}".join(arq_saida.split(os.sep)[:-1])) if i in arq_saida.split(os.sep)[-1]]
        for file in files:
            os.remove(os.path.join(f"{os.sep}".join(arq_saida.split(os.sep)[:-1]), file))
        os.chdir(self.path)
        for i in range(num_threads):
            previvaz_p = PrevivazProcess(thread=i, command=os.path.join(self.path, 'RodaPREVIVAZ.exe') +
                                         " " + arq_postos + " " + arq_entrada + " " + arq_saida +
                                         " " + dir_entrada_path + " " + dir_saida_path +
                                         " " + str(read) + " " + str(ahead_weeks) +
                                         " " + str(i+1) + " " + str(num_threads))
            previvaz_p.run_command()
            previvaz_p_list.append(previvaz_p)
        while True:
            running_process = len([p.status for p in previvaz_p_list if p.status != 'FINISHED'])
            if running_process:
                log.info(msg=f'Waiting {running_process} of {len(previvaz_p_list)} process still running.')
                time.sleep(3)
                continue
            break

        # Remove postos and vazoes etrada temporary files
        os.remove(arq_entrada)
        os.remove(arq_postos)
        shutil.rmtree(dir_saida_path)

        return arq_saida

    def run(self, num_threads: int, output_path: str, ref_date_run: date, modify_arq_entrada: dict = None, data_base: pd.DataFrame = None,
            files_path: str = None, concatenate_type: str = 'overlap_last', one_prevs: bool = True) -> pd.DataFrame:

        # self.input_previvaz_download()

        if files_path:

            # Read prevs
            df_prevs = self.read_prevs(files_path=files_path, concatenate_type=concatenate_type)
            # Incremental Treat
            df_inc = self.incremental_treat(data=df_prevs)
            # Combine Natural and Incremental inflows stations
            data_base = df_inc.combine_first(df_prevs)

        # Historical Data
        postos = list(data_base.columns)
        hist_data = get_historical_inflow_weekly(data_base=self.db_stons, stations=postos, ref_date=self.ref_date)
        hist_data_nat = get_historical_inflow_weekly_nat(data_base=self.db_stons, stations=postos, ref_date=self.ref_date)
        # Data used on previvaz
        data_base_previvaz = hist_data.combine_first(data_base)

        # Select only previvaz stations to run
        postos = sorted(set(int(j.split('_')[0]) for j in [i.split('.')[0] for i in os.listdir(os.path.join(self.path, 'Arq_Entrada'))]))
        input_previvaz = data_base_previvaz.loc[:, postos]

        # A vazão incremental dos postos de Ibitinga (239), N. Avanhandava (242) e Lajeado (273)  usadas no Previvaz deve ser dado pela soma
        # da sua incremental com a incremental da sua montante
        # input_previvaz.loc[:, 239] = input_previvaz.loc[:, 239] / 0.658 (opção 1)
        # Inc.Previvaz(239) = Inc(239) + Inc(238) (opção 2)
        input_previvaz.loc[:, 239] = data_base_previvaz.loc[:, 239] + data_base_previvaz.loc[:, 238]
        # input_previvaz.loc[:, 242] = input_previvaz.loc[:, 242] / 0.283 (opção 1)
        # Inc.Previvaz(242) = Inc(242) + Inc(240) (opção 2)
        input_previvaz.loc[:, 242] = data_base_previvaz.loc[:, 242] + data_base_previvaz.loc[:, 240]
        input_previvaz.loc[:, 273] = data_base_previvaz.loc[:, 273] + data_base_previvaz.loc[:, 257]
        input_previvaz.loc[:, 253] = data_base_previvaz.loc[:, 253] + data_base_previvaz.loc[:, 191]

        # Configuração e execução do Previvaz
        ref_date_arq_entrada = self.get_ref_date_arq_entrada(id_posto=120)  # ATENÇÃO A ESTE POSTO
        ahead_weeks = int((self.ref_date - ref_date_arq_entrada).days/7)
        # TODO: Remover depois
        input_previvaz[input_previvaz < 1] = 1
        ###
        file = self.run_previvaz(data_base=input_previvaz, ahead_weeks=ahead_weeks,
                                 read=0, num_threads=num_threads, modify_arq_entrada=modify_arq_entrada)

        # Vazões previvaz pelo Previvaz
        previvaz_output = self.get_output_previvaz(file=file, num_threads=num_threads)

        # Cálculo a vazão incremental dos postos rodados em conjunto  pelo Previvaz (conjuntos: 238/239, 240/242, 191/253 e 257/273)
        previvaz_output = calc_special_stations(data_base=previvaz_output, station_type='Dist. Incremental')

        # OUTPUT PREVIVAZ + HISTORICAL DATA + CURRENT WEEK
        data_base_output = previvaz_output.combine_first(data_base_previvaz)

        # REGRESSED STATIONS INFLOW OUTPUT
        regressed_inflow_weekly = Regressed().main(discretization='S', data_base=data_base_output)
        # data_base_output = data_base_output.combine_first(regressed_inflow_weekly)
        data_base_output = regressed_inflow_weekly.combine_first(data_base_output)  # INVERTIDO PARA ABRANGER O CASO DE RODAR PREVIVAZ A PARTIR DE PREVS

        # CALCULATION SPECIAL INFLOW STATIONS
        data_base_output = calc_special_stations(data_base=data_base_output, station_type='Aggregate-Tiete', ref_date=self.ref_date)
        data_base_output = calc_inc_nat_stations(data_base=data_base_output)
        data_base_output = calc_special_stations(data_base=data_base_output, station_type='São Francisco', ref_date=self.ref_date)
        data_base_output = calc_special_stations(data_base=data_base_output, station_type='Aggregate-SF')
        data_base_output = calc_special_stations(data_base=data_base_output, station_type='Zeros')

        data_base_output = hist_data_nat.combine_first(data_base_output)

        df_previvaz = get_ve_inflow_stations(data=data_base_output.loc[previvaz_output.index[:6], :])

        # df_previvaz = pd.DataFrame(data=[])

        # DATAFRAME WITH RANGE TIME CORRECT
        # ref_date = next_friday(self.ref_date) + delta(days=7*ahead_weeks)
        init_date = next_friday(date(self.ref_date.year, self.ref_date.month, 1))
        year_weeks = [int(f'{(init_date + delta(days=7 * i)).year}{operative_week(init_date + delta(days=7 * i)):02}')
                      for i in range(6)]

        data_base_output_prevs = data_base_output.loc[year_weeks, :]

        result = get_ve_inflow_stations(data=data_base_output_prevs)

        # GENERATE PREVS
        generate_prevs(data=result, output_path=output_path, ref_date=self.ref_date)

        if not one_prevs:

            new_ref_date = next_friday(ref_date_run)
            new_init_date = next_friday(date(new_ref_date.year, new_ref_date.month, 1))

            if new_init_date != init_date:

                new_year_weeks = [int(f'{(new_init_date + delta(days=7 * i)).year}{operative_week(new_init_date + delta(days=7 * i)):02}') for i in range(6)]
                new_data_base_output_prevs = data_base_output.loc[new_year_weeks, :]
                new_result = get_ve_inflow_stations(data=new_data_base_output_prevs)

                # GENERATE PREVS
                generate_prevs(data=new_result, output_path=output_path, ref_date=new_ref_date)


        return df_previvaz


class PrevivazProcess(object):

    __status: str = 'IDDLE'
    __command: str
    __stdout: str = ''
    __folder: str = ''

    def __init__(self, thread: int, command: str):
        self.__thread = thread
        self.__command = command
        self.__folder = f"{os.sep}".join(command.split(' ')[0].split(os.sep)[:-1])

    @run_async
    def run_command(self):
        self.__status = 'RUNNING'
        self.__process = subprocess.Popen(self.__command, stdout=subprocess.PIPE, cwd=self.__folder)
        log.info(msg=f'Starting Previvaz Process. Running Thread: {self.__thread} | PID: {self.__process.pid}')
        while True:
            output = self.__process.stdout.readline()
            if output == b'EXECUCAO COMPLETA COM SUCESSO\r\n':
                break
            self.__stdout += output.decode()
        self.__status = 'FINISHED'
        self.__process.kill()
        log.info(msg=f'Previvaz Process of thread {self.__thread} was finished successfully.')

    @property
    def status(self):
        return self.__status
