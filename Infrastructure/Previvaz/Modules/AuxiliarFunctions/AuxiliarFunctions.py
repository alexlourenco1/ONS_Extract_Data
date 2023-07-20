import pandas as pd
import numpy as np
import os
import shutil
from datetime import date, timedelta as delta
from ...Infrastructure.Postgre.Postgre import Postgre
from ...Utils.AuxFunctions import next_friday, operative_week, operative_week_month
from ...Config import stations_code, special_stations, incremental_stations, calculated_stations_acomph, calculated_stations


db = Postgre('Postos')


def regressed_parameters() -> pd.DataFrame:

    aux = db.read(f'''SELECT a0, a1, mes, id_posto_regredido, id_posto_base 
                             FROM ons.correlacoes_diarias;''', to_dict=True)

    df = pd.DataFrame(data=aux)

    return df


def calculate_vaz(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Inflow of prevs stations
    :param data: Dataframe with inflow values
    :param ve_stations_only: Returns only Inflow of stations used by ONS
    :return: Inflow DataFrame
    """

    days = data.columns
    while True:
        try:
            df_regressed = regressed_parameters()
            break
        except:
            pass

    for station, config in calculated_stations_acomph.items():

        if config['type'] == 'calculated':

            aux = list()
            for dep_station in config['dep_stations']:
                values = pd.Series(data.loc[dep_station, :])
                aux.append(values)
            data.loc[station, :] = config['formula'](aux).values

        elif config['type'] == 'regressed':

            for day in days:
                a0 = df_regressed[(df_regressed['mes'] == day.month) &
                                  (df_regressed['id_posto_regredido'] == station)]['a0'].values[0]

                a1 = df_regressed[(df_regressed['mes'] == day.month) &
                                  (df_regressed['id_posto_regredido'] == station)]['a1'].values[0]

                values = pd.Series(data.loc[config['dep_stations'], day])
                aux = [values]

                data.loc[station, day] = config['formula'](aux, a0, a1).values[0]

        elif config['type'] == 'regressed_reverse':

            for day in days:
                a = df_regressed[(df_regressed['mes'] == day.month) &
                                 (df_regressed['id_posto_regredido'] == config['dep_stations'][0])]['a0'].values[0]

                b = df_regressed[(df_regressed['mes'] == day.month) &
                                 (df_regressed['id_posto_regredido'] == config['dep_stations'][0])]['a1'].values[0]

                a0 = -a / b
                a1 = 1 / b

                values = pd.Series(data.loc[config['dep_stations'], day])
                aux = [values]

                data.loc[station, day] = config['formula'](aux, a0, a1).values[0]

        elif config['type'] == 'belo_monte':

            for day in days:
                values = pd.Series(data.loc[config['dep_stations'], day])
                aux = [values]
                data.loc[station, day] = config['formula'](aux, day.month-1).values[0]

    data = data.sort_index()

    return data


def get_historical_inflow_weekly(data_base: Postgre, stations: list, ref_date: date) -> pd.DataFrame:

    stations = tuple(stations)

    init_date = next_friday(date.today()) - delta(days=41)
    # init_date = next_friday(date(2022, 1, 15)) - delta(days=41)
    end_date = ref_date - delta(days=7)

    query = f'''select 
                    posto_id, 
                    data, 
                    vazao_nat as vazao_nat,
                    vazao_inc vazao_inc
                from series_diarias.acomph
                -- from series_diarias.vw_vazoes_rdh_acomph
                where data between  %s and %s
                and posto_id in {stations}
                order by data asc;'''

    aux = data_base.read(query=query, params=(init_date, end_date), to_dict=True)

    stations = sorted(list(set([i['posto_id'] for i in aux]))+[168])
    dates = sorted(list(set([i['data'] for i in aux])))
    values = np.zeros((len(dates), len(stations)))
    cont_date = 0
    for day in dates:
        cont_station = 0
        for j in stations:
            if j in list(incremental_stations.keys()):
                values[cont_date, cont_station] = [x['vazao_inc'] for x in aux if x['posto_id'] == j and x['data'] == day][0]
            elif j == 168:
                values[cont_date, cont_station] = [x['vazao_inc'] for x in aux if x['posto_id'] == 169 and x['data'] == day][0]
            else:
                # try:
                values[cont_date, cont_station] = [x['vazao_nat'] for x in aux if x['posto_id'] == j and x['data'] == day][0]
                # except:
                #     values[cont_date, cont_station] = 0

            cont_station += 1

        cont_date += 1

    df = pd.DataFrame(data=values, columns=stations, index=dates)

    # Calculate acomph inflow
    df_result = calculate_vaz(data=df.transpose())

    df = df_result.transpose()

    full_weeks = int(df.shape[0] / 7)
    index = [int(f'{next_friday(df.index[7 * x]).year}{operative_week(next_friday(df.index[7 * x])):02}')
             for x in range(full_weeks)]
    values = np.zeros((len(index), len(df.columns)))
    for pos, station in enumerate(df.columns):
        for week in range(full_weeks):
            values[week, pos] = np.mean(df.iloc[7 * week:7 * (week + 1), pos].values[:])

    df_week = pd.DataFrame(data=values, index=index, columns=df.columns)

    return df_week


def get_historical_inflow_weekly_nat(data_base: Postgre, stations: list, ref_date: date) -> pd.DataFrame:

    stations = tuple(stations)

    init_date = next_friday(date.today()) - delta(days=41)
    # init_date = next_friday(date(2022, 1, 15)) - delta(days=41)
    end_date = ref_date - delta(days=7)

    query = f'''select 
                    posto_id, 
                    data, 
                    vazao_nat as vazao_nat,
                    vazao_inc as vazao_inc
                from series_diarias.acomph
                --from series_diarias.vw_vazoes_rdh_acomph
                where data between  %s and %s
                and posto_id in {stations}
                order by data asc;'''

    aux = data_base.read(query=query, params=(init_date, end_date), to_dict=True)

    stations = sorted(list(set([i['posto_id'] for i in aux]))+[168])
    dates = sorted(list(set([i['data'] for i in aux])))
    values = np.zeros((len(dates), len(stations)))
    cont_date = 0
    for day in dates:
        cont_station = 0
        for j in stations:
            if j == 168:
                values[cont_date, cont_station] = [x['vazao_inc'] for x in aux if x['posto_id'] == 169 and x['data'] == day][0]
            else:
                # try:
                values[cont_date, cont_station] = [x['vazao_nat'] for x in aux if x['posto_id'] == j and x['data'] == day][0]
                # except:
                #     values[cont_date, cont_station] = 0

            cont_station += 1

        cont_date += 1

    df = pd.DataFrame(data=values, columns=stations, index=dates)

    # Calculate acomph inflow
    df_result = calculate_vaz(data=df.transpose())

    df = df_result.transpose()

    full_weeks = int(df.shape[0] / 7)
    index = [int(f'{next_friday(df.index[7 * x]).year}{operative_week(next_friday(df.index[7 * x])):02}')
             for x in range(full_weeks)]
    values = np.zeros((len(index), len(df.columns)))
    for pos, station in enumerate(df.columns):
        for week in range(full_weeks):
            values[week, pos] = np.mean(df.iloc[7 * week:7 * (week + 1), pos].values[:])

    df_week = pd.DataFrame(data=values, index=index, columns=df.columns)

    return df_week


def separated_forecast_inflow(data: pd.DataFrame, ref_date: date, model: str) -> tuple:

    end_current_week = next_friday(ref_date)
    if model == 'SMAP':
        end_next_week = end_current_week + delta(days=7)
    else:
        end_next_week = end_current_week + delta(days=14)

    df_week = data.loc[:end_current_week, :]

    aux = data.loc[end_current_week + delta(days=1):end_next_week, :]
    mean = list()
    for i in range(int(aux.shape[0]/7)):
        mean.append(np.mean(aux.iloc[7*i:7*(i+1), :], axis=0))

    index = [int(f'{next_friday(aux.index[7*i]).year}{operative_week(next_friday(aux.index[7*i])):02}') for i in range(int(aux.shape[0]/7))]

    df_month = pd.DataFrame(data=np.reshape(np.asarray(mean), (len(index), len(aux.columns))),
                            index=index,
                            columns=data.columns)

    return df_week, df_month


def generate_prevs(data: pd.DataFrame, output_path: str, ref_date: date) -> None:

    output_path_month = os.path.join(output_path, ref_date.strftime("%B_%Y"))
    if os.path.exists(output_path_month):
        shutil.rmtree(output_path_month)
    os.mkdir(output_path_month)

    data_base = data.dropna(axis='columns')
    data_base = data_base.loc[:, :].round(0).astype(int)
    data_base[data_base == 0] = 1

    prevs_name = f'prevs.rv{operative_week_month(ref_date)-1}'

    fmt = r'%6s%5s%10s%10s%10s%10s%10s%10s'

    with open(os.path.join(output_path_month, prevs_name), 'w+') as file:
        for i, val in enumerate(list(data_base.keys())):
            row = (i + 1,) + (val,) + tuple(data_base.loc[:, val])
            print(fmt % tuple(row), file=file)
    file.close()


def stations_smap(data_base: Postgre) -> list:

    query_statement = f'''SELECT codigo_usina
                              FROM ons.vw_usina_modelo 
                              where modelo = 'SMAP' and prioridade = 1
                              ORDER BY codigo_usina asc;'''

    aux = data_base.read(query=query_statement)

    aux = [i[0] for i in aux]

    return aux


def get_priority_models(data_base: Postgre) -> dict:

    query = f"""select codigo_usina, modelo
                from ons.vw_usina_modelo
                where prioridade = 1;"""

    aux = data_base.read(query=query, to_dict=True)

    dict_output = dict()
    for i in aux:
        dict_output.update({i['codigo_usina']: i['modelo']})

    return dict_output


def station_hydro_plant() -> dict:
    """
    Read powerplants by station from config
    :return: Dict containing powerplants by station
    """
    values = dict()
    for i in stations_code.keys():
        for j in stations_code[i]['id_nat']:
            if j is not None:
                values[j] = i
        for k in stations_code[i]['id_art']:
            if k is not None:
                values[k] = i

    return values


def shift_list(data: list, delay: int) -> list:

    new_list = data[delay:] + data[:delay]

    return new_list


def calc_special_stations(data_base: pd.DataFrame, station_type: str, ref_date: date = None) -> pd.DataFrame:

    if station_type == 'Aggregate-Tiete':

        pos = int(f'{ref_date.year}{operative_week(ref_date)}')

        stations = {k: v for k, v in special_stations.items() if v['type'] == station_type}
        stations_list = list(stations.keys())
        while stations_list:
            for k, v in stations.items():
                if k in stations_list:
                    aux_2 = list()
                    if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
                        for j in v['dep_stations']:
                            aux_2.append(data_base.loc[pos:, j])
                        aux_3 = tuple(aux_2)
                        data_base.loc[pos:, k] = v['formula'](aux_3)
                        stations_list.remove(k)

    if station_type == 'Aggregate-SF':

        stations = {k: v for k, v in special_stations.items() if v['type'] == station_type}
        stations_list = list(stations.keys())
        while stations_list:
            for k, v in stations.items():
                if k in stations_list:
                    aux_2 = list()
                    if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
                        for j in v['dep_stations']:
                            aux_2.append(data_base.loc[:, j])
                        aux_3 = tuple(aux_2)
                        data_base[k] = v['formula'](aux_3)
                        stations_list.remove(k)

    if station_type == 'Dist. Incremental':

        stations = {k: v for k, v in special_stations.items() if v['type'] == station_type}
        stations_list = list(stations.keys())
        while stations_list:
            for k, v in stations.items():
                if k in stations_list:
                    aux_1 = list()
                    if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
                        aux_1.append(data_base.loc[:, v['dep_stations'][0]])
                        aux_2 = tuple(aux_1)
                        data_base.loc[:, k] = v['formula'](aux_2)
                        stations_list.remove(k)

    if station_type == 'São Francisco':

        reference_date = next_friday(ref_date) + delta(days=14)
        index = int(f'{reference_date.year}{operative_week(reference_date):02}')
        pos = data_base.index.get_loc(index)

        spec_stations = {k: v for k, v in special_stations.items() if v['type'] == station_type}
        spec_stations_list = list(spec_stations.keys())
        while spec_stations_list:
            for k, v in special_stations.items():
                if k in spec_stations_list:
                    aux_2 = list()
                    if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
                        for j, val in enumerate(v['dep_stations']):
                            delay = v['delay_time'][j]
                            aux_2.append(np.roll(data_base.loc[:, val].values, -delay))
                        aux_3 = tuple(aux_2)
                        data_base[k] = list(data_base.loc[:, k].values)[:pos] + list(v['formula'](aux_3))[pos:]
                        spec_stations_list.remove(k)

    if station_type == 'Zeros':

        zeros_stations = {k: v for k, v in special_stations.items() if v['type'] == station_type}
        zeros_stations_list = list(zeros_stations.keys())
        while zeros_stations_list:
            for k, v in zeros_stations.items():
                if k in zeros_stations_list:
                    data_base[k] = np.zeros(data_base.shape[0])
                    zeros_stations_list.remove(k)

    return data_base


def calc_inc_nat_stations(data_base: pd.DataFrame) -> pd.DataFrame:

    inc_stations_list = list(incremental_stations.keys())
    while inc_stations_list:
        for k, v in incremental_stations.items():
            aux_2 = list()
            if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
                for j, val in enumerate(v['dep_stations']):
                    aux_2.append(data_base.loc[:, val])
                aux_3 = tuple(aux_2)
                data_base.loc[:, k] = v['formula_inc_nat'](aux_3)
                inc_stations_list.remove(k)

            # if k in inc_stations_list and v['type'] == 'Aggregate-Tiete':
            #     aux_2 = list()
            #     if all(elem in list(data_base.keys()) for elem in v['dep_stations']):
            #         for j, val in enumerate(v['dep_stations']):
            #             aux_2.append(data_base.loc[:, val])
            #         aux_3 = tuple(aux_2)
            #         data_base[k] = v['formula_inc_nat'](aux_3)
            #         inc_stations_list.remove(k)

    return data_base


def get_ve_inflow_stations(data: pd.DataFrame) -> pd.DataFrame:

    stations_id = sorted([posto for posto, value in calculated_stations.items() if value['ve_included']])

    index = data.index
    columns = stations_id

    values = np.zeros((len(index), len(columns)))
    cont = 0
    for posto_id in stations_id:
        # print(posto_id)
        if posto_id in data.columns:
            if not any(np.isnan(data.loc[:, posto_id].values)):
                values[:, cont] = data.loc[:, posto_id].values
            else:
                aux = list()
                for j, station in enumerate(calculated_stations[posto_id]['dep_stations']):
                    serie = pd.Series(data.loc[:, station].values)
                    aux.append(serie)
                values[:, cont] = calculated_stations[posto_id]['formula'](aux).values
        else:
            aux = list()
            for j, station in enumerate(calculated_stations[posto_id]['dep_stations']):
                serie = pd.Series(data.loc[:, station].values)
                aux.append(serie)
            values[:, cont] = calculated_stations[posto_id]['formula'](aux).values

        cont += 1

    df = pd.DataFrame(data=values, index=index, columns=columns)

    return df.clip(lower=0.)
