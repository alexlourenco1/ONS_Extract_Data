import pandas as pd
from datetime import date, timedelta as delta, datetime as dt
import numpy as np
from dotenv import load_dotenv
from ...Infrastructure.Postgre import Postgre
from ...Utils.AuxFunctions import next_friday, day_by_operative_week

load_dotenv()

db_postos = Postgre(database='Postos')


class Regressed(object):

    def __init__(self):
        pass

    # Function that obtain a list of dict linear regression parameters of each station
    @staticmethod
    def regression_parameters(discretization: str = 'S') -> list:

        values = dict()
        if discretization == 'S':
            values = db_postos.read('SELECT * FROM ons.correlacoes_semanais_teste;', to_dict=True)
        elif discretization == 'D':
            values = db_postos.read('SELECT * FROM ons.correlacoes_diarias_teste;', to_dict=True)

        return values

    # Peso dos meses na semana operativa, ponderado pelo peso dos dias no mês
    @staticmethod
    def days_week_months_old(ref_date: date, nr_weeks: int) -> pd.DataFrame:

        """

        :param ref_date: Date of first day of operative week (Saturday) - Always do it!!
        :param nr_weeks: Number os weeks considered for forecast
        :return: List of days in month for each operative week

        """

        # Define m0, m-1 and m+1 based on initial date reference
        m0 = next_friday(ref_date).month
        y0 = next_friday(ref_date).year
        m_ant = m0 - 1
        y_ant = y0
        m_pos = m0 + 1
        y_pos = y0

        if m_ant == 0:
            m_ant = 12
            y_ant = y0 - 1
        if m_pos == 13:
            m_pos = 1
            y_pos = y0 + 1

        index = [date(y_ant, m_ant, 1), date(y0, m0, 1), date(y_pos, m_pos, 1)]
        columns = np.arange(nr_weeks)

        values = np.zeros((3, nr_weeks))
        data_ref = ref_date
        data = ref_date
        cont_sem: int = 0
        for i in range(int(7*nr_weeks)):
            final_date = next_friday(data_ref)
            if data <= final_date:
                if data.month == m_ant:
                    values[0][cont_sem] += 1
                elif data.month == m0:
                    values[1][cont_sem] += 1
                elif data.month == m_pos:
                    values[2][cont_sem] += 1

            if data == final_date:
                data_ref += delta(days=7)
                cont_sem += 1

            data += delta(days=1)

        values = (1/7) * values

        df = pd.DataFrame(data=values, columns=columns, index=index)

        return df.fillna(0.)

    # Peso dos meses na semana operativa, predominância do mês mais presente na semana operativa
    @staticmethod
    def days_week_months(data_base: pd.DataFrame) -> pd.DataFrame:

        """

        :param data_base: Data base
        :return: List of days in month for each operative week

        """

        df = pd.DataFrame(data=[])

        days = day_by_operative_week(week=int(str(data_base.index[0])[4:]), year=int(str(data_base.index[0])[:4]))
        day_inicio = days['data_inicio']
        day_fim = days['data_fim']

        for week in range(data_base.shape[0]):
            months_week = [(day_inicio + delta(i)).month for i in range(7)]
            if day_inicio.month == day_fim.month:
                df.loc[day_fim.replace(day=1), week] = 1.
            else:
                if 4 <= months_week.count(day_fim.month) <= 6:
                    df.loc[day_fim.replace(day=1), week] = 1.
                    df.loc[day_inicio.replace(day=1), week] = 0.
                else:
                    df.loc[day_fim.replace(day=1), week] = 0.
                    df.loc[day_inicio.replace(day=1), week] = 1.

            day_inicio = day_inicio + delta(days=7)
            day_fim = day_fim + delta(days=7)

        return df.fillna(0.)

    # Function that calculates inflow time series of each regression stations (weekly)
    def inflow_regression_calculate_weekly(self, parameters: list, station_base_series: pd.DataFrame) -> pd.DataFrame:

        # Get list of base and regressed stations
        id_regressed_stations = sorted(list(set([i['id_posto_regredido'] for i in parameters if i['id_posto_base'] in list(station_base_series.columns)])))

        # weight_old = self.days_week_months_old(ref_date=ref_date, nr_weeks=station_base_series.shape[0])
        weight = self.days_week_months(data_base=station_base_series)

        months = [x.month for x in weight.index]
        values = np.zeros((station_base_series.shape[0], len(id_regressed_stations)))
        for i in range(len(id_regressed_stations)):            # varia a cada posto regredido
            base_station = [x['id_posto_base'] for x in parameters if x['id_posto_regredido'] == id_regressed_stations[i]][0]
            for j, val in enumerate(station_base_series.index):    # varia a cada semana
                for k in range(len(months)):                       # varia a cada mês de referência
                    a0 = [x['a0'] for x in parameters if x['mes'] == months[k] and x['id_posto_regredido'] == id_regressed_stations[i]][0]
                    a1 = [x['a1'] for x in parameters if x['mes'] == months[k] and x['id_posto_regredido'] == id_regressed_stations[i]][0]

                    values[j, i] += weight.values[k][j] * (a0 + a1 * station_base_series.loc[val, base_station])

        df = pd.DataFrame(data=values, columns=id_regressed_stations, index=station_base_series.index)

        return df

    # Function that calculates inflow time series of each regression stations (diary)
    @staticmethod
    def inflow_regression_calculate_diary(parameters: list, station_base_series: pd.DataFrame) -> pd.DataFrame:

        # Get list of base and regressed stations
        id_regressed_stations = sorted(list(set([i['id_posto_regredido'] for i in parameters if i['id_posto_base'] in list(station_base_series.columns)])))

        dates_forecast = [station_base_series.index[0] + delta(days=i) for i in range(station_base_series.index.size)]
        values = np.zeros((station_base_series.index.size, len(id_regressed_stations)))
        for i in range(len(id_regressed_stations)):            # varia a cada posto regredido
            for j, val in enumerate(dates_forecast):
                a0 = [x['a0'] for x in parameters if x['mes'] == val.month and x['id_posto_regredido'] == id_regressed_stations[i]][0]
                a1 = [x['a1'] for x in parameters if x['mes'] == val.month and x['id_posto_regredido'] == id_regressed_stations[i]][0]
                base_station = [x['id_posto_base'] for x in parameters if x['mes'] == val.month and x['id_posto_regredido'] == id_regressed_stations[i]][0]

                # print('base_station: ', base_station)
                values[j, i] = a0 + a1 * station_base_series.loc[val, base_station]

        df = pd.DataFrame(data=values, columns=id_regressed_stations, index=dates_forecast)

        return df

    # Main function
    def main(self, discretization: str, data_base: pd.DataFrame) -> pd.DataFrame:

        regressed_inflow_series = pd.DataFrame(data=[])
        if discretization == 'S':
            # Get list of dict of "correlacoes semanais" of data base
            reg = self.regression_parameters(discretization='S')

            # Calculates regressed inflows
            regressed_inflow_series = self.inflow_regression_calculate_weekly(parameters=reg,
                                                                              station_base_series=data_base)
        elif discretization == 'D':

            # Get list of dict of "correlacoes diarias" of data base
            reg = self.regression_parameters(discretization='D')

            # Calculates regressed inflows
            regressed_inflow_series = self.inflow_regression_calculate_diary(parameters=reg,
                                                                             station_base_series=data_base)

        return regressed_inflow_series
