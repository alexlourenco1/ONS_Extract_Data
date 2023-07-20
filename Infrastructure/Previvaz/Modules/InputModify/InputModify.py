import os
import shutil
import pandas as pd


def copy_folder(input_path_folder: str, output_path_folder: str) -> None:
    if not os.path.exists(output_path_folder):
        os.makedirs(output_path_folder)

    input_folder = [x for x in os.listdir(input_path_folder)]
    for j in input_folder:

        s = os.path.join(input_path_folder, j)
        d = os.path.join(output_path_folder, j)
        if not os.path.exists(d):
            if os.path.isdir(s):
                shutil.copytree(s, d, False, None)
            else:
                shutil.copy2(s, d)


def week_prediction_change(station: int, path: str, weeks_add: int) -> None:

    input_path = os.path.join(path, f'{station}.inp')
    df_values = pd.read_fwf(input_path, delimiter='\r\n', header=None, skip_blank_lines=False)

    if int(df_values.iloc[-1, 0].split()[0]) == 0:
        nr_weeks_actual_year = 52
    elif int(df_values.iloc[-1, 0].split()[0]) == 1:
        nr_weeks_actual_year = 53

    original_week = int(df_values.iloc[8, 0])
    original_year = int(df_values.iloc[9, 0])

    new_week = original_week + weeks_add
    if new_week <= nr_weeks_actual_year:
        week = new_week
        year = original_year
    else:
        week = new_week - nr_weeks_actual_year
        year = original_year + 1

    df_values.iloc[8, 0] = f'{week:2}'
    df_values.iloc[9, 0] = f'{year}'

    # df_values_new = df_values.replace(np.nan, '', regex=True)

    # Delete old file
    os.remove(input_path)

    # Open new file
    fin = open(input_path, "w")

    for line in df_values.values[:, 0]:
        if isinstance(line, str):
            fin.write(line)
            fin.write("\n")
        else:
            fin.write("\n")

    fin.close()
