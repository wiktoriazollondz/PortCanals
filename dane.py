import pandas as pd
import numpy as np
import os

meteo_files = [f for f in os.listdir('dane-2021-2025/dane-pogodowe-stacja-gora-gradowa-2021-2025') if f.endswith('.xlsx')]

for file in meteo_files:
    path = os.path.join('dane-2021-2025/dane-pogodowe-stacja-gora-gradowa-2021-2025', file)
    df = pd.read_excel(path, skipfooter=4)
    df = df.dropna(how='all')
    df.columns = df.columns.str.strip()

    cols_to_fix = ['Opad [mm]', 'Temperatura [C]', 'Wilgotność [%]', 'Ciśnienie [hPa]']

    for col in cols_to_fix:
        df[col] = df[col].astype(str).str.replace(',', '.')
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # opady
    df.loc[(df['Opad [mm]'] < 0) | (df['Opad [mm]'] > 100), 'Opad [mm]'] = np.nan

    # temperatura
    df.loc[(df['Temperatura [C]'] < -30) | (df['Temperatura [C]'] > 40), 'Temperatura [C]'] = np.nan

    # wilgotność
    df.loc[(df['Wilgotność [%]'] < 0) | (df['Wilgotność [%]'] > 100), 'Wilgotność [%]'] = np.nan

    # ciśnienie
    df.loc[(df['Ciśnienie [hPa]'] < 950) | (df['Ciśnienie [hPa]'] > 1050), 'Ciśnienie [hPa]'] = np.nan

    # interpolacja danych - dodanie danej na podstawie sąsiednich
    for col in cols_to_fix:
        df[col] = df[col].interpolate(method='linear')

    print(df.isnull().sum())

water_level_files = [f for f in os.listdir('dane-2021-2025/poziom-wody-ujscie-rzeki-strzyza-2021-2025') if f.endswith('.xlsx')]

for file in water_level_files:
    path = os.path.join('dane-2021-2025/poziom-wody-ujscie-rzeki-strzyza-2021-2025', file)
    df = pd.read_excel(path, skipfooter=4, usecols=[0, 1])
    df = df.dropna(how='all')
    df.columns = df.columns.str.strip()

    # poziom wody
    df['Poziom wody [m]'] = df['Poziom wody [m]'].astype(str).str.replace(',', '.')
    df['Poziom wody [m]'] = pd.to_numeric(df['Poziom wody [m]'], errors='coerce')

    # interpolacja danych - dodanie danej na podstawie sąsiednich
    df['Poziom wody [m]'] = df['Poziom wody [m]'].interpolate(method='linear')
    # df.loc[(df['Poziom wody [m]'] < -50) | (df['Poziom wody [m]'] > 100), 'Poziom wody [m]'] = np.nan

    print(df.isnull().sum())
