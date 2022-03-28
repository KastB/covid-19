"""
Description

@author: Bernd Kast
@copyright: Copyright (c) 2018, Siemens AG
@note:  All rights reserved.
"""
import json

import numpy as np
import pandas as pd
import plotly
import requests
from icecream import ic
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# https://www-genesis.destatis.de/genesis/online?operation=abruftabelleBearbeiten&levelindex=1&levelid=1640356498708&auswahloperation=abruftabelleAuspraegungAuswaehlen&auswahlverzeichnis=ordnungsstruktur&auswahlziel=werteabruf&code=12411-0005&auswahltext=&werteabruf=Werteabruf#abreadcrumb

data_uri = "https://ago-item-storage.s3.us-east-1.amazonaws.com/f10774f1c63e40168479a1feb6c7ca74/RKI_COVID19.csv?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDcaCXVzLWVhc3QtMSJGMEQCIGwBWXQMgq336SwxLdYLaKKf2YxRElA6r8gFGTDgVr3RAiBY0Th4H8QRmD%2B2mbWTfb7pPyPfxQ6z%2BTG5MOKA8mJ40iqDBAjA%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAAaDDYwNDc1ODEwMjY2NSIMmBLHMOUHm1BO1GrYKtcDm0fSX3SSq9dGcKrowO12mCAdk4ICVYjts0aWGhPz1R4nX16z8rk1cxc29fH5%2FM8LqCto%2FD7WBF4CLZ%2BGBphxorjVCYJvqtVzywKZsBjRPpWjoRW7zxSBbUSK4xhUi28%2B%2FdDfusUYMr99JZdJdb2ITnOPvEtA5Q6k7kDMz%2F%2FqLMOLJbHGjOCFf5wDSFYTWXh79fGk7tCG%2BYLGTUpc9BtZ28xjFWjdfiFRcCDjyrvPex8jmdtYTp7WaVAJTxp3Nu3U7pJomOR3cVebAK9rz8kzYseKlsInVWnx8j2%2BW%2FAhYwkNsO%2F2YRS6C%2BU8tKB0BuroLs4wdw2q49OAsOjFTHfd52nB4EYIO4uEMpR7CkVDTC5QQ6dJCeW9UiSA0IxvQ9d3JTtjjub1F%2Bqn0LqFEIZ3taMsiuctMCePU76HYxF89M%2FDMgywvUo6ZVyzLBs4uDiqiTzrLNLiJ19omOaR0DCAaGEHNPohUWoRCyaDNTo6UU207%2BIQFrcSEHBuFmfTDqTg9FmI287EIguQxg4HpnEXHp9sPXYLF5fZwkN2gDxkyR8d32gB0n931f9YXDBBBAtAAmvD5u%2FV9DYCTN3IXsWJR10532NYoX54MFJ4IPmGqge%2BMpLrPdyTMLmRh5IGOqYBn76DHVr3OmrxG2lCQhpVZz3CWe4ZwP7qdFUr%2Bld%2FgPoltDDkCvXXC12%2FQP35WsUmCmgY%2Bs549uln5i%2Fk60M%2B67%2Bjpwf276lrQLbwHKTZX9mVpbV3JH0S71nEDaK7GTOyK1vToHqxH1T7wN86E4r7ivFaXM6GxO26W8ExUuew%2Fzqy2F%2FeTU2xOpmWkotqHZgVvRRoP%2BCv4tX3sLrtTfH8pLzObGRnjg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220328T154110Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAYZTTEKKEX2RW5M5B%2F20220328%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=31dc39f4198f36dd46c2514b7419d5cc3f0d31047ae25cf39566bd80119e1840"
# data_uri = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={}"
# data_uri = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&f=json"
# data_uri = "https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0/explore?showTable=true"
path = "raw_rki_data.csv"
rki_file = "rki_data.csv"

age_group_size = {
    "A00-A04": 3969138,
    "A05-A14": 7508662,
    "A15-A34": 18921292,
    "A35-A59": 28666166,
    "A60-A79": 18153339,
    "A80+": 5936434,
    "unbekannt": 100000
}
age_group_factor = dict()
for ag in age_group_size:
    age_group_factor[ag] = 100000.0 / age_group_size[ag]


def json_to_csv():
    with open(path, "r") as fp:
        data = json.load(fp)
    counter = 0
    for d in data:
        counter += len(d["features"])
    print(counter)
    columns = [f["name"] for f in data[0]["fields"]]

    with open(rki_file, "w") as fp:
        fp.writelines([";".join(columns) + "\n"])
    count = 0
    with open(rki_file, "a") as fp:
        for d in data:
            print(f"start new batch {count / len(data)}")
            count += 1
            lines = list()

            for f in d["features"]:
                line = ""
                for c in columns:
                    line = f"{line}{f['attributes'][c]};"
                lines.append(f"{line[:-1]}\n")
            fp.writelines(lines)


def download_data():
    more_data = True
    data = list()
    counter = 0
    while more_data:
        print(counter)
        res = requests.get(data_uri.format(counter)).json()
        data.append(res)
        try:
            if not res["exceededTransferLimit"]:
                more_data = False
        except KeyError:
            more_data = False
        counter += len(res["features"])

    with open(path, "w") as fp:
        json.dump(data, fp)


def read_csv():
    return pd.read_csv(rki_file, sep=";", parse_dates=['Meldedatum', "Refdatum"], date_parser=lambda epoch: pd.to_datetime(epoch, unit='ms'))


def add_trace(df_filtered, fig, all_time, start_dat, end_dat, value_key, date_key, label, secondary, factor, color):
    # aggregate
    df_filtered = df_filtered[df_filtered[value_key] >= 0]
    df_filtered = df_filtered.groupby(date_key)[value_key].sum()
    st = start_dat
    while st < end_dat:
        if st not in df_filtered:
            df_filtered[st] = 0
        st += np.timedelta64(1, "D")
    df_filtered = df_filtered.sort_index()
    if len(df_filtered) == 0:
        return
    df_filtered *= factor
    cumsum = np.cumsum(df_filtered.values)
    if all_time:
        fig.add_trace(go.Scatter(x=df_filtered.keys()[14:].strftime('%Y-%m-%d').values,
                                 y=cumsum,
                                 mode="lines",
                                 name=label,
                                 marker={"color": color}),
                      secondary_y=secondary
                      )

    else:
        # kernel_size = 14
        # kernel = np.ones(kernel_size) / kernel_size
        # data_convolved = np.convolve(df_filtered, kernel, mode='same')
        data_convolved = cumsum[14:] - cumsum[:-14]
        fig.add_trace(go.Scatter(x=df_filtered.keys().strftime('%Y-%m-%d').values[14:],
                                 y=data_convolved,
                                 mode="lines",
                                 name=label,
                                 marker={"color": color}),
                      secondary_y=secondary
                      )
        # fig.add_trace(go.Scatter(x=df_filtered.keys().strftime('%Y-%m-%d').values,
        #                          y=df_filtered,
        #                          mode="markers",
        #                          name=label),
        #               secondary_y=secondary
        #               )


def add_data_to_fig(df, fig, all_time, age, id_bundesland, id_landkreis, typ, secondary, normalize_age=False):
    date_key = "Meldedatum"
    start_dat = np.min(df[date_key].values)
    end_dat = np.max(df[date_key].values)
    df_filtered = df.sort_values(date_key)

    if typ == "death":
        value_key = "AnzahlTodesfall"
    else:
        value_key = "AnzahlFall"
    label = "Gesamt " if all_time else "14d "
    counter = 0
    if not age:

        add_trace(df_filtered, fig, all_time, start_dat, end_dat, value_key, date_key, label, secondary, 1.0, plotly.colors.DEFAULT_PLOTLY_COLORS[-1])
        if id_bundesland:
            for id in range(1, 17):
                df_filtered2 = df_filtered[df_filtered["IdBundesland"] == id]
                color = plotly.colors.DEFAULT_PLOTLY_COLORS[counter % len(plotly.colors.DEFAULT_PLOTLY_COLORS)]
                counter += 1
                add_trace(df_filtered2, fig, all_time, start_dat, end_dat, value_key, date_key, f"{label} {df_filtered2.iloc[0]['Bundesland']}", secondary, 1.0, color)
    else:
        if id_bundesland >= 0:
            df_filtered = df_filtered[df_filtered["IdBundesland"] == id_bundesland]
        if id_landkreis >= 0:
            df_filtered = df_filtered[df_filtered["IdLandkreis"] == id_landkreis]
        for age_group in ['A00-A04', 'A05-A14', 'A15-A34', 'A35-A59', 'A60-A79', 'A80+', 'unbekannt']:
            df_filtered2 = df_filtered[df_filtered["Altersgruppe"] == age_group]
            color = plotly.colors.DEFAULT_PLOTLY_COLORS[counter]
            counter += 1
            add_trace(df_filtered2, fig, all_time, start_dat, end_dat, value_key, date_key, label + age_group, secondary, age_group_factor[age_group] if normalize_age else 1.0, color)


"""
interesting parameters:
    - aggregated / 14 days
    - aggregated / agegroup
    - aggregated / bundesland / landkreis (auswahl)   
    - infected / death 
    - sex
"""


def plot(df):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Infected normalized")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=-1, age=True, typ="inf", secondary=False, normalize_age=True)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=-1, age=True, typ="inf", secondary=True, normalize_age=True)
    #fig.show()
    label = "infected_normalized_age"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Deaths normalized")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=-1, age=True, typ="death", secondary=False, normalize_age=True)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=-1, age=True, typ="death", secondary=True, normalize_age=True)
    #fig.show()
    label = "deaths_normalized_age"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Deaths Guenzburg normalized")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=9774, id_bundesland=-1, age=True, typ="death", secondary=False, normalize_age=True)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=9774, id_bundesland=-1, age=True, typ="death", secondary=True, normalize_age=True)
    #fig.show()
    label = "deaths_normalized_age_gz"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Infected Guenzburg normalized")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=9774, id_bundesland=-1, age=True, typ="inf", secondary=False, normalize_age=True)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=9774, id_bundesland=-1, age=True, typ="inf", secondary=True, normalize_age=True)
    #fig.show()
    label = "infected_normalized_age_gz"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Infected")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=-1, age=True, typ="inf", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=-1, age=True, typ="inf", secondary=True)
    #fig.show()
    label = "infected_age"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Deaths")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=-1, age=True, typ="death", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=-1, age=True, typ="death", secondary=True)
    #fig.show()
    label = "deaths_age"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Deaths Guenzburg")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=9774, id_bundesland=-1, age=True, typ="death", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=9774, id_bundesland=-1, age=True, typ="death", secondary=True)
    # fig.show()
    label = "deaths_age_gz"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Infected Guenzburg")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=9774, id_bundesland=-1, age=True, typ="inf", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=9774, id_bundesland=-1, age=True, typ="inf", secondary=True)
    # fig.show()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Infected Bundesländer")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=True, age=False, typ="inf", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=True, age=False, typ="inf", secondary=True)
    # fig.show()
    label = "infected"
    fig.write_html(f"../docs/plots/rki_{label}.html")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Tote Bundesländer")
    add_data_to_fig(df, fig, all_time=False, id_landkreis=-1, id_bundesland=True, age=False, typ="death", secondary=False)
    add_data_to_fig(df, fig, all_time=True, id_landkreis=-1, id_bundesland=True, age=False, typ="death", secondary=True)
    #fig.show()
    label = "deaths"
    fig.write_html(f"../docs/plots/rki_{label}.html")

ic(download_data())
ic(json_to_csv())
df = read_csv()
ic(plot(df))
