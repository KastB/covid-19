

import pandas as pd
import numpy as np
import plotly.graph_objects as go

import plotly.io as pio
import sys

from plotly.subplots import make_subplots

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

from_cache = False
data_url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
cache_path = "../raw_data.csv"
pd.options.plotting.backend = "plotly"
days_till_healthy = 15
lethality = 0.0157

data_indices = [8, 6, 4, 7, [6, 8]]
labels = ["date",
          "confirmed", "deaths", "currently_infected", "confirmed_100k",
          "deaths_100k", "currently_infected_100k", "est_infected_100k", "deaths_100k_14d"]
interesting_countries = sorted(["China",
                                "Brazil",
                                "Denmark",
                                "France",
                                "Germany",
                                "India",
                                "Italy",
                                "Korea, South",
                                "Sweden",
                                "Egypt",
                                "Israel",
                                "United Kingdom",
                                "Belgium",
                                "Russia",
                                "Spain",
                                "Austria",
                                "Switzerland",
                                "Poland",
                                "US",
                                "South Africa"
                                ])

if from_cache:
    raw_data = pd.read_csv(cache_path)
else:
    raw_data = pd.read_csv(data_url)
    raw_data.to_csv(cache_path, index=False, header=True)
residents = pd.read_csv(StringIO("""Country, Residents
Afghanistan,32890171
Albania,2845955
Algeria,43900000
Andorra,77543
Angola,31127674
Antigua and Barbuda,97895
Argentina,45376763
Armenia,2963000
Australia,25686762
Austria,8915382
Azerbaijan,10095900
Bahamas,385000
Bahrain,1503091
Bangladesh,169536502
Barbados,287025
Belarus,9408400
Belgium,11535652
Belize,419199
Benin,12114193
Bhutan,771612
Bolivia,11670000
Bosnia and Herzegovina,11633371
Botswana,3332593
Brazil,209000000
Brunei,212250204
Bulgaria,6927000
Burkina Faso,20900000
Burma,54410000
Burundi,21510181
Cabo Verde,12309600
Cambodia,15288489
Cameroon,24348251
Canada,38221221
Central African Republic,5633412
Chad,16244513
Chile,19458310
China,1405035800
Colombia,50372424
Comoros,758316
Congo (Brazzaville),5518000
Congo (Kinshasa),89560000
Costa Rica,5111238
Cote d'Ivoire,26380000
Croatia,4058165
Cuba,11193470
Cyprus,1200000
Czechia,10699142
Denmark,5825337
Diamond Princess,1000
Djibouti,962452
Dominica,71808
Dominican Republic,10448499
Ecuador,17596656
Egypt,101096318
El Salvador,6765753
Equatorial Guinea,1454789
Eritrea,3546000
Estonia,1328976
Eswatini,1093238
Ethiopia,100829000
Fiji,889327
Finland,5503335
France,67132000
Gabon,2176766
Gambia,2335504
Georgia,3716858
Germany,83122889
Ghana,30955202
Greece,10724599
Grenada,112003
Guatemala,16858333
Guinea,12559623
Guinea-Bissau,1624945
Guyana,744962
Haiti,11743017
Holy See,130000
Honduras,9304380
Hungary,9769526
Iceland,366700
India,1368870621
Indonesia,269603400
Iran,83895801
Iraq,40150200
Ireland,4977400
Israel,9269200
Italy,60062012
Jamaica,2734093
Japan,125880000
Jordan,10799264
Kazakhstan,18801984
Kenya,47564296
"Korea, South",51600000
Kosovo,1782115
Kuwait,4464521
Kyrgyzstan,6596500
Laos,7231210
Latvia,1898400
Lebanon,6825442
Lesotho,2007201
Liberia,4568298
Libya,6871287
Liechtenstein,38749
Lithuania,2795334
Luxembourg,626108
Madagascar,26251309
Malawi,19129952
Malaysia,32700590
Maldives,383135
Mali,20250833
Malta,514564
Marshall Islands,58000
Mauritania,4173077
Mauritius,1266000
Mexico,127792286
Moldova,2640438
Monaco,38100
Mongolia,3344518
Montenegro,621873
Morocco,36056313
Mozambique,30066648
MS Zaandam,5000
Namibia,2504498
Nepal,29996478
Netherlands,17523481
New Zealand,5093882
Nicaragua,6527691
Niger,23196002
Nigeria,206139587
North Macedonia,2076255
Norway,5374807
Oman,4445262
Pakistan,220892331
Panama,4278500
Papua New Guinea,8935000
Paraguay,7252672
Peru,32625948
Philippines,109345417
Poland,38352000
Portugal,10295909
Qatar,2723624
Romania,19317984
Russia,146748590
Rwanda,12663116
Saint Kitts and Nevis,52823
Saint Lucia,178696
Saint Vincent and the Grenadines,110696
Samoa,196000
San Marino,33630
Sao Tome and Principe,21024
Saudi Arabia,34218169
Senegal,16705608
Serbia,6926705
Seychelles,98462
Sierra Leone,8100318
Singapore,5685807
Slovakia,5460136
Slovenia,2097195
Solomon Islands,694619
Somalia,15893219
South Africa,59622350
South Sudan,13249924
Spain,47329981
Sri Lanka,21803000
Sudan,42938585
Suriname,590100
Sweden,10367232
Switzerland,8632703
Syria,17500657
Taiwan*,23568378
Tajikistan,9313800
Tanzania,57637628
Thailand,66568817
Timor-Leste,1200000
Togo,7706000
Trinidad and Tobago,1366725
Tunisia,11708370
Turkey,83154997
Uganda,41583600
Ukraine,41723998
United Arab Emirates,9366829
United Kingdom,66796807
Uruguay,3530912
US,330538821
Uzbekistan,34488572
Venezuela,28435943
Vietnam,96483981
West Bank and Gaza,1155800
Western Sahara,597000
Yemen,29825968
Zambia,17885422
Zimbabwe,15473818
"""))


def get_residents(country):
    try:
        return residents[residents["Country"] == country].values[0][1]
    except IndexError:
        print("country not found: " + country)


def get_country_data(country):
    return raw_data[raw_data["Country/Region"] == country]


def calculate_statistics(country):
    country_data = get_country_data(country)
    residents = get_residents(country)
    date = country_data["Date"].values
    confirmed = country_data["Confirmed"].values
    deaths = country_data["Deaths"].values
    currently_infected = confirmed.copy()
    currently_infected[days_till_healthy:] -= confirmed[:-days_till_healthy]
    confirmed_100k = confirmed / residents * 100000
    deaths_100k = deaths / residents * 100000
    currently_infected = np.where(currently_infected < 0, 0, currently_infected)
    currently_infected_100k = currently_infected / residents * 100000
    tmp = confirmed - currently_infected
    est_infected_100k = deaths / lethality * np.divide(confirmed, tmp, where=tmp != 0) / residents * 100000

    deaths_100k_14d = deaths_100k.copy()
    deaths_100k_14d[14:] -= deaths_100k[:-14]
    deaths_100k_14d = np.where(deaths_100k_14d < 0, 0, deaths_100k_14d)
    return date, confirmed, deaths, currently_infected, confirmed_100k, deaths_100k, currently_infected_100k, est_infected_100k, deaths_100k_14d


def add_plot(fig, data, dat_index, secondary=False, ):
    for d in data:
        if type(dat_index) is not int:
            index = dat_index[0]
        else:
            index = dat_index
        fig.add_trace(go.Scatter(x=d[1][0], y=d[1][index],
                                 # mode="lines+markers",
                                 mode="lines",
                                 name=d[0]),
                      secondary_y=False
                      )
        if type(dat_index) is not int:
            index = dat_index[1]
            fig.add_trace(go.Scatter(x=d[1][0], y=d[1][index],
                                     # mode="lines+markers",
                                     mode="lines",
                                     name=d[0]),
                          secondary_y=True
                          )


# data = [(country, calculate_statistics(country)) for country in [r[0] for r in residents.values]]
data = [(country, calculate_statistics(country)) for country in interesting_countries]

lines = list()
for d in data_indices:
    # fig = go.Figure()
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.update_xaxes(title_text="Date")
    if type(d) is not int:
        add_plot(fig, data, d)
        label = labels[d[0]]
        fig.update_yaxes(title_text=label)
        fig.update_yaxes(title_text=labels[d[1]], secondary_y=True)
        label += "_"
        label += labels[d[1]]
    else:
        add_plot(fig, data, d)
        label = labels[d]
        fig.update_yaxes(title_text=label)

    # fig.show()
    fig.write_html(f"../docs/plots/jh_{label}.html")
