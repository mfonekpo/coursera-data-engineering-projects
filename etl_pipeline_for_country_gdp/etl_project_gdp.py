import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup


db_table = "Countries_by_GDP"
db_name = "World_Economies.db"

country_name = []
country_gdp = []
year = []

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

html_page = requests.get(url).text
soup = BeautifulSoup(html_page, 'html.parser')

tbody = soup.find_all("tbody")
tbody_tr = tbody[2].find_all("tr")

for row in tbody_tr:
    col = row.find_all("td")
    if len(col) != 0:
        country_name.append(col[0].text.strip())
        gdp_str = col[2].text.strip().replace(',', '') 
        gdp_str = gdp_str.strip().replace('—', '0')
        gdp_int = int(gdp_str)
        country_gdp.append(gdp_int)
        # country_gdp.append(round(float(gdp_int) / 1e9, 2)) #rounding the value to 2 decimal places
        year.append(col[3].text.strip())
        data_dict = {
            "Country": country_name,
            "GDP_USD_billion": country_gdp,
            "Year": year
        }
        imf_data = pd.DataFrame(data_dict)

    else:
        pass

# imf_data.to_csv("../coursera/dataset/imf_gdp.csv", index=False)
imf_data.to_json("../coursera/dataset/Countries_by_GDP.json", orient="records")

conn = sqlite3.connect(db_name)
imf_data.to_sql(db_table, conn, if_exists="replace", index=False)
conn.close()