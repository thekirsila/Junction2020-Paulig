from django.shortcuts import render
from django.http import HttpResponse
import snowflake.connector

def query(qry, schm):
    ctx = snowflake.connector.connect(
        user='dev_edw_junction_team_06',
        password='F5AMxKoRagTi8QzPPknH9tCGhhowBu27',
        account='paulig.west-europe.azure',
        warehouse='WH01',
        database='DEV_EDW_JUNCTION',
        schema=schm
        )
    cs = ctx.cursor()
    try:
        cs.execute(qry)
        one_row = cs.fetchone()
        return one_row
    finally:
        cs.close()
    ctx.close()
    
def calculate30DayChange(table):
    query1 = int(query("SELECT AVG(POPULARITY) FROM " + table + " WHERE DAYS>-30 AND DAYS<=0", 'TEAM_06')[0])
    query2 = int(query("SELECT AVG(POPULARITY) FROM " + table + " WHERE DAYS>0 AND DAYS<=30", 'TEAM_06')[0])
    response = round((query1 - query2)/(query1 + query2) * 100,2)
    return response

def lue():
    conn = snowflake.connector.connect(
        user='dev_edw_junction_team_06',
        password='F5AMxKoRagTi8QzPPknH9tCGhhowBu27',
        account='paulig.west-europe.azure',
        warehouse='WH01',
        database='DEV_EDW_JUNCTION',
        schema='TEAM_06'
    )
    cur = conn.cursor()
    lista_previous = []
    lista_average = []
    lista_nimi = []
    try:
        cur.execute("SELECT FULL_NAME, LIFETIME_VALUE, PREVIOUS_PURCHASES_LAST_MONTH, AVERAGE_PURCHASES_LAST_MONTH FROM LOYAL_CUSTOMERS2 ORDER BY FULL_NAME;")
        for (FULL_NAME, LIFETIME_VALUE, PREVIOUS_PURCHASES_LAST_MONTH, AVERAGE_PURCHASES_LAST_MONTH) in cur:
            lista_nimi.append(FULL_NAME)
            lista_previous.append(float(round(PREVIOUS_PURCHASES_LAST_MONTH,1)))
            lista_average.append(float(round(AVERAGE_PURCHASES_LAST_MONTH,1)))

        sanakirja = {}
        for i in range(len(lista_average)):
            erotus = round(lista_average[i] - lista_previous[i],1)
            sanakirja[lista_nimi[i]] = erotus, lista_average[i], lista_previous[i]

        lista_virallinen_nimi = []
        lista_virallinen_erotus = []
        lista_virallinen_average = []
        lista_virallinen_previous = []

        for s in range(10):
            lista = list({k: v for k, v in sorted(sanakirja.items(), key=lambda item: item[1])})
            key = lista[-s-1]
            lista_virallinen_nimi.append(key)
            lista_virallinen_erotus.append(sanakirja[key][0])
            lista_virallinen_average.append(sanakirja[key][1])
            lista_virallinen_previous.append(sanakirja[key][2])
    finally:
        cur.close()
        
    palautettava = zip(lista_virallinen_nimi, lista_virallinen_average, lista_virallinen_previous, lista_virallinen_erotus)
    return palautettava
        

# Create your views here.
def index(request):
    #response = query()
    response = ""
    return render(request, 'Pauligdashboard/index.html', {'response': response})

def customers(request):
    response = lue()
    return render(request, 'Pauligdashboard/customers.html', {'response': response})

def analytics(request):
    return render(request, 'Pauligdashboard/analytics.html')

def trends(request):
    stable = [calculate30DayChange('PAVLOVA_STABLE'), calculate30DayChange('LEMONPIE_STABLE'), calculate30DayChange('CAPPUCINO_STABLE')]
    stable_name = ['Pavlova', 'Lemonpie', 'Cappucino']
    
    rising = [calculate30DayChange('CHOCOLATE_CROISSANT_RISING')]
    rising_name = ['Croissant']
    
    cycle = [calculate30DayChange('TIRAMISU_TREND_FORECAST'), calculate30DayChange('LATTE_TREND_FORECAST'), calculate30DayChange('HOTCHOCOLATE_TREND_FORECAST'), calculate30DayChange('ESPRESSO_TREND_FORECAST')]
    cycle_name = ['Tiramisu', 'Latte', 'Hot Chocolate', 'Espresso']
    
    return render(request, 'Pauligdashboard/trends.html', {'stable': zip(stable_name, stable), 'rising': zip(rising_name, rising), 'cycle': zip(cycle_name, cycle)})
    