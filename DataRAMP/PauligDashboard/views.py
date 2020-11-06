from django.shortcuts import render
from django.http import HttpResponse
import snowflake.connector

def query(qry):
    ctx = snowflake.connector.connect(
        user='dev_edw_junction_team_06',
        password='F5AMxKoRagTi8QzPPknH9tCGhhowBu27',
        account='paulig.west-europe.azure',
        warehouse='WH01',
        database='DEV_EDW_JUNCTION',
        schema='JUNCTION_2020'
        )
    cs = ctx.cursor()
    try:
        cs.execute(qry)
        one_row = cs.fetchone()
        return one_row
    finally:
        cs.close()
    ctx.close()

# Create your views here.
def index(request):
    response = query("SELECT * FROM WEBSHOP_DATA")
    return render(request, 'Pauligdashboard/base.html', {'response': response})