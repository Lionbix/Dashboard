#-----------------------
# IMPORT
#-----------------------
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

app = dash.Dash(__name__)

import sqlalchemy 
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

import plotly.express as px
import plotly.graph_objects as go

#-----------------------
# POSTGRES Connexion 
#-----------------------
POSTGRES_ADDRESS = 'seelk-data-case-study.c4ickgvli6xi.eu-west-3.rds.amazonaws.com' 
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'guest'
POSTGRES_PASSWORD = 'v9YjMLgyvGdH3mF7gBxgjNRFd' 
POSTGRES_DBNAME = "casestudy"
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,password=POSTGRES_PASSWORD,ipaddress=POSTGRES_ADDRESS,port=POSTGRES_PORT,dbname=POSTGRES_DBNAME))
# Create the connection
cnx = create_engine(postgres_str)

#-----------------------
# table Loading
#-----------------------
product_dimensions = pd.read_sql_query('''SELECT * FROM product_dimensions;''', cnx)
bi_daily = pd.read_sql_query('''SELECT * FROM bi_daily;''', cnx)

#-----------------------------------------------------------
# PYTHON Pre-Processing for table "product_dimensions"
#-----------------------------------------------------------

from collections import Counter
from nltk.tokenize import word_tokenize 
import unicodedata
import nltk
import re
#nltk.download('stopwords')

def strip_accents(text):#Remove accent 
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")
    return str(text)

top_N = 8

words = product_dimensions['product_type']
words = words.astype(str) + ',' #we add a suffix to delimitate the cells between them, to concatenate after. 
words = words.str.cat(sep='')
words = words.lower()
words = strip_accents(words)
words = words.split(",") #We transform the text cleaned as a list. 

word_count = pd.DataFrame(Counter(words).most_common(top_N),
                    columns=['Word', 'Count']).set_index('Word') #Count words redundancies 
word_count=word_count.reset_index()

shipment = product_dimensions.groupby('shipment')['product_code'].count() #Count shipment Distribution 
shipment = shipment.reset_index()

#-----------------------------------------------------------
# PYTHON Pre-Processing for table "bi_daily"
#-----------------------------------------------------------
na = pd.DataFrame()
na['na'] = bi_daily.isna().sum()
na = na.sort_values(by=['na'])
na = na[na['na'] == 60682]#Find empty columns

bi_daily = bi_daily.drop(na.index, axis=1)
bi_daily = bi_daily.drop(['bi_id'], axis=1) 
bi_daily['ingestion_date'] = pd.to_datetime(bi_daily['ingestion_date'])

product_code = product_dimensions[['product_type','shipment','product_code']]
product_code = pd.merge(bi_daily, product_code, left_on='product_code', right_on='product_code')#Merge the two tables 

radar = product_code.groupby('product_type')['created'].count()
radar = radar.reset_index()

# Show by month the evolution of the orders by the quantity and the price
date = pd.DataFrame()
date['date'] = product_code['ingestion_date'].dt.strftime('%m-%Y')
date['date'] = pd.to_datetime(date['date'])
date['type'] = product_code['product_type']
date['price'] = product_code['seller_sales_ttc_eur']

creation = date.groupby('date')['type'].count()
price = date.groupby('date')['price'].sum()
creation = creation.reset_index()
creation = creation.sort_values(by='date')
price = price.reset_index()
price = price.sort_values(by='date')
price['order'] = creation['type'] 

#Compute Sales remuneration & Shipment price by units 
seller = product_code[['seller_sales_ttc_eur','seller_sold_units','seller_shipping_costs_eur','product_code','marketplace','ingestion_date','product_type','shipment']]
seller_unit = seller.groupby('marketplace')['seller_sold_units'].sum()
seller_sales = seller.groupby('marketplace')['seller_sales_ttc_eur'].sum()
seller_shipping = seller.groupby('marketplace')['seller_shipping_costs_eur'].sum()

seller_unit = seller_unit.reset_index()
seller_sales = seller_sales.reset_index()
seller_shipping = seller_shipping.reset_index()

comparaison = pd.merge(seller_sales, seller_unit, left_on='marketplace', right_on='marketplace')
comparaison = pd.merge(comparaison, seller_shipping, left_on='marketplace', right_on='marketplace')
comparaison['sales/units'] = comparaison['seller_sales_ttc_eur']/comparaison['seller_sold_units']
comparaison['shipping/units'] = comparaison['seller_shipping_costs_eur']/comparaison['seller_sold_units']
comparaison['sales/units'] = comparaison['sales/units'].round(2).astype(str) + '€'
comparaison['shipping/units'] = comparaison['shipping/units'].round(2).astype(str) + '€'

#Compute which shipment method looks like the most efficient
mfn = seller[seller['shipment'] == "MFN"]['marketplace'].count()
fba = seller[seller['shipment'] == "FBA"]['marketplace'].count()
shipment_price = seller.groupby('shipment')['seller_shipping_costs_eur'].sum() 
fba = (shipment_price['FBA']/fba).round(2).astype(str) + '€'
mfn = (shipment_price['MFN']/mfn).round(2).astype(str) + '€'
#-----------------------
# GRAPH Creation
#-----------------------
word_count = px.bar(word_count, x='Word', y='Count',color='Word',color_discrete_sequence=px.colors.qualitative.Pastel)
word_count.update_layout(title_text='Number of product in each category')

shipment = px.bar(shipment, x='shipment', y='product_code',color='shipment',color_discrete_sequence=px.colors.qualitative.Pastel)
shipment.update_layout(title_text='Shipment method distribution', yaxis_title="Count")

sunburst = px.sunburst(product_dimensions, path=['marketplace','product_type'],height=600,color_discrete_sequence=px.colors.qualitative.Pastel)
sunburst.update_layout(title_text='Category distribution by MarketPlace')

sell = px.pie(radar, values='created', names='product_type')
sell.update_layout(title_text='Sales distribution by category')

commands = px.scatter(price, x="date", y="order", color="price", size='price',title='Orders evolution')

#-----------------------
# Kepler map creation
#-----------------------

'''
market = product_code.groupby('marketplace')['created'].count()
market = market.reset_index()

import warnings
warnings.filterwarnings("ignore")

market['marketplace'][0] = "Spain"
market['marketplace'][1] = "United Kingdom"
market['marketplace'][2] = "France"
market['marketplace'][3] = "Germany"
market['marketplace'][4] = "Italy"

import json
# Load existing data
with open('custom.geo.json') as f:
    data = json.load(f)

feature = {}
for i in range(0,len(data['features'])):
    a = market[market['marketplace'] == data['features'][i]['properties']['sovereignt']]
    a = str(a['created'][i])
    feature['properties'] =  {'count': ''+a+''}
    data['features'][i]['properties'].update(feature['properties'])
data['features'][3]

import keplergl
w1 = keplergl.KeplerGl()

w1.add_data(data, "geojson")
'''

#-----------------------
# Function
#-----------------------
def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])



#-----------------------
# DASH Implementation
#-----------------------
app.layout = html.Div([
    html.Nav(children=[
        html.H1(html.Center('Client Dashboard')),
        html.H2(html.Center('Thibaut Bizet')),
        html.Img(src="/assets/logo.png"),
    ]),

    html.Div(className="box",children=[
        html.Div(className="dataframe",children=[
            html.H4('''Sales Distribution by Marketplace'''),
            html.Iframe(src="assets/map_vente.html", width="100%", height="600px"),
        ]),

        html.Div(className="dataframe1",children=[
            html.H4('''Sales remuneration by units : '''),
            generate_table(comparaison[['marketplace','sales/units']]),
        ]),

        html.Div(className="dataframe1",children=[
            html.H4('''Shipment price by units : '''),
            generate_table(comparaison[['marketplace','shipping/units']]),
        ]),

        html.Div(className="dataframe1",children=[
            html.H4('''FBA shipping cost per order : '''),
            html.H5(fba),
            html.H4('''MFN shipping cost per order : '''),
            html.H5(mfn),
        ]),
        
        html.Div(children=[
            dcc.Graph(figure=word_count),
            dcc.Graph(figure=shipment),
            dcc.Graph(figure=sunburst),
            dcc.Graph(figure=commands),
            dcc.Graph(figure=sell),
        ]),
    ])
])

if __name__ == '__main__':
    app.run_server(port=8050, host='0.0.0.0',debug=True)