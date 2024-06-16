import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession\
        .builder\
        .appName("RedditSentimentAnalysis")\
        .getOrCreate()

# Application Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("SentimentStream"),
    dcc.Graph(id='sentiment-pie-chart'),
    dcc.Interval(
        id='interval-component',
        interval=600*1000,  # Met à jour toutes les 10 minutes
        n_intervals=0
    )
])

@app.callback(Output('sentiment-pie-chart', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    # Lire les données à partir du fichier parquet
    df = spark.read.parquet("/opt/bitnami/spark/work/sentiment_data")
    # Récupérer les nouvelles données de Spark SQL
    a = df.groupBy("sentiment").count().toPandas()
    # Créer le diagramme en cercle
    fig = px.pie(a, values='count', names='sentiment', title="Sentiment Distribution")
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')