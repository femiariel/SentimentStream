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

# Définir la mise en page de l'application
app.layout = html.Div([
    html.H1("SentimentStream"),  # Titre de l'application
    dcc.Graph(id='sentiment-pie-chart'),  # Graphique pour afficher le diagramme en cercle
    dcc.Interval(
        id='interval-component',
        interval=600*1000,  # Met à jour toutes les 10 minutes
        n_intervals=0  # Nombre d'intervalles initialisé à 0
    )
])

# Callback pour mettre à jour le graphique en fonction de l'intervalle
@app.callback(Output('sentiment-pie-chart', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    # Lire les données à partir du fichier parquet
    df = spark.read.parquet("/opt/bitnami/spark/work/sentiment_data")
    # Agréger les données par sentiment et compter le nombre d'occurrences
    a = df.groupBy("sentiment").count().toPandas()
    # Créer le diagramme en cercle
    fig = px.pie(a, values='count', names='sentiment', title="Sentiment Distribution")
    return fig  # Retourner la figure mise à jour

# Exécuter l'application
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
