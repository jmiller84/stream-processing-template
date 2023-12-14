import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

# Sample monster data - replace this with your data
data = {
    "country": ["USA", "Canada", "UK"],
    "monster_name": ["Dragon", "Goblin", "Troll"],
    "damage": [10000, 5000, 15000]
}
df = pd.DataFrame(data)

# Create the Dash app
app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div([
    html.H1("Monster Attacks"),
    dcc.Graph(
        figure=px.bar(df, x='country', y='damage', color='monster_name', title='Monster Damage by Country')
    )
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, port=7777, host="0.0.0.0")
    
