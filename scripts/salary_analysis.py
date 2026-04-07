from dash import Dash, dcc, callback, Output, Input
import pandas as pd
import plotly.express as px
import dash_mantine_components as dmc
import dash_ag_grid as dag

EXPERIENCE_LEVELS = ["0 an", "2 ans", "5 ans", "10 ans"]
HEADER = ["Métier", "Secteur", EXPERIENCE_LEVELS[0], EXPERIENCE_LEVELS[1], EXPERIENCE_LEVELS[2], EXPERIENCE_LEVELS[3]]

SALARY_DATA = [
    ("Data/AI Engineer", "IT / Conseil", 2800, 3300, 4000, 5250),
    ("Data/AI Engineer", "Industrie", 2600, 3000, 3700, 4750),
    ("Data/AI Engineer", "Public/Parapublic", 2400, 2700, 3100, 3950),
    ("Data Scientist", "IT / Conseil", 2600, 3200, 4000, 5150),
    ("Data Scientist", "Industrie", 2500, 3000, 3700, 4450),
    ("Data Scientist", "Public/Parapublic", 2300, 2600, 3000, 3750),
    ("Technical Sales / Avant-vente AI", "IT / Conseil", 2500, 3200, 4200, 5750),
    ("Technical Sales / Avant-vente AI", "Industrie", 2400, 3000, 3800, 5000),
    ("Data/AI Analyst", "IT / Conseil", 2600, 3000, 3700, 4750),
    ("Data/AI Analyst", "Industrie", 2400, 2800, 3400, 4250),
    ("Data/AI Analyst", "Public/Parapublic", 2200, 2500, 2900, 3600),
]

# DataFrame
df = pd.DataFrame(SALARY_DATA, columns=HEADER)

# Dataframe adapté
df_long = df.melt(
    id_vars=["Métier", "Secteur"],
    value_vars=EXPERIENCE_LEVELS,
    var_name="Expérience",
    value_name="Salaire"
)

app = Dash(__name__)

app.layout = dmc.MantineProvider(
    dmc.Container([
        dmc.Title("TECH SALARY ANALYSIS", c="blue", order=3, mt="md", mb="md"),
        dmc.RadioGroup(
                dmc.Group([dmc.Radio(label=secteur, value=secteur) for secteur in df['Secteur'].unique()]),
            id='secteur-radio',
            value='IT / Conseil',
            mb="md"
        ),

        # [IA debug]
        dag.AgGrid(
            id='data-grid',
            rowData=df.to_dict("records"),
            columnDefs=[{"field": i} for i in df.columns],
            style={"height": 200, "marginBottom": 20}
        ),

        dmc.SimpleGrid([
            dcc.Graph(id='bar-chart'),
            dcc.Graph(id='line-chart')
        ], cols={"base": 1, "md": 2})

    ], fluid=True)
)


@callback(
    Output('data-grid', 'rowData'),
    Output('bar-chart', 'figure'),
    Output('line-chart', 'figure'),
    Input('secteur-radio', 'value')
)
def update_dashboard(selected_secteur):
    # Filtrage [IA debug]
    dff_wide = df[df['Secteur'] == selected_secteur]
    dff_long = df_long[df_long['Secteur'] == selected_secteur]

    # Tableau
    row_data = dff_wide.to_dict("records")

    # Graphique 1
    fig_bar = px.bar(
        dff_long,
        x="Métier",
        y="Salaire",
        color="Expérience",
        barmode="group",
        title=f"Comparaison des Salaires - {selected_secteur}"
    )

    # Graphique 2
    fig_line = px.line(
        dff_long,
        x="Expérience",
        y="Salaire",
        color="Métier",
        markers=True,
        title=f"Évolution des Salaires dans le temps - {selected_secteur}"
    )

    return row_data, fig_bar, fig_line


if __name__ == '__main__':
    app.run(debug=True)