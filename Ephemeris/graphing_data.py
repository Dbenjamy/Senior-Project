import plotly.graph_objs as go

def create_3d_plot(df):

    # Reset index to make it easier to work with
    df_reset = df.reset_index()

    # Create a dictionary to store grav_mag values by location and time
    location_dict = {}
    for _, row in df_reset.iterrows():
        loc = (row['X'], row['Y'], row['Z'])
        if loc not in location_dict:
            location_dict[loc] = {}
        location_dict[loc][row['datetime']] = row['grav_mag']
    
    fig = go.Figure()

    # Get unique locations and initial colors
    locations = list(location_dict.keys())
    initial_colors = [location_dict[loc][df_reset['datetime'].unique()[0]] for loc in locations]

    scatter = go.Scatter3d(
        x=[loc[0] for loc in locations],
        y=[loc[1] for loc in locations],
        z=[loc[2] for loc in locations],
        mode='markers',
        marker=dict(
            size=5,
            color=initial_colors,
            colorscale='Viridis',
            colorbar=dict(title='Grav Mag'),
            opacity=1.0
        )
    )

    fig.add_trace(scatter)

    # Add slider steps
    steps = []
    for time in df_reset['datetime'].unique():
        step_colors = [location_dict[loc][time] for loc in locations]
        step = dict(
            method='restyle',
            args=[{'marker.color': [step_colors]}],
            label=str(time)
        )
        steps.append(step)

    sliders = [dict(
        active=0,
        currentvalue={"prefix": "Time: "},
        pad={"t": 50},
        steps=steps
    )]

    fig.update_layout(sliders=sliders)
    fig.show()