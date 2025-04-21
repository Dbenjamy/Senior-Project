import plotly.graph_objs as go
import pandas as pd

def create_animated_3d_plot(df):
    # Organize gravity magnitudes by location and datetime
    location_dict = {}
    for _, row in df.iterrows():
        loc = (row['X'], row['Y'], row['Z'])
        if loc not in location_dict:
            location_dict[loc] = {}
        location_dict[loc][row['datetime']] = row['grav_mag']

    locations = list(location_dict.keys())
    datetimes = sorted(df['datetime'].unique())

    # Set up base scatter
    initial_colors = [location_dict[loc][datetimes[0]] for loc in locations]
    scatter = go.Scatter3d(
        x=[loc[0] for loc in locations],
        y=[loc[1] for loc in locations],
        z=[loc[2] for loc in locations],
        mode='markers',
        marker=dict(
            size=5,
            color=initial_colors,
            colorscale='Viridis',
            colorbar=dict(title='Magnitude of Gravity'),
            opacity=1.0
        )
    )

    # Create frames
    frames = []
    for time in datetimes:
        frame_colors = [location_dict[loc][time] for loc in locations]
        frame = go.Frame(
            data=[go.Scatter3d(marker=dict(color=frame_colors))],
            name=str(time)
        )
        frames.append(frame)

    # Layout with sliders and animation buttons
    fig = go.Figure(
        data=[scatter],
        layout=go.Layout(
            title="3D Gravity Magnitude Over Time",
            sliders=[{
                'steps': [{
                    'args': [[str(time)], {'frame': {'duration': 400, 'redraw': True},
                                            'mode': 'immediate'}],
                    'label': str(time),
                    'method': 'animate'
                } for time in datetimes],
                'transition': {'duration': 0},
                'x': 0,
                'y': 0,
                'currentvalue': {'prefix': 'Time: '}
            }],
            updatemenus=[{
                'type': 'buttons',
                'direction': 'left',
                'x': 0.1,
                'y': 1.05,
                'xanchor': 'left',
                'yanchor': 'top',
                'pad': {'t': 0, 'r': 10},
                'buttons': [
                    {
                        'label': 'Play',
                        'method': 'animate',
                        'args': [None, {
                            'frame': {'duration': 200, 'redraw': True},
                            'fromcurrent': True,
                            'transition': {'duration': 0},
                            'mode': 'immediate'
                        }]
                    },
                    {
                        'label': 'Pause',
                        'method': 'animate',
                        'args': [[None], {
                            'frame': {'duration': 0, 'redraw': False},
                            'mode': 'immediate',
                            'transition': {'duration': 0}
                        }]
                    }
                ]
            }]

        ),
        frames=frames
    )
    fig.show()
