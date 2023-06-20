import plotly.graph_objects as go
import json

import requests

# url = 'https://raw.githubusercontent.com/plotly/plotly.js/master/test/image/mocks/sankey_energy.json'
# response = requests.get(url)
# data = response.json()
# json.dump(data, open("data.json", "w"))
data = json.load(open("data.json", "r"))

# override gray link colors with 'source' colors
opacity = 0.4
# change 'magenta' to its 'rgba' value to add opacity
data['data'][0]['node']['color'] = ['rgba(255,0,255, 0.8)' if color == "magenta" else color for color in
                                    data['data'][0]['node']['color']]
data['data'][0]['link']['color'] = [data['data'][0]['node']['color'][src].replace("0.8", str(opacity))
                                    for src in data['data'][0]['link']['source']]

fig = go.Figure(data=[go.Sankey(
    valueformat=".0f",
    valuesuffix="TWh",
    # Define nodes
    node=dict(
        pad=15,
        thickness=15,
        line=dict(color="black", width=0.5),
        label=data['data'][0]['node']['label'],
        color=data['data'][0]['node']['color']
    ),
    # Add links
    link=dict(**data['data'][0]['link']))])

fig.update_layout(
    title_text="Energy forecast for 2050<br>Source: Department of Energy & Climate Change, "
               "Tom Counsell via <a href='https://bost.ocks.org/mike/sankey/'>Mike Bostock</a>",
    font_size=10)
# fig.show()
fig.write_image("sankey.png", width=1800, height=1600)
