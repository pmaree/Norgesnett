import numpy as np
from flask import Flask, request, render_template, redirect
import polars as pl
import pandas as pd
import os

from lib import Logging

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from lib.etl import etl_bronze_to_silver

log = Logging()

PATH = os.path.dirname(__file__)

time_format = '%Y-%m-%dT%H:%M:%S'

app = Flask(__name__,template_folder='template')


# http://0.0.0.0:9000/features?sort_by=ami_prod_cnt&descending=1&show_n=100
@app.route('/features')
def sort_features():
    path = PATH+f"/data/bronze/features"

    descending = request.args.get('descending', default=0, type=int)
    sort_by = request.args.get('sort_by', default='fromTime', type=str)
    show_n = request.args.get('show_n', default=10, type=int)

    df = pl.read_parquet(os.path.join(path,'production'))
    if sort_by is not None and sort_by in df.columns:
        return df.sort(by=sort_by, descending=bool(descending)).head(show_n).to_pandas().to_html()
    return df.head(show_n).to_pandas().to_html()


def set_px(fig_cnt:int=1):
    px.defaults.width = 1920*0.95
    px.defaults.height = 1080*0.9/fig_cnt


# plot the raw data for visual inspection
# http://0.0.0.0:9000/plot/raw?topology=S_1262876_T_1262881&ami=707057500080789520
@app.route('/plot/raw')
def plot_raw_ami():
    path = PATH+f"/data/bronze/measurements"

    topology = request.args.get('topology', type=str)
    ami = request.args.get('ami', type=str)

    if topology in ['',None]:
        return redirect('/features?sort_by=ami_prod_cnt&descending=1&show_n=200')

    for file_name in os.listdir(path):
        if topology in file_name.split(sep='_2023')[0]:
            df = pl.read_parquet(os.path.join(path, file_name)).with_columns(topology = pl.lit(topology))
            break

    if ami in ['',None]:
        return df.unique(subset=['meteringPointId','type']).select('meteringPointId','type').sort(by='meteringPointId').to_pandas().to_html()

    df = df.filter(pl.col('meteringPointId') == ami)

    df_p_load = df.filter(pl.col('type')==1).select(['fromTime','value','unit']).sort(by='fromTime').to_pandas()
    df_p_prod = df.filter(pl.col('type')==3).select(['fromTime','value','unit']).sort(by='fromTime').to_pandas()

    set_px(1)

    # plot consumption
    fig1 = px.line(x=df_p_load['fromTime'], y=-df_p_load['value'])
    fig1.data[0].showlegend = True
    fig1.data[0].name = 'Consumption'
    if df_p_prod.shape[0]:
        fig1.add_scatter(x=df_p_prod['fromTime'], y=df_p_prod['value'])
        fig1.data[1].showlegend = True
        fig1.data[1].name = 'Production'
    fig1.update_yaxes(title_text='kWh/h')
    fig1.update_xaxes(title_text='time')
    fig1.update_layout(
        title=dict(text=f"Time series for AMI={ami}, Topology={topology}", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
    )

    # Render the plots
    plot_div1 = fig1.to_html(full_html=False)

    return render_template('raw.html', plot_div1=plot_div1)

# Plot the processed time series for aggegrgaed and average production and consumption for a neighborhood
# http://0.0.0.0:9000/plot/processed?topology=S_1262876_T_1262881&every=1h
@app.route('/plot/processed')
def plot_processed():
    silver_path = PATH+f"/data/silver/"

    topology = request.args.get('topology', type=str)
    ami = request.args.get('ami', type=str)

    every = request.args.get('every', default='1h', type=str)
    date_from = request.args.get('date_from', default='2023-03-01T00:00:00', type=str)
    date_to =  request.args.get('date_to', default='2023-09-01T00:00:00', type=str)

    # initial raw read and pre-filter of measurents in appropriate range
    if os.path.isfile(os.path.join(silver_path, topology)):
        df = pl.read_parquet(os.path.join(silver_path, topology))
    else:
        df = etl_bronze_to_silver(topology,date_from=date_from,date_to=date_to)

    unique_ami = df.unique(subset='meteringPointId').select(pl.col('meteringPointId'))
    if ami is not None:
        if ami not in unique_ami.to_series().to_list():
            return unique_ami.to_pandas().to_html()
        else:
            df = df.filter(pl.col('meteringPointId')==ami)


    # total production and consumption over cluster of prosumers grouped by hourly timestamp
    ami_cnt = df.n_unique(subset='meteringPointId')

    # calculate aggregated and average of columns
    df = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every=every).agg(
        pl.col('p_load_kwh').sum().alias('p_load_sum_kwh'),
        (pl.col('p_load_kwh').sum()/ami_cnt).alias('p_load_avg_kwh'),
        pl.col('p_prod_kwh').sum().alias('p_prod_sum_kwh'),
        (pl.col('p_prod_kwh').sum()/ami_cnt).alias('p_prod_avg_kwh'),
        (pl.col('p_prod_kwh')-pl.col('p_load_kwh')).sum().alias('p_export_sum_kwh'),
        ((pl.col('p_prod_kwh')-pl.col('p_load_kwh')).sum()/ami_cnt).alias('p_export_avg_kwh')
    )

    set_px(2)

    # plot aggregated neighborhood profiles
    fig1 = px.line(x=df['fromTime'], y=-df['p_load_sum_kwh'])
    fig1.add_scatter(x=df['fromTime'], y=df['p_prod_sum_kwh'])
    fig1.add_scatter(x=df['fromTime'], y=df['p_export_sum_kwh'])

    fig1.update_yaxes(title_text='kWh/{every}')
    fig1.update_xaxes(title_text='time')
    fig1.update_layout(
        title=dict(text=f"Aggregated profiles for topology {topology} (aggregation period={every})", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
    )

    # Add legend items
    fig1.data[0].showlegend = True
    fig1.data[0].name = 'Consumption'
    fig1.data[1].showlegend = True
    fig1.data[1].name = 'Production'
    fig1.data[2].showlegend = True
    fig1.data[2].name = 'Grid Export'

    # Averaged profiles for topology
    fig2 = px.line(x=df['fromTime'], y=-df['p_load_avg_kwh'])
    fig2.add_scatter(x=df['fromTime'], y=df['p_prod_avg_kwh'])
    fig2.add_scatter(x=df['fromTime'], y=df['p_export_avg_kwh'])

    fig2.update_yaxes(title_text='kWh/{every}')
    fig2.update_xaxes(title_text='time')
    fig2.update_layout(
        title=dict(text=f"Averaged profiles for topology {topology} (averaged over {ami_cnt} AMI's)", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
    )

    # Add legend items
    fig2.data[0].showlegend = True
    fig2.data[0].name = 'Consumption'
    fig2.data[1].showlegend = True
    fig2.data[1].name = 'Production'
    fig2.data[2].showlegend = True
    fig2.data[2].name = 'Grid Export'

    # Render the plots
    plot_div1 = fig1.to_html(full_html=False)
    plot_div2 = fig2.to_html(full_html=False)

    return render_template('processed.html', plot_div1=plot_div1, plot_div2=plot_div2)

# Plot the processed time series for aggegrgaed and average production and consumption for a neighborhood
# http://0.0.0.0:9000/plot/duckcurve?topology=S_1262876_T_1262881
@app.route('/plot/duckcurve')
def plot_duckcurve():

    path = PATH+f"/data/silver/"

    topology = request.args.get('topology', type=str)
    if topology in ['', None]:
        return redirect('/features?sort_by=ami_prod_cnt&descending=1&show_n=200')

    df = pl.read_parquet(os.path.join(path, topology))
    load_cnt = df.filter(pl.col('p_load_kwh')>0).n_unique('meteringPointId')
    prod_cnt = df.filter(pl.col('p_prod_kwh')>0).n_unique('meteringPointId')

    ami_cnt = df.n_unique('meteringPointId')
    # group AMI's for neighborhood over {every} and solve for total of group
    every = '1h'
    df = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every=every) \
        .agg(pl.col('p_load_kwh').sum().alias('nb_load'),
             pl.col('p_prod_kwh').sum().alias('nb_prod')) \
        .with_columns((pl.col('fromTime').map_elements(lambda datetime: datetime.hour)).alias('hour'))

    df_=df.group_by(by='hour').agg(pl.col('nb_load').max().alias('nb_load_max'),
                                   pl.col('nb_load').mean().alias('nb_load_avg'),
                                   pl.col('nb_prod').max().alias('nb_prod_max')
                                   ).sort(by='hour')

    df_=df_.with_columns([(pl.col('nb_load_max')-pl.col('nb_prod_max')).alias('nb_duck_max'),
                          ((pl.col('nb_load_max')-pl.col('nb_prod_max'))/pl.col('nb_load_avg')*100).alias('nb_nduck_max')])

    # Create figure with secondary y-axis
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces

    fig1.add_trace(
        go.Scatter(x=df_['hour'], y=df_['nb_load_max'], name=f"Load (#AMI={load_cnt})", line=dict(color="#FFC000")),
        secondary_y=False,
    )

    fig1.add_trace(
        go.Scatter(x=df_['hour'], y=df_['nb_prod_max'], name=f"Prod (#AMI={prod_cnt})", line=dict(color="#006400")),
        secondary_y=True,
    )


    # Add figure title
    fig1.update_layout(
        title=dict(text=f"Hourly aggregated profiles for <{topology}>", xanchor='left'),
        width=1920*.9,
        height=1080*.45,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="#000000"
        )
    )

    # Set x-axis title
    fig1.update_xaxes(title_text="time [h]")

    # Set y-axes titles
    fig1.update_yaxes(title_text="Avg. Nhbd. Load kWh/h", secondary_y=False, color="#FFC000")
    fig1.update_yaxes(title_text="Avg. Nhbd. Prod kWh/h", secondary_y=True, color="#006400")

    #
    # Plot duck curve net and normalize
    #
    fig2 = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces

    fig2.add_trace(
        go.Scatter(x=df_['hour'], y=df_['nb_duck_max'], name=f"Duck", line=dict(color="#00008B")),
        secondary_y=False,
    )

    fig2.add_trace(
        go.Scatter(x=df_['hour'], y=df_['nb_nduck_max'], name=f"Norm. Duck (Avg. Load)", line=dict(color="#D3D3D3")),
        secondary_y=True,
    )

    # Add figure title
    fig2.update_layout(
        title=dict(text=f"Hourly aggregated profiles for <{topology}>", xanchor='left'),
        width=1920*.9,
        height=1080*.45,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="#000000"
        )
    )

    # Set x-axis title
    fig2.update_xaxes(title_text="time [h]")

    # Set y-axes titles
    fig2.update_yaxes(title_text="Net kWh/h", secondary_y=False, color="#000000")
    fig2.update_yaxes(title_text="Norm. %", secondary_y=True, color="#D3D3D3")

    return render_template('processed.html', plot_div1=fig1.to_html(full_html=False), plot_div2=fig2.to_html(full_html=False))

# pre-screening neighborhood selection
# http://0.0.0.0:9000/plot/prescreening
@app.route('/plot/prescreening')
def prescreening():
    path = PATH+f"/data/silver/"

    nb_aggmaxp_val = request.args.get('nb_aggmaxp_val', default=1,  type=float)

    df_pl = pl.read_parquet(os.path.join(path,'features'))
    prod_cnt_list = np.linspace(1,
                                df_pl.select(pl.col('ami_prod_cnt').max()).item(),
                                df_pl.select(pl.col('ami_prod_cnt').max()).item())

    df_pd = pd.DataFrame(columns=['prod_ami_cnt','#Neighborhoods'])

    for prod_ami_cnt in prod_cnt_list:
            topology_cnt = df_pl.filter(pl.col('ami_prod_cnt')>=prod_ami_cnt).filter(pl.col('nb_aggmaxp_val')>=nb_aggmaxp_val).n_unique('topology')
            df_pd.loc[len(df_pd.index)] = [prod_ami_cnt,topology_cnt]

    set_px(1)

    # plot consumption
    fig = px.bar(df_pd, x='prod_ami_cnt', y='#Neighborhoods',color='#Neighborhoods')

    fig.update_layout(
        )

    # Add figure title
    fig.update_yaxes(type="log", row=1, col=1)

    fig.update_layout(
        title=f"#Neighborhood with #Plusskunder exceeding {nb_aggmaxp_val} kWh/h hourly aggregated production",
        yaxis_title=' Number of Neighborhoods (log scale)',
        xaxis_title=r'Number of Plusskunder',
        width=1920*.9,
        height=1080*.85,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="#000000"
        )
    )

    # Render the plots
    plot_div = fig.to_html(full_html=False)

    return render_template('raw.html', plot_div1=plot_div)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000, debug=True)
    

