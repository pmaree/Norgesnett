from flask import Flask, request, render_template
import polars as pl
import os

from lib import Logging

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

log = Logging()

PATH = os.path.dirname(__file__)

time_format = '%Y-%m-%dT%H:%M:%S'

app = Flask(__name__,template_folder='template')

def set_px(fig_cnt:int=1):
    px.defaults.width = 1920*0.95
    px.defaults.height = 1080*0.9/fig_cnt

def get_topology_trafo_sec_kva(topology: str) -> float:
    return pl.read_parquet(os.path.join(PATH,f"data/silver/features/load")).filter(pl.col('topology_name')==topology).select('sec_kva').item()

# http://0.0.0.0:9000/features?sort_by=trafo_utilization&descending=1&show_n=100&ami_cnt=10
@app.route('/features')
def sort_features():
    path = PATH+f"/data/silver/features"

    descending = request.args.get('descending', default=0, type=int)
    sort_by = request.args.get('sort_by', default='fromTime', type=str)
    show_n = request.args.get('show_n', default=10, type=int)
    ami_cnt = request.args.get('ami_cnt', default=1, type=int)

    df = pl.read_parquet(os.path.join(path,'load'))

    # Some additional features of interest
    df = df.filter(pl.col('ami_cnt')>ami_cnt).with_columns((pl.col('nb_day_peak_max')/pl.col('sec_kva')*100).alias('trafo_utilization'))

    if sort_by is not None and sort_by in df.columns:
        return df.sort(by=sort_by, descending=bool(descending)).head(show_n).to_pandas().to_html()
    return df.head(show_n).to_pandas().to_html()

# Plot the processed time series for aggegrgaed and average production and consumption for a neighborhood
# http://0.0.0.0:9000/plot/processed?topology=S_502461_T_501524&ami=707057500075217045&every=1h
@app.route('/plot/ami')
def plot_ami():
    silver_path = PATH+f"/data/silver/measurements"

    topology = request.args.get('topology', type=str)
    ami = request.args.get('ami',default='None', type=str)
    every = request.args.get('every', default='1h', type=str)

    df = pl.read_parquet(os.path.join(silver_path, topology))

    if ami == '':
        return df.unique(subset=['meteringPointId']).select('meteringPointId').sort(by='meteringPointId').to_pandas().to_html()
    else:
        df = df.filter(pl.col('meteringPointId') == ami).sort('fromTime').group_by_dynamic('fromTime', every=every).agg(
            pl.col('p_load_kwh').sum().alias('p_load_sum_kwh')).select(['fromTime', 'p_load_sum_kwh']).sort(by='fromTime').to_pandas()

    set_px(1)

    # plot consumption
    fig1 = px.line(x=df['fromTime'], y=df['p_load_sum_kwh'])
    fig1.data[0].showlegend = True
    fig1.data[0].name = 'Consumption'
    fig1.update_yaxes(title_text='kWh/h')
    fig1.update_xaxes(title_text='time')
    fig1.update_layout(
        title=dict(text=f"Neighborhood={topology} time series for AMI={ami} (aggregated={every})", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
    )

    # Render the plots
    plot_div1 = fig1.to_html(full_html=False)

    return render_template('raw.html', plot_div1=plot_div1)

@app.route('/plot/diversity')
# http://0.0.0.0:9000/plot/diversity?topology=S_502461_T_501524
def plot_diversity():
    silver_path = PATH+f"/data/silver/measurements"

    topology = request.args.get('topology', type=str)

    df = pl.read_parquet(os.path.join(silver_path, topology))

    # get neighborhood hourly peak loads
    df_nb  = df.sort('fromTime').group_by_dynamic('fromTime', every='1h').agg(pl.col('p_load_kwh').sum().alias('nb_hourly_peak_load'))

    # get the maximum neighborhood peak load for the day
    df_nb = df_nb.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1d').agg(pl.col('nb_hourly_peak_load').max().alias('nb_daily_max_peak_load'))

    # get the daily peaks of each AMI
    df_ami = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1d', by=['meteringPointId']).agg(pl.col('p_load_kwh').max().alias('ami_daily_peak_load'))

    # sum the peaks for the AMI's over the days
    df_ami = df_ami.sort(by=['fromTime']).group_by_dynamic('fromTime', every='1d').agg(pl.col('ami_daily_peak_load').sum().alias('ami_daily_sum_peak_load'))

    # plot daily sum of AMI peaks, max of neighborhood peak, diversity factor
    set_px(3)

    # plot sum of AMI peaks taken for daily intervals
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])

    fig1.add_trace(go.Scatter(x=df_ami['fromTime'], y=df_ami['ami_daily_sum_peak_load'], name=f"AMI Sum", line=dict(color="#FFC000")),secondary_y=False)
    fig1.add_trace(go.Scatter(x=df_nb['fromTime'], y=df_nb['nb_daily_max_peak_load'], name=f"NB Agg", line=dict(color="#006400")),secondary_y=True)

    fig1.update_xaxes(title_text='time')
    fig1.update_layout(title=dict(text=f"Peak load analysis", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center'))
    fig1.update_yaxes(title_text="AMI Sum kWh", secondary_y=False, color="#FFC000")
    fig1.update_yaxes(title_text="NB Peak kWh", secondary_y=True, color="#006400")

    # plot neighborhood peak load for period intervals of 1 day
    trafo_kva_rating = get_topology_trafo_sec_kva(topology=topology)
    df_trafo = df_nb.with_columns((pl.col('nb_daily_max_peak_load')/trafo_kva_rating*100).alias('trafo_utilization'))

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])

    fig2.add_trace(go.Scatter(x=df_trafo['fromTime'], y=df_trafo['trafo_utilization'], name=f"TF Util. [%]", line=dict(color="#FFC000")),secondary_y=False)
    fig2.add_trace(go.Scatter(x=df_nb['fromTime'], y=df_nb['nb_daily_max_peak_load'], name=f"NB Peak kWh", line=dict(color="#006400", dash='dash')),secondary_y=True)

    fig2.update_xaxes(title_text='time')
    fig2.update_layout(title=dict(text=f"Trafo utilization for daily maximum peaks", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center'))
    fig2.update_yaxes(title_text="Util.", secondary_y=False, color="#FFC000")
    fig2.update_yaxes(title_text="Peak", secondary_y=True, color="#006400")

    # plot the diversity factor for neighborhood
    df_factors = (df_ami.join(df_nb, on='fromTime', validate='1:1').with_columns((pl.col('nb_daily_max_peak_load')/pl.col('ami_daily_sum_peak_load')*100).alias('coincidence_factor')))\
        .with_columns((pl.col('ami_daily_sum_peak_load')/pl.col('nb_daily_max_peak_load')).alias('diversity_factor'))

    fig3 = make_subplots(specs=[[{"secondary_y": True}]])
    fig3.add_trace(go.Scatter(x=df_factors['fromTime'], y=df_factors['coincidence_factor'], name=f"CF [%]", line=dict(color="#FFC000")),secondary_y=False)
    fig3.add_trace(go.Scatter(x=df_factors['fromTime'], y=df_factors['diversity_factor'], name=f"DF", line=dict(color="#006400")),secondary_y=True)

    fig3.update_xaxes(title_text='time')
    fig3.update_layout(title=dict(text=f"Load factors for neighborhood", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center'))
    fig3.update_yaxes(title_text="CF", secondary_y=False, color="#FFC000")
    fig3.update_yaxes(title_text="DF", secondary_y=True, color="#006400")

    return render_template('diversity.html', plot_div1=fig1.to_html(full_html=False), plot_div2=fig2.to_html(full_html=False), plot_div3=fig3.to_html(full_html=False))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000, debug=True)