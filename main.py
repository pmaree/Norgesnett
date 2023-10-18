import os
from flask import Flask, request, render_template
import polars as pl

from lib import Logging
import plotly.express as px

from lib.etl import etl_bronze_to_silver
from lib.preprocessing import prepare_features

log = Logging()

PATH = os.path.dirname(__file__)

time_format = '%Y-%m-%dT%H:%M:%S'

app = Flask(__name__,template_folder='template')

@app.route('/features')
def sort_features():
    path = PATH+f"/data/silver/"

    descending = request.args.get('descending', default=0, type=int)
    sort_by = request.args.get('sort_by', default='fromTime', type=str)
    show_n = request.args.get('show_n', default=10, type=int)

    df = pl.read_parquet(os.path.join(path,'features'))
    if sort_by is not None and sort_by in df.columns:
        return  df.sort(by=sort_by, descending=bool(descending)).head(show_n).to_pandas().to_html()
    return df.head(show_n).to_pandas().to_html()


def set_px(fig_cnt:int=1):
    px.defaults.width = 1920*0.95
    px.defaults.height = 1080*0.9/fig_cnt


# plot the raw data for visual inspection
# http://0.0.0.0:8080/plot/raw?topology=S_1364278_T_1364283&ami=707057500075560028
@app.route('/plot/raw')
def plot_raw_ami():
    path = PATH+f"/data/bronze/measurements"

    topology = request.args.get('topology', default='', type=str)
    ami = request.args.get('ami', default='', type=str)

    for file_name in os.listdir(path):
        if topology in file_name.split(sep='_2023')[0]:
            df = pl.read_parquet(os.path.join(path, file_name)).with_columns(topology = pl.lit(topology))
            break
    if ami == '':
        return df.unique(subset='meteringPointId').select('meteringPointId').to_pandas().to_html()
    else:

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
        fig1.update_yaxes(title_text='kWh')
        fig1.update_xaxes(title_text='time')
        fig1.update_layout(
            title=dict(text=f"Time series for AMI={ami}, Topology={topology}", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
        )

        # Render the plots
        plot_div1 = fig1.to_html(full_html=False)

        return render_template('raw.html', plot_div1=plot_div1)

# Plot the processed time series for aggegrgaed and average production and consumption for a neighborhood
# http://0.0.0.0:8080/plot/processed?topology=S_1364278_T_1364283&every=24h
@app.route('/plot/processed')
def plot_processed():
    silver_path = PATH+f"/data/silver/"

    topology = request.args.get('topology', default='', type=str)
    every = request.args.get('every', default='1h', type=str)
    date_from = request.args.get('date_from', default='2023-03-01T00:00:00', type=str)
    date_to =  request.args.get('date_to', default='2023-09-01T00:00:00', type=str)

    # initial raw read and pre-filter of measurents in appropriate range
    if os.path.isfile(os.path.join(silver_path, topology)):
        df = pl.read_parquet(os.path.join(silver_path, topology))
    else:
        df = etl_bronze_to_silver(topology,date_from=date_from,date_to=date_to)

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

    fig1.update_yaxes(title_text='kWh')
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

    fig2.update_yaxes(title_text='kWh')
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


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, threaded=True, debug=True)
    #prepare_features(date_from='2023-03-01T00:00:00', date_to='2023-09-01T01:00:00')

