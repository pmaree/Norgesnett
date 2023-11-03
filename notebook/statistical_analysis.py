import polars as pl
import os
import plotly.express as px

PATH = os.getcwd()
path = f"{PATH}/../data/silver/"

if __name__ == "__main__":
    topology = 'S_1262876_T_1262881'
    df = pl.read_parquet(os.path.join(path, topology))

    # group AMI's for neighborhood over {every} and solve for total of group
    every = '1h'
    df = df.sort(by=['fromTime']).group_by_dynamic('fromTime', every=every)\
        .agg(pl.col('p_load_kwh').sum().alias(f"nb_load_{every}"),
             pl.col('p_prod_kwh').sum().alias(f"nb_prod_{every}"))\
        .with_columns((pl.col('fromTime').map_elements(lambda datetime: datetime.hour)).alias('hour'))

    df_=df.group_by(by='hour').agg(pl.col(f"nb_load_{every}").mean().alias(f"nb_load_{every}_mean"),
                                   pl.col(f"nb_prod_{every}").mean().alias(f"nb_prod_{every}_mean")
                                   ).sort(by='hour')

    fig = px.line(x=df_['hour'], y=df_[f"nb_load_{every}_mean"])
    fig.add_scatter(x=df_['hour'], y=df_[f"nb_prod_{every}_mean"])

    fig.data[0].showlegend = True
    fig.data[0].name = 'load'
    fig.data[1].showlegend = True
    fig.data[1].name = 'prod'

    fig.update_yaxes(title_text='kWh/h')
    fig.update_xaxes(title_text=every)
    fig.update_layout(
        title=dict(text=f"{every} aggregated neighborhood consumer profiles for {topology}", x=0.5, y=0.95, font=dict(size=18, color='black'), xanchor='center')
    )

    fig.show()

    print('d')

    #n_bin =20
    #bins = np.linspace(0,max(df['nb_load_1h'].max(),df['nb_prod_1h'].max()), n_bin)
    #hist, bin_edges = np.histogram(df.filter(pl.col('hour')==0).select(f"nb_load_{every}").to_series(),
    #                                  bins = np.linspace(0,max(df['nb_load_1h'].max(),df['nb_prod_1h'].max()), n_bin))

