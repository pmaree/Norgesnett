import polars as pl
import os
import plotly.express as px

PATH = os.getcwd()
path = f"{PATH}/../../data/silver/"

if __name__ == "__main__":
    topology = 'S_1262876_T_1262881'
    df = pl.read_parquet(os.path.join(path, topology))

    df = df.with_columns((pl.col('fromTime').map_elements(lambda datetime: datetime.hour)).alias('hour'))

    meter_id_list = df.unique(subset='meteringPointId').select('meteringPointId').to_series().to_list()

    for index, meter_id in enumerate(meter_id_list):
        df_ = df.filter(pl.col('meteringPointId') == meter_id)

        df_=df_.group_by(by='hour').agg(pl.col('p_load_kwh').mean().alias('p_load_max_kwh'),
                                       pl.col('p_prod_kwh').mean().alias('p_prod_max_kwh')
                                       ).sort(by='hour')
        df_=df_.with_columns((pl.col('p_load_max_kwh')-pl.col('p_prod_max_kwh')).alias('p_net_max_kwh'))

        if index==0:
            fig = px.line(x=df_['hour'], y=df_['p_net_max_kwh'])
        else:
            fig.add_scatter(x=df_['hour'], y=df_['p_net_max_kwh'])
        fig.data[index].showlegend = True
        fig.data[index].name =  f"[{index}] {meter_id}"

    fig.show()


