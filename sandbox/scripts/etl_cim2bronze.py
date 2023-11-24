from datetime import datetime
from lxml import etree
import polars as pl
import os

from lib import Logging

log = Logging()

PATH = os.path.dirname(__file__)


def parse_tag(root, tag:str)->dict:
    results = []
    for index, element in enumerate(root.findall(tag, root.nsmap)):
        content = {}
        for child in element.iterchildren():
            if child.text is not  None:
                content[child.tag.split('.')[-1]] = child.text
        results.append(content)
    return results


def parse_transformer(path: str) -> pl.DataFrame:

    tree = etree.parse(path)
    root = tree.getroot()

    tf = {}
    for element in parse_tag(root, 'cim:PowerTransformer'):
        tf['topology'] = path.split('/')[-1].split('.')[0]
        tf['mrid'] = element['mRID']
        tf['name'] = element['name']
        tf['faceplate'] = element['operationlabel']
    for element in parse_tag(root, 'cim:PowerTransformerEnd'):
        winding  = 'primary' if (element['endNumber'] == '1') else 'secondary'
        tf[f'{winding}_rated_voltage'] = element['ratedU']
        tf[f'{winding}_rated_apparent_power'] = element['ratedS']
    return pl.DataFrame(tf)


src_path = PATH + f"/../../data/raw/cim/"
dst_path = PATH + f"/../../data/bronze/transformers/"

file_list = os.listdir(src_path)
df_tf = pl.DataFrame()
unparsed_tf = []
for index, file_name in enumerate(file_list):
    print(f'[{index}] Parse {file_name} CIM file')
    try:
        df_pl = parse_transformer(path=os.path.join(src_path, file_name))
        df_tf = df_pl if df_tf.is_empty() else df_tf.vstack(df_pl)
    except Exception as e:
        unparsed_tf.append(file_name.split('.xml')[0])
        log.exception(msg=f'[{index}] Cannot parse {file_name} CIM file')

dst_file_path = os.path.join(dst_path, "stations")
df_tf.write_parquet(dst_file_path)

if len(unparsed_tf):
    log.warning(f"Unparsed {len(unparsed_tf)} transformers for the following topologies: {unparsed_tf}")
log.info(f"Parsed {index} topologies and saved results at {dst_file_path}")
