from neo4j import GraphDatabase, RoutingControl
import os
import subprocess

URI = "bolt://localhost:7687"
AUTH = ("", "crypticpassword")
PATH = os.path.dirname(__file__)
CIM_PATH = os.path.join(PATH,"../data/raw/cim")

def search_pattern(pattern, directory):
    command = ['grep', '-rl', pattern, directory]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        if output:
            files_found = output.split('\n')
            return files_found
        else:
            return "Pattern not found in any files."
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"

def get_trafo_xml_path(mrid: str):
    return search_pattern(f"<cim:IdentifiedObject.mRID>{mrid}</cim:IdentifiedObject.mRID>", CIM_PATH)[0]

def get_shortest_paths(client: GraphDatabase.driver):
    return client.execute_query("""
    MATCH path = (:N)-[:PlacedIn]->(:ConformLoad)-[:Terminal *allShortest (r,n | 1) (r, n | ((n.IsOpen is not null AND n.IsOpen = false) XOR n.IsOpen is null))]-(c:ConnectivityNode)-[t:Terminal]->(trans:PowerTransformer) 
    WHERE t.SequenceNumber > 1 
    RETURN path
    """, database_="memgraph"
    )

def get_transformer_mrid(client: GraphDatabase.driver):
    return client.execute_query("""
    MATCH (trans:PowerTransformer) 
    RETURN trans
    """)


if __name__ == "__main__":
    with GraphDatabase.driver(URI, auth=AUTH) as client:
        client.verify_connectivity()

        records, summary, keys = get_transformer_mrid(client)
        for record in records:
            mrid = record['trans'].get('Mrid')
            xml_filepath = get_trafo_xml_path(mrid)

        records, summary, keys = get_shortest_paths(client)
        for record in records:
            print(record["path"].nodes)
            print(record["path"].relationships)



