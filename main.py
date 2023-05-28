import csv
from pyorient import OrientDB


def save_to_orientdb():
    client = OrientDB("localhost", 2424)
    client.set_session_token(True)
    session_id = client.connect("root", "Curl@1392$$")
    client.db_create("my_database2", "graph", "plocal")
    # create a new class for nodes
    client.command("CREATE CLASS Node EXTENDS V")
    # read the CSV file
    with open('data.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            # get the source and destination nodes
            source = row[0]
            destination = row[1]
            # check if the nodes already exist
            if not client.query("SELECT FROM Node WHERE name = '{}'".format(source)):
                # create a new node for the source
                client.command("CREATE VERTEX Node SET name = '{}'".format(source))

            if not client.query("SELECT FROM Node WHERE name = '{}'".format(destination)):
                # create a new node for the destination
                client.command("CREATE VERTEX Node SET name = '{}'".format(destination))
            # get the edge name
            edge_name = row[2].replace("-", "_")
            edge_name = edge_name.replace("c", "cc")
            print(edge_name)
            # check if the edge class already exists
            if client.query("SELECT FROM E WHERE name = '{}'".format(edge_name)):
            # create a new edge class
                client.command("CREATE CLASS {} EXTENDS E".format(edge_name))
            # create a new edge between the source and destination nodes

            client.command(
                "CREATE EDGE {} FROM (SELECT FROM Node WHERE name = '{}') TO (SELECT FROM Node WHERE name = '{}')".format(
                    edge_name, source, destination))

    # disconnect from the OrientDB server
    client.db_close()
    return 1


def get_data_from_orientdb():
    # connect to the OrientDB server and select the mydatabase database
    client = OrientDB("localhost", 2424)
    client.set_session_token(True)
    session_id = client.connect("root", "Curl@1392$$")
    client.db_open("my_database2", "root", "Curl@1392$$")

    # read the graph data from the Node and Edge classes
    nodes = client.query("SELECT FROM Node")
    edges = client.query("SELECT FROM E LIMIT -1")

    # write the graph data to a CSV file
    with open('graph.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        # write the header row
        writer.writerow(['source', 'destination', 'edge'])
        # write the node and edge data
        for edge in edges:
            source = client.query("SELECT name FROM Node WHERE @rid = {}".format(edge.oRecordData['out']))
            destination = client.query("SELECT name FROM Node WHERE @rid = {}".format(edge.oRecordData['in']))
            writer.writerow([source[0].oRecordData['name'], destination[0].oRecordData['name'], edge._class])
    # disconnect from the OrientDB server
    client.db_close()
    return 1


def rebuild_indexes():
    # connect to the OrientDB server and select the mydatabase database
    client = OrientDB("localhost", 2424)
    client.set_session_token(True)
    session_id = client.connect("root", "Curl@1392$$")
    client.db_open("my_database2", "root", "Curl@1392$$")
    client.command("REBUILD INDEX *")


def create_edge_class(source, destination, edge_name):
    client = OrientDB("localhost", 2424)
    client.set_session_token(True)
    session_id = client.connect("root", "Curl@1392$$")
    client.db_open("my_database2", "root", "Curl@1392$$")
    # create a new class for nodes
    source = source
    destination = destination
    # check if the nodes already exist
    if not client.query("SELECT FROM Node WHERE name = '{}'".format(source)):
        # create a new node for the source
        client.command("CREATE VERTEX Node SET name = '{}'".format(source))

    if not client.query("SELECT FROM Node WHERE name = '{}'".format(destination)):
        # create a new node for the destination
        client.command("CREATE VERTEX Node SET name = '{}'".format(destination))
    # get the edge name
    edge_name = edge_name
    print(edge_name)
    # check if the edge class already exists
    if client.query("SELECT FROM E WHERE name = '{}'".format(edge_name)):
    # create a new edge class
        client.command("CREATE CLASS {} EXTENDS E".format(edge_name))
    client.command(
        "CREATE EDGE {} FROM (SELECT FROM Node WHERE name = '{}') TO (SELECT FROM Node WHERE name = '{}')".format(
            edge_name, source, destination))
    client.db_close()
    return 1


def delete_edges_by_vertex(vertex_name):
    client = OrientDB("localhost", 2424)
    client.set_session_token(True)
    session_id = client.connect("root", "Curl@1392$$")
    client.db_open("my_database2", "root", "Curl@1392$$")
    client.command(f"DELETE EDGE E WHERE out.name = '{vertex_name}' OR in.name = '{vertex_name}'")
    client.db_close()


if __name__ == '__main__':
    # delete_edges_by_vertex('93')
    # create_edge_class('283', '12', 'ccontent')
    get_data_from_orientdb()