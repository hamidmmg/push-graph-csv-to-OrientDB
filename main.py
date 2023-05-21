import csv
from pyorient import OrientDB

if __name__ == '__main__':

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