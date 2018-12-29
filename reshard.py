import halo
import configargparse
from xml.dom import minidom
from pssh.clients import ParallelSSHClient
import datetime
import clickhouse_driver
import tqdm

# Globals
ssh_client = None
args = None
clickhouse_data_path = None


# Functions
def generator_to_str(gen):
    output = ""
    for line in gen:
        output += line + "\n"
    return output

def get_gen_list(gen):
    output = []
    for line in gen:
        output.append(line)
    return output


if __name__ == "__main__":
    p = configargparse.ArgParser(default_config_files=[])
    p.add('-c', required=False, is_config_file=True, help='config file path')
    p.add_argument('--clickhouse-seed', default='172.29.3.14')
    p.add_argument('--clickhouse-config', default="/etc/clickhouse-server/config.xml")
    p.add_argument('--clickhouse-cluster', default='delmar_prod')
    p.add_argument('--target-database', default='delmar')
    p.add_argument('--target-table', default='sputnik')
    p.add_argument('--batch-size', type=int, default=10, help="Number of partitions to rewrite at once")

    args = p.parse_args()

    with halo.Halo(text='Connecting to {}'.format(args.clickhouse_seed), spinner='dots'):

        hosts = [args.clickhouse_seed]
        client = ParallelSSHClient(hosts, user='root', pkey='/home/colum/.ssh/id_rsa')

        conf_cmd = client.run_command('cat {}'.format(args.clickhouse_config))
        if conf_cmd[args.clickhouse_seed]['exit_code'] != 0:
            print("ERROR: Unable to read clickhouse config")
            exit(1)

        conf = conf_cmd[args.clickhouse_seed]['stdout']

    with halo.Halo(text='Parsing Configuration', spinner='dots'):
        tree = minidom.parseString(generator_to_str(conf))

        clickhouse_data_path = tree.getElementsByTagName('path')[0].firstChild.data

        # Get servers
        # We're assuming that the cluster tag is unique in the config file ... which it should be
        try:
            shard = tree.getElementsByTagName(args.clickhouse_cluster)[0]
        except IndexError:
            print("Failed to find cluster in configuration")
            exit(1)

        shards = shard.getElementsByTagName('shard')
        servers = []
        for _shard in shards:
            # TODO We're going to need to figure out who is the master if we have more than one replica
            # For now, assume 0 is master
            replica = _shard.getElementsByTagName('replica')[0]
            server = replica.getElementsByTagName('host')[0].firstChild.data
            port = replica.getElementsByTagName('port')[0].firstChild.data
            servers.append((server, port))

    with halo.Halo(text='Establishing connections to all nodes in cluster', spinner='dots'):
        # SSH
        hosts = []
        for host in servers:
            hosts.append(host[0])
        ssh_client = ParallelSSHClient(hosts, user='root', pkey='/home/colum/.ssh/id_rsa')

        # Clickhouse
        clickhouse_connection_pool = dict()
        for server in servers:
            clickhouse_connection_pool[server[0]] = clickhouse_driver.Client(server[0], port=server[1])

    print("Connected to {} servers".format(len(servers)))

    with halo.Halo(text='Creating Staging tables'.format(server[0]), spinner='dots') as heh:
        for server in clickhouse_connection_pool:
            table = clickhouse_connection_pool[server].execute(
                "SHOW CREATE TABLE {db}.{table};".format(db=args.target_database, table=args.target_table))[0][0]
            table = table.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
            table = table.replace(args.target_table, '{}_tmprshrd'.format(args.target_table))

            clickhouse_connection_pool[server].execute(table)

    # Partition -> [servers]
    partitions = dict()
    with halo.Halo(text='Collecting Partition information', spinner='dots') as heh:
        for server in clickhouse_connection_pool:
            _query = "SELECT partition, rows FROM system.parts WHERE database = '{db}' AND table = '{table}' ORDER BY partition ASC;".format(db=args.target_database, table=args.target_table)
            results = clickhouse_connection_pool[server].execute(_query)

            for partition in results:
                _name = partition[0][1:-1]  # I think this is weirdness with the python library
                if _name not in partitions:
                    partitions[_name] = []

                if server not in partitions[_name]:
                    partitions[_name].append(server)

    work_partitions = []
    with halo.Halo(text='Figuring out required work', spinner='dots') as heh:
        n_servers = len(servers)
        for partition in partitions:
            if len(partitions[partition]) < n_servers:
                work_partitions.append(partition)

    print("There are {} partitions that need to be re-distributed across the cluster".format(len(work_partitions)))

    src_path = clickhouse_data_path + "/data/{db}/{table}/detached".format(db=args.target_database, table=args.target_table)
    dst_path = clickhouse_data_path + "/data/{db}/{table}_tmprshrd/detached".format(db=args.target_database,
                                                                                    table=args.target_table)

    stage = []
    for _work in work_partitions:
        if len(stage) < args.batch_size:
            stage.append(_work)
        else:
            print("Processing {} partitions: {}".format(len(stage), stage))
            with halo.Halo(text='Detaching Partitions', spinner='dots') as heh:
                for work_item in stage:
                    for server in servers:
                        _query = "ALTER TABLE {db}.{table} DETACH PARTITION '{part}';".format(db=args.target_database,
                                                                                                       table=args.target_table,
                                                                                                       part=work_item)
                        clickhouse_connection_pool[server[0]].execute(_query)

            with halo.Halo(text='Moving Partitions', spinner='dots') as heh:
                mv_cmd = "mv -f {}/* {}".format(src_path, dst_path)
                ssh_client.run_command(mv_cmd)

            with halo.Halo(text='Collecting unattached partition information', spinner='dots') as heh:
                ls_cmd = "ls -1q {}".format(dst_path)
                """
                        attach_cmd = "ls -1q {path} | cut -d'_' -f1 | xargs -P 100 -i date -d @\"\{\}\" +\"%Y-%m-%d %H:%M:%S\" | " \
                             "xargs -P 10 -i clickhouse-client -h {host} -q \"alter table {db}.{table}_tmprshrd attach " \
                             "partition '\{\}';\"".format(path=dst_path, db=args.target_database, table=args.target_table)
                """
                parts = ssh_client.run_command(ls_cmd)

            for server in parts:
                _parts = get_gen_list(parts[server]['stdout'])
                print("Server {} has {} detached partitions".format(server, len(_parts)))
                with tqdm.tqdm(_parts, desc="Re-ataching", unit="ops") as bar:
                    for _spart in bar:
                        name = datetime.datetime.utcfromtimestamp(int(_spart.split('_')[0])).strftime("%Y-%m-%d %H:%M:%S")
                        _query = "ALTER TABLE {db}.{table}_tmprshrd ATTACH PARTITION '{part}';".format(db=args.target_database,
                                                                                                       table=args.target_table,
                                                                                                       part=str(name))
                        clickhouse_connection_pool[server].execute(_query)

            with halo.Halo(text='Re-writing & cleaning up data', spinner='dots') as heh:
                for server in servers:
                    _query = "INSERT INTO {db}.{table}_dist SELECT * FROM {db}.{table}_tmprshrd".format(db=args.target_database,
                                                                                                   table=args.target_table)
                    clickhouse_connection_pool[server[0]].execute(_query)
                    clickhouse_connection_pool[server[0]].execute(
                        "TRUNCATE TABLE {db}.{table}_tmprshrd;".format(db=args.target_database, table=args.target_table,
                                                                       part=partition))

            stage = []


    print("done")
