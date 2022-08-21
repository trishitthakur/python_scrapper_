import cassandra

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement


class CassandraManagement:

    def create_connection(self, record):
        cloud_config= {'secure_connect_bundle':r'C:\Users\TRISHIT\Downloads\secure-connect-test.zip'}
        auth_provider = PlainTextAuthProvider('JEjwRnWEXFuyzzMDSLuLANpT', 'YMxoWB5.0fPoXIhQiT6.vyFG2liWdkpiv,ZTZhulYnPp3QIlKSCcR4IFT8SN93uiMtwAHW,,O_xIuZEdhsW3hY-k903Ax7L7kjythGfexjPaIUsv1g8ySGsvjfriFKWt')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()

        print("session establ")

        row = session.execute("select release_version from system.local")

        #print(row)

        try:

            batch = BatchStatement()

            row = session.execute("CREATE KEYSPACE home WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};")

            #row=session.execute("use home;")

            row=session.execute("CREATE TABLE pyproj(product_name text,product_searched text,price text, offer_details text, discount_percent text, EMI text, rating text ,comment text, customer_name text, review_age text);")

            batch.add(SimpleStatement("INSERT INTO pyproj(product_name, product_searched, price, offer_details, discount_percent, EMI, rating, comment, customer_name, review_age) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"))

            session.execute(batch, record)

        except Exception as e:

            raise Exception(f"(insertRecord): Something went wrong on inserting record\n" + str(e))


