
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
#create a DataStream Catelog
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
ds = env.from_collection([("Alice", 12), ("Bob", 10), ("Alice", 100)],
                          )

# ds = env.from_collection(data,
#                           type_info=Types.ROW_NAMED(
#                           ["a", "b"],
#                           [Types.STRING(), Types.INT()]))
#input_table = table_env.from_data_stream(ds).alias("address", "block")

# prepare the catalog
# register Table API tables in the catalog
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('source_table', table)

# create Table API table from catalog
new_table = table_env.from_path('source_table')
new_table.to_pandas()

# register the Table object as a view and query it
# the query contains an aggregation that produces updates
# #res_table = t_env.sql_query("SELECT address, SUM(block) FROM InputTable GROUP BY address")
# res_table = t_env.sql_query("SELECT * FROM InputTable")

# # interpret the updating Table as a changelog DataStream
# res_stream = t_env.to_changelog_stream(res_table)

# # add a printing sink and execute in DataStream API
# res_stream.print()
# env.execute()
