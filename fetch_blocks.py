
from web3 import Web3
from web3.middleware import geth_poa_middleware
import ray
from ray.util import inspect_serializability
import pymongo
import gc
from time import sleep  
from json import dumps  
from kafka import KafkaProducer  

@ray.remote
def fetchBlocks(block):
        my_producer = KafkaProducer(  
            bootstrap_servers = ['localhost:9092'],  
            value_serializer = lambda x:dumps(x).encode('utf-8')  
        )  
        web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        for tx_hash in web3.eth.get_block(block).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            if contractAddress != None:

                my_producer.send('testtopic', value = contractAddress)
                sleep(0.005)
        del web3
        del block
        gc.collect()

def start():   
    infura_url= "wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

    block= web3.eth.get_block('latest').number + 1
    del web3
    del infura_url
    while block>=0:
        block-=1
        fetchBlocks.remote(block)
            
start()