#!/usr/bin/env python

import sys
import json
import urllib2
import requests
import sqlite3
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('config.ini')


def main():

    get_blocks()


def clean_block(block_dict):

    try:
        c = block_dict['result'].copy()
        del c['nonce']
        del c['transactions']
        del c['size']
        del c['logsBloom']

        c['gasLimit'] = int(c['gasLimit'], 16)
        c['number'] = int(c['number'], 16)
        c['difficulty'] = int(c['difficulty'], 16)
        c['gasUsed'] = int(c['gasUsed'], 16)
        c['totalDifficulty'] = int(c['totalDifficulty'], 16)

    except Exception as e:
        print "Could not alter keys in block dictionary: %s" % e

    return c


def clean_trans(trans_dict):

    try:
        c = trans_dict['result'].copy()
        del c['input']
        del c['nonce']

        c['gas'] = int(c['gas'], 16)
        c['value'] = int(c['value'], 16)
        c['blockNumber'] = int(c['blockNumber'], 16)
        c['transactionIndex'] = int(c['transactionIndex'], 16)
        c['gasPrice'] = int(c['gasPrice'], 16)

    except Exception as e:
        print "Could not alter keys in transaction dictionary: %s" % e

    return c


def db_insert(blocks, transactions):
    
    try:
        conn = sqlite3.connect(config.get("database", "blockexplorer_db"))
        c = conn.cursor()
    except Exception as e:
        print "Could not connect to database: %s" % e

    if (config.get("general", "debug")) == "on":
        print "Inserting %d blocks and %d transactions into database" % (len(blocks),len(transactions))

    try:
        print blocks
        c.executemany('INSERT INTO blocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', blocks)
        c.executemany('INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?)', transactions)
    except Exception as e:
        print "Could not insert values into database: %s. Exiting." % e
        sys.exit(0)



def get_transactions(trans_dict):

    for transaction in trans_dict:
        trans_call = '{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["' + str(transaction['hash']) + '"],"id":1}'
        try:
            res = requests.post(config.get("rpc", "url"), data=trans_call, allow_redirects=True)
            res = json.loads(res.content)
        except Exception as e:
            print "Error on performing HTTP request against RPC-Server: %s" % e
            sys.exit(0)

    if res:
        return res
    return False



def get_blocks():

    block_counter = 0
    blocks = []
    transactions = []

    while True:

        if (config.get("general", "debug")) == "on":
            status_blocks = "Number of blockchain ids parsed: %d" % block_counter
            print "\r", status_blocks,

        try:
            block_call = '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[' + str(block_counter) + ', true],"id":1}'
	    res = requests.post(config.get("rpc", "url"), data=block_call, allow_redirects=True)
        except Exception as e:
            print "Error on performing HTTP request against RPC-Server: %s" % e
            sys.exit(0)

	block_dict = json.loads(res.content)

        if block_dict['result']:
            c = clean_block(block_dict)
            blocks.append(tuple([v for v in c.values()])) 
        
            if len(block_dict['result']['transactions']) > 0:
                trans_dict = get_transactions(block_dict['result']['transactions'])
                c = clean_trans(trans_dict) 
                transactions.append(tuple([v for v in c.values()]))

            block_counter+=1
        
        else:
            if (config.get("general", "debug")) == "on":
                print "Reached a none existing block (%d), end of blockchain?. Exiting." % block_counter
                return False

        if (block_counter % int(config.get("database", "num_inserts"))) == 0:
            res = db_insert(blocks, transactions)
            blocks = []
            transactions = []



if __name__ == "__main__":
    main()
