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


def db_insert(blocks, transactions):
    
    try:
        conn = sqlite3.connect(config.get("database", "blockexplorer_db"))
        c = conn.cursor()
    except Exception as e:
        print "Could not connect to database: %s" % e

    try:
        c.executemany('INSERT INTO blocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', blocks)
        c.executemany('INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?)', transaction)
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

            blocks.append(tuple([v for v in block_dict['result'].values()]))    
            if len(block_dict['result']['transactions']) > 0:
                trans_dict = get_transactions(block_dict['result']['transactions'])
                transactions.append(tuple([v for v in trans_dict['result'].values()]))
            
            if (block_counter % int(config.get("database", "insert_limit"))) == 0:
                res = db_insert(blocks, transactions)
                blocks = []
                transactions = []
        
        else:
            if (config.get("general", "debug")) == "on":
                print "Reached a none existing block (%d). Exiting." % block_counter
                return False

        block_counter+=1


if __name__ == "__main__":
    main()
