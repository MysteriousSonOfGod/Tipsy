#!/usr/bin/env python3.5
import pymysql.cursors
import sys
import json
import requests
import os
from utils import parsing, output
# 0.2.0a-Rw (comment-less): Aareon Sullivan 2017


class Query_db():
    def __init__(self, config):
        config_mysql = config["mysql"]
        host = config_mysql["db_host"]
        try:
            port = int(config_mysql["db_port"])
        except KeyError:
            port = 3306
        db_user = config_mysql["db_user"]
        db_pass = config_mysql["db_pass"]
        db = config_mysql["db"]
        self.connection = pymysql.connect(
            host=host,
            port=port,
            user=db_user,
            password=db_pass,
            db=db)

    def select(self, query_array, query_dict, query_from, method):
        query = "SELECT "
        for item in query_array:
            query = query + item + ","
            query = query[:-1]

        query = query + " FROM " + query_from + " WHERE "
        for key, value in query_dict.items():
            query = query + "`" + key + "` = '" + str(value) + "' AND"
        query = query[:-3]
        result_set = self.execute(query, method)
        return result_set

    def update(self, query_dict_column, query_dict_row, query_from):
        query = "UPDATE `" + query_from + "` SET "
        for key, value in query_dict_row.items():
            query = query + "`" + key + "` = '" + str(value) + "', "
        query = query[:-2]
        query = query + " WHERE "

        for key, value in query_dict_column.items():
            query = query + "`" + key + "` = '" + str(value) + "'"
        self.execute(query, "commit")
        return

    def insert(self, query_dict, query_from):
        query = "INSERT INTO `" + query_from + "` ("
        for key, value in query_dict.items():
            query = query + "`" + key + "`, "
        query = query[:-2] + ") VALUES ("

        for key, value in query_dict.items():
            query = query + "'" + str(value) + "', "
        query = query[:-2] + ")"
        self.execute(query, "commit")
        return

    def delete(self, query_dict, query_from):
        query = "DELETE FROM `" + query_from + "` WHERE "
        for key, value in query_dict.items():
            query = query + "`" + key + "` = '" + str(value) + "' AND "
        query = query[:-4] + "LIMIT 1"
        self.execute(query, "commit")
        return

    def execute(self, query, method):
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query)
        if method == "commit":
            self.connection.commit()
            cursor.close()
        if method == "fetchone":
            result_set = cursor.fetchone()
            cursor.close()
            return result_set
        if method == "rowcount":
            result_set = cursor.rowcount
            cursor.close()
            return result_set


class Walletnotify:
    def check_txs(self, config, txid, transactions):
        self.coin = config["coin"]
        self.txid = txid

        for transaction in transactions["details"]:
            confirmations = transactions["confirmations"]

            if transaction["category"] == "send":
                self.check_send(transaction, confirmations)

            else:
                self.check_received(transaction, confirmations)

    def check_send(self, transaction, confirmations):
        if confirmations == 0:
            result_set = query.select(
                ["balance"], {'snowflake': transaction["account"]}, "db", "fetchone")

            rowcount = query.select(["txid"], {
                                      'txid': self.txid, 'account': transaction["account"]}, "unconfirmed", "rowcount")

            if rowcount < 1:
                query.({'account': transaction["account"], 'amount': transaction["amount"],
                                'category': transaction["category"], 'txid': self.txid}, "unconfirmed")

                new_balance = float(
                    result_set["balance"]) + float(transaction["amount"])

                query.update({'snowflake': transaction["account"]}, {
                               'balance': new_balance, 'lasttxid': self.txid}, "db")

                message = """{}: SEND (unconfirmed); account: {},
                new balance: {}, amount: {}""".format(self.coin, transaction["account"], new_balance, transaction["amount"])
                output.success(message)
        else:
            query.delete({'account': transaction["account"], 'amount': transaction["amount"],
                            'category': transaction["category"], 'txid': self.txid}, "unconfirmed")
            message = """{}: SEND (confirmed); account: {},
            amount: {}""".format(self.coin, transaction["account"], transaction["amount"])
            output.success(message)

        return

    def check_received(self, transaction, confirmations):
        if confirmations == 0:
            rowcount = query.select(["txid"], {
                                      'txid': self.txid, 'account': transaction["account"]}, "unconfirmed", "rowcount")

            if rowcount == 1:
                query.({'account': transaction["account"], 'amount': transaction["amount"],
                                'category': transaction["category"], 'txid': self.txid}, "unconfirmed")

                message = """{}: {} (unconfirmed); account: {},
                amount: {}""".format(self.coin, transaction["category"].upper(), transaction["account"], transaction["amount"])
                output.success(message)

        else:
            query.delete({'account': transaction["account"], 'amount': transaction["amount"],
                            'category': transaction["category"], 'txid': self.txid}, "unconfirmed")

            if transaction["category"] == "generate":
                result_set = query.select(["balance", "staked"], {
                                            'snowflake': transaction["account"]}, "db", "fetchone")

                new_balance = float(
                    result_set["balance"]) + float(transaction["amount"])

                new_staked = float(
                    result_set["balance"]) + float(transaction["amount"])

                dict_for_column_update = {
                    'balance': new_balance, 'staked': new_staked, 'lasttxid': self.txid}

                query.update(
                    {'snowflake': transaction["account"]}, dict_for_column_update, "db")

            else:
                result_set = query.select(
                    ["balance"], {'snowflake': transaction["account"]}, "db", "fetchone")

                new_balance = float(
                    result_set["balance"]) + float(transaction["amount"])

                dict_for_column_update = {
                    'balance': new_balance, 'lasttxid': self.txid}

                query.update(
                    {'snowflake': transaction["account"]}, dict_for_column_update, "db")

            message = """{}: {} (confirmed); account: {},
            new balance: {}, amount: {}""".format(self.coin, transaction["category"].upper(),transaction["account"], new_balance, transaction["amount"])
            output.success(message)
        return

    def update_balance(self, transaction):
        result_set = query.select(
            ["balance"], {'snowflake': transaction["account"]}, "db", "fetchone")

        if transaction["category"] != "generated":
            new_balance = float(
                result_set["balance"]) + float(transaction["amount"])

            query.update({'snowflake': transaction["account"]}, {
                           'balance': new_balance, 'lasttxid': self.txid}, "db")
        else:
            new_balance = float(
                result_set["balance"]) + float(transaction["amount"])

            result_set = query.select(
                ["staked"], {'snowflake': transaction["account"]}, "db", "fetchone")

            new_staked = float(result_set["staked"]) + \
                float(transaction["amount"])

            query.update({'snowflake': transaction["account"]}, {'balance': new_balance, 'staked': new_staked,
                                                                   'lasttxid': self.txid}, "db")

        message = """{}: UPDATED; account: {},
        new balance: {}, amount: {}""".format(self.coin, transaction["account"], new_balance, transaction["amount"])
        output.success(message)
        return


if __name__ == "__main__":
    txid = str(sys.argv[1])
    config_path = os.getcwd()+"/walletnotify/walletnotify.json"
    config = parsing.parse_json(config_path)
    notify = Walletnotify()
    query = Query_db(config)

    def gettransaction(config, txid):
        config_rpc = config["rpc"]
        rpc_host = config_rpc["rpc_host"]
        rpc_port = config_rpc["rpc_port"]
        rpc_credentials = (
            config_rpc["rpc_user"], config_rpc["rpc_pass"])
        serverURL = 'http://' + rpc_host + ':' + rpc_port
        headers = {'content-type': 'application/json'}
        payload = json.dumps(
            {"method": "gettransaction", "params": [txid], "jsonrpc": "2.0"})
        response = requests.get(serverURL, headers=headers, data=payload,
                                auth=(rpc_credentials))
        return response.json()['result']

    transactions = gettransaction(config, txid)
    notify.check_txs(config, txid, transactions)
