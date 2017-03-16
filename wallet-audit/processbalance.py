import pymysql.cursors, sys, json, requests
from utils import parsing, output

#2.5.2a

class ProcessUserBalance:
    def __init__(self):
        self.config = parsing.parse_json("./walletnotify.json")
        ##MySQL
        self.config_mysql = self.config["mysql"]
        self.host = self.config_mysql["db_host"]
        try:
            self.port = int(self.config_mysql["db_port"])
        except KeyError:
            self.port = 3306
        self.db_user = self.config_mysql["db_user"]
        self.db_pass = self.config_mysql["db_pass"]
        self.db = self.config_mysql["db"]
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.db_user,
            password=self.db_pass,
            db=self.db)
        ##RPC
        self.config_rpc = self.config["rpc"]
        self.rpc_host = self.config_rpc["rpc_host"]
        self.rpc_port = self.config_rpc["rpc_port"]
        self.rpc_user = self.config_rpc["rpc_user"]
        self.rpc_pass = self.config_rpc["rpc_pass"]
        self.serverURL = 'http://' + self.rpc_host + ':' + self.rpc_port
        self.headers = {'content-type': 'application/json'}

    def listtransactions(self, user):
        payload = json.dumps({"method": "listtransactions", "params": [user, 1000], "jsonrpc": "2.0"})
        response = requests.get(self.serverURL, headers=self.headers, data=payload,
                                auth=(self.rpc_user, self.rpc_pass))
        return response.json()['result']

    def calculate(self, user):
        get_transactions = self.listtransactions(user["snowflake"])
        i = len(get_transactions)-1
        lasttxid = get_transactions[i]["txid"]
        new_balance = 0
        for tx in reversed(get_transactions):
            new_balance += float(tx["amount"])
        to_exec = """UPDATE db
        SET balance = %s, lasttxid = %s
        WHERE snowflake = %s"""
        self.cursor.execute(to_exec, (str(new_balance), str(user["snowflake"]), lasttxid))
        output.success("Updated user ({}) balance: {} & txid: {}".format(user["snowflake"], str(new_balance), lasttxid))
        self.connection.commit()

    def process_balance(self):
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        to_exec = """SELECT snowflake
                FROM db"""
        self.cursor.execute(to_exec)
        result_set = self.cursor.fetchall()
        for user in result_set:
            self.calculate(user)

if __name__ == "__main__":
    user_bal = ProcessUserBalance()
    user_bal.process_balance()
