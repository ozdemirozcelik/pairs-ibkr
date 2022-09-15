from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.execution import ExecutionFilter
import threading
import time
from datetime import datetime
import pandas as pd
import socket
import requests


class Exec_Filter(ExecutionFilter):
    # Filter fields
    def __init__(self, clientID):
        self.clientId = clientID
        self.acctCode = ""
        self.time = ""
        self.symbol = ""
        self.secType = ""
        self.exchange = ""
        self.side = ""


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.exec_status_df = pd.DataFrame(
            columns=["Symbol", "SecType", "Currency", "Execution"]
        )

    def error(self, reqId, errorCode, errorString):
        pass
        # print(f'\n{time_str()} - Client-2 is active')
        # print("Error {} {} {}".format(reqId,errorCode,errorString))

    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        dictionary = {
            "ReqId:": reqId,
            "Symbol": contract.symbol,
            "SecType": contract.secType,
            "Currency": contract.currency,
            "Execution": execution,
        }
        self.exec_status_df = self.exec_status_df.append(dictionary, ignore_index=True)

    # create socket function for winerror 1003
    def _socketShutdown(self):
        self.conn.lock.acquire()
        try:
            if self.conn.socket is not None:
                self.conn.socket.shutdown(socket.SHUT_WR)
        finally:
            self.conn.lock.release()


# added time_str to make it easy to timestamp print statements
def time_str():
    return datetime.now().strftime("%H:%M:%S.%f")


def websocket_con():
    app3.run()


def get_filled_orders(connection_port):
    global app3, exec_status_df

    exec_dict = {"Ticker": "", "OrID": "", "AvgPrice": ""}

    app3 = TradingApp()

    app3.connect("127.0.0.1", connection_port, clientId=3)

    if not app3.isConnected():

        print(
            f"\n{time_str()} - Client 3 cannot establish TWS connection to check for the filled orders"
        )
        exec_dict = {}

    else:

        print(
            f"\n{time_str()} - Client 3 established TWS connection to check for the filled orders"
        )

        # starting a separate daemon thread to execute the websocket connection
        con_thread = threading.Thread(target=websocket_con, daemon=True)
        con_thread.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        filter = Exec_Filter(1)
        app3.reqExecutions(301, filter)
        time.sleep(3)

        exec_status_df = app3.exec_status_df  # details of the order status

        # GET ORDER ID
        exec_series1 = exec_status_df["Execution"]
        exec_series2 = exec_series1.apply(str)  # convert series values into strings

        OrID_List = []
        AvgPrice_List = []

        for index, value in exec_series2.items():  # parse series values into a list
            append_1 = value.split("OrderId:", 1)[1]
            OrID_List.append(append_1.split(",")[0].replace(" ", ""))

            append_2 = value.split("AvgPrice:", 1)[1]
            AvgPrice_List.append(append_2.split(",")[0].replace(" ", ""))

        exec_dict = {
            "Ticker": exec_status_df["Symbol"],
            "OrID": OrID_List,
            "AvgPrice": AvgPrice_List,
        }  # create dictionary from lists
        # exec_df = pd.DataFrame(exec_dict) #convert dictionary to pandas dataframe

        array_orderID = exec_dict["OrID"]  # convert dic item to list
        array_avgprice = exec_dict["AvgPrice"]  # convert dic item to list

        uniq_list = list(set(array_orderID))

        uniq_dict = dict.fromkeys(uniq_list, "")

        for o, p in zip(array_orderID, array_avgprice):
            uniq_dict[o] = p

        # close socket
        app3._socketShutdown()
        time.sleep(0.5)

        app3.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(f"\n{time_str()} - TWS disconnected after checking for the filled orders")

    return uniq_dict


def update_filled_orders(connection_port, PASSPHRASE, API_PUT_UPDATE):

    global filled_orders

    filled_orders = get_filled_orders(connection_port)  # get unique filled order dict

    if bool(filled_orders):

        for o, p in filled_orders.items():
            print(f"\n{time_str()} - updating order:{o} with price:{p}")
            # logger.info(f'updating order:{o} with price:{p}')
            time.sleep(0.5)

            send_data = {
                "passphrase": PASSPHRASE,
                "price": round(float(p), 2),
                "order_id": int(o),
            }
            try:
                response = requests.put(API_PUT_UPDATE, json=send_data)

                if response.status_code == 200:
                    print(f"\n{time_str()} - order {o} is updated")
                else:
                    print(
                        f"\n{time_str()} - an error occurred updating the order {o} with price:{p}"
                    )
                    # logger.error(f"an error occurred updating the order {o} with price:{p}")

            except requests.Timeout:
                # back off and retry
                print(f"\n{time_str()} - timeout error")
                pass

            except requests.ConnectionError:
                print(f"\n{time_str()} - connection error")
                pass

    else:

        print(f"\n{time_str()} - no fulfilled orders to update")
        # logger.info('no fulfilled orders to update')

    time.sleep(0.5)


# ENABLE TO TEST:
# import os

# print("check if path is correct:", os.getcwd())
# import configparser

# config = configparser.ConfigParser()
# config.read("config_private.ini")
# environment = config.get("environment", "ENV")
# account_number = config.get(environment, "ACCOUNT_NUMBER")
# API_UPDATE_PNL = config.get(environment, "API_UPDATE_PNL")
# API_PUT_UPDATE = config.get(environment, "API_PUT_UPDATE")
# PASSPHRASE = config.get(environment, "PASSPHRASE")
# connection_port = int(config.get(environment, "CONNECTION_PORT"))

# update_filled_orders(connection_port, PASSPHRASE, API_PUT_UPDATE)
