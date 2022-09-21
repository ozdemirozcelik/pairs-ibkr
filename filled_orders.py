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
    global app3, exec_status_df, output_df

    app3 = TradingApp()

    app3.connect("127.0.0.1", connection_port, clientId=3)

    if not app3.isConnected():

        print(
            f"\n{time_str()} - Client 3 cannot establish TWS connection to check for the filled orders"
        )

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
        CumQty_List =[]
        
        filled_df = pd.DataFrame(
            columns=[
                "Ticker",
                "OrID",
                "AvgPrice",
                "CumQty"
            ]
        )
        
        output_df = filled_df.copy()

        for index, value in exec_series2.items():  # parse series values into a list
            append_1 = value.split("OrderId:", 1)[1]
            OrID_List.append(append_1.split(",")[0].replace(" ", ""))

            append_2 = value.split("AvgPrice:", 1)[1]
            AvgPrice_List.append(append_2.split(",")[0].replace(" ", ""))
            
            append_3 = value.split("CumQty:", 1)[1]
            CumQty_List.append(append_3.split(",")[0].replace(" ", ""))

        array_ticker = exec_status_df["Symbol"]  # convert dic item to array
           
        filled_df["Ticker"] = array_ticker      
        filled_df["OrID"] = OrID_List
        filled_df["AvgPrice"] = AvgPrice_List
        filled_df["CumQty"] = CumQty_List
       
        filled_df['CumQty'] = filled_df['CumQty'].astype(float)
        filled_df["AvgPrice"] = filled_df['AvgPrice'].astype(float)

        uniq_orids = list(set(OrID_List))  # get a list of unique order ids
        uniq_tickers = list(set(array_ticker))  # get a list of unique ticker

        for orid in uniq_orids:
            filled_df_filtered = filled_df.loc[(filled_df['OrID'] == orid)]

            for ticker in uniq_tickers:  # get the latest order realization for each ticker
                filled_df_filtered1 = filled_df_filtered.loc[(filled_df_filtered['Ticker'] == ticker)]
                if not filled_df_filtered1.empty:
                    filled_df_filtered2 = filled_df_filtered1.loc[
                        filled_df_filtered['CumQty'] == filled_df_filtered1.CumQty.max()]
                    output_df = output_df.append(filled_df_filtered2.iloc[0], ignore_index=True)

        # close socket
        app3._socketShutdown()
        time.sleep(0.5)

        app3.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(f"\n{time_str()} - TWS disconnected after checking for the filled orders")

    return output_df


def update_filled_orders(connection_port, PASSPHRASE, API_PUT_UPDATE):

    global filled_orders

    filled_orders = get_filled_orders(connection_port)  # get unique filled order dataframe
   
    if not filled_orders.empty:
        
        for ind in filled_orders.index:           
            print(f"\n{time_str()} - updating order:{filled_orders['OrID'][ind]} with price:{filled_orders['AvgPrice'][ind]}")
            # logger.info(f'updating order:{filled_orders['OrID'][ind]} with price:{filled_orders['AvgPrice'][ind]}')
            time.sleep(0.5)
                       
            send_data = {
                "passphrase": PASSPHRASE,
                "price": filled_orders["AvgPrice"][ind],
                "order_id": filled_orders["OrID"][ind],
                "symbol": filled_orders["Ticker"][ind],
                "filled_qty": filled_orders["CumQty"][ind]
            }

            try:
                response = requests.put(API_PUT_UPDATE, json=send_data)

                if response.status_code == 200:
                    print(f"\n{time_str()} - order {filled_orders['OrID'][ind]} for {filled_orders['Ticker'][ind]} is updated")
                else:
                    print(
                        f"\n{time_str()} - an error occurred updating the order {filled_orders['OrID'][ind]} for {filled_orders['Ticker'][ind]}"
                    )
                    # logger.error(f"an error occurred updating the order {filled_orders['OrID'][ind]} for {filled_orders['Ticker'][ind]}")

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
# #filled_orders = get_filled_orders(connection_port) 
# update_filled_orders(connection_port, PASSPHRASE, API_PUT_UPDATE)
