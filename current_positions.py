from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
from datetime import datetime
import pandas as pd
import socket
import requests
import sys

# TODO: error handling and logging
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.pos_df = pd.DataFrame(
            columns=[
                "Account",
                "Symbol",
                "SecType",
                "Currency",
                "Position",
                "Avg cost",
                "ConId",
                "ReqId",
                "UnrealizedPnL",
            ]
        )
        self.pnl_single = pd.DataFrame(
            columns=[
                "ReqId",
                "Position",
                "DailyPnL",
                "UnrealizedPnL",
                "RealizedPnL",
                "Value",
            ]
        )

    def error(self, reqId, errorCode, errorString):
        pass
        # print("Error {} {} {}".format(reqId,errorCode,errorString))
        # logger.error("Error {} {} {}".format(reqId,errorCode,errorString))

    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {
            "Account": account,
            "Symbol": contract.symbol,
            "SecType": contract.secType,
            "Currency": contract.currency,
            "Position": position,
            "Avg cost": avgCost,
            "ConId": contract.conId,
        }
        self.pos_df = self.pos_df.append(dictionary, ignore_index=True)

    def pnlSingle(self, reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value):
        super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        dictionary = {
            "ReqId": reqId,
            "Position": pos,
            "DailyPnL": dailyPnL,
            "UnrealizedPnL": unrealizedPnL,
            "RealizedPnL": realizedPnL,
            "Value": value,
        }
        self.pnl_single = self.pnl_single.append(dictionary, ignore_index=True)

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


def websocket_con5():
    app5.run()


def websocket_con6():
    app6.run()


def get_all_positions(connection_port):
    global app5, position_df

    position_df = pd.DataFrame(
        columns=["Account", "Symbol", "SecType", "Currency", "Position", "Avg cost"]
    )

    app5 = TradingApp()

    app5.connect("127.0.0.1", connection_port, clientId=5)

    if not app5.isConnected():

        print(
            f"\n{time_str()} - Client 5 cannot establish TWS connection to check for the current positions"
        )

    else:

        print(
            f"\n{time_str()} - Client 5 established TWS connection to check for the current positions"
        )
        # logger.info(f'\n{time_str()} - Client 5 established TWS connection to check for the current positions')

        # starting a separate daemon thread to execute the websocket connection
        con_thread = threading.Thread(target=websocket_con5, daemon=True)
        con_thread.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app5.reqPositions()
        time.sleep(2)
        position_df = app5.pos_df

        # close socket
        app5._socketShutdown()
        time.sleep(0.5)

        app5.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(
            f"\n{time_str()} - TWS disconnected after checking for the current positions"
        )

    return position_df


def get_all_positions_withpnl(account_number, connection_port):
    global app6, position_df_pnl

    position_df_pnl = pd.DataFrame(
        columns=[
            "Account",
            "Symbol",
            "SecType",
            "Currency",
            "Position",
            "Avg cost",
            "ConId",
            "ReqId",
            "UnrealizedPnL",
        ]
    )

    app6 = TradingApp()

    app6.connect("127.0.0.1", connection_port, clientId=6)

    if not app6.isConnected():

        print(
            f"\n{time_str()} - Client 6 cannot establish TWS connection to check for the current positions"
        )
        # logger.warning(f'\n{time_str()} - Client 6 cannot established TWS connection to check for the current positions')

    else:

        print(
            f"\n{time_str()} - Client 6 established TWS connection to check for the current positions"
        )
        # logger.info(f'\n{time_str()} - Client 6 established TWS connection to check for the current positions')

        # starting a separate daemon thread to execute the websocket connection
        con_thread = threading.Thread(target=websocket_con6, daemon=True)
        con_thread.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app6.reqPositions()
        time.sleep(2)
        position_df_pnl = app6.pos_df

        global pnl_single_df, pnl_single_df_pnl

        # dropping ALL duplicate values
        position_df_pnl.drop_duplicates(subset=["ConId"], keep="last", inplace=True)

        # dropping ALL duplicate values
        position_df_pnl.drop_duplicates(subset=["ConId", "Symbol"])

        # change na values to zero
        position_df_pnl["UnrealizedPnL"] = position_df_pnl["UnrealizedPnL"].fillna(0)
        position_df_pnl["ReqId"] = position_df_pnl["ReqId"].fillna(0)

        print(
            f"\n{time_str()} - Client 5 established TWS connection to update unrealized PNL"
        )
        # logger.info(f'\n{time_str()} - Client 5 established TWS connection to update unrealized PNL')

        # create unrealized pnl list
        for ind in position_df_pnl.index:
            conid = position_df_pnl["ConId"][ind]
            print(
                "Getting pos. information of ticker: ", position_df_pnl["Symbol"][ind]
            )
            # logger.info(f'\'Updating ticker: {position_df_pnl["Symbol"][ind]}')

            # initiate pnl flow
            app6.reqPnLSingle(
                conid, account_number, "", conid
            )  # give unique request id
            position_df_pnl.loc[ind, "ReqId"] = int(conid)

            time.sleep(0.3)

        # wait for the flow to complete
        time.sleep(2)

        # get PNL df
        pnl_single_df = app6.pnl_single

        # setting as index but permanently
        position_df_pnl.set_index("ReqId", inplace=True)

        # sorting by first name #check if necessary
        # pnl_single_df.sort_values("ReqId", inplace = True)

        # dropping ALL duplicate values
        pnl_single_df.drop_duplicates(subset="ReqId", keep="last", inplace=True)
        # pnl_single_df.sort_values('ReqId').drop_duplicates('ReqId',keep='last')

        array_reqID = pnl_single_df["ReqId"]  # convert dic item to list
        array_pnl = pnl_single_df["UnrealizedPnL"]  # convert dic item to list

        for o, p in zip(array_reqID, array_pnl):

            if not p == sys.float_info.max:
                position_df_pnl.loc[o, "UnrealizedPnL"] = int(p)

            else:
                position_df_pnl.loc[o, "UnrealizedPnL"] = 0

        # close socket
        app6._socketShutdown()
        time.sleep(0.5)

        app6.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(
            f"\n{time_str()} - TWS disconnected after checking for the current positions"
        )

    return position_df_pnl


def get_position(ticker, connection_port):
    pos_df = get_all_positions(connection_port)

    search1 = pos_df.loc[pos_df["Symbol"] == ticker]
    search1_pos = search1["Position"]
    search1_list = search1_pos.values.tolist()

    if search1_list:
        pos_value = search1_list[0]
    else:
        pos_value = 0

    return pos_value


# edit & use if you want to send and save the data as a json file
# needs Server_URL_Update and necessary coding on the server side
def update_positions_as_json(account_number, Server_URL_Update, connection_port):
    global json_df

    pos_df = get_all_positions_withpnl(account_number, connection_port)

    json_df = pos_df.to_json()

    # if you need to write to file
    # pos_df.to_json (r'../templates/jason_file.json')

    try:
        response = requests.post(Server_URL_Update, json=json_df, timeout=10)
    except requests.Timeout:
        # back off and retry
        print(f"\n{time_str()} - timeout error during json update")
        pass
    except requests.ConnectionError:
        print(f"\n{time_str()} - connection error during json update")
        pass

    if response:
        print(f"\n{time_str()} - Portfolio position json file sent to server")


def update_positions(account_number, connection_port, PASSPHRASE, API_UPDATE_PNL):

    # defined to observe in the varibale explorer (spyder)
    global array_symbol, array_position, array_cost, array_pnl, positions_and_pnl

    pos_df = get_all_positions_withpnl(account_number, connection_port)

    array_symbol = pos_df["Symbol"]
    array_position = pos_df["Position"]
    array_cost = pos_df["Avg cost"]
    array_pnl = pos_df["UnrealizedPnL"]

    positions_and_pnl = zip(array_symbol, array_position, array_cost, array_pnl)

    if not pos_df.empty:

        for s, p, c, u in positions_and_pnl:
            print(f"\n{time_str()} - updating symbol:{s} with position:{p}")
            time.sleep(0.5)

            send_data = {
                "passphrase": PASSPHRASE,
                "symbol": s,
                "active_pos": int(p),
                "active_cost": round(float(c), 3),
                "active_pnl": int(u),
            }
            try:
                response = requests.put(API_UPDATE_PNL, json=send_data, timeout=10)

                if response.status_code == 200:
                    print(f"\n{time_str()} - symbol {s} is updated")
                else:
                    print(
                        f"\n{time_str()} - an error occurred updating the symbol {s} with position:{p}"
                    )
                    # logger.error(f"an error occurred updating the symbol {s} with price:{p}")
            except requests.Timeout:
                # back off and retry
                print(f"\n{time_str()} - timeout error")
                pass

            except requests.ConnectionError:
                print(f"\n{time_str()} - connection error")
                pass
    else:

        print(f"\n{time_str()} - no positions to update")
        # logger.info('no positions to update')


# ENABLE TO TEST:
# import os

# print("check if path is correct:", os.getcwd())
# import configparser

# config = configparser.ConfigParser()
# config.read("config_private.ini")
# environment = config.get("environment", "ENV")
# account_number = config.get(environment, "ACCOUNT_NUMBER")
# API_UPDATE_PNL = config.get(environment, "API_UPDATE_PNL")
# PASSPHRASE = config.get(environment, "PASSPHRASE")
# connection_port = int(config.get(environment, "CONNECTION_PORT"))


# #pos_df_withoutpnl = get_all_positions(connection_port)
# #pos_df_withpnl = get_all_positions_withpnl(account_number, connection_port)
# #pos_value = get_position("LNT", connection_port)
# update_positions(account_number, connection_port, PASSPHRASE, API_UPDATE_PNL)
