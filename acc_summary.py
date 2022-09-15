from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time, pytz
from datetime import datetime
import pandas as pd
import socket
import json
import requests

# TODO: error handling and logging
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.acc_summary = pd.DataFrame(
            columns=["ReqId", "Account", "Tag", "Value", "Currency"]
        )
        self.pnl_summary = pd.DataFrame(
            columns=["ReqId", "DailyPnL", "UnrealizedPnL", "RealizedPnL"]
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
        # print(f'\n{time_str()} - Client-2 is active')
        # print("Error {} {} {}".format(reqId,errorCode,errorString))

    def accountSummary(self, reqId, account, tag, value, currency):
        super().accountSummary(reqId, account, tag, value, currency)
        dictionary = {
            "ReqId": reqId,
            "Account": account,
            "Tag": tag,
            "Value": value,
            "Currency": currency,
        }
        self.acc_summary = self.acc_summary.append(dictionary, ignore_index=True)

    def pnl(self, reqId, dailyPnL, unrealizedPnL, realizedPnL):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        dictionary = {
            "ReqId": reqId,
            "DailyPnL": dailyPnL,
            "UnrealizedPnL": unrealizedPnL,
            "RealizedPnL": realizedPnL,
        }
        self.pnl_summary = self.pnl_summary.append(dictionary, ignore_index=True)

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


def websocket_con():
    app4.run()


def get_acc_summary(account_number, connection_port):
    global app4, acc_summ_df

    # acc_summ_dict = { "NetLiquidation":"","AvailableFunds":"","GrossPositionValue":"","MaintMarginReq":"","BuyingPower":""}
    acc_summ_dict = {}

    app4 = TradingApp()

    app4.connect("127.0.0.1", connection_port, clientId=4)

    if not app4.isConnected():

        print(
            f"\n{time_str()} - Client 4 cannot establish TWS connection to check for the account summary"
        )
        acc_summ_dict = {}

    else:

        print(
            f"\n{time_str()} - Client 4 established TWS connection to check for the account summary"
        )

        # starting a separate daemon thread to execute the websocket connection
        con_thread = threading.Thread(target=websocket_con, daemon=True)
        con_thread.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app4.reqAccountSummary(
            401,
            "All",
            "NetLiquidation,AvailableFunds,GrossPositionValue,MaintMarginReq,BuyingPower",
        )
        # app4.reqAccountSummary(1, "All", "$LEDGER:ALL")
        # app4.reqAccountSummary(1, "All", "$LEDGER:CAD")
        time.sleep(2)
        acc_summ_df_1 = app4.acc_summary
        acc_summ_df = acc_summ_df_1.loc[acc_summ_df_1["Account"] == account_number]

        # GET ACC DETAILS
        acc_series1 = acc_summ_df["Tag"]  # convert DF column to series
        # acc_series1_1 = acc_series1.apply(str) #convert series values into strings

        acc_series2 = acc_summ_df["Value"]  # convert DF column to series
        # acc_series2_2 = acc_series2.apply(str) #convert series values into strings

        # convert DF to DICT (values are strings)
        for index, value in acc_series1.items():
            acc_summ_dict[value] = acc_series2[index]

        time.sleep(0.5)
        app4.cancelAccountSummary(401)

        # close socket
        app4._socketShutdown()
        time.sleep(0.5)

        app4.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(
            f"\n{time_str()} - TWS disconnected after checking for the account summary"
        )

    return acc_summ_dict


def get_acc_summary_with_pnl(account_number, connection_port):
    global app4

    # acc_summ_dict = { "NetLiquidation":"","AvailableFunds":"","GrossPositionValue":"","MaintMarginReq":"","BuyingPower":""}
    acc_summ_dict = {}

    app4 = TradingApp()

    app4.connect("127.0.0.1", connection_port, clientId=4)

    if not app4.isConnected():

        print(
            f"\n{time_str()} - Client 4 cannot establish TWS connection to check for the account summary"
        )
        acc_summ_dict = {}

    else:

        print(
            f"\n{time_str()} - Client 4 established TWS connection to check for the account summary"
        )

        # starting a separate daemon thread to execute the websocket connection
        con_thread = threading.Thread(target=websocket_con, daemon=True)
        con_thread.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app4.reqAccountSummary(
            402,
            "All",
            "NetLiquidation,AvailableFunds,GrossPositionValue,MaintMarginReq,BuyingPower",
        )
        # app4.reqAccountSummary(1, "All", "NetLiquidation,AvailableFunds,GrossPositionValue,MaintMarginReq,BuyingPower")
        # app4.reqAccountSummary(1, "All", "$LEDGER:ALL")
        # app4.reqAccountSummary(1, "All", "$LEDGER:CAD")
        time.sleep(2)
        acc_summ_df_1 = app4.acc_summary
        acc_summ_df = acc_summ_df_1.loc[acc_summ_df_1["Account"] == account_number]

        # GET ACC DETAILS
        acc_series1 = acc_summ_df["Tag"]  # convert DF column to series
        # acc_series1_1 = acc_series1.apply(str) #convert series values into strings

        acc_series2 = acc_summ_df["Value"]  # convert DF column to series
        # acc_series2_2 = acc_series2.apply(str) #convert series values into strings

        # convert DF to DICT (values are strings)
        for index, value in acc_series1.items():
            # acc_summ_dict[value] = '{:,.2f}'.format(round(float(acc_series2[index])))
            acc_summ_dict[value] = "{:,.0f}".format(round(float(acc_series2[index])))

        app4.reqPnL(403, account_number, "")

        time.sleep(1)
        global pnl_summ_df
        pnl_summ_df = app4.pnl_summary

        if not pnl_summ_df.empty:
            acc_summ_dict["DailyPnL"] = "{:,.1f}".format(
                round(float(pnl_summ_df.iloc[0]["DailyPnL"]), 1)
            )
            acc_summ_dict["UnrealizedPnL"] = "{:,.1f}".format(
                round(float(pnl_summ_df.iloc[0]["UnrealizedPnL"]), 1)
            )
            acc_summ_dict["RealizedPnL"] = "{:,.1f}".format(
                round(float(pnl_summ_df.iloc[0]["RealizedPnL"]), 1)
            )

        time.sleep(0.5)
        app4.cancelAccountSummary(402)
        app4.cancelPnL(403)
        # app2.cancelPnLSingle(3)

        # close socket
        app4._socketShutdown()
        time.sleep(0.5)

        app4.disconnect()

        # The following join will wait for the thread to end
        con_thread.join()

        print(
            f"\n{time_str()} - TWS disconnected after checking for the account summary"
        )

    return acc_summ_dict

# edit & use if you want to send and save the data as a json file
# needs Server_URL_Update and necessary coding on the server side
def update_acc_pnl_as_json(Server_URL_PNL, account_number, connection_port):
    global json_dic

    pos_dic = get_acc_summary_with_pnl(account_number, connection_port)

    date_format = "%Y-%m-%d %H:%M:%S"
    date_now = datetime.now(tz=pytz.utc)
    date_now_formatted = date_now.strftime(date_format)  # format as string

    pos_dic["UpdateTime"] = date_now_formatted

    json_dic = json.dumps(pos_dic)

    response = requests.post(Server_URL_PNL, json=json_dic)

    if response:
        print(f"\n{time_str()} - Account summary json file sent to server")


def post_acc_pnl(PASSPHRASE, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT):

    global pnl_dic # used to see in the varibale explorer of spyder

    pnl_dic = get_acc_summary_with_pnl(ACCOUNT_NUMBER, CONNECTION_PORT)

    if bool(pnl_dic):

            send_data = {
                "passphrase":PASSPHRASE,
                "AvailableFunds": float(pnl_dic["AvailableFunds"].replace(',','')),
                "BuyingPower": float(pnl_dic["BuyingPower"].replace(',','')),
                "DailyPnL": float(pnl_dic["DailyPnL"].replace(',','')),
                "GrossPositionValue": float(pnl_dic["GrossPositionValue"].replace(',','')),
                "MaintMarginReq": float(pnl_dic["MaintMarginReq"].replace(',','')),
                "NetLiquidation": float(pnl_dic["NetLiquidation"].replace(',','')),
                "RealizedPnL": float(pnl_dic["RealizedPnL"].replace(',','')),
                "UnrealizedPnL": float(pnl_dic["UnrealizedPnL"].replace(',',''))           
            }

            try:
                response = requests.post(API_PUT_PNL, json=send_data, timeout=10)
    
                if response.status_code == 201:
                    print(f"\n{time_str()} - account summary is posted")
                else:
                    print(
                        f"\n{time_str()} - an error occurred posting the account summary"
                    )
                    #logger.error(f"an error occurred updating the symbol {s} with price:{p}")
                    
            except requests.Timeout:
                # back off and retry
                print(f"\n{time_str()} - timeout error")
                pass

            except requests.ConnectionError:
                print(f"\n{time_str()} - connection error")
                pass
    else:

        print(f"\n{time_str()} - account summary is empty")
        # logger.info('account summary is empty"')

def update_acc_pnl(PASSPHRASE, API_GET_PNL, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT):

    global pnl_recent_dic, pnl_dic # used to see in the varibale explorer of spyder

    try:
        print(f"\n{time_str()} - getting the latest PNL record")
        response = requests.get(API_GET_PNL, timeout=5)
        response_list_dic = response.json()["signals"]  # list of dic from json

        if response_list_dic:
            # get the most recent PNL record
            pnl_recent_dic = response_list_dic[0]
    
            # get the latest row id
            rowid = pnl_recent_dic["rowid"]
            
            date_format = "%Y-%m-%d %H:%M:%S"
            date_now = datetime.now(tz=pytz.utc)
            date_now_formatted = date_now.strftime(date_format)  # format as string
           
            pnl_dic = get_acc_summary_with_pnl(ACCOUNT_NUMBER, CONNECTION_PORT)
        
            if bool(pnl_dic):
        
                    send_data = {
                        "passphrase":PASSPHRASE,
                        "rowid":int(rowid),
                        "timestamp":date_now_formatted,
                        "AvailableFunds": float(pnl_dic["AvailableFunds"].replace(',','')),
                        "BuyingPower": float(pnl_dic["BuyingPower"].replace(',','')),
                        "DailyPnL": float(pnl_dic["DailyPnL"].replace(',','')),
                        "GrossPositionValue": float(pnl_dic["GrossPositionValue"].replace(',','')),
                        "MaintMarginReq": float(pnl_dic["MaintMarginReq"].replace(',','')),
                        "NetLiquidation": float(pnl_dic["NetLiquidation"].replace(',','')),
                        "RealizedPnL": float(pnl_dic["RealizedPnL"].replace(',','')),
                        "UnrealizedPnL": float(pnl_dic["UnrealizedPnL"].replace(',',''))           
                    }

                    response = requests.put(API_PUT_PNL, json=send_data, timeout=10)
        
                    if response.status_code == 200:
                        print(f"\n{time_str()} - account summary is updated")
                    else:
                        print(
                            f"\n{time_str()} - an error occurred updating the account summary"
                        )
                        #logger.error(f"an error occurred updating the symbol {s} with price:{p}")
            else:
        
                print(f"\n{time_str()} - account summary is empty")
                # logger.info('account summary is empty"')

    except requests.Timeout:
        # back off and retry
        print(f"\n{time_str()} - timeout error")
        pass

    except requests.ConnectionError:
        print(f"\n{time_str()} - connection error")
        pass

#ENABLE TO TEST:

import os

print("check if path is correct:", os.getcwd())
import configparser

config = configparser.ConfigParser()
config.read("config_private.ini")
environment = config.get("environment", "ENV")
ACCOUNT_NUMBER = config.get(environment, "ACCOUNT_NUMBER")
CONNECTION_PORT = int(config.get(environment, "CONNECTION_PORT"))
API_PUT_PNL = config.get(environment, "API_PUT_PNL")
API_GET_PNL = config.get(environment, "API_GET_PNL")
PASSPHRASE = config.get(environment, "PASSPHRASE")

# account_summary_dict = get_acc_summary(
#     ACCOUNT_NUMBER, CONNECTION_PORT
# )  # get the acc summary as dict
# account_summary_pnl_dict = get_acc_summary_with_pnl(
#     ACCOUNT_NUMBER, CONNECTION_PORT
# )  # get the pnl as df

# post_acc_pnl(PASSPHRASE, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT)

update_acc_pnl(PASSPHRASE, API_GET_PNL, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT)

