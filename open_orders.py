from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
from datetime import datetime
import pandas as pd
import socket


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.order_df = pd.DataFrame(
            columns=[
                "PermId",
                "ClientId",
                "OrderId",
                "Account",
                "Symbol",
                "SecType",
                "Exchange",
                "Action",
                "OrderType",
                "TotalQty",
                "CashQty",
                "LmtPrice",
                "AuxPrice",
                "Status",
            ]
        )
        self.status_order_df = pd.DataFrame(
            columns=[
                "OrderStatusId",
                "Status",
                "Filled",
                "Remaining",
                "AvgFillPrice",
                "PermId",
                "ParentId",
                "LastFillPrice",
                "ClientId",
                "WhyHeld",
                "MktCapPrice",
            ]
        )

    def error(self, reqId, errorCode, errorString):
        pass
        # print(f'\n{time_str()} - Client-2 is active')
        # print("Error {} {} {}".format(reqId,errorCode,errorString))

    def openOrder(self, orderId, contract, order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        dictionary = {
            "PermId": order.permId,
            "ClientId": order.clientId,
            "OrderId": orderId,
            "Account": order.account,
            "Symbol": contract.symbol,
            "SecType": contract.secType,
            "Exchange": contract.exchange,
            "Action": order.action,
            "OrderType": order.orderType,
            "TotalQty": order.totalQuantity,
            "CashQty": order.cashQty,
            "LmtPrice": order.lmtPrice,
            "AuxPrice": order.auxPrice,
            "Status": orderState.status,
        }
        self.order_df = self.order_df.append(dictionary, ignore_index=True)

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        super().orderStatus(
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )
        status_dictionary = {
            "OrderStatusId": orderId,
            "Status": status,
            "Filled": filled,
            "Remaining": remaining,
            "AvgFillPrice": avgFillPrice,
            "PermId": permId,
            "ParentId": parentId,
            "LastFillPrice": lastFillPrice,
            "ClientId": clientId,
            "WhyHeld": whyHeld,
            "MktCapPrice": mktCapPrice,
        }
        self.status_order_df = self.status_order_df.append(
            status_dictionary, ignore_index=True
        )

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
    app2.run()


# TODO: error handling and logging
def get_all_orders(connection_port):
    global app2

    app2 = TradingApp()

    app2.connect("127.0.0.1", connection_port, clientId=2)

    if not app2.isConnected():

        print(
            f"\n{time_str()} - Client 2 cannot establish TWS connection to check for the open orders"
        )
        position_df = app2.order_df

    else:

        print(
            f"\n{time_str()} - Client 2 established TWS connection to check for the open orders"
        )

        # starting a separate daemon thread to execute the websocket connection
        con_thread2 = threading.Thread(target=websocket_con, daemon=True)
        con_thread2.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app2.reqAllOpenOrders()
        # app.reqAutoOpenOrders(True) # should use with clientId=0
        # app.reqOpenOrders() # does not consider manually submitted orders
        time.sleep(1)
        position_df = app2.order_df

        # close socket
        app2._socketShutdown()
        time.sleep(0.5)
        app2.disconnect()

        # The following join will wait for the thread to end
        con_thread2.join()

        print(f"\n{time_str()} - TWS disconnected after checking for the open orders")

    return position_df


def get_all_status(connection_port):
    global app2

    app2 = TradingApp()

    app2.connect("127.0.0.1", connection_port, clientId=2)

    if not app2.isConnected():

        print(
            f"\n{time_str()} - Client 2 cannot establish TWS connection to check for the open order status"
        )
        status_df = app2.status_order_df

    else:

        print(
            f"\n{time_str()} - Client 2 established TWS connection to check for the open order status"
        )

        # starting a separate daemon thread to execute the websocket connection
        con_thread2 = threading.Thread(target=websocket_con, daemon=True)
        con_thread2.start()
        time.sleep(
            0.5
        )  # some latency added to ensure that the connection is established

        app2.reqAllOpenOrders()
        # app.reqAutoOpenOrders(True) # should use with clientId=0
        # app.reqOpenOrders() # does not consider manually submitted orders
        time.sleep(1)
        status_df = app2.status_order_df

        # close socket
        app2._socketShutdown()
        time.sleep(0.5)
        app2.disconnect()

        # The following join will wait for the thread to end
        con_thread2.join()

        print(
            f"\n{time_str()} - TWS disconnected after checking for the open order status"
        )

    return status_df


def get_order_position(ticker, connection_port):
    order_df = get_all_orders(connection_port)

    search1 = order_df.loc[order_df["Symbol"] == ticker]
    search1_act = search1["Action"]
    search1_qty = search1["TotalQty"]  # dataframe to series

    search1_qty_list = search1_qty.values.tolist()  # series to list
    search1_act_list = search1_act.values.tolist()

    if search1_qty_list and search1_act_list:
        for index in range(len(search1_qty_list)):
            # print(index)
            if search1_act_list[index] == "SELL":  # check for negatives
                search1_qty_list[index] = -1 * search1_qty_list[index]

        pos_value = sum(search1_qty_list)

    else:
        pos_value = 0

    return pos_value


def get_order_position_except_manual(ticker, connection_port):
    order_df = get_all_orders(connection_port)

    search1 = order_df.loc[order_df["Symbol"] == ticker]
    search1_orderId = search1["OrderId"]
    search1_act = search1["Action"]
    search1_qty = search1["TotalQty"]  # dataframe to series

    search1_qty_list = search1_qty.values.tolist()  # series to list
    search1_act_list = search1_act.values.tolist()
    search1_orderId_list = search1_orderId.values.tolist()  # series to list

    sum_list = []

    if search1_qty_list and search1_act_list:
        for index in range(len(search1_qty_list)):
            if not search1_orderId_list[index] == 0:
                if search1_act_list[index] == "SELL":  # check for negatives
                    sum_list.append(-1 * search1_qty_list[index])
                else:
                    sum_list.append(search1_qty_list[index])

        pos_value = sum(sum_list)

    else:
        pos_value = 0

    return pos_value


def get_order_status(order_id, connection_port):

    status_df = get_all_status(connection_port)

    search1 = status_df.loc[status_df["OrderStatusId"] == order_id]

    search1_remaining = search1["Remaining"]  # dataframe to series
    search1_remaining_list = search1_remaining.values.tolist()

    search1_filled = search1["Filled"]  # dataframe to series
    search1_filled_list = search1_filled.values.tolist()

    search1_avgprice = search1["AvgFillPrice"]  # dataframe to series
    search1_avgprice_list = search1_avgprice.values.tolist()

    remaining_and_filled = [
        search1_remaining_list[0],
        search1_filled_list[0],
        search1_avgprice_list[0],
    ]

    # returns a list [remaining contacts, filled contracts, avg_price of filled order]
    return remaining_and_filled


def get_order_id(ticker, connection_port):
    order_df = get_all_orders(connection_port)

    search1 = order_df.loc[order_df["Symbol"] == ticker]

    search1_orderId = search1["OrderId"]

    search1_orderId_list = search1_orderId.values.tolist()  # series to list

    return search1_orderId_list


def get_order_ticker(orderid, connection_port):
    order_df = get_all_orders(connection_port)

    search1 = order_df.loc[order_df["OrderId"] == orderid]

    search1_symbol = search1["Symbol"]

    search1_symbol_list = search1_symbol.values.tolist()  # series to list

    return search1_symbol_list


# ENABLE TO TEST
# import os
# print("check if path is correct:", os.getcwd())
# import configparser
# config = configparser.ConfigParser()
# config.read('config_private.ini')
# environment = config.get('environment', 'ENV')
# connection_port = int(config.get(environment, 'CONNECTION_PORT'))
#
# order_id = get_order_id("AAPL",connection_port)
# if order_id:
#     remaining_value = get_order_status(get_order_id("NMFC",connection_port)[0],connection_port)

# status_df = get_all_status(connection_port)

# order_ticker = get_order_ticker(0,connection_port)
