# import from TWS API
from ibapi.client import EClient  # needs to be inherited
from ibapi.wrapper import EWrapper  # needs to be inherited
from ibapi.contract import Contract
from ibapi.order import Order

# import custom code
from filled_orders import update_filled_orders
from acc_summary import get_acc_summary, update_acc_pnl, post_acc_pnl
from current_positions import get_position, update_positions
from open_orders import get_order_position_except_manual, get_order_id, get_order_status
from dublicate_orders import get_dublicate_orders

# import others
import threading
import time
from datetime import datetime
import pandas as pd
import socket
import asyncio, requests
import nest_asyncio  # patches asyncio to allow nested use of asyncio
import logging.handlers
from pathlib import Path
import configparser


# inherit EWrapper and EClient classes, and edit instance methods
class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.errors_received = pd.DataFrame(
            columns=["reqId", "errorCode", "errorString"]
        )
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

    def error(self, reqId, errorCode, errorString):
        print("Error {} {} {}".format(reqId, errorCode, errorString))
        logger.error("Error {} {} {}".format(reqId, errorCode, errorString))
        self.errors_received.loc[len(self.errors_received)] = [
            reqId,
            errorCode,
            errorString,
        ]

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        # print("NextValidId:", orderId)
        self.nextValidOrderId = orderId

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

    # socket function to handle winerror 1003 problem
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
    app.run()


# creating object of the Contract class for US stocks
# SMART is selected as default, but can be problematic with some certain stocks
# In case of ERROR 200 order should be rerouted to another exchange (exp: ISLAND)
def contractIB(
    symbol, sec_type="STK", currency="USD", exchange="SMART", primaryExchange="ISLAND"
):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    # Specify the Primary Exchange attribute to avoid contract ambiguity
    contract.primaryExchange = primaryExchange
    return contract


# creating object of the limit order class
# not used at the moment, kept for later use
def limitOrder(direction, quantity, lmt_price, tif="DAY"):
    order = Order()
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = lmt_price
    order.account = ACCOUNT_NUMBER  # Edit for other accounts
    order.tif = tif
    return order


# creating object of the market order class
def marketOrder(direction, quantity, tif="DAY"):
    order = Order()
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    order.account = ACCOUNT_NUMBER  # Edit for other accounts
    order.tif = tif
    return order


# creating object of the relative order class
def relativeOrder(direction, quantity, tif="DAY"):
    order = Order()
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.action = direction
    order.orderType = "REL"  # https://www.interactivebrokers.com/en/index.php?f=613
    order.totalQuantity = quantity
    order.account = ACCOUNT_NUMBER
    order.tif = tif

    # you can define an absolute cap that works like a limit price,
    # and will prevent your order from being executed above or below a specified price level
    # Orders with a "0" offset are submitted as limit orders at the best bid/ask
    # and will move up and down with the market to continue to match the inside quote.
    # order.auxPrice = offsetAmount
    order.auxPrice = "0"
    # enable if you want to bid/offer more aggresive price(such as 0.2% better than best bid/offer)
    # order.percentOffset = "0.002"
    return order


# TODO: problem creating this type of order. Gettin Error 387
# creating object of the relative passive order class
# Passive Relative orders provide a means for traders to seek a less aggressive price than the National Best Bid and Offer (NBBO)
# The Passive Relative order is similar to the Relative/Pegged-to-Primary order,
# except that the Passive relative subtracts the offset from the bid and the Relative adds the offset to the bid.
def relativepassiveOrder(direction, quantity):
    order = Order()
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.action = direction
    order.orderType = "PASSV REL"
    order.totalQuantity = quantity
    order.account = ACCOUNT_NUMBER
    order.auxPrice = "0"
    # order.percentOffset = "0.002"
    return order


# TODO: has to set a limit price for this type of order
def peggedtomidOrder(direction, quantity, limitPrice):
    order = Order()
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.action = direction
    order.orderType = "PEG MID"
    order.totalQuantity = quantity
    order.account = ACCOUNT_NUMBER
    order.auxPrice = "0"
    order.lmtPrice = limitPrice

    return order


# Create a function to simplify order creation
def MyOrder(**order_details):
    print(order_details["direction"].upper())
    print(order_details["order_quantity"])
    print(order_details["order_type"])

    order_direction = order_details["direction"].upper()
    order_quantity = order_details["order_quantity"]
    order_type = order_details["order_type"]

    # relative order as default
    my_order = relativeOrder(order_direction, abs(order_quantity))

    if order_type == "MARKET":
        my_order = marketOrder(order_direction, abs(order_quantity))
    elif order_type == "LIMIT" and "limit_price" in order_details:
        order_price = order_details["limit_price"]
        print(order_details["limit_price"])
        my_order = limitOrder(order_direction, abs(order_quantity), order_price)

    return my_order


# main function to check webhooks and send orders.
# get waiting trade signals dictionary from the web backend.
# loosely coupled architecture is used, when server app is down or there is no connection to it
# ,then the trading signals start waiting in a 'message queue'
# waiting signals are handled with LIFO method.
async def check_signals():
    # define global variables
    global app

    # print checking message and time
    print(f"\n{time_str()} - checking for tradingview webhook signals")

    try:
        response = requests.get(API_GET_SIGNAL_WAITING, timeout=5)
        response_list_dic = response.json()[
            "signals"
        ]  # creates a list of dictionaries from json

    except requests.Timeout:
        # back off and retry
        print(f"\n{time_str()} - timeout error")
        pass

    except requests.ConnectionError:
        print(f"\n{time_str()} - connection error")
        pass

    if response_list_dic:

        print(f"\n{time_str()} - order(waiting or rerouted) received from the server")
        logger.info("order(waiting or rerouted) received from the server")

        # define variables to update in the loop
        trade_confirm = True
        cancel_order = False
        status_msg = ""
        synced_order = False

        # 1-check if there are more than one orders waiting for the same ticker

        duplicate_orders = get_dublicate_orders(response_list_dic)

        if duplicate_orders:
            print(f"\n{time_str()} - duplicate orders detected")
            logger.warning("duplicate orders detected")

            # cancel duplicate waiting orders, keep the most recent
            for i in range(len(duplicate_orders)):

                api_url = API_GET_SIGNAL + str(duplicate_orders[i])
                try:
                    signal_dic = requests.get(api_url, timeout=5).json()
                except requests.Timeout:
                    print(f"\n{time_str()} - timeout error (dub. order)")
                    pass
                except requests.ConnectionError:
                    print(f"\n{time_str()} - connection error (dub. order)")
                    pass

                signal_dic["passphrase"] = PASSPHRASE
                signal_dic["order_status"] = "canceled"
                signal_dic["status_msg"] = "duplicate order"

                response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                if response.status_code == 200:
                    print(
                        f"\n{time_str()} - duplicate order information updated for rowid:",
                        str(duplicate_orders[i]),
                    )
                    logger.info(
                        f"duplicate order information updated for rowid:{duplicate_orders[i]}"
                    )
                else:
                    print(
                        f"\n{time_str()}- duplicate order information update problem; code:",
                        response.status_code,
                    )
                    logger.error(
                        f"duplicate order information update problem; code:{response.status_code}"
                    )

        # 1-continue if no duplicate tickers
        else:
            # get the first available ticker dictionary in the waiting list
            signal_dic = response_list_dic[0]

            # assign dic values
            order_status = signal_dic["order_status"]
            rowid = signal_dic["rowid"]
            order_action = signal_dic["order_action"]
            order_contracts = signal_dic["order_contracts"]
            hedge_param = signal_dic["hedge_param"]
            new_order_action = "none"

            # negate short positions
            if signal_dic["mar_pos"] == "short":
                mar_pos_size = -signal_dic["mar_pos_size"]
            else:
                mar_pos_size = signal_dic["mar_pos_size"]

            if signal_dic["pre_mar_pos"] == "short":
                pre_mar_pos_size = -signal_dic["pre_mar_pos_size"]
            else:
                pre_mar_pos_size = signal_dic["pre_mar_pos_size"]

            print(f'\n{time_str()} - received signal (Equation:{signal_dic["ticker"]})')
            logger.info(f'received signal (Equation:{signal_dic["ticker"]})')

            # 2-check pyramiding (assumes fixed order size) and order validity
            # pyramiding is not allowed, only one position is allowed per ticker
            # order position sizes should also be valid

            # check rare condition (if waiting order created due to sync)
            if signal_dic["status_msg"] == "created due to sync":

                status_msg = "+synced(new order)"

                cancel_order = False

                synced_order = True

            else:
                # check if order positions have value or not
                if [
                    x
                    for x in (
                        mar_pos_size,
                        pre_mar_pos_size,
                        mar_pos_size,
                        pre_mar_pos_size,
                    )
                    if x is None
                ]:

                    print(f"\n{time_str()} - order positions size is missing")
                    logger.warning(f"order position size is missing")

                    cancel_order = True

                    status_msg = "missing position size"

                # check if positions add up
                if (
                    signal_dic["order_action"] == "buy"
                    and mar_pos_size != pre_mar_pos_size + order_contracts
                ) or (
                    signal_dic["order_action"] == "sell"
                    and mar_pos_size != pre_mar_pos_size - order_contracts
                ):

                    print(f"\n{time_str()} - order positions mismatch")
                    logger.warning(f"order positions mismatch")

                    cancel_order = True

                    status_msg = "position size mismatch"

                # check pyramiding
                elif (mar_pos_size > 0 and pre_mar_pos_size > 0) or (
                    mar_pos_size < 0 and pre_mar_pos_size < 0
                ):

                    cancel_order = True

                    status_msg = "pyramiding"

            # cancel order if true
            if cancel_order:

                trade_confirm = False

                signal_dic["passphrase"] = PASSPHRASE
                signal_dic["order_status"] = "canceled"
                signal_dic["status_msg"] = status_msg

                response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                if response.status_code == 200:
                    print(
                        f"\n{time_str()} - order information ({status_msg}) updated for rowid:",
                        rowid,
                    )
                    logger.info(
                        f"order information ({status_msg}) updated for rowid:{rowid}"
                    )
                else:
                    print(
                        f"\n{time_str()}- order information ({status_msg}) update problem; code:",
                        response.status_code,
                    )
                    logger.error(
                        f"order information ({status_msg}) updated problem; code:{response.status_code}"
                    )

            # 3-check active orders
            # only one active order is allowed at once, cancel the others
            # manual orders (with IB API order ID 0) cannot be modified/cancelled from the IB API

            if trade_confirm:

                # TODO: check this rare error
                # Error 2718 10148 OrderId 2718 that needs to be cancelled cannot be cancelled, state: PendingCancel.
                # Active order canceled due to multiple active orders, ID: 2710

                # get the list of active orders with the same ticker
                order_id_list = get_order_id(signal_dic["ticker1"], CONNECTION_PORT)

                # TODO: can you get the order id list for the order created for SYNC_PAIR and then cancel?
                if len(order_id_list) > 0:

                    print(f"\n{time_str()} - Multiple active order detected")
                    logger.info("Multiple active order detected")

                    # IB connection parameters
                    app = TradingApp()
                    app.connect("127.0.0.1", CONNECTION_PORT, clientId=1)

                    # starting a separate daemon thread to execute the websocket connection
                    con_thread = threading.Thread(target=websocket_con, daemon=True)
                    con_thread.start()
                    time.sleep(
                        0.5
                    )  # some latency added to ensure that the connection is established

                    if not app.isConnected():
                        print(
                            f"\n{time_str()} - Client 1 cannot establish TWS connection to cancel orders"
                        )
                        logger.warning(
                            "Client 1 cannot establish TWS connection to cancel orders"
                        )

                    else:
                        print(
                            f"\n{time_str()} - Client 1 established TWS connection to cancel orders"
                        )
                        logger.info(
                            "Client 1 established TWS connection to cancel orders"
                        )

                        # check for active orders before sending new order
                        for index in range(len(order_id_list)):

                            # If there are partially filled orders, get_order_status as a list of the following:
                            # order_status_list[0] = remaining contacts
                            # order_status_list[1] = filled contracts
                            # order_status_list[2] = avg_price of filled order
                            orderid_to_cancel = order_id_list[index]
                            order_status_list = get_order_status(
                                int(orderid_to_cancel), CONNECTION_PORT
                            )
                            if signal_dic["ticker_type"] == "pair":
                                orderid2_to_cancel = (
                                    order_id_list[index] + 1
                                )  # next order ID as the child
                                order_status2_list = get_order_status(
                                    int(orderid2_to_cancel), CONNECTION_PORT
                                )

                            print(
                                f"\n{time_str()} - Order amount to cancel(ticker1): ",
                                str(order_status_list[0]),
                            )
                            print(
                                f"\n{time_str()} - Order amount filled (ticker1): ",
                                str(order_status_list[1]),
                            )
                            logger.info(
                                f"Order amount to cancel(ticker1):{order_status_list[0]}"
                            )
                            logger.info(
                                f"Order amount filled(ticker1): {order_status_list[1]}"
                            )
                            if signal_dic["ticker_type"] == "pair":
                                print(
                                    f"\n{time_str()} - Order amount to cancel(ticker2): ",
                                    str(order_status2_list[0]),
                                )
                                print(
                                    f"\n{time_str()} - Order amount filled(ticker2): ",
                                    str(order_status2_list[1]),
                                )
                                logger.info(
                                    f"Order amount to cancel(ticker2:{order_status2_list[0]}"
                                )
                                logger.info(
                                    f"Order amount filled(ticker2): {order_status2_list[1]}"
                                )

                            # cancel old active order before sending new order
                            app.cancelOrder(orderid_to_cancel)
                            time.sleep(
                                0.5
                            )  # some latency added to ensure that the connection is established

                            # update database with partially filled orders if SYNC_PAIR is NOT active

                            # TODO: look for a better way of implementing this.
                            # if SYNC_PAIR is on create related child orders and update prices accordingly.
                            # this loop assumes that the second order(ticker2) of the pair is a market order,
                            # when market order is used, realizations for the first and second order are always propotional.
                            # when SYNC_PAIR is on, it is high likely that relative type order is used for both tickers
                            # and realizations may not be propotional

                            if order_status_list[1] > 0 and not SYNC_PAIR:

                                array_orderid = [int(orderid_to_cancel)]
                                array_avgprice = [order_status_list[2]]

                                if signal_dic["ticker_type"] == "pair":
                                    array_orderid.append(int(orderid2_to_cancel))
                                    array_avgprice.append(order_status2_list[2])

                                # save partially filled order prices
                                # zip() with n arguments returns an iterator that generates tuples of length n
                                # TODO: test more for the partially filled orders with TWS (for single and pairs)
                                for o, p in zip(array_orderid, array_avgprice):
                                    print(
                                        f"\n{time_str()} - updating order:{o} with price:{p}"
                                    )
                                    logger.info(f"updating order:{o} with price:{p}")
                                    time.sleep(0.5)

                                    # updating partially filled orders
                                    # (!) assumes that the partially filled order keeps the hedge ratio
                                    send_data = {
                                        # update fill prices
                                        "passphrase": PASSPHRASE,
                                        "price": round(float(p), 2),
                                        "order_id": int(o),
                                        # indicate that this is a partial fill
                                        "partial": True,
                                        # change contract amount to filled amount
                                        "order_contracts": abs(
                                            int(order_status_list[1])
                                        ),
                                    }

                                    response = requests.put(
                                        API_PUT_UPDATE, json=send_data
                                    )

                                    if response.status_code == 200:
                                        print(f"\n{time_str()} - order {o} is updated")
                                    else:
                                        print(
                                            f"\n{time_str()} - an error occurred updating the order {o}"
                                        )
                                        logger.error(
                                            f"an error occurred updating the order {o}"
                                        )

                            # if cancelling all amount, not partially filled
                            else:
                                # updating canceled order status
                                send_data = {
                                    # update fill prices
                                    "passphrase": PASSPHRASE,
                                    "order_id": orderid_to_cancel,
                                    # indicate that this order is canceled
                                    "cancel": True,
                                }

                                response = requests.put(API_PUT_UPDATE, json=send_data)

                                if response.status_code == 200:
                                    print(
                                        f"\n{time_str()} - Active order canceled due to multiple active orders, ID: ",
                                        orderid_to_cancel,
                                    )
                                    logger.info(
                                        f"Active order canceled due to multiple active orders, ID: {orderid_to_cancel}"
                                    )
                                else:
                                    print(
                                        f"\n{time_str()} - an error occurred updating the order ID {orderid_to_cancel}"
                                    )
                                    logger.error(
                                        f"an error occurred updating the order ID {orderid_to_cancel}"
                                    )

                        # close socket connection
                        app._socketShutdown()
                        time.sleep(0.5)
                        app.disconnect()

                        # the following join will wait for the thread to end
                        con_thread.join()
                        print(
                            f"\n{time_str()} - TWS disconnected after processing orders"
                        )
                        logger.info("TWS disconnected after processing orders")

                # 4-sync market position with the latest valid order data (sync to mar_pos_size)
                # attention: manual orders are not taken into account

                # calculate expected ticker position with active orders and before new(current) order is sent
                # (i.e. the expected previous position)

                # get current ticker portfolio position
                pos_ticker1 = get_position(signal_dic["ticker1"], CONNECTION_PORT)

                # get open order position
                # this is a precaution: active orders should already be canceled in the previous step
                order_pos_ticker1 = get_order_position_except_manual(
                    signal_dic["ticker1"], CONNECTION_PORT
                )

                # expected portfolio with active orders (without considering new(current) order)
                expected_pre_mar_pos_size = order_pos_ticker1 + pos_ticker1

                # keep this to be used later
                old_order_contracts = order_contracts

                # compare calculation to ticker's previous position,
                # then sync according to market position
                if expected_pre_mar_pos_size != pre_mar_pos_size:

                    # set current order to sync with the latest signal 'mar_pos_size'
                    order_contracts = int(mar_pos_size - expected_pre_mar_pos_size)
                    print(
                        f"\n{time_str()} - order contract amount changed due to sync: ",
                        order_contracts,
                    )
                    logger.info(
                        f"order contract amount changed due to sync: {order_contracts}"
                    )

                    # prepare status msg for sync
                    status_msg = (
                        "+synced("
                        + str(old_order_contracts)
                        + "->"
                        + str(order_contracts)
                        + ")"
                    )

                    if order_contracts < 0:
                        order_action = "sell"
                    elif order_contracts > 0:
                        order_action = "buy"
                    else:
                        # no order if zero
                        trade_confirm = False

                        # mark active order as canceled due to sync
                        signal_dic["passphrase"] = PASSPHRASE
                        signal_dic["order_status"] = "canceled"
                        signal_dic["status_msg"] = status_msg

                        response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                        if response.status_code == 200:
                            print(
                                f"\n{time_str()} - order not created due to zero order amount after sync."
                            )
                            logger.info(
                                "order not created due to zero order amount after sync."
                            )
                        else:
                            print(
                                f"\n{time_str()} - an error occurred updating status after sync."
                            )
                            logger.info("an error occurred updating status after sync.")

                # 4-sync cont.> sync second pair if SYNC_PAIR is activated

                if SYNC_PAIR and signal_dic["ticker_type"] == "pair":

                    print(f"\n{time_str()} - SYNC_PAIR is active")

                    # get second ticker portfolio position
                    pos_size_ticker2 = int(
                        get_position(signal_dic["ticker2"], CONNECTION_PORT)
                    )

                    # get open order position for second ticker
                    # this is necessary, open orders for ticker2 are not cancelled before
                    order_pos_ticker2 = get_order_position_except_manual(
                        signal_dic["ticker2"], CONNECTION_PORT
                    )

                    # calculate expected and market positions, order_contracts are always positive
                    if signal_dic["order_action"] == "buy":
                        expected_pos_ticker2 = (
                            -int(order_contracts * float(hedge_param))
                            + pos_size_ticker2
                            + order_pos_ticker2
                        )
                    else:
                        expected_pos_ticker2 = (
                            int(order_contracts * float(hedge_param))
                            + pos_size_ticker2
                            + order_pos_ticker2
                        )

                    # calculate expected and market positions, mar_pos_size is directional
                    mar_pos_size_ticker2 = -int(
                        round(mar_pos_size * float(hedge_param))
                    )

                    new_order_contracts2 = (
                        mar_pos_size_ticker2 - pos_size_ticker2 - order_pos_ticker2
                    )

                    print(
                        f"\n{time_str()} -\n pos tick2: {pos_size_ticker2},\n exp pos tick2: {expected_pos_ticker2},\n mar pos tick2: {mar_pos_size_ticker2},\n new_order_contracts2(ticker2): {new_order_contracts2}"
                    )
                    logger.info(
                        f"pos tick2: {pos_size_ticker2}, exp pos tick2: {expected_pos_ticker2}, mar pos tick2: {mar_pos_size_ticker2}, new_order_contracts2(ticker2): {new_order_contracts2}"
                    )

                    # prepare new order details
                    if mar_pos_size_ticker2 < 0:
                        new_mar_pos = "short"
                    elif mar_pos_size_ticker2 > 0:
                        new_mar_pos = "long"
                    else:
                        new_mar_pos = "flat"

                    if pos_size_ticker2 < 0:
                        new_pos = "short"
                    elif pos_size_ticker2 > 0:
                        new_pos = "long"
                    else:
                        new_pos = "flat"

                    if new_order_contracts2>=0:
                        new_order_action = "buy"
                        new_comment = "Enter Long due to sync"
                    elif new_order_contracts2<0:
                        new_order_action = "sell"
                        new_comment = "Enter Short due to sync"

                    # create a new order for ticker2 if 1st ticker has "zero" order size
                    if order_contracts == 0 and new_order_contracts2 != 0:

                        send_data = {
                            "passphrase": PASSPHRASE,
                            # bypass is needed to create an order with a ticker that is already active in a pair
                            "bypass_ticker_status": True,
                            "order_action": new_order_action,
                            "order_contracts": abs(new_order_contracts2),
                            "mar_pos": new_mar_pos,
                            "mar_pos_size": abs(mar_pos_size_ticker2),
                            "pre_mar_pos": new_pos,
                            "pre_mar_pos_size": abs(pos_size_ticker2),
                            "order_comment": new_comment,
                            "order_status": "waiting",
                            "ticker": signal_dic["ticker2"],
                            "status_msg": "created due to sync",
                        }

                        # print(send_data)

                        # create new webhook with post request
                        response = requests.post(API_PUT_SIGNAL, json=send_data)

                        # check if created successfully
                        if response.status_code == 201:
                            print(
                                f"\n{time_str()} - New webhook created due to sync for ticker: {signal_dic['ticker2']}"
                            )
                            logger.info(
                                f"New webhook created due to sync for ticker: {signal_dic['ticker2']}"
                            )
                        else:
                            print(
                                f"\n{time_str()} - an error occurred creating webhook for ticker: {signal_dic['ticker2']}"
                            )
                            logger.error(
                                f"an error occurred creating webhook for ticker: {signal_dic['ticker2']}"
                            )

                    # change hedge parameter if 1st ticker has valid order size
                    else:
                        if order_contracts != 0:                        
                            new_hedge_param = abs(
                                round(new_order_contracts2 / order_contracts, 10)
                            )  # order_contracts is always positive

                            # check if contact difference is more than a certain amount, not necessary to be zero
                            # usually 1 or 2 contracts is in acceptable range
                            if abs(expected_pos_ticker2 - mar_pos_size_ticker2) > 2:
    
                                hedge_param = new_hedge_param
    
                                print(
                                    f"\n{time_str()} - New Hedge Param to be used:",
                                    hedge_param,
                                )
                                logger.info(f"New Hedge Param to be used: {hedge_param}")
    
                            else:
                                print(f"\n{time_str()} - No Change on Hedge Param:")

            # 5-check for available funds before sending an order
            # (active orders are not taken into consideration, define fund floor considering that)
            # TODO: check if it makes sense to include a margin for active order amounts
            # TODO: config hard limit for order amount (in $), or stock contract size?

            if (
                CHECK_FUNDS_FOR_TRADE
                and trade_confirm
                and not mar_pos_size == 0
                and not order_contracts == 0
            ):

                # get the acc summary as dict
                account_summary_dict = get_acc_summary(ACCOUNT_NUMBER, CONNECTION_PORT)

                # get available funds (margin for active orders are not included)
                avab_funds = float(account_summary_dict["AvailableFunds"])

                print(f"\n{time_str()} - available funds: ", avab_funds)
                logger.info(f"available funds: {avab_funds} ")

                if avab_funds > AVAILABLE_FUND_FLOOR:

                    print(f"\n{time_str()} - trade is allowed, sufficient funds.")
                    logger.info("trade is allowed, sufficient funds.")

                else:

                    print(f"\n{time_str()} - not sufficient funds to trade!")
                    logger.warning(f"not sufficient funds to trade!")

                    trade_confirm = False

                    signal_dic["passphrase"] = PASSPHRASE
                    signal_dic["order_status"] = ("canceled",)
                    signal_dic["status_msg"] = "insufficient funds"

                    response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                    if response.status_code == 200:
                        print(
                            f"\n{time_str()} - server updated with insufficient fund information."
                        )
                        logger.info(
                            "server updated with insufficient fund information."
                        )
                    else:
                        print(
                            f"\n{time_str()} - an error occurred updating status after insufficient funds."
                        )
                        logger.info(
                            "an error occurred updating status after insufficient funds."
                        )

            # 6-send order if confirmed
            # TODO: how to filter last minute orders, if exchange is closed no error message is received
            # for exp.: cancel all open orders x min before session is closed
            # reqContractDetails(id, contract) will return a contractDetails object with tradingHours as a field.
            # https://stackoverflow.com/questions/48380455/interactive-brokers-tws-api-python-how-to-get-trading-day-info
            # - day orders submitted with “RTH ONLY” will be canceled between 4:00-4:05 pm.
            # - day orders submitted after 4:05 pm with “RTH ONLY” will be queued for the next day.

            if trade_confirm:

                # orders for pairs: 2nd order is dependent to first order
                # best practice: 1st order is relative & 2nd order is set to market order

                # IB connection parameters
                app = TradingApp()
                app.connect(
                    "127.0.0.1", CONNECTION_PORT, clientId=1
                )  # check for port number

                # if there is a critical TWS or Gateway connection error
                if not app.isConnected():
                    print(
                        f"\n{time_str()} - Client 1 cannot establish TWS connection, will try again"
                    )
                    logger.warning(
                        "Client 1 cannot establish TWS connection, will try again"
                    )

                    order_created = False

                    # not updating the server with connection errors, will try again

                else:
                    print(f"\n{time_str()} - Client 1 established TWS connection")
                    logger.info("Client 1 established TWS connection")

                    # starting a separate daemon thread to execute the websocket connection
                    con_thread = threading.Thread(target=websocket_con, daemon=True)
                    con_thread.start()
                    time.sleep(
                        1
                    )  # some latency added to ensure that the connection is established

                    # check connection error status
                    errors_con_df = app.errors_received

                    # define critical errors that blocks order creation
                    con_error_codes_array = [2110, 2157]

                    # critical errors
                    con_critical_errors_df = errors_con_df.loc[
                        errors_con_df["errorCode"].isin(con_error_codes_array)
                    ]

                    # define error boolean flags
                    critical_con_err = not con_critical_errors_df.empty

                    if critical_con_err:
                        print(
                            f"\n{time_str()} - critical errors detected during connection, will not create orders"
                        )
                        logger.warning(
                            "critical errors detected during connection, will not create orders"
                        )

                        order_created = False

                        # not updating the server with connection errors, will try again

                    else:
                        # get security and exchange data for ticker 1
                        api_url = API_GET_TICKER + str(signal_dic["ticker1"])

                        try:
                            stock_dic = requests.get(api_url, timeout=5).json()
                        except requests.Timeout:
                            print(f"\n{time_str()} - timeout error (get stock details)")
                            pass
                        except requests.ConnectionError:
                            print(
                                f"\n{time_str()} - connection error (get stock details)"
                            )
                            pass

                        order_exchange1 = stock_dic["xch"]
                        primary_exchange1 = stock_dic["prixch"]
                        sec_type1 = stock_dic["sectype"]
                        currency1 = stock_dic["currency"]
                        order_type1 = stock_dic["order_type"]

                        # prepare crypto time in force
                        if sec_type1 == "CRYPTO":
                            timeinforce1 = "IOC"

                        # prepare symbolticker 1
                        if sec_type1 == "STK":
                            symbol1 = signal_dic["ticker1"]
                        else:
                            symbolarray1 = signal_dic["ticker1"].split(".", 1)
                            symbol1 = symbolarray1[0]
                            if len(symbolarray1) > 1:
                                symbol1_2 = symbolarray1[1]
                                # DELETE: check if the FX pair is suitable to trade
                                # if sec_type1 == "CASH":
                                #     if currency1 != symbol1_2 :
                                #         print(f"\n{time_str()} -pair currencies does not match!")
                                #         logger.warning(f"pair currencies does not match!")

                                #         trade_confirm = False

                                #         signal_dic["passphrase"] = PASSPHRASE
                                #         signal_dic["order_status"] = ("canceled",)
                                #         signal_dic["status_msg"] = "currency mismatch"

                                #         response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                        # get exchange data for ticker 2
                        if signal_dic["ticker_type"] == "pair":

                            api_url = API_GET_TICKER + str(signal_dic["ticker2"])

                            try:
                                stock_dic = requests.get(api_url, timeout=5).json()
                            except requests.Timeout:
                                print(
                                    f"\n{time_str()} - timeout error (get stock details)"
                                )
                                pass
                            except requests.ConnectionError:
                                print(
                                    f"\n{time_str()} - connection error (get stock details)"
                                )
                                pass

                            order_exchange2 = stock_dic["xch"]
                            primary_exchange2 = stock_dic["prixch"]
                            sec_type2 = stock_dic["sectype"]
                            currency2 = stock_dic["currency"]
                            order_type2 = stock_dic["order_type"]

                            # prepare symbol
                            if sec_type2 == "STK":
                                symbol2 = signal_dic["ticker2"]
                            else:
                                symbolarray2 = signal_dic["ticker2"].split(".", 1)
                                symbol2 = symbolarray2[0]
                                if len(symbolarray2) > 1:
                                    symbol2_2 = symbolarray2[1]

                        # prepare order for single trading
                        if signal_dic["ticker_type"] == "single":

                            app.reqIds(-1)  # function to trigger nextValidId function
                            time.sleep(
                                0.5
                            )  # need to provide some lag for the nextValidId
                            order_id1 = app.nextValidOrderId

                            print(f"\n{time_str()} - Order ID1 is: {order_id1}")
                            logger.info(f"Order ID1 is: {order_id1}")

                            # prepare the order

                            # send with market order if rerouted
                            if order_status == "rerouted":
                                order_type1 = "MARKET"

                            # only limit order for crypto
                            if sec_type1 == "CRYPTO":
                                order_type1 = "LIMIT"

                            first_order = MyOrder(
                                direction=order_action,
                                order_type=order_type1,
                                order_quantity=order_contracts,
                                limit_price=signal_dic["order_price"],
                            )

                            # place the 1st order
                            if order_status != "rerouted":
                                app.placeOrder(
                                    order_id1,
                                    contractIB(
                                        symbol1,
                                        sec_type=sec_type1,
                                        currency=currency1,
                                        exchange=order_exchange1,
                                        primaryExchange=primary_exchange1,
                                    ),
                                    first_order,
                                )
                            else:  # forward to primary exchange with market order if rerouted
                                app.placeOrder(
                                    order_id1,
                                    contractIB(
                                        symbol1,
                                        sec_type=sec_type1,
                                        currency=currency1,
                                        exchange=primary_exchange1,
                                    ),
                                    first_order,
                                )

                            time.sleep(0.2)

                        # not able to trade cryptos in pairs at the moment due to limitations for limit orders
                        elif signal_dic["ticker_type"] == "pair" and (
                            sec_type2 == "CRYPTO" or sec_type1 == "CRYPTO"
                        ):

                            print(
                                f"\n{time_str()} -not able to use CRYPTO tickers in pairs trading!"
                            )
                            logger.warning(
                                f"not able to use CRYPTO tickers in pairs trading!"
                            )

                            trade_confirm = False

                            signal_dic["passphrase"] = PASSPHRASE
                            signal_dic["order_status"] = ("canceled",)
                            signal_dic["status_msg"] = "crypto in pairs"

                            response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                        # prepare order for pair trading
                        else:

                            app.reqIds(-1)  # function to trigger nextValidId function
                            time.sleep(
                                0.5
                            )  # need to provide some lag for the nextValidId
                            order_id1 = app.nextValidOrderId

                            print(f"\n{time_str()} - Order ID1 is: {order_id1}")
                            logger.info(f"Order ID1 is: {order_id1}")

                            # prepare 1st order

                            if order_action == "buy":
                                order_action2 = "sell"
                            else:
                                order_action2 = "buy"

                            # send with market orders if rerouted
                            if order_status == "rerouted":
                                order_type1 = "MARKET"
                                order_type2 = "MARKET"

                            first_order = MyOrder(
                                direction=order_action,
                                order_type=order_type1,
                                order_quantity=order_contracts,
                            )

                            # prepare 2nd order

                            second_order = MyOrder(
                                # check if 2nd ticker order direction has changed due to SYNC_PAIR
                                direction=order_action2
                                if (new_order_action != order_action)
                                else order_action,
                                order_type=order_type2,
                                # hedgeParam defines the quantity
                                order_quantity=0,
                            )

                            # wait for the parent ID
                            first_order.transmit = False

                            second_order.parentId = order_id1  # attach to first order
                            second_order.hedgeType = "P"  # pairs trading
                            second_order.hedgeParam = abs(
                                hedge_param
                            )  # multiplier for 2nd order

                            # place the 1st order
                            if order_status != "rerouted":
                                app.placeOrder(
                                    order_id1,
                                    contractIB(
                                        symbol1,
                                        sec_type=sec_type1,
                                        currency=currency1,
                                        exchange=order_exchange1,
                                        primaryExchange=primary_exchange1,
                                    ),
                                    first_order,
                                )
                            else:  # forward to primary exchange with market order if rerouted
                                app.placeOrder(
                                    order_id1,
                                    contractIB(
                                        symbol1,
                                        sec_type=sec_type1,
                                        currency=currency1,
                                        exchange=primary_exchange1,
                                    ),
                                    first_order,
                                )

                            time.sleep(0.2)

                            app.reqIds(-1)  # function to trigger nextValidId function
                            time.sleep(
                                0.5
                            )  # need to provide some lag for the nextValidId function to be triggered
                            order_id2 = app.nextValidOrderId
                            print(f"\n{time_str()} - Order ID2 is: {order_id2}")
                            logger.info(f"Order ID2 is: {order_id2}")

                            # place 2nd order
                            if order_status != "rerouted":
                                app.placeOrder(
                                    order_id2,
                                    contractIB(
                                        symbol2,
                                        sec_type=sec_type2,
                                        currency=currency2,
                                        exchange=order_exchange2,
                                        primaryExchange=primary_exchange2,
                                    ),
                                    second_order,
                                )
                            else:  # forward to primary exchange with market order if rerouted
                                app.placeOrder(
                                    order_id2,
                                    contractIB(
                                        symbol2,
                                        sec_type=sec_type2,
                                        currency=currency2,
                                        exchange=primary_exchange2,
                                    ),
                                    second_order,
                                )

                        time.sleep(0.2)

                        # enable to see in the variable explorer
                        global errors_df, all_errors_df, errors_df_final, reroute_errors_df_filtered, critical_errors_df_filtered, other_errors_df_filtered

                        # check and update the error status
                        errors_df = app.errors_received

                        time.sleep(0.5)

                        # define critical errors that block order creation
                        # Error 200: The contract description specified for <Symbol> is ambiguous
                        # Error 200: No security definition has been found for the request.
                        # Error 200: Invalid destination exchange specified
                        reroute_error_codes_array = [200]
                        # try to reroute once, then include error 200 in critical array
                        # Error 387: Unsupported order type for this exchange and security type.
                        # Error 320: reading request. Missing parent order
                        # Error 463: You must enter a valid price
                        # Error 321: Error validating request.-'bN' : cause - The size value cannot be zero
                        # Error 10052: Invalid time in force
                        # Error 110: The price does not conform to the minimum price variation for this contract
                        critical_error_codes_array = [
                            110,
                            200,
                            201,
                            202,
                            320,
                            321,
                            387,
                            463,
                            10052,
                        ]

                        # after session order is created with error 399
                        # Error 399: your order will not be placed at the exchange until 2022-06-07 09:30:00 US/Eastern
                        # other_errors_codes_array = [399]

                        reqId_array = [order_id1]

                        if signal_dic["ticker_type"] == "pair":
                            reqId_array.append(order_id2)

                        # errors inlcluding reqId=-1 (not real error string)
                        all_errors_df = errors_df.loc[errors_df["reqId"] != -1]

                        # get reroute errors (and filter by reqId)
                        reroute_errors_df = errors_df.loc[
                            errors_df["errorCode"].isin(reroute_error_codes_array)
                        ]
                        reroute_errors_df_filtered = reroute_errors_df.loc[
                            reroute_errors_df["reqId"].isin(reqId_array)
                        ]

                        # get critical errors (and filter by reqId)
                        critical_errors_df = errors_df.loc[
                            errors_df["errorCode"].isin(critical_error_codes_array)
                        ]
                        critical_errors_df_filtered = critical_errors_df.loc[
                            critical_errors_df["reqId"].isin(reqId_array)
                        ]

                        # get all others (and filter by reqId)
                        other_errors_df = all_errors_df.loc[
                            ~all_errors_df["errorCode"].isin(critical_error_codes_array)
                        ]
                        other_errors_df_filtered = other_errors_df.loc[
                            other_errors_df["reqId"].isin(reqId_array)
                        ]

                        # create empty dataframe and string to be used for creating error strings
                        errors_df_final = pd.DataFrame()
                        error_string_pair = ""

                        # set order status and message
                        order_created = True
                        order_status = "created"

                        # define error boolean flags
                        reroute_error_flag = not reroute_errors_df.empty
                        critical_error_flag = not critical_errors_df.empty
                        other_error_flag = not other_errors_df.empty

                        # check routing status and mark as critical error if already rerouted
                        if reroute_error_flag and (
                            signal_dic["order_status"] != "rerouted"
                        ):

                            print(
                                f"\n{time_str()} - critical errors detected, will reroute to primary exchange as market order"
                            )
                            logger.warning(
                                "critical errors detected, will reroute to primary exchange as market order"
                            )

                            # get error ids
                            error_id_reroute = reroute_errors_df_filtered[
                                "reqId"
                            ].tolist()  # dataframe to series

                            # set order status to reroute order to another exchange
                            status_msg = status_msg + "+route"
                            order_created = False
                            order_status = "rerouted"

                            # print order messages
                            for index in range(len(error_id_reroute)):
                                print(
                                    f"\n{time_str()} - rerouting order: ",
                                    error_id_reroute[index],
                                )
                                logger.info(
                                    f"rerouting order: {error_id_reroute[index]}"
                                )
                                # will get error 10147 if you try to cancel the order because order is not created
                                # keeping it here as a reminder
                                # app.cancelOrder(error_id_reroute[index])

                        elif critical_error_flag:
                            print(
                                f"\n{time_str()} - critical errors detected when placing order"
                            )
                            logger.warning(
                                "critical errors detected when placing order"
                            )

                            # set order status and message
                            status_msg = status_msg + "+crit. err"
                            order_created = False
                            order_status = "critical err"

                            errors_df_final = critical_errors_df_filtered

                        elif other_error_flag:
                            print(
                                f"\n{time_str()} - errors detected when placing order"
                            )
                            logger.warning("errors detected when placing order")

                            # set order status and message
                            status_msg = status_msg + "+err"
                            order_created = True
                            order_status = "error"

                            errors_df_final = other_errors_df_filtered

                        # prepare error string
                        if not errors_df_final.empty:

                            error_id = errors_df_final[
                                "reqId"
                            ].tolist()  # dataframe to series
                            error_code = errors_df_final[
                                "errorCode"
                            ].tolist()  # dataframe to series
                            error_string = errors_df_final[
                                "errorString"
                            ].tolist()  # dataframe to series

                            error_string_id1 = ""
                            error_string_id2 = ""

                            # create first order error string
                            for index in range(len(error_id)):
                                if error_id[index] == order_id1:
                                    error_string_id1 = (
                                        "| "
                                        + str(error_id[index])
                                        + "/"
                                        + str(error_code[index])
                                        + " - "
                                        + error_string[index]
                                        + error_string_id1
                                        + "  "
                                    )

                            # prepare second order error string
                            if signal_dic["ticker_type"] == "pair":
                                for index in range(len(error_id)):
                                    if error_id[index] == order_id2:
                                        error_string_id2 = (
                                            "| "
                                            + str(error_id[index])
                                            + "/"
                                            + str(error_code[index])
                                            + " - "
                                            + error_string[index]
                                            + error_string_id2
                                            + "  "
                                        )

                            error_string_pair = error_string_id1 + error_string_id2

                    # close socket
                    app._socketShutdown()
                    time.sleep(0.5)
                    app.disconnect()

                    # the following join will wait for the thread to end
                    con_thread.join()
                    print(f"\n{time_str()} - TWS disconnected after processing orders")
                    logger.info("TWS disconnected after processing orders")

                    if order_created:

                        signal_dic["passphrase"] = PASSPHRASE
                        signal_dic["order_status"] = order_status
                        signal_dic["status_msg"] = status_msg
                        signal_dic["error_msg"] = error_string_pair
                        signal_dic["order_id1"] = int(order_id1)
                        signal_dic["order_contracts"] = abs(order_contracts)
                        if signal_dic["ticker_type"] == "pair":
                            signal_dic["order_id2"] = int(order_id2)

                    else:

                        signal_dic["passphrase"] = PASSPHRASE
                        signal_dic["order_status"] = order_status
                        signal_dic["status_msg"] = status_msg
                        signal_dic["error_msg"] = error_string_pair

                    # check rare condition (if waiting order created due to sync)
                    if synced_order:
                        # bypass is needed to create an order with a ticker that is already active in a pair
                        signal_dic["bypass_ticker_status"] = True

                    response = requests.put(API_PUT_SIGNAL, json=signal_dic)

                    if response.status_code == 200:
                        print(
                            f"\n{time_str()} - server updated with active order information"
                        )
                        logger.info("server updated with active order information.")
                    else:
                        print(
                            f"\n{time_str()} - an error occurred updating server with active order information."
                        )
                        logger.info(
                            "an error occurred updating server with active order information."
                        )


#######################
### ASYNC FUNCTIONS ###
#######################

# to update filled order information
async def update_orders():
    update_filled_orders(CONNECTION_PORT, PASSPHRASE, API_PUT_UPDATE)
    # to update ticker positions and pnl
    update_positions(ACCOUNT_NUMBER, CONNECTION_PORT, PASSPHRASE, API_UPDATE_PNL)
    # to update the most recent account pnl record
    update_acc_pnl(
        PASSPHRASE, API_GET_PNL, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT
    )

# to register account pnl(historically)
async def post_pnl():
    post_acc_pnl(PASSPHRASE, API_PUT_PNL, ACCOUNT_NUMBER, CONNECTION_PORT)


# to define priodic time intervals
async def run_periodically(interval, periodic_function):
    while True:
        await asyncio.gather(asyncio.sleep(interval), periodic_function())


##############
### CONFIG ###
##############

# get configuration variables
config = configparser.ConfigParser()
# change to your config file name
config.read("config_private.ini")
environment = config.get("environment", "ENV")
ACCOUNT_NUMBER = config.get(environment, "ACCOUNT_NUMBER")
CONNECTION_PORT = int(config.get(environment, "CONNECTION_PORT"))
PASSPHRASE = config.get(environment, "PASSPHRASE")
SYNC_PAIR = config.getboolean(environment, "SYNC_PAIR")
API_PUT_SIGNAL = config.get(environment, "API_PUT_SIGNAL")
API_PUT_UPDATE = config.get(environment, "API_PUT_UPDATE")
API_GET_SIGNAL_WAITING = config.get(environment, "API_GET_SIGNAL_WAITING")
API_GET_SIGNAL = config.get(environment, "API_GET_SIGNAL")
API_GET_PAIR = config.get(environment, "API_GET_PAIR")
API_GET_TICKER = config.get(environment, "API_GET_TICKER")
API_UPDATE_PNL = config.get(environment, "API_UPDATE_PNL")
API_PUT_PNL = config.get(environment, "API_PUT_PNL")
API_GET_PNL = config.get(environment, "API_GET_PNL")
CHECK_FUNDS_FOR_TRADE = config.getboolean(environment, "CHECK_FUNDS_FOR_TRADE")
AVAILABLE_FUND_FLOOR = int(config.get(environment, "AVAILABLE_FUND_FLOOR"))
LOGFILE_NAME = config.get(environment, "LOGFILE_NAME")
print("==> ACTIVE ENVIRONMENT: ", environment)


##############
### LOGGER ###
##############

# define log path
Path("logs").mkdir(parents=True, exist_ok=True)

# get or create a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.INFO)

# define file handler and set formatter
# log_date = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
log_date = datetime.now().strftime("%Y_%m_%d")  # keeping one log per day
filename = LOGFILE_NAME + log_date + ".log"
file_handler = logging.FileHandler(filename)
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

# for later
# sys.stderr.write = log.error  #(will log every error, nothing will be on console/stdout)
# sys.stdout.write = log.info

# add stream handler to logger
# stdout_handler = logging.StreamHandler(sys.stdout) #output things to stdout in addition to the log file
# logger.addHandler(stdout_handler)

# Logs
# logger.debug('A debug message')
# logger.info('An info message')
# logger.warning('Something is not right.')
# logger.error('A Major error has happened.')
# logger.critical('Fatal error. Cannot continue')
# logger.exception("Division by zero problem")

#################
### MAIN LOOP ###
#################

# fix for asyncio function daemon thread problem
nest_asyncio.apply()

# Tried to use a microservices approach even tough some components are monolithic.
# Trading signals are stored in web servers, and queued.
# Server app gets queued signals, turning into orders.
# Getting filled order information is a separate thread.
# Updating PNL and positions is a separate thread

loop = asyncio.get_event_loop()

loop.create_task(run_periodically(10, check_signals))  # cycle in 10 secs

loop.create_task(run_periodically(60 * 60, post_pnl))  # cycle in 60 min

loop.create_task(run_periodically(60 * 5, update_orders))  # cycle in 5 min

loop.run_forever()
