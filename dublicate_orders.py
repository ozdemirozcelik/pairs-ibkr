import collections


def get_dublicate_orders(dic_list_to_check):
    # ENABLE TO TEST AND EXPLORE VARIABLES
    # global a_set, dupes, dupes_list, dupes_invalid_orders, dupes_valid_order

    dupes_invalid_orders = []
    ticker_list = []
    rowid_list = []

    # populate list of tickers and rowids to check if there are any duplicates
    for index in range(len(dic_list_to_check)):
        ticker_list.append(dic_list_to_check[index]["ticker"])
        rowid_list.append(dic_list_to_check[index]["rowid"])

    # create a set of duplicate tickers (unordered and unindexed): {'NMFC-0.72*ROIC', 'MA-3*V'}
    # set does not contain duplicate tickers
    a_set = set(ticker_list)

    # check for duplicate tickers
    contains_duplicates = len(ticker_list) != len(a_set)

    if contains_duplicates:

        # create a list of duplicates (ordered and indexed) : ['NMFC-0.72*ROIC', 'MA-3*V']
        dupes = [
            item
            for item, count in collections.Counter(ticker_list).items()
            if count > 1
        ]

        # alternative set() method with no index
        # seen = set()
        # dupes = set([x for x in ticker_list if x in seen or seen.add(x)])   # no index list

        rowid_dubs_list = []
        dupes_list = []  # list of order id lists : [[53, 52], [51, 50]]
        dupes_valid_order = []

        # parse through duplicates and find valid orders
        for i in range(len(dupes)):
            temp_list = []
            for index in range(len(ticker_list)):
                if ticker_list[index] == dupes[i]:
                    rowid_dubs_list.append(rowid_list[index])
                    temp_list.append(rowid_list[index])
            dupes_list.append(temp_list)

        # create valid order list, most recent order is valid: [53, 51]
        for i in range(len(dupes_list)):
            dupes_valid_order.append(max(dupes_list[i]))

        # create invalid orders list from the rest : [52, 50]
        dupes_invalid_orders = [
            item for item in rowid_dubs_list if item not in dupes_valid_order
        ]

    return dupes_invalid_orders


# ENABLE TO TEST FROM TWS
# import requests
# api_get_signal_waiting = "http://127.0.0.1:5000/v3/signals/status/waiting/0"
# response = requests.get(api_get_signal_waiting, timeout=5)
# response_list_dic = response.json()['signals']  # creates a list of dictionaries from json
# dublicate_orders = get_dublicate_orders(response_list_dic)

# ENABLE TO TEST OFFLINE
# response_list_dic = [{'rowid': 53, 'timestamp': '2022-06-02 18:30:32', 'ticker': 'NMFC-0.72*ROIC', 'order_action': 'sell', 'order_contracts': 20, 'order_price': 200.0, 'mar_pos': 'long', 'mar_pos_size': 20, 'pre_mar_pos': 'flat', 'pre_mar_pos_size': 0, 'order_comment': 'Enter Short', 'order_status': 'waiting', 'ticker_type': 'pair', 'stk_ticker1': 'NMFC', 'stk_ticker2': 'ROIC', 'hedge_param': 0.72, 'order_id1': None, 'order_id2': None, 'stk_price1': None, 'stk_price2': None, 'fill_price': None, 'slip': None, 'error_msg': None}, {'rowid': 52, 'timestamp': '2022-06-02 18:30:25', 'ticker': 'NMFC-0.72*ROIC', 'order_action': 'buy', 'order_contracts': 20, 'order_price': 200.0, 'mar_pos': 'long', 'mar_pos_size': 20, 'pre_mar_pos': 'flat', 'pre_mar_pos_size': 0, 'order_comment': 'Enter Long', 'order_status': 'waiting', 'ticker_type': 'pair', 'stk_ticker1': 'NMFC', 'stk_ticker2': 'ROIC', 'hedge_param': 0.72, 'order_id1': None, 'order_id2': None, 'stk_price1': None, 'stk_price2': None, 'fill_price': None, 'slip': None, 'error_msg': None}, {'rowid': 51, 'timestamp': '2022-06-02 18:16:32', 'ticker': 'MA-3*V', 'order_action': 'buy', 'order_contracts': 20, 'order_price': 200.0, 'mar_pos': 'long', 'mar_pos_size': 20, 'pre_mar_pos': 'flat', 'pre_mar_pos_size': 0, 'order_comment': 'Enter Long', 'order_status': 'waiting', 'ticker_type': 'pair', 'stk_ticker1': 'MA', 'stk_ticker2': 'V', 'hedge_param': 3.0, 'order_id1': None, 'order_id2': None, 'stk_price1': None, 'stk_price2': None, 'fill_price': None, 'slip': None, 'error_msg': None}, {'rowid': 50, 'timestamp': '2022-06-02 18:16:29', 'ticker': 'MA-3*V', 'order_action': 'buy', 'order_contracts': 20, 'order_price': 200.0, 'mar_pos': 'long', 'mar_pos_size': 20, 'pre_mar_pos': 'flat', 'pre_mar_pos_size': 0, 'order_comment': 'Enter Long', 'order_status': 'waiting', 'ticker_type': 'pair', 'stk_ticker1': 'MA', 'stk_ticker2': 'V', 'hedge_param': 3.0, 'order_id1': None, 'order_id2': None, 'stk_price1': None, 'stk_price2': None, 'fill_price': None, 'slip': None, 'error_msg': None}]
# dublicate_orders = get_dublicate_orders(response_list_dic)
