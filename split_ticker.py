# -*- coding: utf-8 -*-
"""
SPLIT
"""

import re


def splitticker_simple(ticker_string):
    problem_flag = False

    eq12 = ticker_string.split("-")
    eq1 = eq12[0].rsplit(":", maxsplit=1)
    eq2 = eq12[1].split(":")
    ticker_pair1 = eq1[len(eq1) - 1]
    ticker_pair2 = eq2[len(eq2) - 1]
    eq22 = eq2[0].split("*")

    # print (eq1)
    # print (eq2)
    # print (eq22)

    if len(eq22) == 1:
        ticker_const = 1
    else:
        ticker_const = eq22[0]

    ticker_all = {
        "ticker1": ticker_pair1,
        "ticker2": ticker_pair2,
        "constant": ticker_const,
        "problem": problem_flag,
    }

    return ticker_all


def splitticker(ticker_string):
    problem_flag = False

    eq12 = ticker_string.split("-")

    print(len(eq12))

    if len(eq12) != 2:

        problem_flag = True

        ticker_all = {
            "ticker1": "TEST",
            "ticker2": "TEST",
            "constant": 0,
            "problem": problem_flag,
        }

        return ticker_all

    else:

        eq1 = re.findall(r"[-+]?\d*\.\d+|\d+", eq12[0])
        eq2 = re.findall(r"[-+]?\d*\.\d+|\d+", eq12[1])

        if len(eq1) > 0:
            eq11 = eq12[0].replace(eq1[0], "")
        else:
            eq11 = eq12[0]

        if len(eq2) > 0:
            eq22 = eq12[1].replace(eq2[0], "")
        else:
            eq22 = eq12[1]

        eq11 = eq11.replace("*", "")
        eq22 = eq22.replace("*", "")

        print(eq11)
        print(eq22)

        eq111 = eq11.rsplit(":", maxsplit=1)
        ticker_pair1_almost = eq111[len(eq111) - 1]

        if "." in ticker_pair1_almost:  # For Class A,B type stocks EXP: BF.A BF.B
            ticker_pair1 = ticker_pair1_almost.replace(".", " ")
        else:
            ticker_pair1 = "".join(
                char for char in ticker_pair1_almost if char.isalnum()
            )

            if ticker_pair1_almost != ticker_pair1:
                problem_flag = True

        eq222 = eq22.rsplit(":", maxsplit=1)
        ticker_pair2_almost = eq222[len(eq222) - 1]
        # ticker_pair2_almost = ticker_pair2_almost.replace(".",' ') # For Class A,B type stocks EXP: BF.A BF.B

        if "." in ticker_pair1_almost:  # For Class A,B type stocks EXP: BF.A BF.B
            ticker_pair2 = ticker_pair2_almost.replace(".", " ")
        else:
            ticker_pair2 = "".join(
                char for char in ticker_pair2_almost if char.isalnum()
            )

            if ticker_pair2_almost != ticker_pair2:
                problem_flag = True

        # print(eq111)
        # print(eq222)

        # print(ticker_pair1_almost)
        # print(ticker_pair2_almost)

        # if ticker_pair2_almost != ticker_pair2:
        #    problem_flag = True

        if len(eq2) == 0:
            ticker_const = 1
        else:
            ticker_const = eq2[0]

        if len(eq1) != 0:
            if eq1[0] != 1:
                problem_flag = True

        # print (eq12)
        # print (eq1)
        # print (eq2)
        # print (eq11)
        # print (eq22)

        ticker_all = {
            "ticker1": ticker_pair1,
            "ticker2": ticker_pair2,
            "constant": ticker_const,
            "problem": problem_flag,
        }

        return ticker_all


# TEST

# pair_equation = "TEST 4"
# pair_equation = "NYSE:LNT-NYSE:FTS*2.2"
# pair_equation = "NYSE:LNT*2-NYSE:FTS"
# pair_equation = "NYSE:LNT-NYSE:FTS/3"
# pair_equation = "1.3*NYSE:LNT-NYSE:FTS*2.2"
# pair_equation = "NYSE:LNT-1.25*NYSE:FTS"
# pair_equation = "LNT-1.25*NYSE:FTS"
# pair_equation = "NYSE:LNT-NYSE:FTS"
# pair_equation = "BF.A-0.7*NYSE:BF.B"

# dict_ticker = splitticker(pair_equation)

# print (dict_ticker["ticker1"])
# print (dict_ticker["ticker2"])
# print (dict_ticker["constant"])
# print (dict_ticker["problem"])
