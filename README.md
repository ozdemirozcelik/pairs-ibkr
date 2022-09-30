# Pairs-IBKR Python Pair Trading Bot using Interactive Brokers TWS API

For trading tickers in pairs (also possible to trade a single stock, fx, crypto).

* Trades on Interactive Brokers using [TWS-API](https://interactivebrokers.github.io/tws-api/introduction.html)
* Integrated with [Pairs-API](https://github.com/ozdemirozcelik/pairs-api-v3)
* Recommended to use with [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php)
* Compatible with TradingView webhooks

# Watch Demo

[![Watch on YouTube](https://ozdemirozcelik.github.io/utubelink.png)](https://www.youtube.com/watch?v=-jfJ6g-fZpI "Watch on YouTube")


# Use Cases

With Pairs-API you can:
- integrate with [Pairs-API](https://github.com/ozdemirozcelik/pairs-api-v3) trade platform
- validate orders (ticker information, order size, dublicate orders, pair synchronization, minimum funds etc.) 
- send real time orders (Relative, Market, Limit) to Interactive Brokers
- get filled order information
- get account summary
- 
# Considerations

Considering for the next version:

- improve error handling, add unit tests
- using historical data from Interactive Brokers for:
  - automated pair selection by using historical data from IB
  - custom signal generator

# Requirements

* ibapi==9.76.1 (TWS API - manual installation for the newest version)
* nest_asyncio
* pytz
* pandas
* requests


# Installation

Download and install IB API for the latest version:
[Installing & Configuring TWS for the Python API](https://www.youtube.com/watch?v=xqLkzDMvLz4)

inherited "ibapi" folder in the repository from TWS-API docs.

### clone git repository:
```bash
$ git clone https://github.com/ozdemirozcelik/pairs-ibkr.git pairs-ibkr
````
### create and activate virtual environment:
````bash
$ pip install virtualenv
(conda install virtualenv)

$ cd pairs-ibkr
cd pairs-ibkr (windows)

$ python -m venv ibkr-env
(conda create --name ibkr-env)

$ source ibkr-env/bin/activate
.\ibkr-env\scripts\activate (windows)
(conda activate ibkr-env)
````
### install requirements:

````
$ pip install -r requirements.txt
(conda install --file requirements_conda.txt)
````
try this if conda fails to install requirements:
````
(conda config --append channels conda-forge)
````
check this out if you get an invalidversionspec error:

https://github.com/conda/conda/issues/9519#issuecomment-690486632


# Configuration

* configure config.ini file in line with [Pairs-API](https://github.com/ozdemirozcelik/pairs-api-v3) and IB account details:

```ini
# DESCRIPTIONS:
# CHECK_FUNDS_FOR_TRADE=> Check available fund floor before sending an order
# AVAILABLE_FUND_FLOOR=> available funds contingency amount, less than this amount will result 'no trade'
# CONNECTION_PORT=> define port for socket connection
# SYNC_PAIR=> if True: change hedge parameter according to the active position of ticker 2 (sync from flat to pos only)
# NO_DUBS=> if True: do no create order if duplicate ticker1 is used
# PASSPHRASE=>  this is the passphrase for webhooks
# API_PUT_SIGNAL=> API resource to PUT (update) signals
# API_PUT_PRICE=> API resource to PUT (update) order fill prices
# API_GET_SIGNAL=>  API resource to GET signal rowid
# API_GET_SIGNAL_WAITING=>  API resource to GET signals with the status of waiting
# API_GET_SIGNAL_ROUTE=>  API resource to GET signals with the status of rerouted
# ACCOUNT_NUMBER=>  IB account number to get account and position details
# LOGFILE_NAME=>  folder and filename for logs

[environment]
# enable environment to work with
ENV : development
# ENV : stage
# ENV : production

[development]
CHECK_FUNDS_FOR_TRADE : False
AVAILABLE_FUND_FLOOR : 5000
CONNECTION_PORT : 7497
PASSPHRASE : webhook
SYNC_PAIR : True
API_PUT_SIGNAL : http://127.0.0.1:5000/v3/webhook
API_PUT_UPDATE : http://127.0.0.1:5000/v3/signal/updateorder
API_GET_SIGNAL : http://127.0.0.1:5000/v3/signal/
API_GET_PAIR : http://127.0.0.1:5000/v3/pair/
API_GET_TICKER : http://127.0.0.1:5000/v3/ticker/
API_UPDATE_PNL : http://127.0.0.1:5000/v3/ticker/updatepnl
API_PUT_PNL : http://127.0.0.1:5000/v3/regpnl
API_GET_PNL : http://127.0.0.1:5000/v3/pnl/1
API_GET_SIGNAL_WAITING : http://127.0.0.1:5000/v3/signals/status/waiting/0
API_GET_SIGNAL_ROUTE : http://127.0.0.1:5000/v3/signals/status/rerouted/0
ACCOUNT_NUMBER = DU######
LOGFILE_NAME : logs\logdev_

[stage]
...

[production]
...
````
* double check your config file name. config.ini has the account number and webhook passphrase so pay attention before pushing to your public repository:
````python
(app.py)
# get configuration variables
config = configparser.ConfigParser()
# change to your config file name
config.read("config_private.ini")
...
````


# Contributing

Pull requests are welcome.




