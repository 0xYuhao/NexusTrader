OKX Exchange
===============

Okx Account Types
-----------------

There are 3 types of account in Okx. If your server is hosted by ``AWS``, you should use ``OkxAccountType.AWS`` for live trading, else use ``OkxAccountType.LIVE`` for live trading. For testing, you can use ``OkxAccountType.DEMO``.

- ``OkxAccountType.LIVE``: Live account
- ``OkxAccountType.AWS``: AWS account
- ``OkxAccountType.DEMO``: Demo account

.. code-block:: python

    from tradebot.exchange.okx import OkxAccountType

    account_type = OkxAccountType.LIVE # or OkxAccountType.AWS or OkxAccountType.DEMO

Okx Config
-----------

The most important part is the ``config`` object. You need to pass the ``config`` object to the ``Engine`` class. The ``config`` receives the following parameters:

- ``strategy_id``: The id of the strategy.
- ``user_id``: The id of the user.
- ``strategy``: The strategy class.
- ``basic_config``: The basic config of the exchange.
- ``public_conn_config``: The public connector config of the exchange.
- ``private_conn_config``: The private connector config of the exchange.

.. code-block:: python

    from tradebot.constants import settings
    from tradebot.exchange.okx import OkxAccountType
    from tradebot.config import Config, PublicConnectorConfig, PrivateConnectorConfig, BasicConfig

    OKX_API_KEY = settings.OKX.DEMO_1.api_key
    OKX_SECRET = settings.OKX.DEMO_1.secret
    OKX_PASSPHRASE = settings.OKX.DEMO_1.passphrase

    # The strategy code....

    config = Config(
        strategy_id="okx_buy_and_sell",
        user_id="user_test",
        strategy=Demo(),
        basic_config={
            ExchangeType.OKX: BasicConfig(
                api_key=OKX_API_KEY,
                secret=OKX_SECRET,
                passphrase=OKX_PASSPHRASE,
                testnet=True,
            )
        },
        public_conn_config={
            ExchangeType.OKX: [
                PublicConnectorConfig(
                    account_type=OkxAccountType.DEMO,
                )
            ]
        },
        private_conn_config={
            ExchangeType.OKX: [
                PrivateConnectorConfig(
                    account_type=OkxAccountType.DEMO,
                )
            ]
        }
    )

    engine = Engine(config)

    if __name__ == "__main__":
        try:
            engine.start()
        finally:
            engine.dispose()
