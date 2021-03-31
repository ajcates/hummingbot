# ViteX Connector

## About ViteX

ViteX is a completely decentralized exchange powered by Vite blockchain. It supports on-chain order matching and smart contract-enabled trading fee collection and dividend distribution.
ViteX is accessible at [https://vitex.net](https://vitex.net).

## Using the Connector

To use the ViteX connector to trade using Hummingbot, you will need to provide your Vite address, API key and secret key.

```
Enter your Vite address >>>
Enter your ViteX API key >>>
Enter your ViteX API secret >>>
```

API keys are stored locally for the operation of the Hummingbot client only. At no point will API keys be shared to CoinAlpha or be used in any way other than to authorize transactions required for the operation of Hummingbot.

!!! tip "Copying and pasting into Hummingbot"
    See [this page](/faq/troubleshooting/#paste-items-from-clipboard-in-putty) for more instructions in our Support section.

### Creating ViteX API Keys

This article below in ViteX documentation shows step-by-step instructions on how to create API keys in ViteX exchange.

* [Private API Authorization](https://vite.wiki/dex/api/dex-apis.html#private-api-authorization)

!!! warning
    It's highly recommended to enable API authorization ONLY on specific trading pairs for current Hummingbot strategies.

## Miscellaneous Info

### Minimum Order Sizes

Minimum order sizes will vary by trading pair. 
Typically, the minimum value of an order must be at least 0.0001 BTC, 0.001 ETH, 50 VITE or 1 USDT.

### Transaction Fees

Transactions on the Vite blockchain do not consume gas like those on Ethereum, but another resource referred to as “quota.” 
Traders will need to stake VITE in order to obtain the required quota.

The ViteX trading fees will be paid out in “basic currencies” such as VITE, ETH, BTC and USDT.
There are two types of fee charges:

1\. ViteX charges a Base Trading Fee of 0.2%. 

2\. ViteX Operators running their own trading zones may charge an additional fee ("Operator Fee") up to 0.2%. 

In other words, the minimum fees that a trader will pay is 0.2% (Base Trading Fee + 0 Operator Fee) and the maximum is 0.4% (Base Trading Fee + 0.2% Operator Fee). 

The above fees are the same for both the makers and the takers.

A trader can choose to stake 10,000 (or 1,000,000) VITE to lower the Base Trading Fee from 0.2% to 0.1% (or 0%). 
However, any additional Operator fees will remain the same.
