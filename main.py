import asyncio
import ccxt.async_support as ccxt
import os
import signal
# from dotenv import load_dotenv # 移除 dotenv
from decimal import Decimal, ROUND_DOWN, getcontext
# 直接填写你的 API Key 和 Secret
API_KEY = 'YOUR_BINANCE_API_KEY_HERE' # <-- 在这里填写你的 API Key
API_SECRET = 'YOUR_BINANCE_API_SECRET_HERE' # <-- 在这里填写你的 API Secret

# 用户指定的 USDT 基础资金量，程序会尝试将这么多 USDT 分配到目标币种
# 注意：首次运行时，如果账户 USDT 不足此数量，则只会使用可用 USDT 进行分配
INITIAL_USDT_AMOUNT = 1000.0

# 用户指定的目标币种列表 (USDT 交易对的基础币种)
TARGET_CURRENCIES = ["BTC", "ETH", "BNB", "XRP", "ADA"] # 示例：平衡这5个币种

# 定时平衡的间隔 (秒)
REBALANCE_INTERVAL_SECONDS = 60 * 60 # 示例：每 60 分钟平衡一次

# 打印当前比例的间隔 (秒)
REPORT_INTERVAL_SECONDS = 5 * 60 # 示例：每 5 分钟打印一次

# 再平衡触发的最小偏差阈值 (百分比)
REBALANCE_THRESHOLD_PERCENTAGE = 0.5 # 示例：偏差超过 0.5% 才触发再平衡

BASE_CURRENCY = "USDT"

# 设置 Decimal 精度
getcontext().prec = 18 # 为 Decimal 运算设置足够精度

# --- Helper Function for Decimal Precision (Adapted for CCXT) ---
def safe_round_down_amount(amount_decimal, market):
    """
    根据 CCXT market 信息，安全地向下取整基础币种数量 (amount)。
    返回处理后的 Decimal 数量。
    """
    if amount_decimal is None or amount_decimal <= 0:
        return Decimal('0')

    # 获取数量精度 (通常是小数位数)
    amount_precision = market.get('precision', {}).get('amount')
    # 获取最小下单数量限制
    min_amount_str = market.get('limits', {}).get('amount', {}).get('min')
    min_amount = Decimal(str(min_amount_str)) if min_amount_str is not None else Decimal('0')

    rounded_amount = amount_decimal

    # 1. 应用精度 (向下取整)
    if amount_precision is not None:
        # amount_precision 通常代表小数位数
        # 例如: 8 -> Decimal('1e-8'), 0 -> Decimal('1e-0') or Decimal('1')
        decimal_places = int(amount_precision)
        quantizer = Decimal('1e-' + str(decimal_places))
        rounded_amount = amount_decimal.quantize(quantizer, rounding=ROUND_DOWN)
        # print(f"Debug: Raw: {amount_decimal}, Places: {decimal_places}, Quantizer: {quantizer}, Rounded: {rounded_amount}")


    # 2. 应用最小数量限制
    if rounded_amount < min_amount:
        # print(f"Debug: Rounded amount {rounded_amount} is less than min amount {min_amount}. Returning 0.")
        return Decimal('0') # 如果向下取整后小于最小允许量，则无法下单

    return rounded_amount

# --- Balancer Class ---
class CCXTBalancer:
    def __init__(self, api_key, api_secret, initial_usdt_amount, target_currencies, rebalance_interval, report_interval, rebalance_threshold):
        # 初始化 CCXT 交易所对象 (异步)
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True, # 启用内置的速率限制处理
            'options': {
                 'defaultType': 'spot', # 确保是现货交易
            }
        })
        # 注意: ccxt 现在默认启用 async 支持，无需显式 'async_support': True
        # 但导入时使用 ccxt.async_support 更明确

        self.initial_usdt_amount = Decimal(str(initial_usdt_amount))
        self.target_currencies = target_currencies
        self.rebalance_interval_ms = rebalance_interval * 1000
        self.report_interval_ms = report_interval * 1000
        self.rebalance_threshold_percentage = Decimal(str(rebalance_threshold))
        self.base_currency = BASE_CURRENCY
        self.target_allocations = {
            currency: Decimal(1) / len(target_currencies) for currency in target_currencies
        }
        self.markets = {} # 存储加载后的市场信息 {symbol: market_data}
        self.last_rebalance_time = 0
        self.last_report_time = 0
        self.stop_event = asyncio.Event() # 用于优雅停止

        # 创建 ccxt 格式的 symbols (e.g., "BTC/USDT")
        self.target_symbols = [f"{c}/{self.base_currency}" for c in target_currencies]

        # 添加 USDT 以获取其余额
        self._assets_to_fetch = set(target_currencies + [self.base_currency])

    async def load_market_info(self):
        """从交易所加载并处理市场信息。"""
        print("Loading market information...")
        try:
            # 加载所有市场的交易规则和精度信息
            await self.exchange.load_markets()
            print("Market information loaded.")

            # 筛选出目标交易对的市场信息
            valid_targets = []
            loaded_markets = {}
            for symbol in self.target_symbols:
                if symbol in self.exchange.markets:
                    market_data = self.exchange.markets[symbol]
                    if market_data.get('active', False):
                        loaded_markets[symbol] = market_data
                        # 从 symbol 中提取基础货币
                        base_currency = market_data.get('base')
                        if base_currency:
                             valid_targets.append(base_currency)
                        else:
                             print(f"Warning: Could not determine base currency for symbol {symbol}. Skipping.")

                    else:
                        print(f"Warning: Market {symbol} is not active. Removing {self.exchange.markets[symbol].get('base')} from target list.")
                else:
                    # 从 symbol 字符串中猜测基础货币用于打印警告
                    base_curr_guess = symbol.split('/')[0]
                    print(f"Warning: Could not find market info for {symbol}. Removing {base_curr_guess} from target list.")

            # 更新目标列表和分配比例
            self.target_currencies = list(set(valid_targets)) # 去重并更新
            self.target_symbols = [f"{c}/{self.base_currency}" for c in self.target_currencies]
            self.markets = loaded_markets # 只存储有效且活跃的市场信息

            if not self.target_currencies:
                 print("Error: No valid/active markets found for any target currencies.")
                 return False

            self.target_allocations = {
                currency: Decimal(1) / len(self.target_currencies) for currency in self.target_currencies
            }
            self._assets_to_fetch = set(self.target_currencies + [self.base_currency])

            print(f"Active target currencies: {', '.join(self.target_currencies)}")
            return True

        except ccxt.NetworkError as e:
            print(f"Network error loading market info: {e}")
            return False
        except ccxt.ExchangeError as e:
            print(f"Exchange error loading market info: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error loading market info: {e}")
            return False

    async def fetch_portfolio_data(self):
        """获取当前账户余额和目标币种的价格。"""
        try:
            # 并行获取余额和价格
            balance_task = self.exchange.fetch_balance()
            tickers_task = self.exchange.fetch_tickers(self.target_symbols)

            balance_result, tickers_result = await asyncio.gather(
                balance_task,
                tickers_task,
                return_exceptions=True # 如果一个失败，另一个能继续
            )

            # 检查结果
            if isinstance(balance_result, Exception):
                print(f"Error fetching balance: {balance_result}")
                balance = None
            else:
                balance = balance_result

            if isinstance(tickers_result, Exception):
                print(f"Error fetching tickers: {tickers_result}")
                tickers = None
            else:
                tickers = tickers_result

            if balance is None or tickers is None:
                 return None, None # 如果任一失败，则认为整体失败

            return balance, tickers

        except ccxt.NetworkError as e:
            print(f"Network error fetching portfolio data: {e}")
            return None, None
        except ccxt.ExchangeError as e:
            print(f"Exchange error fetching portfolio data: {e}")
            return None, None
        except Exception as e:
            print(f"Unexpected error fetching portfolio data: {e}")
            return None, None

    def calculate_allocations(self, balance, tickers):
        """计算当前投资组合总价值和各币种的分配比例。"""
        total_value_usdt = Decimal('0')
        current_allocations_usdt = {} # 各币种的 USDT 价值
        current_balances = {} # 各币种的可用数量

        # 1. 处理基础货币 (USDT)
        usdt_free = Decimal(str(balance['free'].get(self.base_currency, 0.0)))
        current_balances[self.base_currency] = usdt_free
        current_allocations_usdt[self.base_currency] = usdt_free
        total_value_usdt += usdt_free
        print(f" {self.base_currency} balance: {usdt_free:.8f}")

        # 2. 处理目标币种
        for currency in self.target_currencies:
            symbol = f"{currency}/{self.base_currency}"
            asset_balance = Decimal(str(balance['free'].get(currency, 0.0)))
            current_balances[currency] = asset_balance

            ticker = tickers.get(symbol)
            price = Decimal(str(ticker['last'])) if ticker and ticker.get('last') is not None else None

            if price is not None and price > 0 and asset_balance > 0:
                value_usdt = asset_balance * price
                current_allocations_usdt[currency] = value_usdt
                total_value_usdt += value_usdt
                print(f" {currency} balance: {asset_balance:.8f} @ {price:.8f} {self.base_currency} = {value_usdt:.8f} {self.base_currency}")
            else:
                current_allocations_usdt[currency] = Decimal('0') # 价值为 0
                price_status = "(Price unknown or zero)" if price is None or price <= 0 else ""
                print(f" {currency} balance: {asset_balance:.8f} {price_status}")

        # 3. 计算百分比分配
        current_allocations_percentage = {}
        if total_value_usdt > 0:
            target_value_per_currency = total_value_usdt / len(self.target_currencies) if self.target_currencies else Decimal('0')
            for currency in self.target_currencies:
                 value_usdt = current_allocations_usdt.get(currency, Decimal('0'))
                 current_percent = (value_usdt / total_value_usdt) * 100
                 current_allocations_percentage[currency] = current_percent
        else:
             print("Warning: Total portfolio value is zero, cannot calculate percentages.")
             target_value_per_currency = Decimal('0')

        return total_value_usdt, current_allocations_percentage, current_allocations_usdt, target_value_per_currency, current_balances

    async def report_current_allocations(self):
        """获取数据并打印当前的分配报告。"""
        balance, tickers = await self.fetch_portfolio_data()

        if balance is None or tickers is None:
            print("Failed to fetch portfolio data for reporting.")
            return

        total_value_usdt, current_allocations_percent, _, _, _ = self.calculate_allocations(balance, tickers)

        print(f"\n--- Current Portfolio Allocation ({self.base_currency} Total Value: {total_value_usdt:.2f}) ---")

        if not self.target_currencies:
            print(" No target currencies configured.")
            print("--------------------------------------------")
            return

        target_weight_percent = (Decimal(1) / len(self.target_currencies)) * 100 if self.target_currencies else Decimal('0')

        if current_allocations_percent:
            for currency in self.target_currencies:
                current_percent = current_allocations_percent.get(currency, Decimal('0'))
                deviation = abs(current_percent - target_weight_percent)
                status = "OK"
                if deviation >= self.rebalance_threshold_percentage:
                    status = f"REBALANCE NEEDED ({deviation:.2f}% deviation)"
                    # 使用 ANSI escape codes 输出黄色警告
                    print(f" {currency}: {current_percent:.2f}% (Target: {target_weight_percent:.2f}%) - \033[93m{status}\033[0m")
                else:
                    print(f" {currency}: {current_percent:.2f}% (Target: {target_weight_percent:.2f}%) - {status}")
        else:
            print(" No allocation data available.")

        print("--------------------------------------------")
        self.last_report_time = self.exchange.milliseconds()

    async def rebalance(self):
        """执行再平衡操作。"""
        print("\nInitiating rebalance...")

        balance, tickers = await self.fetch_portfolio_data()

        if balance is None or tickers is None:
            print("Failed to fetch portfolio data for rebalancing.")
            return

        total_value_usdt, _, current_allocations_usdt, target_value_per_currency, _ = self.calculate_allocations(balance, tickers)

        if total_value_usdt <= 0 or not self.target_currencies:
            print("Cannot rebalance: Total portfolio value is zero or no target currencies.")
            return

        print(f"Target value per currency: {target_value_per_currency:.8f} {self.base_currency}")

        trades_to_execute = [] # List of (symbol, side, amount, params, currency)

        for currency in self.target_currencies:
            symbol = f"{currency}/{self.base_currency}"
            market = self.markets.get(symbol)

            if not market:
                 print(f"Warning: Market info not found for {symbol}, skipping rebalance for {currency}.")
                 continue

            current_value_usdt = current_allocations_usdt.get(currency, Decimal('0'))
            difference_usdt = target_value_per_currency - current_value_usdt # 正数: 买入; 负数: 卖出
            abs_difference_usdt = abs(difference_usdt)

            # 检查阈值 (与总资产比较)
            deviation_percentage = (abs_difference_usdt / total_value_usdt) * 100 if total_value_usdt > 0 else Decimal('0')
            if deviation_percentage < self.rebalance_threshold_percentage:
                print(f" {currency}: Deviation {abs_difference_usdt:.8f} {self.base_currency} ({deviation_percentage:.2f}%) below threshold, no trade needed.")
                continue

            # 获取最新价格用于计算
            try:
                # 在下单前获取最新的 ticker 数据可能更准确
                ticker = await self.exchange.fetch_ticker(symbol)
                price = Decimal(str(ticker['last'])) if ticker and ticker.get('last') is not None else None
                if price is None or price <= 0:
                     print(f"Skipping trade for {currency}: Could not fetch valid recent price for {symbol}.")
                     continue
            except Exception as e:
                 print(f"Skipping trade for {currency}: Error fetching recent ticker for {symbol}: {e}")
                 continue


            # 获取最小名义价值 (MinNotional)
            min_cost_str = market.get('limits', {}).get('cost', {}).get('min')
            min_notional = Decimal(str(min_cost_str)) if min_cost_str is not None else Decimal('0')

            if difference_usdt > 0: # 需要买入
                side = 'buy'
                usdt_to_spend = abs_difference_usdt

                # 检查最小名义价值
                if usdt_to_spend < min_notional:
                    print(f"Skipping BUY for {currency}: Required USDT {usdt_to_spend:.8f} is less than MIN_NOTIONAL {min_notional:.8f}.")
                    continue

                # 准备市价买入参数 (使用 quoteOrderQty)
                # 使用 cost_to_precision 格式化 USDT 金额
                quote_amount_str = self.exchange.cost_to_precision(symbol, float(usdt_to_spend))
                params = {'quoteOrderQty': quote_amount_str}
                amount = None # 市价买入使用 quoteOrderQty 时，amount 应为 None

                print(f" {currency}: Plan to BUY ~{quote_amount_str} {self.base_currency} worth.")
                trades_to_execute.append((symbol, side, amount, params, currency))

            else: # 需要卖出
                side = 'sell'
                usdt_to_receive = abs_difference_usdt
                # 使用 Decimal 进行精确计算
                quantity_to_sell_raw = usdt_to_receive / price

                # 使用 safe_round_down_amount 处理精度和最小数量
                quantity_to_sell_decimal = safe_round_down_amount(quantity_to_sell_raw, market)

                if quantity_to_sell_decimal <= 0:
                     print(f"Skipping SELL for {currency}: Calculated quantity after rounding ({quantity_to_sell_decimal}) is zero or negative.")
                     continue

                # 再次检查最小名义价值 (使用处理后的数量)
                estimated_notional = quantity_to_sell_decimal * price
                if estimated_notional < min_notional:
                    print(f"Skipping SELL for {currency}: Estimated notional value {estimated_notional:.8f} (Qty: {quantity_to_sell_decimal}) is less than MIN_NOTIONAL {min_notional:.8f}.")
                    continue

                # 准备市价卖出参数 (使用 amount)
                # 将 Decimal 转换为 float 以传递给 create_order
                amount = float(quantity_to_sell_decimal)
                params = {} # 市价卖出不需要特殊参数

                print(f" {currency}: Plan to SELL {amount:.{market['precision']['amount']}f} units for ~{usdt_to_receive:.8f} {self.base_currency}.")
                trades_to_execute.append((symbol, side, amount, params, currency))

        print("\n--- Executing Trades ---")

        # 排序：优先执行卖单 (为了回收 USDT 以便买入)
        trades_to_execute.sort(key=lambda x: x[1] == 'sell', reverse=True)

        for symbol, side, amount, params, currency in trades_to_execute:
            print(f"Attempting {side.upper()} {currency} ({symbol})... Amount: {amount}, Params: {params}")
            try:
                order = await self.exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side=side,
                    amount=amount, # Base quantity for SELL, None for BUY with quoteOrderQty
                    params=params  # Contains quoteOrderQty for BUY
                )
                # ccxt 返回的订单信息结构可能略有不同，确保访问正确的字段
                print(f"  Order successful: ID {order.get('id', 'N/A')} - Status {order.get('status', 'N/A')}")
                # print(f"  Full order response: {order}") # Optional: print full order details

            except ccxt.InsufficientFunds as e:
                 print(f"  Order failed for {currency} ({side}): Insufficient funds. {e}")
                 # 如果卖单失败，可能影响后续买单，但继续尝试其他交易
                 # 如果买单失败，说明 USDT 不足，可以继续尝试其他买单（如果资金够）或停止
                 # 简单起见，这里只打印错误，不中断循环
            except ccxt.InvalidOrder as e:
                 # 这可能包括过滤器错误（最小数量、最小名义价值等）
                 print(f"  Order failed for {currency} ({side}): Invalid order parameters (check limits/precision). {e}")
            except ccxt.NetworkError as e:
                 print(f"  Order failed for {currency} ({side}): Network error. {e}")
            except ccxt.ExchangeError as e:
                 print(f"  Order failed for {currency} ({side}): Exchange error. {e}")
            except Exception as e:
                print(f"  An unexpected error occurred during order placement for {currency} ({side}): {e}")

            # 短暂暂停以避免触发速率限制
            await asyncio.sleep(0.6) # 稍长一点的暂停

        print("--- Trade Execution Finished ---")
        # 交易后立即报告最新分配情况
        await self.report_current_allocations()
        self.last_rebalance_time = self.exchange.milliseconds() # 更新最后平衡时间


    async def perform_initial_allocation(self):
        """执行初始分配购买（如果需要）。"""
        print("\nPerforming initial allocation buy (if needed)...")
        try:
            balance, tickers = await self.fetch_portfolio_data()
            if balance is None or tickers is None:
                print("\033[91mFailed to fetch initial portfolio data. Cannot perform initial buy check. Exiting.\033[0m")
                return False # 表示初始分配失败

            available_usdt = Decimal(str(balance['free'].get(self.base_currency, 0.0)))
            target_initial_usdt = self.initial_usdt_amount

            # 决定实际用于首次购买的USDT量：取 目标初始USDT 和 可用USDT 中的较小值
            usdt_to_allocate = min(target_initial_usdt, available_usdt)

            if usdt_to_allocate <= 0:
                 print("No USDT available or target amount is zero. Skipping initial allocation.")
                 return True # 没有分配任务，不算失败

            print(f"Available USDT: {available_usdt:.2f}, Target Initial: {target_initial_usdt:.2f}")
            print(f"Will allocate based on {usdt_to_allocate:.2f} USDT.")

            if usdt_to_allocate < target_initial_usdt * Decimal('0.9'):
                 print(f"Warning: Allocating significantly less ({usdt_to_allocate:.2f}) than target initial amount ({target_initial_usdt:.2f}) due to available balance.")

            initial_buy_trades = []
            # 防止除以零
            num_target_currencies = len(self.target_currencies)
            usdt_per_coin = usdt_to_allocate / num_target_currencies if num_target_currencies > 0 else Decimal('0')

            if usdt_per_coin <= 0 and num_target_currencies > 0:
                 print("Warning: Calculated USDT per coin is zero or negative. Skipping initial buy plan.")
                 return True # 计划分配金额不足，不算失败

            print(f"Target initial USDT value per coin: {usdt_per_coin:.8f}")

            for currency in self.target_currencies:
                symbol = f"{currency}/{self.base_currency}"
                market = self.markets.get(symbol)

                if not market:
                    print(f"Warning: Market info missing for {symbol}, skipping initial buy for {currency}.")
                    continue

                # 获取最小名义价值 (MinNotional)
                min_cost_str = market.get('limits', {}).get('cost', {}).get('min')
                min_notional = Decimal(str(min_cost_str)) if min_cost_str is not None else Decimal('0')

                usdt_buy_amount = usdt_per_coin # 计划为这个币花费的USDT

                # 检查最小名义价值
                if usdt_buy_amount < min_notional:
                    print(f"Skipping initial BUY for {currency}: Target USDT amount {usdt_buy_amount:.8f} less than minimum notional {min_notional:.8f}.")
                    continue

                # 格式化 quoteOrderQty
                quote_amount_str = self.exchange.cost_to_precision(symbol, float(usdt_buy_amount))
                params = {'quoteOrderQty': quote_amount_str}
                amount = None

                initial_buy_trades.append((symbol, 'buy', amount, params, currency, usdt_buy_amount))
                print(f" Initial buy plan for {currency}: Spend ~{quote_amount_str} {self.base_currency}")

            if not initial_buy_trades:
                 print("No initial buy trades planned.")
                 return True # 没有需要执行的初始购买，不算失败

            print("\n--- Executing Initial Buys ---")
            total_spent_estimation = Decimal('0')
            for symbol, side, amount, params, currency, value_usdt in initial_buy_trades:
                print(f"Attempting Initial BUY {currency} ({symbol}) for ~{value_usdt:.8f} USDT...")
                try:
                    order = await self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=side,
                        amount=amount,
                        params=params
                    )
                    print(f"  Initial order successful: ID {order.get('id', 'N/A')} - Status {order.get('status', 'N/A')}")
                    # 理论上这里应该根据实际成交来更新 spent
                    # 但市价单通常很快且接近预期，简单累加计划花费作为估计
                    total_spent_estimation += value_usdt
                except ccxt.InsufficientFunds as e:
                    print(f"  Initial order failed for {currency}: Insufficient funds. {e}")
                    # 如果初始买入时资金不足，可能是配置的 INITIAL_USDT_AMOUNT 太高或可用资金不够，
                    # 简单起见，遇到资金不足就停止后续初始购买
                    break
                except ccxt.InvalidOrder as e:
                    print(f"  Initial order failed for {currency}: Invalid order (check limits). {e}")
                except ccxt.NetworkError as e:
                    print(f"  Initial order failed for {currency}: Network error. {e}")
                except ccxt.ExchangeError as e:
                    print(f"  Initial order failed for {currency}: Exchange error. {e}")
                except Exception as e:
                    print(f"  An unexpected error occurred during initial order placement for {currency}: {e}")

                await asyncio.sleep(0.6) # 短暂暂停

            print(f"--- Initial Buys Finished (Estimated spend: {total_spent_estimation:.2f} USDT) ---")
            return True # 初始购买流程完成（即使部分或全部因错误被跳过/失败）

        except Exception as e:
             print(f"\033[91mError during initial allocation phase: {e}\033[0m")
             return False # 表示初始分配遇到严重错误

    async def run(self):
        """主策略执行循环。"""
        print("Binance Multi-Currency Balancer (Python/CCXT Async) starting...")

        # 直接检查硬编码的 API Key/Secret
        if API_KEY == 'YOUR_BINANCE_API_KEY_HERE' or API_SECRET == 'YOUR_BINANCE_API_SECRET_HERE':
             print("\033[91mError: Please replace 'YOUR_BINANCE_API_KEY_HERE' and 'YOUR_BINANCE_API_SECRET_HERE' with your actual API credentials.\033[0m")
             await self.close()
             return

        if not API_KEY or not API_SECRET:
            print("\033[91mError: API Key or Secret are empty.\033[0m")
            await self.close()
            return


        # 加载市场信息
        if not await self.load_market_info():
             print("\033[91mFailed to load initial market info. Exiting.\033[0m")
             await self.close()
             return

        if not self.target_currencies:
             print("\033[91mError: No valid target currencies to manage after loading markets. Exiting.\033[0m")
             await self.close()
             return

        print(f"Target Currencies: {', '.join(self.target_currencies)}")
        target_weight_percent_initial = (Decimal(1) / len(self.target_currencies)) * 100 if self.target_currencies else Decimal('0')
        print(f"Target Allocation per coin: {target_weight_percent_initial:.2f}%")
        print(f"Rebalance Interval: {self.rebalance_interval_ms / 1000} seconds")
        print(f"Report Interval: {self.report_interval_ms / 1000} seconds")
        print(f"Rebalance Threshold: {self.rebalance_threshold_percentage:.2f}%")
        print(f"Initial USDT Amount (Target): {self.initial_usdt_amount:.2f}")

        # --- 执行初始分配 ---
        initial_success = await self.perform_initial_allocation()
        # 即使初始分配部分成功，如果遇到关键错误（如无法获取数据），也应退出
        if not initial_success:
             print("Initial allocation failed critically. Exiting.")
             await self.close()
             return

        # --- 初始化时间戳并开始主循环 ---
        print("\nStarting periodic monitoring and rebalancing loop...")
        # 获取当前时间并初始化上一次报告/再平衡时间
        current_time_ms = self.exchange.milliseconds()
        self.last_report_time = current_time_ms
        self.last_rebalance_time = current_time_ms
        await self.report_current_allocations() # 初始分配后立即报告一次

        # 计算下一次报告和再平衡的绝对时间点
        next_report_time_ms = self.last_report_time + self.report_interval_ms
        next_rebalance_time_ms = self.last_rebalance_time + self.rebalance_interval_ms

        try:
            while not self.stop_event.is_set():
                current_time_ms = self.exchange.milliseconds()

                # 检查是否需要报告
                if current_time_ms >= next_report_time_ms:
                    await self.report_current_allocations()
                    # 更新下一次报告时间（基于当前时间，避免漂移）
                    next_report_time_ms = self.exchange.milliseconds() + self.report_interval_ms
                    # print(f"Debug: Next report scheduled for {next_report_time_ms} ms") # Optional Debug

                # 检查是否需要再平衡
                if current_time_ms >= next_rebalance_time_ms:
                    await self.rebalance()
                    # 更新下一次再平衡时间（基于当前时间）
                    next_rebalance_time_ms = self.exchange.milliseconds() + self.rebalance_interval_ms
                    # print(f"Debug: Next rebalance scheduled for {next_rebalance_time_ms} ms") # Optional Debug


                # 计算下一次事件（报告或再平衡）的时间点
                next_event_time_ms = min(next_report_time_ms, next_rebalance_time_ms)

                # 计算需要睡眠的时间（毫秒），确保不小于 0
                sleep_duration_ms = max(0, next_event_time_ms - self.exchange.milliseconds())

                # 睡眠，但允许被 stop_event 中断
                # 确保睡眠时间至少为 1 秒，避免频繁轮询
                sleep_duration_seconds = max(1.0, sleep_duration_ms / 1000.0)
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=sleep_duration_seconds)
                except asyncio.TimeoutError:
                    pass # 超时是正常的，表示没有收到停止信号，继续循环

        except asyncio.CancelledError:
            print("Main loop cancelled.")
        except Exception as e:
            print(f"\nAn unexpected error occurred in the main loop: {e}")
        finally:
            print("Shutting down...")
            await self.close()

    def stop(self):
        """设置停止事件，用于优雅关闭。"""
        print("Stop signal received, initiating shutdown...")
        self.stop_event.set()

    async def close(self):
        """关闭交易所连接。"""
        if self.exchange:
            try:
                # 确保在关闭前取消所有等待中的请求
                # self.exchange.close() 是异步方法，需要 await
                await self.exchange.close()
                print("Exchange connection closed.")
            except Exception as e:
                print(f"Error closing exchange connection: {e}")

# --- Main Execution ---
async def main():
    balancer = CCXTBalancer(
        api_key=API_KEY,
        api_secret=API_SECRET,
        initial_usdt_amount=INITIAL_USDT_AMOUNT,
        target_currencies=TARGET_CURRENCIES.copy(), # 使用副本，防止列表在函数内部被修改
        rebalance_interval=REBALANCE_INTERVAL_SECONDS,
        report_interval=REPORT_INTERVAL_SECONDS,
        rebalance_threshold=REBALANCE_THRESHOLD_PERCENTAGE
    )

    loop = asyncio.get_running_loop()

    # 设置信号处理程序
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, balancer.stop)
        except NotImplementedError:
            print(f"Warning: Signal handling for {sig} not supported on this platform.")
        except ValueError as e:
             print(f"Warning: Could not add signal handler for {sig}: {e}") # Catch potential errors if loop is not running

    await balancer.run()

if __name__ == "__main__":
    try:
        # asyncio.run() 会创建一个新的事件循环并在最后关闭它
        # 它会自动处理一些细节，比如在进程退出时清理资源
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt caught in main. Exiting.")
    except Exception as e:
         print(f"\nUnhandled exception in main execution: {e}")
