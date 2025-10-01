import io
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Local imports are used within functions to prevent circular dependencies
# with the 'bot' module, which will be importing this one.

def format_isk(value):
    """Formats a number into a human-readable ISK string."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def _prepare_chart_data(character_id, start_of_period):
    """
    Prepares all data needed for chart generation.
    1. Fetches all historical financial events.
    2. Builds the inventory state up to the start of the chart period.
    3. Returns the initial inventory state and all events within the period.
    """
    import bot
    all_transactions = bot.get_historical_transactions_from_db(character_id)
    full_journal = bot.get_full_wallet_journal_from_db(character_id)
    fee_ref_types = {'transaction_tax', 'market_provider_tax', 'brokers_fee'}

    all_events = []
    for tx in all_transactions:
        all_events.append({'type': 'tx', 'data': tx, 'date': datetime.fromisoformat(tx['date'].replace('Z', '+00:00'))})
    for entry in full_journal:
        if entry['ref_type'] in fee_ref_types:
            all_events.append({'type': 'fee', 'data': entry, 'date': entry['date']})
    all_events.sort(key=lambda x: x['date'])

    inventory = defaultdict(list)
    events_before_period = [e for e in all_events if e['date'] < start_of_period]

    for event in events_before_period:
        if event['type'] == 'tx':
            tx = event['data']
            if tx.get('is_buy'):
                inventory[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price']})
            else: # Sale
                remaining_to_sell = tx['quantity']
                lots = inventory.get(tx['type_id'], [])
                if lots:
                    consumed_count = 0
                    for lot in lots:
                        if remaining_to_sell <= 0: break
                        take = min(remaining_to_sell, lot['quantity'])
                        remaining_to_sell -= take
                        lot['quantity'] -= take
                        if lot['quantity'] == 0: consumed_count += 1
                    inventory[tx['type_id']] = lots[consumed_count:]

    events_in_period = [e for e in all_events if e['date'] >= start_of_period]
    return inventory, events_in_period


def generate_last_day_chart(character_id: int):
    """
    Generates a chart for the last 24 hours, processing events chronologically to ensure accuracy.
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import bot
    character = bot.get_character_by_id(character_id)
    if not character: return None

    now = datetime.now(timezone.utc)
    start_of_period = now - timedelta(days=1)

    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None

    hour_labels = [(start_of_period + timedelta(hours=i)).strftime('%H:00') for i in range(24)]
    hourly_sales = {label: 0 for label in hour_labels}
    hourly_fees = {label: 0 for label in hour_labels}
    hourly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    for i in range(24):
        hour_start = start_of_period + timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)
        hour_label = hour_start.strftime('%H:00')

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < hour_end:
            event = events_in_period[event_idx]
            event_type, data = event['type'], event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:
                    sale_value = data['quantity'] * data['unit_price']
                    hourly_sales[hour_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                hourly_fees[hour_label] += fee_amount
                accumulated_profit -= fee_amount
            event_idx += 1
        hourly_cumulative_profit.append(accumulated_profit)

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(hour_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(hourly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(hourly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)
    ax2 = ax.twinx()
    final_profit_line = [0] + hourly_cumulative_profit
    ax2.plot(range(25), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(25), final_profit_line, color="lime", alpha=0.3, zorder=1)
    ax.set_title(f'Performance for {character.name} (Last 24 Hours)', color='white', fontsize=16)
    ax.set_xlabel('Hour (UTC)', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], hour_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf


def _generate_daily_breakdown_chart(character_id: int, days_to_show: int):
    """Helper to generate charts with a daily breakdown (last 7/30 days)."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import bot
    character = bot.get_character_by_id(character_id)
    if not character: return None

    now = datetime.now(timezone.utc)
    start_of_period = (now - timedelta(days=days_to_show-1)).replace(hour=0, minute=0, second=0, microsecond=0)

    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None

    days = [(start_of_period + timedelta(days=i)) for i in range(days_to_show)]
    label_format = '%d' if days_to_show == 30 else '%m-%d'
    bar_labels = [d.strftime(label_format) for d in days]
    daily_sales = {label: 0 for label in bar_labels}
    daily_fees = {label: 0 for label in bar_labels}
    daily_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    for day_start in days:
        day_end = day_start + timedelta(days=1)
        day_label = day_start.strftime(label_format)
        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < day_end:
            event = events_in_period[event_idx]
            event_type, data = event['type'], event['data']
            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:
                    sale_value = data['quantity'] * data['unit_price']
                    daily_sales[day_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                daily_fees[day_label] += fee_amount
                accumulated_profit -= fee_amount
            event_idx += 1
        daily_cumulative_profit.append(accumulated_profit)

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(daily_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(daily_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)
    ax2 = ax.twinx()
    final_profit_line = [0] + daily_cumulative_profit
    ax2.plot(range(days_to_show + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(days_to_show + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)
    ax.set_title(f'Performance for {character.name} (Last {days_to_show} Days)', color='white', fontsize=16)
    ax.set_xlabel('Date', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf


def generate_last_7_days_chart(character_id: int):
    """Generates a chart for the last 7 days."""
    return _generate_daily_breakdown_chart(character_id, 7)


def generate_last_30_days_chart(character_id: int):
    """Generates a chart for the last 30 days."""
    return _generate_daily_breakdown_chart(character_id, 30)


def generate_all_time_chart(character_id: int):
    """Generates a monthly breakdown chart for the character's entire history."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import bot
    character = bot.get_character_by_id(character_id)
    if not character: return None

    inventory, events_in_period = _prepare_chart_data(character_id, datetime.min.replace(tzinfo=timezone.utc))
    if not events_in_period: return None

    start_date = events_in_period[0]['date']
    end_date = datetime.now(timezone.utc)
    months = []
    current_month = start_date.replace(day=1)
    while current_month <= end_date:
        months.append(current_month)
        next_month_val = current_month.month + 1
        next_year_val = current_month.year
        if next_month_val > 12:
            next_month_val = 1
            next_year_val += 1
        current_month = current_month.replace(year=next_year_val, month=next_month_val)

    bar_labels = [m.strftime('%Y-%m') for m in months]
    monthly_sales = {label: 0 for label in bar_labels}
    monthly_fees = {label: 0 for label in bar_labels}
    monthly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    for month_start in months:
        month_label = month_start.strftime('%Y-%m')
        next_month_start = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < next_month_start:
            event = events_in_period[event_idx]
            event_type, data = event['type'], event['data']
            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:
                    sale_value = data['quantity'] * data['unit_price']
                    monthly_sales[month_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                monthly_fees[month_label] += fee_amount
                accumulated_profit -= fee_amount
            event_idx += 1
        monthly_cumulative_profit.append(accumulated_profit)

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(monthly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(monthly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)
    ax2 = ax.twinx()
    final_profit_line = [0] + monthly_cumulative_profit
    ax2.plot(range(len(months) + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(len(months) + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)
    ax.set_title(f'Performance for {character.name} (All Time)', color='white', fontsize=16)
    ax.set_xlabel('Month', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf