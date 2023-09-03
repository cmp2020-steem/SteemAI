import datetime
from steem import Steem

def get_curation_rewards(account, days):
    # Create a Steem client
    client = Steem()

    # Set the time range
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=days)

    # Get the account's curation rewards during the time range
    history = []
    start = -1
    limit = 100
    rewards_found = False
    conversion_rate = float(
        client.steemd.get_dynamic_global_properties()["total_vesting_fund_steem"].replace(' STEEM', '')) / float(
        client.steemd.get_dynamic_global_properties()["total_vesting_shares"].replace(' VESTS', ''))

    curation_rewards = []
    # Loop until the last result is before the start time
    while not rewards_found:
        # Get a batch of results
        batch = client.steemd.get_account_history(account=account, index_from=start, limit=limit)

        # Filter the batch for curation rewards
        full_trx = []
        for tx in batch:
            if tx[1]["op"][0] == "curation_reward":
                timestamp = datetime.datetime.strptime(tx[1]["timestamp"], "%Y-%m-%dT%H:%M:%S")
                if timestamp >= start_time:
                    curation_rewards.append(float(tx[1]['op'][1]['reward'].replace(' VESTS', '')) * conversion_rate)
                    full_trx.append(tx)
                else:
                    rewards_found = True
                    break
        start = batch[-1][0] - 101

    # Get the current conversion rate

    # Convert the rewards from vests to SP

    total_rewards = 0
    for reward in curation_rewards:
        total_rewards += reward

    return total_rewards

print(get_curation_rewards('cub1', 7))