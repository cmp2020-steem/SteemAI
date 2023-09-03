def get_block(days_ago, current_block):
    blocks_per_day = 20*60*24
    print(blocks_per_day)
    return current_block - (blocks_per_day * days_ago)