
percentile_path = '../SteemAI/percentiles_to_predict.txt'
with open(percentile_path, 'r') as f:
    percs = eval(f.read())

def get_line_for_table(title, author, permlink, score, hp):
    return f"| [{title}](https://www.steemit.com/@{author}/{permlink}) | [{author}](https://www.steemit.com/@{author}) | {score} | {hp} |"

print(get_line_for_table("""A Beginner's Guide to Formatting on the Steem Blockchain (in Markdown/HTML)""", 'cmp2020', 'a-beginner-s-guide-to-formatting-on-the-steem-blockchain-in-markdown-html', 10, 80))