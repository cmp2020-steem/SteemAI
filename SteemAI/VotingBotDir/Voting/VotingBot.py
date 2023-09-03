from steem import Steem
from steem.account import Account
import datetime

class VotingBot(Account):
    def __init__(self, account, posting_key):
        Account.__init__(self, account)
        self.account = account
        self.posting = posting_key
        self.s = Steem(keys=[posting_key])

    def get_current_voting_power(self):
        account = self.s.get_accounts([self.account])[0]
        voting_power = account['voting_power']

        # Get the account's history
        current_index = int(self.s.get_account_history(self.account, -1, 1)[0][0])
        found = False
        while not found:
            history = self.s.get_account_history(self.account, current_index, 100)

            # Find the last vote in the history
            for entry in history:
                if entry[1]['op'][0] == 'vote':
                    last_vote_time = entry[1]['timestamp']
                    found = True
                    break

            current_index -= 100
        # Convert the timestamp to a datetime object
        last_vote_datetime = datetime.datetime.strptime(last_vote_time, '%Y-%m-%dT%H:%M:%S')

        # Calculate the number of days that have passed since the last vote
        current_time = datetime.datetime.utcnow()
        secs_since_last_vote = (current_time - last_vote_datetime).seconds

        # Adjust the voting power by the number of days that have passed since the last vote
        adjusted_voting_power = voting_power + secs_since_last_vote * 1/86400
        return adjusted_voting_power/100

    def get_post_voted_on(self, author, permlink):
        votes = self.s.steemd.get_active_votes(author, permlink)
        for vote in votes:
            if vote['voter'] == self.account:
                return True
        return False


    def vote(self, post, weight):
        identifier = f"@{post['author']}/{post['permlink']}"
        try:
            self.s.commit.vote(identifier, float(weight), self.account)
        except Exception as e:
            print(e)
