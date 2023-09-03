from Downloading.SteemSQL import SSQL
import pandas as pd
from datetime import datetime, timedelta
from SteemAI import DeepPreprocess as dp
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from matplotlib.lines import Line2D # for the legend
from sklearn.linear_model import LinearRegression

db = SSQL()

def display_avg_value_over_time(value, start_date:datetime=None, end_date:datetime=None, days:int=None, scores:list=[]):
    if end_date == None:
        end_date = datetime.now()
    if days != None:
        start_date = end_date - timedelta(days=days)

    data = db.get_data('cub1_rewards')
    data = data[(data['payout_date'] >= start_date) & (data['payout_date'] <= end_date)]
    data['payout_date'] = data['payout_date'].dt.date
    data = data.dropna()
    if scores != None:
        if len(scores)== 0:
            avg_values = data.groupby(['payout_date']).mean()[[value]]
            sns.lineplot(avg_values, x='payout_date', y=value)
        else:
            data = data[data['score'].isin(scores)]
            avg_values = data.groupby(['payout_date', 'score']).mean()[[value]]
            sns.lineplot(avg_values, x='payout_date', y=value, hue='score', palette="tab10")
    count = data.groupby(data['payout_date']).size().reset_index(name='count')['count']

    #avg_values['count'] = count.values

    plt.show()

def display_total_value_over_time(value:str='cub1_rewards_steem', start_date:datetime=None, end_date:datetime=None, days:int=None):
    if end_date == None:
        end_date = datetime.now()
    if days != None:
        start_date = end_date - timedelta(days=days)

    data = db.get_data('cub1_rewards')
    data = data[(data['payout_date'] >= start_date) & (data['payout_date'] <= end_date)]
    print(data[value].sum())
    data['payout_date'] = data['payout_date'].dt.date
    data = data.dropna()
    count = data.groupby(data['payout_date']).size().reset_index(name='count')['count']
    avg_values = data.groupby(data['payout_date']).sum()[[value]].reset_index()
    avg_values['count'] = count.values
    fig, ax = plt.subplots()
    sns.barplot(avg_values, x='payout_date', y=value, ax=ax)
    fig.autofmt_xdate()
    n = 8
    xtick_locations = ax.get_xticks()[::n]
    xtick_labels = [ax.get_xticklabels()[i].get_text() for i in range(0, len(ax.get_xticks()), n)]
    plt.xticks(xtick_locations, xtick_labels)
    plt.show()

def display_avg_week_value(value:str='cub1_rewards_steem', weeks:int=4):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks)
    data = db.get_data('cub1_rewards')
    data = data[(data['payout_date'] >= start_date) & (data['payout_date'] <= end_date)]
    data.set_index('payout_date', inplace=True)
    data = data.resample('W').sum()
    data = data.dropna()
    data.index = data.index.strftime('%Y-%m-%d')
    data[value].plot.bar()
    plt.xlabel('Date')
    plt.ylabel(f'Weekly total {value}')
    plt.title('Weekly Totals by Date')
    plt.show()

def display_predicted_vs_actual_percentile(curator, days):
    votes = db.get_data(f'{curator}_votes')
    votes['created'] = pd.to_datetime(votes['created'])
    latest_date = datetime.now() - timedelta(days=8)
    earlist_date = latest_date - timedelta(days=days)
    votes = votes[~votes['highest_percentile'].isnull()]
    votes = votes[votes['created'] >= earlist_date]
    votes = votes[votes['val_at_vote']<=1]
    print(f'Votes After Earliest {len(votes)}')
    votes = votes[votes['created'] <= latest_date]
    print(f'Votes After latest {len(votes)}')
    grouped = votes.groupby(['highest_percentile', 'final_percentile']).size().reset_index(name='size')

    # Merge the grouped data back to the original DataFrame
    votes = pd.merge(votes, grouped, on=['highest_percentile', 'final_percentile'], how='left')

    votes = votes.dropna()
    votes = votes[~votes.isnull()]

    votes = votes.dropna()
    votes = votes[~votes.isnull()]

    # Calculate the counts based on the frequency of data points
    counts = votes.groupby(['final_percentile', 'highest_percentile']).size().reset_index(name='count')

    # Merge the counts with the original data
    weighted_votes = votes.merge(counts, on=['final_percentile', 'highest_percentile'])

    x = weighted_votes['final_percentile'].values
    y = weighted_votes['highest_percentile'].values
    weights = weighted_votes['count'].values
    color = weighted_votes['confusion'].values

    min_size = 10
    max_size = 100
    normalized_weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))
    sizes = min_size + normalized_weights * (max_size - min_size)

    fig, ax = plt.subplots(figsize=(12, 8))
    sc = ax.scatter(x=x, y=y, c=color, cmap='coolwarm', alpha=0.8, s=sizes)

    X = x.reshape((-1, 1))

    # Initialize linear regression object with weights
    linear_regressor = LinearRegression()
    linear_regressor.fit(X, y, sample_weight=weights)

    # Make predictions
    x_pred = np.linspace(0, 100, num=200).reshape(-1, 1)
    y_pred = linear_regressor.predict(x_pred)

    residuals = y - linear_regressor.predict(X)
    std_residuals = np.std(residuals)

    ci_upper = y_pred + 1 * std_residuals
    ci_lower = y_pred - 1 * std_residuals

    # Plot regression line
    ax.plot(x_pred, y_pred, color="purple", lw=4)

    plt.fill_between(x_pred.squeeze(), ci_lower, ci_upper, color='blue', alpha=0.1)

    # Add colorbar
    cbar = plt.colorbar(sc)
    cbar.set_label('Confusion')

    ax.set_title('Final Percentile vs. Highest Predicted Percentile')
    ax.set_xlabel('Final Percentile')
    ax.set_ylabel('Highest Predicted Percentile')

    '''size_legend_elements = [plt.Line2D([0], [0], marker='o', color='w', markersize=10, label='Min Weight'),
                            plt.Line2D([0], [0], marker='o', color='w', markersize=50, label='Max Weight')]
    ax.legend(handles=size_legend_elements, title='Sizes', loc='upper right')'''
    plt.ylim(64, None)
    plt.show()

def display_confusion_vs_efficiency(curator, days):
    votes = db.get_data(f'{curator}_votes')
    votes['created'] = pd.to_datetime(votes['created'])
    latest_date = datetime.now() - timedelta(days=8)
    earlist_date = latest_date - timedelta(days=days)
    votes = votes[~votes['highest_percentile'].isnull()]
    votes = votes[votes['created'] >= earlist_date]
    votes = votes[votes['val_at_vote'] <= 1]
    print(f'Votes After Earliest {len(votes)}')
    votes = votes[votes['created'] <= latest_date]
    print(f'Votes After latest {len(votes)}')
    grouped = votes.groupby(['highest_percentile', 'final_percentile']).size().reset_index(name='size')

    # Merge the grouped data back to the original DataFrame
    votes = pd.merge(votes, grouped, on=['highest_percentile', 'final_percentile'], how='left')

    votes = votes.dropna()
    votes = votes[~votes.isnull()]

    votes = votes.dropna()
    votes = votes[~votes.isnull()]

    # Calculate the counts based on the frequency of data points
    counts = votes.groupby(['final_percentile', 'highest_percentile']).size().reset_index(name='count')

    # Merge the counts with the original data
    weighted_votes = votes.merge(counts, on=['final_percentile', 'highest_percentile'])

    x = weighted_votes['efficiency'].values
    y = weighted_votes['score'].values
    weights = weighted_votes['count'].values
    color = weighted_votes['confusion'].values

    min_size = 10
    max_size = 100
    normalized_weights = (weights - np.min(weights)) / (np.max(weights) - np.min(weights))
    sizes = min_size + normalized_weights * (max_size - min_size)

    fig, ax = plt.subplots(figsize=(12, 8))
    sc = ax.scatter(x=x, y=y, alpha=0.8, s=sizes)

    X = x.reshape((-1, 1))

    # Initialize linear regression object with weights
    linear_regressor = LinearRegression()
    linear_regressor.fit(X, y, sample_weight=weights)

    # Make predictions
    x_pred = np.linspace(0, X.max(), num=200).reshape(-1, 1)
    y_pred = linear_regressor.predict(x_pred)

    residuals = y - linear_regressor.predict(X)
    std_residuals = np.std(residuals)

    ci_upper = y_pred + 1 * std_residuals
    ci_lower = y_pred - 1 * std_residuals

    # Plot regression line
    ax.plot(x_pred, y_pred, color="purple", lw=4)

    plt.fill_between(x_pred.squeeze(), ci_lower, ci_upper, color='blue', alpha=0.1)

    # Add colorbar

    ax.set_title('Final Percentile vs. Highest Predicted Percentile')
    ax.set_xlabel('Confusion')
    ax.set_ylabel('Efficiency')

    '''size_legend_elements = [plt.Line2D([0], [0], marker='o', color='w', markersize=10, label='Min Weight'),
                            plt.Line2D([0], [0], marker='o', color='w', markersize=50, label='Max Weight')]
    ax.legend(handles=size_legend_elements, title='Sizes', loc='upper right')'''
    plt.show()

display_avg_week_value('cub1_rewards_steem', weeks=52)