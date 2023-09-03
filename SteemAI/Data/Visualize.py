import math
from random import randint


def calculate_weight(voting_power, score):
    if voting_power >= 70:
        # Calculate weight based on a linear scale between 20% and 80%
        weight_range = 80 - 20
        weight = 20 + (weight_range * (score - 1) / 54)
    else:
        # Calculate weight based on a linear scale between 10% and 60%
        weight_range = 60 - 10
        weight = 10 + (weight_range * (score - 1) / 54)

    # Adjust weight based on voting power
    weight = weight * (voting_power / 100)

    # Ensure weight is within the 1-100 range
    return max(1, min(100, weight))

def roll(odds):
    return randint(0, odds) == 0


def sigmoid_choice(input_value):
    k = 0.1  # You can adjust this constant to control the steepness of the curve
    x0 = 50  # Midpoint of the curve

    odds = 1 / (1 + math.exp(-k * (input_value - x0)))
    return odds

def decide_to_vote_sigmoid_scaled(score, vp, val):
    if val > 1:
        print (f"Value ({val}) too high! Score of {score}. Returning!")
        return -1
    weight = int(calculate_weight(vp, score))
    base = round(((1 - sigmoid_choice(vp)) + .5) * 3) + 2
    sc = int(score/10) - 1
    if sc < 0:
        sc = 0
    roll_input = base - sc
    print(f'vp: {vp} score: {score} odds: 1/{roll_input} weight: {weight}%')
    if roll(roll_input):
        return weight
    else:
        return 0


# Test the function
voting_power = 1  # Example voting power (between 0 and 100)
score = 1  # Example score (between 1 and 55)
decide_to_vote_sigmoid_scaled(score, voting_power, 0.5)