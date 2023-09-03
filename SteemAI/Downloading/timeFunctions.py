from datetime import datetime

def convertTime(date):
    try:
        return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    except Exception:
        return date

def convertTimeStamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')

def getTime(date):
    return int(round(date.timestamp()))
