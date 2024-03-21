from datetime import datetime
date = '2024-03-21'
dt = datetime.strptime(date, '%Y-%m-%d').day
print(dt)