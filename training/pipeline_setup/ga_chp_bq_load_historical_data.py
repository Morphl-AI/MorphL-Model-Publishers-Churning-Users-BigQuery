import datetime
from sys import argv, exit


def get_record(i, num_days, ref_dt):
    dt = ref_dt + datetime.timedelta(days=num_days)

    return (i, {'days_worth_of_data_to_load': str(num_days),
                'asYYYY-MM-DD': dt.strftime('%Y-%m-%d'),
                'as_py_code': dt})


now = datetime.datetime.now()

# Enter start date for the training interval
start_date = input(
    'Enter your start date for retrieving the training data (YYYY-MM-DD):')

try:
    start_date = datetime.datetime.strptime(str(start_date), '%Y-%m-%d')
except ValueError:
    print('Incorrect data format, should be YYYY-MM-DD')
    exit(1)

if start_date > now - datetime.timedelta(days=6):
    print('The start date should be at least 5 days the past')
    exit(1)

# Keep only those training intervals that don't exceed the current date
OPTIONS = []
for opt in [5, 10, 30, 60, 90, 120, 180, 270, 365]:
    delta = now - (start_date + datetime.timedelta(days=opt))
    if delta.days > 0:
        OPTIONS.append(opt)

# Compose list of options for training interval
opt_len = len(OPTIONS)
valid_inputs = set([str(i+1) for i in range(opt_len)])
lookup_dict = \
    dict([get_record(i + 1, num_days_ago, start_date)
          for (i, num_days_ago) in enumerate(OPTIONS)])

for _ in range(5):
    print('')

# Display options for training interval
print('How much historical data should be loaded?\n')
for (j, num_days) in enumerate(OPTIONS):
    choice = j + 1
    print('{}) {} - {} ({} days worth of data)'.format(
        choice,
        start_date.strftime('%Y-%m-%d'),
        lookup_dict[choice]['asYYYY-MM-DD'],
        num_days))

# Select training interval option
print('')
entered_choice = input(
    'Select one of the numerical options 1 thru {}: '.format(opt_len))
print('')
if entered_choice in valid_inputs:
    choice = int(entered_choice)

    # Set the training window start (how many days in the past)
    with open(argv[1], 'w') as fh1:
        delta = (now - lookup_dict[choice]['as_py_code']).days
        fh1.write(str(delta))

    # Set the training window interval (how many days of data to train on)
    with open(argv[2], 'w') as fh2:
        fh2.write(lookup_dict[choice]['days_worth_of_data_to_load'])

    # Set the today's date as py code
    with open(argv[3], 'w') as fh3:
        fh3.write(now.__repr__())

else:
    print('No valid choice was selected, aborting.')
    print('')
    exit(1)
