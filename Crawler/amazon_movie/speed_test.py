import datetime
import pyodbc
import html
import time

import matplotlib.pyplot as plt
import numpy as np

# use ggplot style for more sophisticated visuals
plt.style.use('ggplot')


def live_plotter(x_vec, y1_data, line1, identifier='', pause_time=0.1):
    if line1 == []:
        # this is the call to matplotlib that allows dynamic plotting
        plt.ion()
        fig = plt.figure(figsize=(13, 6))
        ax = fig.add_subplot(111)
        # create a variable for the line so we can later update it
        line1, = ax.plot(x_vec, y1_data, '-o', alpha=0.8)
        # update plot label/title
        plt.ylabel('Y Label')
        plt.title('Title: {}'.format(identifier))
        plt.show()

    # after the figure, axis, and line are created, we only need to update the y-data
    line1.set_ydata(y1_data)
    # adjust limits if new data goes beyond bounds
    if np.min(y1_data) <= line1.axes.get_ylim()[0] or np.max(y1_data) >= line1.axes.get_ylim()[1]:
        plt.ylim([np.min(y1_data) - np.std(y1_data), np.max(y1_data) + np.std(y1_data)])
    # this pauses the data so the figure/axis can catch up - the amount of pause can be altered above
    plt.pause(pause_time)

    # return line so we can update it again in the next iteration
    return line1


server = 'localhost'
database = 'ETL_ass_1'
username = 'zzj'
password = 'zzjHH'

x = np.linspace(0, 1, 100)[:]
y = np.zeros_like(x)
y = 10
line = []
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
last_time = 0
try:
    while True:
        cursor.execute("select count(*) from movie_info")
        row = cursor.fetchone()
        if row:
            delta = row[0] - last_time
            last_time = row[0]
            remaining = 253059 - last_time
            time_to_finish = remaining / delta
            predict = datetime.datetime.now() + datetime.timedelta(minutes=time_to_finish)
            print('inserted #{} total #{} finish {}'.format(delta, row[0], predict))

        time.sleep(60)

finally:
    cursor.close()
    cnxn.close()
