import System
import os
import tkinter as tk
from PIL import ImageTk
from functools import partial
import plotly.graph_objects as go

global current_stock


class GraphicalUI:

    def __init__(self) -> None:
        self.imageLabel = None
        self.plot_frame = None
        self.time_frame = None
        self.graph_frame = None
        self.stock_frame = None
        self.canvas = None
        self.background = None
        self.root_image_label = None
        self.current_stock = None
        self.root = None
        self.sys = System.System()
        self.HEIGHT = 650
        self.WIDTH = 1000
<<<<<<< HEAD
        GraphicalUI.delete_images()
=======
        self.delete_images()
>>>>>>> 90f66e589bd5df0c8dcbe722161ae059ba0ea3e8
        self.setup_frames()
        self.create_stock_buttons(self.sys.stocks)
        self.root.mainloop()

    @staticmethod
    def delete_images():
        dir_name = "./images"
        test = os.listdir(dir_name)

        for item in test:
            if item.endswith(".png"):
                os.remove(os.path.join(dir_name, item))

    def setup_frames(self):
        self.root = tk.Tk()
        self.root.title("STOCK ANALYSIS")
        self.current_stock = ''

        self.root_image_label = tk.Label(self.root)
        self.root_image_label.pack()

        self.background = ImageTk.PhotoImage(file="images/background/background.png")
        self.root_image_label.configure(image=self.background)
        self.root_image_label.image = self.background

        self.canvas = tk.Canvas(self.root, height=self.HEIGHT, width=self.WIDTH, bg='#ffffff')
        self.canvas.pack()

        self.stock_frame = tk.Frame(self.root, bg='#ffffff')  # , bg = '#ffdbfb'
        self.stock_frame.place(relx=0.05, rely=0.005, relwidth=0.25, relheight=0.9)

        self.graph_frame = tk.Frame(self.root, bg='#ffffff')  # , bg = '#cfdafc'
        self.graph_frame.place(relx=0.35, rely=0.15, relwidth=0.6, relheight=0.7)

        self.time_frame = tk.Frame(self.graph_frame, bg='#ffffff')  # , bg = '#d6fccf'
        self.time_frame.place(relx=0.05, rely=0.85, relwidth=0.9, relheight=0.1)

        self.plot_frame = tk.Frame(self.graph_frame, bg='#ffffff')  # , bg = '#d6fccf'
        self.plot_frame.place(relx=0, rely=0.05, relwidth=1, relheight=0.7)

        self.imageLabel = tk.Label(self.plot_frame)
        self.imageLabel.pack()

    def create_stock_buttons(self, stocks):
        for i, j in zip(range(1, len(stocks) + 1), stocks):
            button = tk.Button(self.stock_frame, bd=1, relief='ridge', text="%s" % (j),
                               command=partial(self.plot, '15 min', j))
            button.place(relx=0.2, rely=0.2 * i, relwidth=0.6, relheight=0.1)

    def create_time_buttons(self, stock, time_stamps=['15 min', '1 H', '1 D', '1 W']):
        for i, j in zip(range(0, len(time_stamps)), time_stamps):
            button = tk.Button(self.time_frame, bd=1, relief='ridge', text="%s" % (j),
                               command=partial(self.plot, j, stock))
            button.place(relx=0.06 * i * 1.7, rely=0.15, relwidth=0.09, relheight=0.7)

    def create_ta_dropdown(self, technical_analysis_tools=['SMA', 'EMA', 'RSI', 'Bollinger']):
        def_value = tk.StringVar(self.time_frame)
        def_value.set("-Technical Analysis Tools-")
        drop = tk.OptionMenu(self.time_frame, def_value, *technical_analysis_tools)
        drop.place(relx=0.45, rely=0.1, relwidth=0.5, relheight=0.8)

    def graph_visualisation(self, x_axis, y_axis, custom, label, duration):
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=x_axis, y=y_axis, customdata=custom, hovertemplate='<br>'
                                 .join(
            ['Open : $%{y:.2f}', 'Date : %{x}', '%{customdata[0]}', '%{customdata[1]}', '%{customdata[2]}']), ))

        fig.update_layout(xaxis_title="Date", yaxis_title=f'{label}')

        file_name = "images/{0}{1}.png".format(label, duration)
        if not os.path.exists(file_name):
            fig.write_image(file_name)

        image = ImageTk.PhotoImage(file=file_name)
        self.imageLabel.configure(image=image)
        self.imageLabel.image = image

    def get_current_stock(self):
        return self.current_stock

    def set_current_stock(self, stock):
        self.current_stock = stock
        return self.current_stock

    def plot(self, frequency, stock_name=''):
        if stock_name == '':
            stock_name = self.get_current_stock()

        datetime, close_val, extras = [], [], []

        if frequency == '1 W':
            data = self.sys.weekly_data

        if frequency == '1 D':
            data = self.sys.daily_data

        if frequency == '1 H':
            data = self.sys.hourly_data
            stock_name = self.set_current_stock(stock_name)

        if frequency == '15 min':
            data = self.sys.intraday_data

        for i in data[stock_name]:
            datetime.append(i)
            close_val.append(float(data[stock_name][i]['Close']))
            extras.append(["Open: " + str(data[stock_name][i]['Open']), "High: " + str(data[stock_name][i]['High']),
                           "Low: " + str(data[stock_name][i]['Low'])])

        extras.reverse()
<<<<<<< HEAD

        self.create_time_buttons(stock_name)
        self.create_ta_dropdown()
        self.graph_visualisation(datetime, close_val, extras, stock_name.upper(), frequency)


=======
        
        
        self.create_time_buttons(stock_name)
        self.create_TA_dropdown()
        self.graph_visualisation(datetime, close_val, extras, stock_name.upper(), frequency)
>>>>>>> 90f66e589bd5df0c8dcbe722161ae059ba0ea3e8
