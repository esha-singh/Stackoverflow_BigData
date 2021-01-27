import kivy
from kivy.app import App
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput
from kivy.uix.button import Button
import os
import sys
import CassandraHelper
import matplotlib.pyplot as plt
import numpy as np
from kivy.uix.dropdown import DropDown
from kivy.uix.button import Button
from kivy.base import runTouchApp


class AnnualTrendsClientGrid(GridLayout):
    def __init__(self, **kwargs):
        super(AnnualTrendsClientGrid, self).__init__(**kwargs)
        self.cols = 1

        self.inside = GridLayout(padding=[30, 2, 30, 2], spacing=2)
        self.inside.cols = 1

        self.inside.add_widget(Label(text="Domain: ", size_hint_y=None, height=32))
        self.dropdown = DropDown()
        result_set = []
        for domain in CassandraHelper.get_all_domains().current_rows:
            result_set.append(domain.domain)

        domains = list(set(result_set))
        domains.sort()
        for domain in domains:
            btn = Button(text=domain, size_hint_y=None, height=32)
            btn.bind(on_release=lambda btn: self.dropdown.select(btn.text))
            self.dropdown.add_widget(btn)

        self.mainbutton = Button(text='Select domain', size_hint_y=None, height=35)
        self.mainbutton.bind(on_release=self.dropdown.open)
        self.dropdown.bind(on_select=lambda instance, x: setattr(self.mainbutton, 'text', x))
        self.inside.add_widget(self.mainbutton)

        self.inside.add_widget(Label(text="Year to investigate trending topics: ", size_hint_y=None, height=32))
        self.year = TextInput(multiline=False, size_hint_y=None, height=32)
        self.inside.add_widget(self.year)

        self.inside.add_widget(Label(text="Top k topics: ", size_hint_y=None, height=32))
        self.k = TextInput(multiline=False, size_hint_y=None, height=32)
        self.k.text = "10"
        self.inside.add_widget(self.k)

        self.btnGetTopk = Button(text="Get top k topics", size_hint_y=None, height=35)
        self.btnGetTopk.bind(on_press=self.visualize_trending_topics)
        self.inside.add_widget(self.btnGetTopk)

        self.inside.add_widget(Label(text="Topic to visualize trend:", size_hint_y=None, height=32))
        self.topic = TextInput(multiline=False, size_hint_y=None, height=32)
        self.inside.add_widget(self.topic)

        self.btnVisualizeTrend = Button(text="Visualize topic trend from 2015 to 2019", size_hint_y=None, height=35)
        self.btnVisualizeTrend.bind(on_press=self.visualize_annual_tag_trend)
        self.inside.add_widget(self.btnVisualizeTrend)

        self.add_widget(self.inside)

    def visualize_trending_topics(self, instance):
        year = int(self.year.text)
        k = int(self.k.text)
        domain = self.mainbutton.text
        tags = CassandraHelper.get_annual_trend_tag_by_year(domain, year)
        if tags[0].tags:
            all_tags = tags[0].tags.items()
            all_tags = sorted(all_tags, key=lambda x: x[1])
            all_tags.reverse()
            all_tags = all_tags[0:k]

            x_pos = np.arange(len(all_tags))
            plt.bar(x_pos, [x[1] for x in all_tags], align='center', alpha=0.5)
            plt.xticks(x_pos, [x[0] for x in all_tags])
            plt.ylabel('Usage')
            plt.title('Top '+ str(k) +' Topics')

            plt.show()
        else:
            print("No topics found for the given domain and year.")

    def visualize_annual_tag_trend(self, instance):
        domain = self.mainbutton.text
        tag = self.topic.text
        years = {2015: 0, 2016: 0, 2017: 0, 2018: 0, 2019: 0}
        for year in years:
            tags = CassandraHelper.get_annual_trend_tag_by_year(domain, year)
            if tags[0].tags.get(tag):
                years[year] = tags[0].tags.get(tag)
        
        years = sorted(years.items() ,  key=lambda x: x[0] )
        x_pos = np.arange(len(years))
        years_x = [x[0] for x in years]
        years_x[-1] = "2019\ntill Aug"
        plt.plot(x_pos, [x[1] for x in years])
        plt.xticks(x_pos, years_x)
        plt.ylabel('Usage')
        plt.xlabel('Year')
        plt.title('Topic Trend - ' + tag)

        plt.show()

class MyApp(App):
    def build(self):
        self.title = "Annual Topics Trends Client"
        return AnnualTrendsClientGrid()

if __name__ == "__main__":
    MyApp().run()
