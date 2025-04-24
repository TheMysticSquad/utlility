from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.textinput import TextInput
from kivy.uix.label import Label
from kivy.uix.button import Button
from .db_manager import DBManager

class ConsumerApp(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(orientation='vertical', **kwargs)
        self.db = DBManager()

        self.ca_input = TextInput(hint_text="Enter CA Number", multiline=False)
        self.ca_input.bind(on_text_validate=self.fetch_category)
        self.add_widget(self.ca_input)

        self.category_label = Label(text="Category: ")
        self.add_widget(self.category_label)

        self.reading_input = TextInput(hint_text="Enter Meter Reading", multiline=False, input_filter='int')
        self.add_widget(self.reading_input)

        self.save_btn = Button(text="Save")
        self.save_btn.bind(on_press=self.save_data)
        self.add_widget(self.save_btn)

    def fetch_category(self, instance):
        ca = self.ca_input.text
        consumer = self.db.get_consumer(ca)
        if consumer:
            category, reading = consumer[1], consumer[2]
            self.category_label.text = f"Category: {category}"
            self.reading_input.text = str(reading)
        else:
            self.category_label.text = "Category: Domestic"

    def save_data(self, instance):
        ca = self.ca_input.text
        category = self.category_label.text.split(": ")[-1]
        reading = int(self.reading_input.text)
        self.db.insert_or_update(ca, category, reading)
        self.category_label.text = "Category: "
        self.ca_input.text = ""
        self.reading_input.text = ""
        print("Saved Successfully!")

class ConsumerMeterApp(App):
    def build(self):
        return ConsumerApp()
