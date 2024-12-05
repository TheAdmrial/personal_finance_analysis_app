import PySimpleGUI as sg

# All the stuff inside your window.
layout = [  [sg.Text('Date'), sg.Text('Description'), sg.Text('Amount')]
          ,[sg.Text(f'<display date>'),sg.Text(f'<display desc.>'),sg.Text(f'$ <display amount>')]
          ,[sg.Text('Company'),sg.Text('Category')]
          ,[sg.Input(),sg.Input()]
          ,[sg.Button('Confirm'), sg.Button('Skip'),sg.Button('Cancel')] ]

# Create the Window
window = sg.Window('Categorize Expenses', layout)

# Event Loop to process "events" and get the "values" of the inputs
while True:
    event, values = window.read()
    if event == sg.WIN_CLOSED or event == 'Cancel': # if user closes window or clicks cancel
        break
    print('You entered ', values[0])

window.close()