import PySimpleGUI as sg

# All the stuff inside your window.
layout = [ [sg.Column([[sg.Text('Date', p = 0, border_width=0)]
                        ,[sg.Text(f'<display date>',p=0,border_width=0)]
                        , [sg.Text('Company',p = 0, border_width=0)]
                        ,[sg.Input(p = 0, border_width=0)]]) #column 1 
            ,sg.Column([[sg.Text('Description', p = 0, border_width=0)]
                          ,[sg.Text(f'<display desc.>', p = 0, border_width=0)]
                          ,[sg.Text()]]) # column 2
            , sg.Column([[sg.Text('Amount', p = 0, border_width=0)]
                           ,[sg.Text(f'$ <display amount>', p = 0, border_width=0)]
                           ,[sg.Text('Category', p = 0, border_width=0)]
                           ,[sg.Input(p = 0, border_width=0)]])] # column 3
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