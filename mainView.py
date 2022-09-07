from masterfile import cria_masterfiles, cria_zip
import PySimpleGUI as sg

sg.theme('NeutralBlue')

def gera_interface():    
    layout = [
        [sg.Text('Selecione o pack',size=(20,1),font='consolas')],
        [sg.InputText(key="path_name",size=(54, 1)),sg.FileBrowse(target='path_name')],
        [sg.Text('Entre com os elementos da coluna Inventory Name',size=(48,1),font='consolas')],
        [sg.Multiline("", size=(61, 10), key="inventory_name") ],
        [sg.Button("Enter"),sg.Button("Zip")]  
    ]


    #janela = sg.Window("Altaia Masterfiles Automation TIM", layout, icon='esse.ico')
    janela = sg.Window("Altaia Masterfiles Automation TIM", layout)
   
        
    ziper = False
    while True:
        print(ziper)
        evento, valores = janela.read()
        if evento == sg.WIN_CLOSED:
            break
        if evento == "Enter":
            ziper = True
            cria_masterfiles(valores["path_name"], valores["inventory_name"].split('\n'))
        if evento == "Zip" and ziper == True:
            cria_zip(valores["path_name"])
    janela.close()

        