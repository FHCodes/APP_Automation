from masterfile import cria_masterfiles
import PySimpleGUI as sg
import time

sg.theme('LightBlue2')

def gera_interface():
    inicio1 = time.time()
    layout = [
        [sg.Text('Selecione o pack desejado')],
        [sg.InputText(key="path_name"),sg.FileBrowse(target='path_name')],
        [sg.Text('Entre com os elementos da coluna Inventory Name')],
        [sg.Multiline("", size=(52, 10), key="inventory_name") ],
        [sg.Button("Enter")]  
    ]

    janela = sg.Window("Altaia Automation", layout)

    while True:
        evento, valores = janela.read()
        if evento == sg.WIN_CLOSED:
            break
        if evento == "Enter":
            cria_masterfiles(valores["path_name"], valores["inventory_name"].split('\n'))
            sg.popup('Sucesso !!!')

    janela.close()
    fim1 = time.time()
    print("Interface: ",fim1 - inicio1)
        