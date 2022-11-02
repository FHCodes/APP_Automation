from masterfile import cria_masterfiles, cria_zip
from arq_exportacao import renomeia_arquivos
import PySimpleGUI as sg

sg.theme('NeutralBlue')

def gera_interface():    
    layout1 = [
        [sg.Text('Selecione o pack',size=(20,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_name",size=(54, 1)),sg.FileBrowse(target='path_name')],
        [sg.Text('Entre com os elementos da coluna Inventory Name',size=(48,1),font='Cambria',background_color='Grey')],
        [sg.Multiline("", size=(61, 10), key="inventory_name") ],
        [sg.Button("Criar"),sg.Button("Zip")]
    ]

    layout2 = [
        [sg.Text('Selecione a pasta com os arquivos de exportação',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_name2",size=(54, 1)),sg.FolderBrowse(target='path_name2')],
        [sg.Button("Renomear")]
    ]

    tab_group = [
        
        [sg.TabGroup([[
            sg.Tab('MasterFiles', layout1, background_color='Grey'),
            sg.Tab('Arquivos de Exportação', layout2, background_color='Grey')]],
            title_color='Black',
            tab_background_color='Gainsboro',
            selected_title_color='white',
            font='Cambria',
           selected_background_color='Gray'  
            )
           
    ]]

  
    janela = sg.Window("APP AUTOMATION - Versão 2", tab_group, icon='icons/esse.ico')
   
        
    ziper = False
    while True:
        print(ziper)
        evento, valores = janela.read()
        
        if evento == sg.WIN_CLOSED:
            break
       
        if evento == "Criar":
            ziper = True
            cria_masterfiles(valores["path_name"], valores["inventory_name"].split('\n'))
        
        if evento == "Renomear":
            print("Entrou aqui")
            renomeia_arquivos(valores["path_name2"])

        if evento == "Zip" and ziper == True:
            cria_zip(valores["path_name"])

    janela.close()

        