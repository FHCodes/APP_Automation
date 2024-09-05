from masterfile import cria_masterfiles, cria_zip
from renomearDBN1 import renomeia_arquivos
from modeloDBN0 import modelo_DBN0
from modeloDBN1 import modelo_DBN1
import PySimpleGUI as sg

sg.theme('NeutralBlue')

def gera_interface():    
    layout1 = [
        [sg.Text('Selecione o pack:',size=(20,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_name",size=(65, 1)),sg.FileBrowse(target='path_name')],
        [sg.Text('Entre com o nome da pasta da masterfile:',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.InputText("", size=(74, 1), key="caminho_master") ],
        [sg.Text('Entre com os elementos da coluna Inventory Name:',size=(40,1),font='Cambria',background_color='Grey'),sg.Checkbox("Masterfiles Oracle",background_color='Grey',font='Cambria',key="Vmf")],
        [sg.Multiline("", size=(72, 10), key="inventory_name") ],
        [sg.Button("Criar"),sg.Button("Zip")]
    ]

    layout2 = [
        [sg.Text('Selecione a pasta com os arquivos exportados do relacionamento DBN1:',size=(58,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_name2",size=(65, 1)),sg.FolderBrowse(target='path_name2')],
        [sg.Image("icons/Altaia.png",background_color='Grey',size=(523,250))],
        [sg.Button("Renomear")]
    ]

    layout3 = [
        [sg.Text('Onde deseja salvar:',size=(20,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_DBN0",size=(65, 1)),sg.FolderBrowse(target='path_DBN0')],
        [sg.Text('Entre com o nome de inventário das DBN0s:',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.Multiline("", size=(72, 10), key="lista_DBN0") ],
        [sg.Text('Entre com o schema:',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.InputText("", size=(74, 1), key="schema_DBN0") ],
        [sg.Button("Gerar DBN0")]
    ]

    layout4 = [
        [sg.Text('Onde deseja salvar:',size=(20,1),font='Cambria',background_color='Grey')],
        [sg.InputText(key="path_DBN1",size=(65, 1)),sg.FolderBrowse(target='path_DBN1')],
        [sg.Text('Entre com o nome de inventário das DBN0s:',size=(50,1),font='Cambria',background_color='Grey')],
        [sg.Multiline("", size=(72, 7), key="lista_DBN1")],
        [sg.Text('Entre com o schema:',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.InputText("", size=(74, 1), key="schema_DBN1") ],
        [sg.Text('Entre com o nome lógico da classe:',size=(40,1),font='Cambria',background_color='Grey')],
        [sg.InputText("", size=(74, 1), key="classe_DBN1") ],
        [sg.Button("Gerar DBN1")]
    ]
    
    tab_group = [
        
        [sg.TabGroup([[
            sg.Tab('MasterFiles', layout1, background_color='Grey'),
            sg.Tab('Renomear DBN1', layout2, background_color='Grey'),
            sg.Tab('Modelo exp DBN0', layout3, background_color='Grey'),
            sg.Tab('Modelo exp DBN1', layout4, background_color='Grey')]],
            title_color='Black',
            tab_location="centertop",
            border_width=1,
            tab_background_color='Gainsboro',
            selected_title_color='white',
            font='Cambria',
           selected_background_color='Gray'  
            )
           
    ]]

  
    janela = sg.Window("APP AUTOMATION - Versão 4.0", tab_group, icon='icons/esse.ico')
   
        
    ziper = False
    while True:
        evento, valores = janela.read()
        
        if evento == sg.WIN_CLOSED:
            break
       
        if evento == "Criar":
            ziper = True
            cria_masterfiles(valores["path_name"], valores["inventory_name"].split('\n'),valores["Vmf"],valores["caminho_master"])
        
        if evento == "Organizar":
            ziper = True
            mostra_lista(valores["path_name"], valores["dados_app_conf"].split('\n'))
        
        if evento == "Renomear":
            renomeia_arquivos(valores["path_name2"])
        
        if evento == "Gerar DBN0":
            modelo_DBN0(valores["lista_DBN0"].split('\n'), valores["schema_DBN0"].rstrip('\n'),valores["path_DBN0"])

        if evento == "Gerar DBN1":
            modelo_DBN1(valores["lista_DBN1"].split('\n'), valores["schema_DBN1"].rstrip('\n'),valores["path_DBN1"],valores["classe_DBN1"])

        if evento == "Zip" and ziper == True:
            cria_zip(valores["path_name"])

    janela.close()

        