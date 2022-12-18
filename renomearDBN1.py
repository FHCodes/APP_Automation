from os import listdir, rename
from alerta import msg_alerta_alert, msg_alerta_erro, msg_alerta_sucesso

def renomeia_arquivos(path):
    try:

        files = [f for f in listdir(path) if "%3C-%3E" in f]

        if not files:
            msg_alerta_alert('','Nenhum arquivo que precise ser renomeado foi encontrado nessa pasta')

        else:

            for i in files:
                newFile = i.split("%3C-%3E")
                newFile = "-".join(newFile)
                
                print(path + '/'  + i)
                print(path +'/' + newFile)
                
                rename(path + '/'  + i, path +'/' + newFile)
            msg_alerta_sucesso('SUCESSO','Seus arquivos foram renomeados!')

    except BaseException as err:
        msg_alerta_erro('Houve um erro inesperado',f'{err}')