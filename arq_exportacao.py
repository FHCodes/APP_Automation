from os import listdir, rename
from os.path import isfile

def renomeia_arquivos(path):

    files = [f for f in listdir(path)]

    for i in files:
        newFile = i.split("%3C-%3E")
        newFile = "-".join(newFile)
        
        print(path + '/'  + i)
        print(path +'/' + newFile)
        
        rename(path + '/'  + i, path +'/' + newFile)