import os
import shutil

class CreateAndDeleteFolder:
    def create_folder(self, path_name_xlsm:str):
        temp = path_name_xlsm.split('/')
        nome_pack = temp.pop().split('.')
        nome_pack.pop()
        nome_pack = '.'.join(nome_pack)
        temp = '/'.join(temp)
        path_name = f"{temp}/MASTERFILES_{nome_pack}"
        if not os.path.exists(path_name):
            os.makedirs(path_name)
        return path_name

    def delete_folder(self, path_name):
        shutil.rmtree(path_name)