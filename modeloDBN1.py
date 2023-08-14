from alerta import msg_alerta_erro, msg_alerta_sucesso

def modelo_DBN1(DBN0s,schema,path,classe):
   
    try:  
        for dbn0 in DBN0s:
            with open(f"{path}/MODELO_DE_EXPORTACAO_DBN1.txt", "a") as arquivo:            
                arquivo.write(f"    - name: {classe}<->{dbn0}\n")
                arquivo.write(f"      connection: {schema.upper()}\n")
                arquivo.write(f"      newConnection: {schema.upper()}\n")
        
        msg_alerta_sucesso('SUCESSO','Modelo de exportação de DBN1 criado com sucesso')
        
    except BaseException as err:
            msg_alerta_erro('Houve um erro inesperado',f'{err}')