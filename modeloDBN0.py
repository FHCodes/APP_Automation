from alerta import msg_alerta_alert, msg_alerta_erro, msg_alerta_sucesso

def modelo_DBN0(DBN0s,schema,path):
    try:  
        for dbn0 in DBN0s:
            with open(f"{path}/MODELO_DE_EXPORTACAO_DBN0.txt", "a") as arquivo:            
                arquivo.write(f"    - name: {dbn0}\n")
                arquivo.write(f"      connection: {schema.upper()}\n")
                arquivo.write(f"      newConnection: {schema.upper()}\n")
        
        msg_alerta_sucesso('SUCESSO','Modelo de exportação de DBN0 criado com sucesso')
        
    except BaseException as err:
                msg_alerta_erro('Houve um erro inesperado',f'{err}')