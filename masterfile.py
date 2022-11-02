from types import NoneType
import openpyxl
import os
from alerta import msg_alerta_alert, msg_alerta_erro, msg_alerta_sucesso
import zipfile

def cria_masterfiles(path_name_xlsm,inventory_name):

    path_name = criando_pasta(path_name_xlsm)
    Data_Sources_list, logic_eric, table_name_dic,table_name_list, Data_Sources_Attr_list, pk_list, Data_Sources_Map_list = reducao_dados(path_name_xlsm,inventory_name)
   
    #teste(Data_Sources_list, logic_eric, table_name_dic,table_name_list, Data_Sources_Attr_list, pk_list, Data_Sources_Map_list)

    if not Data_Sources_list and len(inventory_name) > 0 :
        msg_alerta_erro('','Nenhum Inventory Name encontrado no pack')
    
    elif len(table_name_list) <= len(inventory_name) and inventory_name[0] != '':
        diff = set(table_name_list).symmetric_difference(set(inventory_name))
        
        if diff:
            for dif in list(diff):
                msg_alerta_alert(dif,'Inventory Name não encontrado no pack')

        try:
            mtf_prt1(inventory_name,Data_Sources_list, path_name)
            mtf_prt2(inventory_name, Data_Sources_Attr_list,path_name)
            mtf_prt3(inventory_name, Data_Sources_Map_list, table_name_dic, path_name)
            acx(inventory_name, Data_Sources_list, pk_list, path_name)
            
            msg_alerta_sucesso('SUCESSO','MASTERFILES E ACX CRIADOS')
        
        except BaseException as err:
                msg_alerta_erro('Houve um erro inesperado',f'{err}')

            

def criando_pasta(path_name_xlsm):
    temp = path_name_xlsm.split('/')
    temp.pop()
    temp = '/'.join(temp)
    path_name = temp + '/MASTERFILES'

    if not os.path.exists(path_name):
        os.makedirs(path_name)
    
    return path_name


def reducao_dados(path_name_xlsm,inventory_name):
    book = openpyxl.load_workbook(path_name_xlsm)

    Data_Sources = book['3. Data Sources']
    Data_Sources_list = []
    logic_eric = {}
    table_name_dic = {}
    table_name_list = []


    Data_Sources_Attr = book['3. Data Sources Attr & Count']
    Data_Sources_Attr_list = []
    pk_list = []

    Data_Sources_Map = book['3. Data Sources Map']
    Data_Sources_Map_list = []


    for rows in Data_Sources.iter_rows(min_row=4,max_row=4):
        for i, row in enumerate(rows):
            
            if row.value == 'Inventory Name':
                col_Inv_Name = i
            if row.value == 'Table Name':
                col_Tab_Name = i
            if row.value == 'Schema':
                col_Schema = i
            if row.value == 'Description':
                col_Desc = i
    
    for rows in Data_Sources_Attr.iter_rows(min_row=4,max_row=4):
        for i, row in enumerate(rows):

            if row.value == 'Source Name':
                col_SN = i
            if row.value == 'Attribute/Counter Name':
                col_ACN = i
            if row.value == 'Attribute/Counter Physical Name':
                col_ACPN = i
            if row.value == 'Data Type':
                col_DT = i
            if row.value == 'Metrics Attribute Type':
                col_MAT = i
            if row.value == 'Description':
                col_D = i
    
    for rows in Data_Sources_Map.iter_rows(min_row=4,max_row=4):
        for i, row in enumerate(rows):

            if row.value =='Enrichment Table Name':
                col_ETN = i
            if row.value =='Enrichment Attribute Name':
                col_EAN = i
            if row.value =='DBNO Table Name':
                col_DTN = i
            if row.value =='DBN0 Attribute Name':
                col_DAN = i
            if row.value =='AdHoc Join Type': 
                col_AJT = i  
                #print("VALOR",i)
    
    
    
    for i in inventory_name:
        #Data_Sources
        for rows in Data_Sources.iter_rows(min_row=5):
            if rows[col_Inv_Name].value == i:
                table_name_dic[rows[col_Inv_Name].value] = rows[col_Tab_Name].value
                table_name_list.append(rows[col_Inv_Name].value)

                if rows[col_Desc].value != None and "'" in rows[col_Desc].value:
                    desc = rows[col_Desc].value.split("'")
                    desc = "".join(desc)
                    Data_Sources_list.append((rows[col_Inv_Name].value,rows[col_Tab_Name].value,rows[col_Schema].value.split("_")[-1].upper(),desc, rows[col_Schema].value))
                else:
                    Data_Sources_list.append((rows[col_Inv_Name].value,rows[col_Tab_Name].value,rows[col_Schema].value.split("_")[-1].upper(),rows[col_Desc].value, rows[col_Schema].value))
    
            if rows[col_Inv_Name].value == i and rows[col_Schema].value.split("_")[-1].upper() == "ENRICH":    
                logic_eric[rows[col_Inv_Name].value] = "ENRICH"
            elif rows[col_Inv_Name].value == i and rows[col_Schema].value.split("_")[-1].upper() != "ENRICH":
                logic_eric[rows[col_Inv_Name].value] = "DBN0"
        
        #Data_Sources_Attr
        for rows in Data_Sources_Attr.iter_rows(min_row=5):
            if rows[col_SN].value == i:
                if rows[col_D].value != None and "'" in rows[col_D].value:
                    desc1 = rows[col_D].value.split("'")
                    desc1 = "".join(desc1)
                    Data_Sources_Attr_list.append((rows[col_SN].value,rows[col_ACN].value,rows[col_ACPN].value,rows[col_DT].value,rows[col_MAT].value,desc1))
                    print(desc1)
                else:
                    Data_Sources_Attr_list.append((rows[col_SN].value,rows[col_ACN].value,rows[col_ACPN].value,rows[col_DT].value,rows[col_MAT].value,rows[col_D].value))
                
                if rows[5].value.upper() == 'PK':
                    pk_list.append((rows[col_SN].value,rows[col_ACPN].value,rows[col_MAT].value))
         
         #Data_Sources_Map   
        for rows in Data_Sources_Map.iter_rows(min_row=5):
            if i == rows[col_DTN].value:
                Data_Sources_Map_list.append((rows[col_ETN].value,rows[col_EAN].value, rows[col_DTN].value,rows[col_DAN].value,rows[col_AJT].value))
               # print(f"{rows[col_ETN].value}, {rows[col_EAN].value}, {rows[col_DTN].value} ,{rows[col_DAN].value}, {rows[col_AJT].value}")
    
    return Data_Sources_list, logic_eric, table_name_dic, table_name_list, Data_Sources_Attr_list, pk_list, Data_Sources_Map_list
        

def mtf_prt1(inventory_name,Data_Sources_list, path_name):   
    #Masterfiles - parte1
    
    for i in inventory_name:
            for ds in Data_Sources_list:
                if ds[2] != 'ENRICH' and ds[0] == i:
                    with open(f"{path_name}/{i.lower()}.mas", "a") as arquivo:            
                        arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLPSTGR, REMARKS='{ds[3]}', $\n")
                        arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")
                        arquivo.write("FIELDNAME=SEQ_NUMBER, ALIAS=seq_number, USAGE=P21, ACTUAL=P11, $\n")
                        arquivo.write("FIELDNAME=INSERT_DATE, ALIAS=insert_date, USAGE=HYYMDs, ACTUAL=HYYMDs, $\n")
                        arquivo.write("FIELDNAME=SOURCE_ID, ALIAS=source_id, USAGE=A255V, ACTUAL=A255V, $\n")
                        arquivo.write("FIELDNAME=CONTENT_ID, ALIAS=content_id, USAGE=A256V, ACTUAL=A256V, $\n")
                
                elif ds[2] == 'ENRICH' and ds[0] == i:
                    with open(f"{path_name}/{i.lower()}.mas", "a") as arquivo:
                        arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLPSTGR,\n")
                        arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")
        
def mtf_prt2(inventory_name, Data_Sources_Attr_list,path_name):
    #Masterfiles - parte2
    for i in inventory_name:
        for dsa in Data_Sources_Attr_list:
            if dsa[0] == i:
                with open(f"{path_name}/{i.lower()}.mas", "a") as arquivo:
                    if dsa[3] == "TIMESTAMP(3)" and dsa[4] != "Constant":
                        arquivo.write(f"FIELDNAME={dsa[2]}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', DESCRIPTION='{dsa[5]}',USAGE=HYYMDs, ACTUAL=HYYMDs,\n    MISSING=ON, $\n")
                    elif dsa[3][0:7] == "VARCHAR" and dsa[4] != "Constant":
                        arquivo.write(f"FIELDNAME={dsa[2]}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', DESCRIPTION='{dsa[5]}',USAGE=A255V, ACTUAL=A255V,\n    MISSING=ON, $\n")
                    elif dsa[3] == "NUMBER" and dsa[4] != "Constant":
                        arquivo.write(f"FIELDNAME={dsa[2]}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', DESCRIPTION='{dsa[5]}',USAGE=D20.2, ACTUAL=D8,\n    MISSING=ON, $\n")
                   
                    

def mtf_prt3(inventory_name,Data_Sources_Map_list,table_name_dic, path_name):                
   #Masterfiles - parte3
    for i in inventory_name:
        for dsm in Data_Sources_Map_list:
            if dsm[2] == i and dsm[4]:
                with open(f"{path_name}/{i.lower()}.mas", "a") as arquivo:
                    arquivo.write(f"SEGMENT={dsm[0]}, SEGTYPE=KU,PARENT={table_name_dic[dsm[2]]}, CRFILE={dsm[0]}, CRINCLUDE=ALL , CRJOINTYPE={dsm[4].replace(' ', '_').upper()}, JOIN_WHERE={table_name_dic[dsm[2]]}.{dsm[3].upper()} EQ {dsm[0]}.{dsm[1].upper()};,$\n")

def acx(inventory_name, Data_Sources_list, pk_list, path_name):                       
    #Criação do acx
    for i in inventory_name:
        lista_pk = []
        for ds in Data_Sources_list:
            if ds[0] == i:
                with open(f"{path_name}/{i.lower()}.acx", "a") as arquivo:
                    arquivo.write(f"SEGNAME={ds[1]},\n")
                    arquivo.write(f"    TABLENAME={ds[4].lower()}.{ds[1].lower()},\n")
                    arquivo.write(f"    CONNECTION={ds[4].upper()},\n")
                    for pk in pk_list:
                        if pk[0] == i:
                            lista_pk.append(pk[1])
                    arquivo.write(f"    KEY={'/'.join(lista_pk)}, $")
         
def cria_zip(path_name_xlsm):
    temp = path_name_xlsm.split('/')
    temp.pop()
    temp = '/'.join(temp)
    path_name = temp + '/MASTERFILES'

    if not os.path.exists(path_name+',zip'):
        z = zipfile.ZipFile(f'{temp}/MASTERFILES.zip', 'w', zipfile.ZIP_DEFLATED)
        z.write(path_name)
        z.close()