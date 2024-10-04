import pandas as pd
import os
import shutil
from collections import Counter
from alerta import msg_alerta_alert, msg_alerta_erro, msg_alerta_sucesso
import zipfile

def create_folder(path_name_xlsm:str):
    temp = path_name_xlsm.split('/')
    nome_pack = temp.pop().split('.')
    nome_pack.pop()
    nome_pack = '.'.join(nome_pack)
    temp = '/'.join(temp)
    path_name = f"{temp}/MASTERFILES_{nome_pack}"
    if not os.path.exists(path_name):
        os.makedirs(path_name)
    return path_name

def delete_folder(path_name):
    shutil.rmtree(path_name)

class MasterfileCreator:
    def __init__(self, path_name_xlsm, inventory_name, status, caminho_master, path_name_created):
        self.path_name_xlsm = path_name_xlsm
        self.inventory_name = inventory_name
        self.status = status  # True for Oracle, False for PostgreSQL
        self.caminho_master = caminho_master
        self.path_name = path_name_created
        self.data_processor = DataProcessor(path_name_xlsm, inventory_name, self.path_name)

    def create_masterfiles(self, progress_callback=None):
        
        try:
            result = True
            # Inicialização do progresso
            if progress_callback:
                progress_callback(5)
                
            # Check for input inventory_name duplicated
            inventory_name_counter = Counter(self.inventory_name)
            dup_inventory_name = [item for item, count in inventory_name_counter.items() if count > 1]
            if dup_inventory_name:
                elementos = '\n'.join(dup_inventory_name)
                plural = 'elementos' if len(dup_inventory_name) > 1 else 'elemento'
                verbo = 'foram' if len(dup_inventory_name) > 1 else 'foi'
                msg_alerta_erro(
                    "Ocorreu um erro ao gerar as masterfiles!",
                    f"O(s) seguinte(s) {plural} da coluna Inventory Name, {verbo} inserido(s) mais de uma vez. Favor verificar:\n{elementos}"
                )
                progress_callback(0)
                result = False
                return result
                
            # Atualização do progresso
            if progress_callback:
                progress_callback(15)

            # Processamento dos dados
            (data_sources_list, table_name_dic, table_name_list,
             data_sources_attr_list, pk_list, data_sources_map_list) = self.data_processor.process_data()
            
            
            # Combina todos os elementos de data_sources_list em um set
            data_sources_elements = set().union(*data_sources_list)

            # Encontra os elementos em Inventory Name que não estão no data_sources_elements
            missing_elements = set(self.inventory_name) - data_sources_elements

            if missing_elements:
                sorted_elements = sorted(missing_elements)
                elements_str = '\n'.join(sorted_elements)
                msg_alerta_alert(
                    "Alerta!",
                    f"Elemento(s) não encontrado(s) no pack:\n{elements_str}"
                )
                progress_callback(0)
                result = False
                return result   
                
            # Atualização do progresso
            if progress_callback:
                progress_callback(30)

            inventory_name_extracted = [item[0] for item in data_sources_list]
            count = Counter(inventory_name_extracted)
            dup_data_sources = [value for value, qtd in count.items() if qtd > 1]

            if dup_data_sources:
                msg_alerta_erro(
                    "Ocorreu um erro ao gerar as masterfiles!",
                    f"O valor ({dup_data_sources[0]}) da coluna Inventory Name está repetido na aba '3. Data Sources'"
                )
                result = False
                return result

            elif len(table_name_list) <= len(inventory_name_extracted) and inventory_name_extracted[0] != '':
                print("table_name_list")
                print(table_name_list)
                print("inventory_name_extracted")
                print(inventory_name_extracted)
                diff = set(table_name_list).symmetric_difference(set(inventory_name_extracted))
                print("diff")
                print(diff)
                if diff:
                    for dif in list(diff):
                        msg_alerta_alert(dif, 'Inventory Name não encontrado no pack')

                # Atualização do progresso
                if progress_callback:
                    progress_callback(50)

                try:
                    masterfile_generator = MasterfileGenerator(self.status, self.path_name, self.caminho_master)
                    masterfile_generator.generate(inventory_name_extracted, data_sources_list,
                                                  data_sources_attr_list, data_sources_map_list, table_name_dic)
                except BaseException as err:
                    msg_alerta_erro('Ocorreu um erro ao gerar as masterfiles!', f'{err}')
                    result = False
                    return result

                # Atualização do progresso
                if progress_callback:
                    progress_callback(80)

                try:
                    acx_generator = ACXGenerator(self.path_name)
                    acx_generator.generate(inventory_name_extracted, data_sources_list, pk_list)
                except Exception:
                    msg_alerta_erro(
                        'Ocorreu um erro ao tentar gerar as masterfiles!',
                        'Erro na geração do ACX.'
                    )
                    result = False
                    return result

                # Finalização do progresso
                if progress_callback:
                    progress_callback(100)
            
                msg_alerta_sucesso('SUCESSO','MASTERFILES E ACX CRIADOS')
            return result
        except Exception as e:
            msg_alerta_erro('Ocorreu um erro inesperado ao gerar as masterfiles!', str(e))
            result = False
            return result

class DataProcessor:
    def __init__(self, path_name_xlsm, inventory_name, path_name):
        self.path_name_xlsm = path_name_xlsm
        self.inventory_name = inventory_name
        self.path_name = path_name

    def process_data(self):
        try:
            
            # Carrega os cabeçalhos das abas relevantes
            data_sources_header = self._carregar_planilha('3. Data Sources')
            data_sources_attr_header = self._carregar_planilha('3. Data Sources Attr & Count')
            data_sources_map_header = self._carregar_planilha('3. Data Sources Map')

            # Valida cabeçalhos
            self._validar_cabecalho(data_sources_header, 1, 'Inventory Name', "O cabeçalho deve ficar na 4 linha (Data Sources)")
            self._validar_cabecalho(data_sources_attr_header, 1, 'Source Name', "O cabeçalho deve ficar na 4 linha (Data Sources Attr)")
            self._validar_cabecalho(data_sources_map_header, 1, 'Enrichment Table Name', "O cabeçalho deve ficar na 4 linha (Data Sources Map)")

            # Extrai índices das colunas
            col_indices_DS = self._extrair_indices_cabecalho(
                data_sources_header, ['Inventory Name', 'Table Name', 'Schema', 'Description']
            )
            col_indices_DSA = self._extrair_indices_cabecalho(
                data_sources_attr_header, [
                    'Source Name', 'Attribute/Counter Name', 'Attribute/Counter Physical Name',
                    'Data Type', 'Mediation Type', 'Metrics Attribute Type', 'Description'
                ]
            )
            col_indices_DSM = self._extrair_indices_cabecalho(
                data_sources_map_header, [
                    'Enrichment Table Name', 'Enrichment Attribute Name', 'DBNO Table Name', 'DBN0 Attribute Name', 'AdHoc Join Type'
                ]
            )

            # Lê os dados das planilhas
            data_sources = self._ler_dados('3. Data Sources', list(col_indices_DS.values()))
            data_sources_attr = self._ler_dados('3. Data Sources Attr & Count', list(col_indices_DSA.values()))
            data_sources_map = self._ler_dados('3. Data Sources Map', list(col_indices_DSM.values()))

            # Filtra os dados com base no inventory_name
            ds_filtered = self._filtrar_dados_por_inventory(data_sources, self.inventory_name, 0)
            dsa_filtered = self._filtrar_dados_por_inventory(data_sources_attr, self.inventory_name, 0)
            dsm_filtered = self._filtrar_dados_por_inventory(data_sources_map, self.inventory_name, 2)

            # Processa os dados filtrados
            data_sources_list, table_name_dic, table_name_list = self._processar_data_sources(ds_filtered)
            data_sources_attr_list, pk_list = self._processar_data_sources_attr(dsa_filtered)
            data_sources_map_list = self._processar_data_sources_map(dsm_filtered)

            return (
                data_sources_list, table_name_dic, table_name_list,
                data_sources_attr_list, pk_list, data_sources_map_list
            )

        except Exception as e:
            msg_alerta_erro(
                'Ocorreu um erro ao gerar as masterfiles!',
                'Limpe formatos e filtros da planilha e verifique se o nome das abas estão corretos.'
            )
            raise e

    def _carregar_planilha(self, sheet_name, nrows=4):
        try:
            return pd.read_excel(self.path_name_xlsm, sheet_name=sheet_name, nrows=nrows, engine='calamine')
        except Exception as e:
            msg_alerta_erro("ERRO!",f"Erro ao realizar a leitura da aba ({sheet_name}) da planilha.\n {e}")
            SystemExit
            
    def _validar_cabecalho(self, df, col_index, valor_esperado, msg_erro):
        if df.iloc[2, col_index].strip() != valor_esperado:
            msg_alerta_erro("Ocorreu um erro ao gerar as masterfiles!", msg_erro)
            raise SystemExit

    def _extrair_indices_cabecalho(self, df, colunas_interesse):
        header_row = df.iloc[2]
        return {col: header_row.tolist().index(col) for col in colunas_interesse}

    def _ler_dados(self, sheet_name, colunas, skiprows=3):
        try:
            df = pd.read_excel(
                self.path_name_xlsm, sheet_name=sheet_name,
                usecols=colunas, skiprows=skiprows, header=0, engine='calamine'
            )
            if '3. Data Sources Attr & Count' in sheet_name:
                df[['Attribute/Counter Name', 'Attribute/Counter Physical Name']] = df[
                    ['Attribute/Counter Name', 'Attribute/Counter Physical Name']
                ].fillna("NA")
            df.columns = [f'col_{i}' for i in range(len(colunas))]
            return df
        except Exception as e:
            raise Exception(f"Erro ao ler os dados da aba {sheet_name}: {e}")

    def _filtrar_dados_por_inventory(self, df, inventory_name, col_index):
        return df[df[f'col_{col_index}'].isin(inventory_name)]

    def _processar_descricao(self, descricao):
        if isinstance(descricao, str) and "'" in descricao:
            return descricao.replace("'", "")
        return descricao

    def _processar_data_sources(self, ds_filtered):
        table_name_dic = {}
        table_name_list = []
        data_sources_list = []

        for _, row in ds_filtered.iterrows():
            table_name_dic[row['col_0']] = row['col_1']
            table_name_list.append(row['col_0'])
            descricao = self._processar_descricao(row['col_3'])

            data_sources_list.append(
                (
                    row['col_0'],
                    row['col_1'],
                    row['col_2'].split("_")[-1].upper(),
                    descricao,
                    row['col_2']
                )
            )
        return data_sources_list, table_name_dic, table_name_list

    def _processar_data_sources_attr(self, dsa_filtered):
        data_sources_attr_list = []
        pk_list = []

        for _, row in dsa_filtered.iterrows():
            descricao = self._processar_descricao(row['col_6'])
            data_sources_attr_list.append(
                (
                    row['col_0'], row['col_1'], row['col_2'], row['col_3'],
                    row['col_4'], row['col_5'], descricao
                )
            )
            try:
                if row['col_4'].upper() == 'PK':
                    pk_list.append((row['col_0'], row['col_2'], row['col_4']))
            except AttributeError:
                msg_alerta_erro(
                    'Ocorreu um erro ao gerar as masterfiles!',
                    '"Coluna Mediation Type da aba 3. Data Sources Attr & Count não pode estar vazia"'
                )
                raise SystemExit

        return data_sources_attr_list, pk_list

    def _processar_data_sources_map(self, dsm_filtered):
        data_sources_map_list = []

        for _, row in dsm_filtered.iterrows():
            data_sources_map_list.append(
                (
                    row['col_0'], row['col_1'], row['col_2'],
                    row['col_3'], row['col_4']
                )
            )
        return data_sources_map_list

class MasterfileGenerator:
    def __init__(self, status, path_name, caminho_master):
        self.status = status  # True for Oracle, False for PostgreSQL
        self.path_name = path_name
        self.caminho_master = caminho_master

    def generate(self, inventory_name, data_sources_list, data_sources_attr_list, data_sources_map_list, table_name_dic):
        self._generate_part1(inventory_name, data_sources_list)
        self._generate_part2(inventory_name, data_sources_attr_list)
        self._generate_part3(inventory_name, data_sources_map_list, table_name_dic)

    def _generate_part1(self, inventory_name, data_sources_list):
        for i in inventory_name:
            for ds in data_sources_list:
                if ds[0] == i:
                    if self.status:
                        self._generate_part1_oracle(ds)
                    else:
                        self._generate_part1_postgre(ds)

    def _generate_part1_oracle(self, ds):
        with open(f"{self.path_name}/{ds[0].lower()}.mas", "a") as arquivo:
            if ds[2] != 'ENRICH':
                arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLORA, REMARKS='{ds[3]}', $\n")
                arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")
                arquivo.write("FIELDNAME=SEQ_NUMBER, ALIAS=SEQ_NUMBER, USAGE=P21, ACTUAL=P11, $\n")
                arquivo.write("FIELDNAME=INSERT_DATE, ALIAS=INSERT_DATE, USAGE=HYYMDS, ACTUAL=HYYMDS, $\n")
            else:
                arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLORA,\n")
                arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")

    def _generate_part1_postgre(self, ds):
        with open(f"{self.path_name}/{ds[0].lower()}.mas", "a") as arquivo:
            if ds[2] != 'ENRICH':
                arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLPSTGR, REMARKS='{ds[3]}', $\n")
                arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")
                arquivo.write("FIELDNAME=SEQ_NUMBER, ALIAS=seq_number, USAGE=P21, ACTUAL=P11, $\n")
                arquivo.write("FIELDNAME=INSERT_DATE, ALIAS=insert_date, USAGE=HYYMDS, ACTUAL=HYYMDS, $\n")
                arquivo.write("FIELDNAME=SOURCE_ID, ALIAS=source_id, USAGE=A255V, ACTUAL=A255V, $\n")
                arquivo.write("FIELDNAME=CONTENT_ID, ALIAS=content_id, USAGE=A256V, ACTUAL=A256V, $\n")
            else:
                arquivo.write(f"FILENAME={ds[1]}, SUFFIX=SQLPSTGR,\n")
                arquivo.write(f"  SEGMENT={ds[1]}, SEGTYPE=S0, $\n")

    def _generate_part2(self, inventory_name, data_sources_attr_list):
        for i in inventory_name:
            for dsa in data_sources_attr_list:
                if dsa[0] == i:
                    if self.status:
                        self._generate_part2_oracle(dsa)
                    else:
                        self._generate_part2_postgre(dsa)

    def _generate_part2_oracle(self, dsa):
        with open(f"{self.path_name}/{dsa[0].lower()}.mas", "a") as arquivo:
            if dsa[3] == "TIMESTAMP(3)" and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].upper()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=HYYMDS, ACTUAL=HYYMDS,\n    MISSING=ON, $\n"
                )
            elif dsa[3].startswith("VARCHAR") and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].upper()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=A255V, ACTUAL=A255V,\n    MISSING=ON, $\n"
                )
            elif dsa[3] == "NUMBER" and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].upper()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=D20.2, ACTUAL=D8,\n    MISSING=ON, $\n"
                )

    def _generate_part2_postgre(self, dsa):
        with open(f"{self.path_name}/{dsa[0].lower()}.mas", "a") as arquivo:
            if dsa[3] == "TIMESTAMP(3)" and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=HYYMDS, ACTUAL=HYYMDS,\n    MISSING=ON, $\n"
                )
            elif dsa[3].startswith("VARCHAR") and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=A255V, ACTUAL=A255V,\n    MISSING=ON, $\n"
                )
            elif dsa[3] == "NUMBER" and dsa[5] != "Constant":
                arquivo.write(
                    f"FIELDNAME={dsa[1].upper()}, ALIAS={dsa[2].lower()}, TITLE='{dsa[1]}', "
                    f"DESCRIPTION='{dsa[6]}',USAGE=D20.2, ACTUAL=D8,\n    MISSING=ON, $\n"
                )

    def _generate_part3(self, inventory_name, data_sources_map_list, table_name_dic):
        if self.caminho_master:
            self._generate_part3_with_path(inventory_name, data_sources_map_list, table_name_dic)
        else:
            self._generate_part3_without_path(inventory_name, data_sources_map_list, table_name_dic)

    def _generate_part3_with_path(self, inventory_name, data_sources_map_list, table_name_dic):
        dsm = data_sources_map_list[:]
        for i in inventory_name:
            lista_ETN = [dsm_item[0] for dsm_item in dsm if dsm_item[2] == i]
            lista_ETN_uni = list(set(lista_ETN))
            lista_res = [(k, lista_ETN.count(k)) for k in lista_ETN_uni]

            for res in lista_res:
                for j, dsm_item in enumerate(dsm):
                    if dsm_item[2] == i and res[0] == dsm_item[0]:
                        try:
                            with open(f"{self.path_name}/{i.lower()}.mas", "a") as arquivo:
                                join_type = dsm_item[4].replace(' ', '_').upper()
                                parent_table = table_name_dic[dsm_item[2]]
                                if res[1] == 1:
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={self.caminho_master}/{dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()};,$\n"
                                    )
                                elif res[1] == 2:
                                    next_dsm_item = dsm[j + 1]
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={self.caminho_master}/{dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()} AND {parent_table}.{next_dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_item[1].upper()};,$\n"
                                    )
                                elif res[1] == 3:
                                    next_dsm_items = dsm[j + 1:j + 3]
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={self.caminho_master}/{dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()} AND {parent_table}.{next_dsm_items[0][3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_items[0][1].upper()} AND {parent_table}.{next_dsm_items[1][3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_items[1][1].upper()};,$\n"
                                    )
                            break
                        except BaseException:
                            msg_alerta_erro(
                                "Ocorreu um erro ao gerar as masterfiles!",
                                "Favor preencher as colunas 'DBN0 Attribute Physical Name', 'Enrichment Attribute Physical Name' e 'AdHoc Join Type' presentes na aba 3. Data Source Map."
                            )
                            raise

    def _generate_part3_without_path(self, inventory_name, data_sources_map_list, table_name_dic):
        dsm = data_sources_map_list[:]
        for i in inventory_name:
            lista_ETN = [dsm_item[0] for dsm_item in dsm if dsm_item[2] == i]
            lista_ETN_uni = list(set(lista_ETN))
            lista_res = [(k, lista_ETN.count(k)) for k in lista_ETN_uni]

            for res in lista_res:
                for j, dsm_item in enumerate(dsm):
                    if dsm_item[2] == i and res[0] == dsm_item[0]:
                        try:
                            with open(f"{self.path_name}/{i.lower()}.mas", "a") as arquivo:
                                join_type = dsm_item[4].replace(' ', '_').upper()
                                parent_table = table_name_dic[dsm_item[2]]
                                if res[1] == 1:
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()};,$\n"
                                    )
                                elif res[1] == 2:
                                    next_dsm_item = dsm[j + 1]
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()} AND {parent_table}.{next_dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_item[1].upper()};,$\n"
                                    )
                                elif res[1] == 3:
                                    next_dsm_items = dsm[j + 1:j + 3]
                                    arquivo.write(
                                        f"SEGMENT={dsm_item[0]}, SEGTYPE=KU,PARENT={parent_table}, "
                                        f"CRFILE={dsm_item[0]}, CRINCLUDE=ALL , "
                                        f"CRJOINTYPE={join_type}, JOIN_WHERE={parent_table}.{dsm_item[3].upper()} "
                                        f"EQ {dsm_item[0]}.{dsm_item[1].upper()} AND {parent_table}.{next_dsm_items[0][3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_items[0][1].upper()} AND {parent_table}.{next_dsm_items[1][3].upper()} "
                                        f"EQ {dsm_item[0]}.{next_dsm_items[1][1].upper()};,$\n"
                                    )
                            break
                        except BaseException:
                            msg_alerta_erro(
                                "Ocorreu um erro ao gerar as masterfiles!",
                                "Favor preencher as colunas 'DBN0 Attribute Physical Name', 'Enrichment Attribute Physical Name' e 'AdHoc Join Type' presentes na aba 3. Data Source Map."
                            )
                            raise

class ACXGenerator:

    def __init__(self, path_name):
        self.path_name = path_name

    def generate(self, inventory_name, data_sources_list, pk_list):
        for i in inventory_name:
            lista_pk = [pk[1] for pk in pk_list if pk[0] == i]
            for ds in data_sources_list:
                if ds[0] == i:
                    with open(f"{self.path_name}/{i.lower()}.acx", "a") as arquivo:
                        arquivo.write(f"SEGNAME={ds[1]},\n")
                        arquivo.write(f"    TABLENAME={ds[4].lower()}.{ds[1].lower()},\n")
                        arquivo.write(f"    CONNECTION={ds[4].upper()},\n")
                        arquivo.write(f"    KEY={'/'.join(lista_pk)}, $")
