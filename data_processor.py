import pandas as pd
from alerta import msg_alerta_erro

class DataProcessor:
    def __init__(self, path_name_xlsm, inventory_name, path_name):
        self.path_name_xlsm = path_name_xlsm
        self.inventory_name = inventory_name
        self.path_name = path_name

    def process_data(self):
        try:
            # Define informações das planilhas e configurações
            sheet_info = self._definir_informacoes_sheet_name()

            # Carrega os DataFrames das planilhas
            data_frames = self._carregar_data_frames(sheet_info)

            # Valida os cabeçalhos das planilhas
            self._processar_cabecalhos(data_frames, sheet_info)

            # Extrai os índices das colunas de interesse
            column_indices = self._extrair_indices_colunas(data_frames, sheet_info)

            # Lê os dados das sheet_names com base nos índices das colunas
            data_sheets = self._processa_leitura_de_dados(sheet_info, column_indices)

            # Filtra os dados com base no inventory_name
            filtered_data = self._filtrar_dados(data_sheets, sheet_info)

                
            # Processa os dados filtrados e retorna os resultados necessários.
            data_sources_list, table_name_dic, table_name_list = self._processar_data_sources(filtered_data['data_sources'])
            data_sources_attr_list, pk_list = self._processar_data_sources_attr(filtered_data['data_sources_attr'])
            data_sources_map_list = self._processar_data_sources_map(filtered_data['data_sources_map'])
            #---------------------------------------------------------------------------------------------
            
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

    def _definir_informacoes_sheet_name(self):
            """
            Define as informações das abas da planilha, incluindo nomes, validações e colunas de interesse.
            """
            sheet_info = {
                'data_sources': {
                    'sheet_name': '3. Data Sources',
                    'header_validation': {
                        'col_index': 1,
                        'expected_value': 'Inventory Name',
                        'error_msg': "O cabeçalho deve ficar na 4ª linha (Data Sources)"
                    },
                    'columns_of_interest': ['Inventory Name', 'Table Name', 'Schema', 'Description'],
                    'filter_col_index': 0
                },
                'data_sources_attr': {
                    'sheet_name': '3. Data Sources Attr & Count',
                    'header_validation': {
                        'col_index': 1,
                        'expected_value': 'Source Name',
                        'error_msg': "O cabeçalho deve ficar na 4ª linha (Data Sources Attr)"
                    },
                    'columns_of_interest': [
                        'Source Name', 'Attribute/Counter Name', 'Attribute/Counter Physical Name',
                        'Data Type', 'Mediation Type', 'Metrics Attribute Type', 'Description'
                    ],
                    'filter_col_index': 0
                },
                'data_sources_map': {
                    'sheet_name': '3. Data Sources Map',
                    'header_validation': {
                        'col_index': 1,
                        'expected_value': 'Enrichment Table Name',
                        'error_msg': "O cabeçalho deve ficar na 4ª linha (Data Sources Map)"
                    },
                    'columns_of_interest': [
                        'Enrichment Table Name', 'Enrichment Attribute Name', 'DBNO Table Name',
                        'DBN0 Attribute Name', 'AdHoc Join Type'
                    ],
                    'filter_col_index': 2
                }
            }
            return sheet_info

    def _carregar_data_frames(self, sheet_info):
        """
        Carrega os DataFrames das planilhas especificadas em sheet_info.
        """
        sheet_names = [info['sheet_name'] for info in sheet_info.values()]
        data_frames_raw = self.__carregar_planilha(sheet_names)
        data_frames = {}
        for key, info in sheet_info.items():
            sheet_name = info['sheet_name']
            data_frames[key] = data_frames_raw[sheet_name]
        return data_frames

    def __carregar_planilha(self, sheet_names, nrows=4):
        try:
            return pd.read_excel(self.path_name_xlsm, sheet_name=sheet_names, nrows=nrows, engine='calamine')
        except Exception as e:
            msg_alerta_erro("ERRO!", f"Erro ao realizar a leitura das abas ({sheet_names}) da planilha.\n {e}")
            SystemExit

    def _processar_cabecalhos(self, data_frames, sheet_info):
        """
        Valida os cabeçalhos das planilhas com base nas informações de validação em sheet_info.
        """
        for key, info in sheet_info.items():
            df = data_frames[key]
            validation = info['header_validation']
            self.__validar_cabecalho(
                df,
                validation['col_index'],
                validation['expected_value'],
                validation['error_msg']
            )
            
    def __validar_cabecalho(self, df, col_index, valor_esperado, msg_erro):
        try:
            valor_encontrado = df.iloc[2, col_index].strip()
            if valor_encontrado != valor_esperado:
                msg_alerta_erro("Ocorreu um erro ao gerar as masterfiles!", f"{msg_erro}\nEsperado: '{valor_esperado}', Encontrado: '{valor_encontrado}'")
                raise SystemExit
        except Exception as e:
            msg_alerta_erro("ERRO!", f"Erro ao validar o cabeçalho.\n {e}")
            raise SystemExit

    def _extrair_indices_colunas(self, data_frames, sheet_info):
        """
        Extrai os índices das colunas de interesse para cada DataFrame.
        """
        column_indices = {}
        for key, info in sheet_info.items():
            df = data_frames[key]
            columns_of_interest = info['columns_of_interest']
            indices = self.__extrair_indices_cabecalho(df, columns_of_interest)
            column_indices[key] = indices
        return column_indices
    
    def __extrair_indices_cabecalho(self, df, colunas_interesse):
        header_row = df.iloc[2]
        return {col: header_row.tolist().index(col) for col in colunas_interesse}

    def _processa_leitura_de_dados(self, sheet_info, column_indices):
        """
        Lê os dados das planilhas com base nos índices das colunas extraídas.
        """
        data_sheets = {}
        for key, info in sheet_info.items():
            sheet_name = info['sheet_name']
            columns = list(column_indices[key].values())
            data_sheets[key] = self.__ler_dados(sheet_name, columns)
        return data_sheets

    def __ler_dados(self, sheet_name, colunas, skiprows=3):
        try:
            df = pd.read_excel(
                self.path_name_xlsm, sheet_name=sheet_name,
                usecols=colunas, skiprows=skiprows, header=0, engine='calamine'
            )
            if '3. Data Sources Attr & Count' in sheet_name:
                # Preenche valores nulos específicos desta planilha
                df[['Attribute/Counter Name', 'Attribute/Counter Physical Name']] = df[
                    ['Attribute/Counter Name', 'Attribute/Counter Physical Name']
                ].fillna("NA")
            df.columns = [f'col_{i}' for i in range(len(colunas))]
            return df
        except Exception as e:
            raise Exception(f"Erro ao ler os dados da aba {sheet_name}: {e}")

    def _filtrar_dados(self, data_sheets, sheet_info):
        """
        Filtra os dados das planilhas com base no inventory_name e nos índices de coluna especificados.
        """
        filtered_data = {}
        for key, df in data_sheets.items():
            col_index = sheet_info[key]['filter_col_index']
            filtered_df = self.__filtrar_dados_por_inventory(df, self.inventory_name, col_index)
            filtered_data[key] = filtered_df
        return filtered_data

    def __filtrar_dados_por_inventory(self, df, inventory_name, col_index):
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