import pandas as pd
import sys
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
        Parâmetros:
            sheet_info (dict): Dicionário contendo informações sobre as planilhas e suas chaves.
        Retorno:
            dict: Dicionário de DataFrames, com a chave do dicionário de entrada como chave e o DataFrame como valor.
        Exceções:
            Gera mensagens de erro detalhadas se houver falha na leitura ou manipulação das planilhas.
        """
        try:
            # Extraindo os nomes das abas (planilhas) de sheet_info
            sheet_names = [info['sheet_name'] for info in sheet_info.values()]

            try:
                data_frames_raw = self.__carregar_planilha(sheet_names)
            except Exception as e:
                msg_alerta_erro("Erro!",f"Erro ao carregar a planilha.\nDetalhes: {e}")
                raise

            # Inicializando o dicionário para armazenar os DataFrames
            data_frames = {}

            # Iterando sobre o sheet_info para preencher o dicionário data_frames
            for key, info in sheet_info.items():
                try:
                    sheet_name = info['sheet_name']
                    if sheet_name not in data_frames_raw:
                        msg_alerta_erro("Planilha não encontrada!",
                                        "A planilha '{sheet_name}' especificada não foi encontrada nos dados carregados. Verifique se o nome da aba está correto.")
                        raise
                    data_frames[key] = data_frames_raw[sheet_name]
                except Exception as e:
                    raise Exception(f"Erro ao processar a planilha '{sheet_name}' para a chave '{key}'. Detalhes do erro: {e}")
            return data_frames   
        except Exception as e:
            # Retorna ou loga a mensagem de erro detalhada para facilitar a identificação do problema
            msg_alerta_erro("ERRO!", f"Erro ao carregar os DataFrames. Detalhes: {e}")
            raise

    def __carregar_planilha(self, sheet_names, nrows=4):
        """
        Carrega as abas especificadas de uma planilha Excel.
        """
        try:
            # Tentativa de carregar as planilhas especificadas
            return pd.read_excel(self.path_name_xlsm, sheet_name=sheet_names, nrows=nrows, engine='calamine')
        except ValueError as e:
            # Erro específico para aba inválida ou outros valores incorretos
            msg_alerta_erro("Erro na leitura das planilhas!", f"Erro ao ler as abas {sheet_names} no arquivo '{self.path_name_xlsm}'.\n"
                                                            f"Possivelmente uma ou mais abas não existem.\nDetalhes: {e}")
            raise     
        except pd.errors.EmptyDataError as e:
            # Erro para arquivos ou abas vazias
            msg_alerta_erro("Arquivo vazio!", f"O arquivo '{self.path_name_xlsm}' ou as abas {sheet_names} estão vazias.\n"
                                            f"Verifique o conteúdo do arquivo e tente novamente.\nDetalhes: {e}")
            raise
        except Exception as e:
            # Tratamento geral para qualquer outro erro inesperado
            msg_alerta_erro("Erro desconhecido", f"Ocorreu um erro ao ler as abas {sheet_names} do arquivo '{self.path_name_xlsm}'.\n"
                                                f"Por favor, tente novamente ou verifique o arquivo.\nDetalhes: {e}")
            raise

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
        try:
            for key, info in sheet_info.items():
                try:
                    df = data_frames[key]
                except KeyError:
                    msg_alerta_erro("ERRO!",f"O DataFrame com a chave '{key}' não foi encontrado. Verifique se os nomes das abas estão corretos.")
                    raise
                
                columns_of_interest = info['columns_of_interest']

                # Chama a função para extrair os índices das colunas
                try:
                    indices = self.__extrair_indices_cabecalho(df, columns_of_interest)
                except ValueError as e:
                    raise ValueError(f"Erro ao extrair os índices das colunas para a aba '{key}'. Detalhes: {e}")
                
                column_indices[key] = indices

            return column_indices

        except Exception as e:
            # Tratamento geral para qualquer erro inesperado
            raise Exception(f"Erro ao extrair índices de colunas. Detalhes: {e}")
    
    def __extrair_indices_cabecalho(self, df, colunas_interesse):
        """
        Extrai os índices das colunas de interesse do cabeçalho do DataFrame.
        """
        try:
            # Verifica se a linha 2 do DataFrame (o cabeçalho) existe
            if df.shape[0] <= 2:
                msg_alerta_erro("ERRO!","Não foi possível acessar a linha do cabeçalho.")
                raise
            # Obtém a linha do cabeçalho
            header_row = df.iloc[2]
            # Tenta encontrar os índices das colunas de interesse
            try:
                return {col: header_row.tolist().index(col) for col in colunas_interesse}
            except ValueError as e:
                msg_alerta_erro("ERRO!","Coluna não encontrada no cabeçalho. Verifique se os nomes das colunas estão corretos.")
                raise   
        except Exception as e:
            # Tratamento geral para qualquer erro inesperado
            msg_alerta_erro("ERRO!",f"Erro ao extrair índices do cabeçalho. Detalhes: {e}")
    
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

        try:
            for key, df in data_sheets.items():
                col_index = sheet_info[key]['filter_col_index']
                try:
                    filtered_df = self.__filtrar_dados_por_inventory(df, self.inventory_name, col_index)
                except KeyError as e:
                    msg_alerta_erro("Erro ao Filtrar Dados!",f"Erro ao tentar filtrar os dados da aba '{key}'. Coluna de índice {col_index} não encontrada.\nDetalhes: {e}")
                    raise
                except Exception as e:
                    msg_alerta_erro("Erro ao Processar Filtro!",f"Erro inesperado ao filtrar os dados para a aba '{key}'.\nDetalhes: {e}")
                    raise
                filtered_data[key] = filtered_df
            return filtered_data

        except Exception as e:
            # Tratamento geral para qualquer erro inesperado
            msg_alerta_erro(
                "Erro Geral no Filtro!",
                f"Ocorreu um erro ao filtrar os dados.\nDetalhes: {e}"
            )
            raise


    def __filtrar_dados_por_inventory(self, df, inventory_name, col_index):
        """
        Filtra o DataFrame com base no nome do inventário e no índice da coluna especificada.
        """
        try:
            # Verifica se o índice da coluna está correto
            if col_index < 0 or col_index >= len(df.columns):
                msg_alerta_erro("Erro no Índice de Coluna!",f"Índice da coluna '{col_index}' está fora do intervalo para o DataFrame. Verifique se o índice especificado é válido.")
                raise IndexError(f"Índice da coluna {col_index} inválido.")
            
            # Filtra os dados com base no inventário
            return df[df[f'col_{col_index}'].isin(inventory_name)]
        except KeyError as e:
            msg_alerta_erro("Erro ao Acessar Coluna!",f"A coluna de índice {col_index} não foi encontrada no DataFrame.\nDetalhes: {e}")
            raise
        except Exception as e:
            msg_alerta_erro("Erro ao Filtrar DataFrame",f"Ocorreu um erro ao tentar filtrar o DataFrame com base no inventário. Detalhes: {e}")
            raise

    def _processar_descricao(self, descricao):
        if isinstance(descricao, str) and "'" in descricao:
            return descricao.replace("'", "")
        return descricao

    def _processar_data_sources(self, ds_filtered):
        table_name_dic = {}
        table_name_list = []
        data_sources_list = []

        if not self.__verificar_linha_vazia(ds_filtered):
            print('entrou')
            return
        
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

    def __verificar_linha_vazia(self,ds_filtered):
        # Colunas que não podem estar vazias
        columns_to_check = ['col_0', 'col_1', 'col_2']
        
        coluna_vazia = ''  # Para armazenar quais colunas estão vazias
        for index, row in ds_filtered.iterrows():
            # Checar se há valores nulos ou vazios nas colunas especificadas
            for col in columns_to_check:
                if pd.isnull(row[col]) or row[col] == '':
                    # Traduzir o nome da coluna para mensagem de erro
                    if col == 'col_0':
                        coluna_vazia = 'Inventory Name'
                    elif col == 'col_1':
                        coluna_vazia = 'Table Name'
                    elif col == 'col_2':
                        coluna_vazia = 'Schema'

            # Se encontrar uma coluna vazia, interrompe o processamento e exibe a mensagem de erro
            print(coluna_vazia)
            if coluna_vazia:
                
                msg_alerta_erro(
                    "Erro na aba '3. Data Sources'", 
                    f"A coluna '{coluna_vazia}' está vazia na linha: {index + 5}."
                )
                return False
        return True