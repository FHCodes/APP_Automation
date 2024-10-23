from data_processor import DataProcessor
from masterfile_generator import MasterfileGenerator
from acx_generator import ACXGenerator
from alerta import msg_alerta_alert, msg_alerta_erro, msg_alerta_sucesso
from collections import Counter

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
            #Verifica se o elemento(Inventory_Name) fornecido esta duplicado
            if not self._has_input_duplicated():
                self._update_progress(progress_callback,0)
                return False
            self._update_progress(progress_callback,15) # Atualização do progresso
            #---------------------------------------------------------------------
            
            # Processamento dos dados
            (data_sources_list, table_name_dic, table_name_list,
             data_sources_attr_list, pk_list, data_sources_map_list) = self.data_processor.process_data()
            
            self._update_progress(progress_callback,30) # Atualização do progresso
            
            # Extrai os nomes de inventário
            inventory_name_extracted = [item[0] for item in data_sources_list]        
        
            #Verifica se o Inventory_Name fornecido existe no pack
            if not self._has_value_in_sheet(data_sources_list):
                self._update_progress(progress_callback,0)
                return False
            self._update_progress(progress_callback,45)
            #------------------------------------------------------
            
            # Verifica diferenças entre os nomes de tabela e os nomes de inventário
            if not self.check_inventory_and_table_diff(table_name_list, inventory_name_extracted):
                self._update_progress(progress_callback,0)
                return False
            self._update_progress(progress_callback,60)
            #----------------------------------------------------------------------------------        
            
            # Verifica duplicatas
            if not self.has_duplicates_in_sheet(inventory_name_extracted):
                self._update_progress(progress_callback,0)
                return False
            self._update_progress(progress_callback,75)
            #-------------------------------------------------------------
            
            # Gera as masterfiles
            if not self.generate_masterfiles(inventory_name_extracted, data_sources_list, 
                                            data_sources_attr_list, data_sources_map_list, table_name_dic):
                self._update_progress(progress_callback,0)
                return False
            self._update_progress(progress_callback,90)
            #---------------------------------------------------------------------------------------------------------------------------------------------

            # Gera o ACX
            if not self.generate_acx(inventory_name_extracted, data_sources_list, pk_list):
                self._update_progress(progress_callback,0)
                return False
                 
            # Finalização do progresso
            self._update_progress(progress_callback,100)
            
            msg_alerta_sucesso('SUCESSO','MASTERFILES E ACX CRIADOS')
            return result
        except Exception as e:
            msg_alerta_erro('Ocorreu um erro inesperado ao gerar as masterfiles!', str(e))
            result = False
            return result


    def has_duplicates_in_sheet(self, inventory_name_list):
        """
        Verifica se há nomes de inventário duplicados.
        """
        count = Counter(inventory_name_list)
        duplicates = [value for value, qtd in count.items() if qtd > 1]
        if duplicates:
            msg_alerta_erro(
                "Ocorreu um erro ao gerar as masterfiles!",
                f"O valor ({duplicates[0]}) da coluna Inventory Name está repetido na aba '3. Data Sources'"
            )
            return False
        return True

    def check_inventory_and_table_diff(self, table_name_list, inventory_name_list):
        """
        Verifica diferenças entre os nomes das tabelas e os nomes de inventário.
        """
        if len(table_name_list) <= len(inventory_name_list) and inventory_name_list[0] != '':
            diff = set(table_name_list).symmetric_difference(set(inventory_name_list))
            if diff:
                for item in diff:
                    msg_alerta_erro("Erro:", f"Inventory Name não encontrado no pack:\n{item}")

                return False
        return True

    def generate_masterfiles(self, inventory_name_list, data_sources_list, data_sources_attr_list, data_sources_map_list, table_name_dic):
        """
        Gera as masterfiles usando o MasterfileGenerator.
        """
        try:
            masterfile_generator = MasterfileGenerator(self.status, self.path_name, self.caminho_master)
            masterfile_generator.generate(
                inventory_name_list,
                data_sources_list,
                data_sources_attr_list,
                data_sources_map_list,
                table_name_dic
            )
            return True
        except Exception as err:
            msg_alerta_erro('Erro ao gerar as masterfiles!', str(err))
            return False

    def generate_acx(self, inventory_name_list, data_sources_list, pk_list):
        """
        Gera o ACX usando o ACXGenerator.
        """
        try:
            acx_generator = ACXGenerator(self.path_name)
            acx_generator.generate(inventory_name_list, data_sources_list, pk_list)
            return True
        except Exception as err:
            msg_alerta_erro(
                'Erro!',
                f'Erro na geração do ACX: {err}'
            )
            return False

    def _has_input_duplicated(self):
        
        dup_inventory_name = [item for item, count in Counter(self.inventory_name).items() if count > 1]
        if dup_inventory_name: 
            elementos = '\n'.join(dup_inventory_name)
            
            if len(dup_inventory_name) > 1:
                msg_erro = f"Elementos(Inventory_Name) duplicados:\n {elementos}"
            else:
                msg_erro = f"Elemento(Inventory_Name) duplicado:\n {elementos}"
            msg_alerta_erro("Erro!", msg_erro)
            
            return False
        return True
                        
    def _has_value_in_sheet(self, data_sources_list):
        missing_elements = set(self.inventory_name) - set().union(*data_sources_list)
        if missing_elements:
            elements_str = '\n'.join(sorted(missing_elements))
            msg_alerta_alert(
                "Alerta!",
                f"Elemento(s) não encontrado(s) no pack:\n{elements_str}"
            )
            return False
        return True

    def _update_progress(self, progress_callback,percent:int):
        if progress_callback:
            progress_callback(percent)
    