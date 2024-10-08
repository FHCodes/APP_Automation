import os
import sys

from PyQt5.QtWidgets import (
    QWidget, QTabWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QTextEdit, QPushButton, QFileDialog, QCheckBox, QProgressBar, QMessageBox
)
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtCore import Qt, QThread, pyqtSignal

from masterfile_processor import MasterfileCreator
from renomearDBN1 import RenameFiles
from modeloDBN0 import DBN0Model
from modeloDBN1 import DBN1Model
from utils import CreateAndDeleteFolder


class Worker(QThread):
    """Worker thread para processamento de criação de masterfiles."""
    progress_changed = pyqtSignal(int)
    finished = pyqtSignal(bool)

    def __init__(self, masterfile_creator):
        super().__init__()
        self.masterfile_creator = masterfile_creator

    def run(self):
        """Executa o processo de criação de masterfiles."""
        try:
            result = self.masterfile_creator.create_masterfiles(self.update_progress)
            self.finished.emit(result)
        except Exception as e:
            print(f"Erro: {e}")
            self.finished.emit(False)

    def update_progress(self, value):
        """Atualiza o progresso."""
        self.progress_changed.emit(value)


class TabMasterFiles(QWidget):
    """Aba para criação de MasterFiles."""
    disable_tabs_signal = pyqtSignal()
    enable_tabs_signal = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.parent = parent
        self.init_ui()
        self.worker = None

    def init_ui(self):
        layout = QVBoxLayout()

        # Selecione o pack
        hbox1 = QHBoxLayout()
        label1 = QLabel('Selecione o pack:')
        self.path_name = QLineEdit()
        browse_button1 = QPushButton('Browse')
        browse_button1.clicked.connect(self.browse_pack)
        hbox1.addWidget(label1)
        hbox1.addWidget(self.path_name)
        hbox1.addWidget(browse_button1)
        layout.addLayout(hbox1)

        # Nome da pasta da masterfile
        hbox2 = QHBoxLayout()
        label2 = QLabel('Entre com o nome da pasta da masterfile:')
        self.masterfile_folder = QLineEdit()
        hbox2.addWidget(label2)
        hbox2.addWidget(self.masterfile_folder)
        layout.addLayout(hbox2)

        # Inventory Name e Checkbox
        hbox3 = QHBoxLayout()
        label3 = QLabel('Entre com os elementos da coluna Inventory Name:')
        self.vmf_checkbox = QCheckBox("Masterfiles Oracle")
        hbox3.addWidget(label3)
        hbox3.addWidget(self.vmf_checkbox)
        layout.addLayout(hbox3)

        self.inventory_name = QTextEdit()
        self.inventory_name.setAcceptRichText(False)
        layout.addWidget(self.inventory_name)

        # Barra de Progresso
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        # Botão Criar
        hbox4 = QHBoxLayout()
        create_button = QPushButton("Criar")
        create_button.clicked.connect(self.on_create_button_clicked)
        hbox4.addWidget(create_button)
        layout.addLayout(hbox4)

        self.setLayout(layout)

    def resource_path(self, relative_path):
        """Obtém o caminho absoluto para recursos."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    def browse_pack(self):
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Selecione o pack", "", "Excel Files (*.xls *.xlsx)", options=options)
        if file_name:
            self.path_name.setText(file_name)

    def on_create_button_clicked(self):
        path_name = self.path_name.text()
        inventory_names = self.inventory_name.toPlainText().split('\n')

        vmf = self.vmf_checkbox.isChecked()
        masterfile_folder = self.masterfile_folder.text()
        self.progress_bar.setValue(5)

        # Verifica se o caminho do pack foi inserido
        if not path_name.strip():
            QMessageBox.critical(self, "Erro!", "Insira o caminho do pack antes de prosseguir!")
            return
        
        #Verifica se o arquivo existe
        if not os.path.exists(path_name):
            QMessageBox.critical(self, "Erro!", "Insira um arquivo válido!")
            return

        #Verifica se o formato do arquivo eh valido
        if not path_name.lower().endswith(('.xls', '.xlsx')):
            QMessageBox.critical(self, "Arquivo Inválido!", "Insira um formato válido. Ex: xls/xlsx")
            return

        inventory_names = [item.strip() for item in inventory_names if item.strip()]
        if not inventory_names:
            QMessageBox.critical(self, "Erro!", "Insira ao menos um Inventory Name antes de prosseguir.")
            return
    
        self.path_name_created = CreateAndDeleteFolder().create_folder(path_name)
        # Desabilitar as abas durante o processamento
        # self.disable_tabs_signal.emit()
        # self.progress_bar.setValue(0)

        # Criar o objeto MasterfileCreator
        self.masterfile_creator = MasterfileCreator(
            path_name, inventory_names, vmf, masterfile_folder, self.path_name_created
        )

        # Criar e iniciar o thread
        self.worker = Worker(self.masterfile_creator)
        self.worker.progress_changed.connect(self.update_progress)
        self.worker.finished.connect(self.on_create_masterfiles_finished)
        self.worker.start()

    def update_progress(self, value):
        """Atualiza a barra de progresso."""
        self.progress_bar.setValue(value)

    def on_create_masterfiles_finished(self, success):
        """Finaliza o processo de criação."""
        self.enable_tabs_signal.emit()
        if not success:
            CreateAndDeleteFolder().delete_folder(self.path_name_created)
        else:
            self.progress_bar.setValue(0)#Zera o progresso apos a execucao ter sido finalizada com sucesso
        self.worker = None
    
class TabRenomearDBN1(QWidget):
    """Aba para renomear DBN1."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Selecionar pasta
        hbox1 = QHBoxLayout()
        label1 = QLabel('Selecione a pasta com os arquivos exportados do relacionamento DBN1:')
        self.path_name2 = QLineEdit()
        browse_button2 = QPushButton('Browse')
        browse_button2.clicked.connect(self.browse_dnb1_folder)
        hbox1.addWidget(label1)
        hbox1.addWidget(self.path_name2)
        hbox1.addWidget(browse_button2)
        layout.addLayout(hbox1)

        # Imagem
        pixmap = QPixmap(self.resource_path("icons/Altaia.png"))
        image_label = QLabel()
        image_label.setPixmap(pixmap)
        image_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(image_label)

        # Botão Renomear
        rename_button = QPushButton("Renomear")
        rename_button.clicked.connect(self.on_rename_button_clicked)
        layout.addWidget(rename_button)

        self.setLayout(layout)

    def resource_path(self, relative_path):
        """Obtém o caminho absoluto para recursos."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    def browse_dnb1_folder(self):
        options = QFileDialog.Options()
        folder_name = QFileDialog.getExistingDirectory(self, "Selecione a pasta", options=options)
        if folder_name:
            self.path_name2.setText(folder_name)

    def on_rename_button_clicked(self):
        path_name2 = self.path_name2.text()
        if not path_name2.strip():
            QMessageBox.critical(self, "Erro!", "Insira o caminho da pasta antes de prosseguir!")
            return
        RenameFiles.renomeia_arquivos(path_name2)

class TabModeloExpDBN0(QWidget):
    """Aba para gerar modelo exp DBN0."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Onde deseja salvar
        hbox1 = QHBoxLayout()
        label1 = QLabel('Onde deseja salvar:')
        self.path_dbn0 = QLineEdit()
        browse_button3 = QPushButton('Browse')
        browse_button3.clicked.connect(self.browse_save_path_dbn0)
        hbox1.addWidget(label1)
        hbox1.addWidget(self.path_dbn0)
        hbox1.addWidget(browse_button3)
        layout.addLayout(hbox1)

        # Nome de inventário das DBN0s
        label2 = QLabel('Entre com o nome de inventário das DBN0s:')
        self.lista_dbn0 = QTextEdit()
        self.lista_dbn0.setAcceptRichText(False)
        layout.addWidget(label2)
        layout.addWidget(self.lista_dbn0)

        # Schema
        hbox2 = QHBoxLayout()
        label3 = QLabel('Entre com o schema:')
        self.schema_dbn0 = QLineEdit()
        hbox2.addWidget(label3)
        hbox2.addWidget(self.schema_dbn0)
        layout.addLayout(hbox2)

        # Botão Gerar DBN0
        generate_dbn0_button = QPushButton("Gerar DBN0")
        generate_dbn0_button.clicked.connect(self.on_generate_dbn0_button_clicked)
        layout.addWidget(generate_dbn0_button)

        self.setLayout(layout)

    def browse_save_path_dbn0(self):
        options = QFileDialog.Options()
        folder_name = QFileDialog.getExistingDirectory(self, "Onde deseja salvar", options=options)
        if folder_name:
            self.path_dbn0.setText(folder_name)

    def on_generate_dbn0_button_clicked(self):
        lista_dbn0 = self.lista_dbn0.toPlainText().split('\n')
        schema_dbn0 = self.schema_dbn0.text().strip()
        path_dbn0 = self.path_dbn0.text()

        if not path_dbn0.strip():
            QMessageBox.critical(self, "Erro!", "Insira o caminho onde deseja salvar!")
            return
        if not lista_dbn0 or not any(lista_dbn0):
            QMessageBox.critical(self, "Erro!", "Insira ao menos um nome de inventário!")
            return
        if not schema_dbn0:
            QMessageBox.critical(self, "Erro!", "Insira o schema!")
            return

        DBN0Model.modelo_DBN0(lista_dbn0, schema_dbn0, path_dbn0)

class TabModeloExpDBN1(QWidget):
    """Aba para gerar modelo exp DBN1."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Onde deseja salvar
        hbox1 = QHBoxLayout()
        label1 = QLabel('Onde deseja salvar:')
        self.path_dbn1 = QLineEdit()
        browse_button4 = QPushButton('Browse')
        browse_button4.clicked.connect(self.browse_save_path_dbn1)
        hbox1.addWidget(label1)
        hbox1.addWidget(self.path_dbn1)
        hbox1.addWidget(browse_button4)
        layout.addLayout(hbox1)

        # Nome de inventário das DBN1s
        label2 = QLabel('Entre com o nome de inventário das DBN1s:')
        self.lista_dbn1 = QTextEdit()
        self.lista_dbn1.setAcceptRichText(False)
        layout.addWidget(label2)
        layout.addWidget(self.lista_dbn1)

        # Schema
        hbox2 = QHBoxLayout()
        label3 = QLabel('Entre com o schema:')
        self.schema_dbn1 = QLineEdit()
        hbox2.addWidget(label3)
        hbox2.addWidget(self.schema_dbn1)
        layout.addLayout(hbox2)

        # Nome lógico da classe
        hbox3 = QHBoxLayout()
        label4 = QLabel('Entre com o nome lógico da classe:')
        self.classe_dbn1 = QLineEdit()
        hbox3.addWidget(label4)
        hbox3.addWidget(self.classe_dbn1)
        layout.addLayout(hbox3)

        # Botão Gerar DBN1
        generate_dbn1_button = QPushButton("Gerar DBN1")
        generate_dbn1_button.clicked.connect(self.on_generate_dbn1_button_clicked)
        layout.addWidget(generate_dbn1_button)

        self.setLayout(layout)

    def browse_save_path_dbn1(self):
        options = QFileDialog.Options()
        folder_name = QFileDialog.getExistingDirectory(self, "Onde deseja salvar", options=options)
        if folder_name:
            self.path_dbn1.setText(folder_name)

    def on_generate_dbn1_button_clicked(self):
        lista_dbn1 = self.lista_dbn1.toPlainText().split('\n')
        schema_dbn1 = self.schema_dbn1.text().strip()
        path_dbn1 = self.path_dbn1.text()
        classe_dbn1 = self.classe_dbn1.text().strip()

        if not path_dbn1.strip():
            QMessageBox.critical(self, "Erro!", "Insira o caminho onde deseja salvar!")
            return
        if not lista_dbn1 or not any(lista_dbn1):
            QMessageBox.critical(self, "Erro!", "Insira ao menos um nome de inventário!")
            return
        if not schema_dbn1:
            QMessageBox.critical(self, "Erro!", "Insira o schema!")
            return
        if not classe_dbn1:
            QMessageBox.critical(self, "Erro!", "Insira o nome lógico da classe!")
            return

        DBN1Model.modelo_DBN1(lista_dbn1, schema_dbn1, path_dbn1, classe_dbn1)

class GeraInterface(QWidget):
    """Interface principal da aplicação."""
    def __init__(self):
        super().__init__()
        self.init_ui()

    def resource_path(self, relative_path):
        """Obtém o caminho absoluto para recursos."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    def init_ui(self):
        self.setWindowTitle("APP AUTOMATION - Versão 4.1")
        self.setWindowIcon(QIcon(self.resource_path("icons/esse.ico")))
        self.setGeometry(100, 100, 800, 600)

        # Aplicando o estilo
        self.setStyleSheet("""
            QWidget {
                background-color: #686868;
                color: #f0f0f0;
                font-family: 'Cambria';
                font-size: 12pt;
            }
            QTabWidget::pane { 
                border: 1px solid #6c6c6c; 
                background-color: #A9A9A9;
            }
            QTabBar::tab {
                background-color: #A9A9A9;
                border: 1px solid #5a5a5a;
                padding: 10px;
                font-family: 'Cambria';
            }
            QTabBar::tab:selected {
                background-color: #D3D3D3;
                color: #000000;
            }
            QLabel {
                color: #f0f0f0;
            }
            QLineEdit, QTextEdit {
                background-color: #D3D3D3;
                color: #000000;
                border: 1px solid #5a5a5a;
                padding: 5px;
            }
            QPushButton {
                background-color: #C0C0C0;
                color: #000000;
                border: 1px solid #5a5a5a;
                padding: 5px;
            }
            QPushButton:hover {
                background-color: #B0B0B0;
            }
        """)

        # Criar Tab Widget
        self.tabs = QTabWidget()

        # Criar abas
        self.tab_masterfiles = TabMasterFiles(self)
        self.tab_renomear_dbn1 = TabRenomearDBN1(self)
        self.tab_modelo_exp_dbn0 = TabModeloExpDBN0(self)
        self.tab_modelo_exp_dbn1 = TabModeloExpDBN1(self)

        # Conectar sinais
        self.tab_masterfiles.disable_tabs_signal.connect(self.disable_tabs)
        self.tab_masterfiles.enable_tabs_signal.connect(self.enable_tabs)

        self.tabs.addTab(self.tab_masterfiles, "MasterFiles")
        self.tabs.addTab(self.tab_renomear_dbn1, "Renomear DBN1")
        self.tabs.addTab(self.tab_modelo_exp_dbn0, "Modelo exp DBN0")
        self.tabs.addTab(self.tab_modelo_exp_dbn1, "Modelo exp DBN1")

        # Layout principal
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.tabs)
        self.setLayout(main_layout)

    def disable_tabs(self):
        """Desabilita todas as abas."""
        self.tabs.setEnabled(False)

    def enable_tabs(self):
        """Habilita todas as abas."""
        self.tabs.setEnabled(True)