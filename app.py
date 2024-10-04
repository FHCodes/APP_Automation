import sys
from view import GeraInterface
from PyQt5.QtWidgets import  QApplication


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = GeraInterface()
    ex.show()
    sys.exit(app.exec_())