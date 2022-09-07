from tkinter import messagebox

def msg_alerta_erro(titulo='', msg='Houve um erro erro inesperado'):
    messagebox.showerror(titulo, msg)
    
def msg_alerta_alert(titulo='', msg='Houve um erro erro inesperado'):
    messagebox.showwarning(titulo, msg)
   
def msg_alerta_sucesso(titulo='', msg='Houve um erro erro inesperado'):
    messagebox.showinfo(titulo, msg)