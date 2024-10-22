import tkinter as tk
from tkinter import filedialog


def getExcelFile():
    root = tk.Tk()
    root.withdraw()
    filePath = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
    root.destroy()
    return filePath