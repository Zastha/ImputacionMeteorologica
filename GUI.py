import limpiar_datos as ld
import datascraper as ds
import generador_data_sintetica_v1 as gdsv1
import tkinter as tk
import threading
from tkinter import ttk, filedialog

estaciones_scrapeables = [ 
    '25001', '25003', '25009', '25014', '25015', '25019', '25021', '25022', '25023', '25025',
    '25050', '25062', '25064', '25074', '25077', '25078', '25080', '25081', '25087', '25093',
    '25030', '25033', '25036', '25037', '25038', '25041', '25042', '25044', '25045', '25046',
    '25100', '25102', '25103', '25105', '25106', '25107', '25110', '25115', '25119', '25150',
    '25155', '25158', '25161', '25171', '25172', '25173', '25176', '25178', '25183', '25186'
]

class AppController:
    def __init__(self, root):
        self.root = root
        self.root.title("Limpieza y generación de datos")
        self.root.geometry("400x500")

        self.porcentaje = tk.StringVar(value="100")
        self.generar_logs_calidad = tk.BooleanVar(value=True)

        self.etiqueta_carpeta = tk.Label(root, text="Selecciona la carpeta con los datos a limpiar")
        self.etiqueta_carpeta.pack(pady=5)

        self.boton_scrapear = tk.Button(root, text="Scrapear datos", command=self.scrapear_datos)
        self.boton_scrapear.pack(pady=5)

        tk.Label(root, text="% máximo de datos faltantes aceptable").pack()
        tk.Entry(root, textvariable=self.porcentaje, width=5).pack(pady=2)

        self.boton_seleccionar = tk.Button(root, text="Seleccionar carpeta", command=self.seleccionar_carpeta)
        self.boton_seleccionar.pack(pady=5)

        self.checkbox_calidad = tk.Checkbutton(root, text="Crear registro de calidad de datos", variable=self.generar_logs_calidad)
        self.checkbox_calidad.pack(pady=5)

        self.barra_progreso = ttk.Progressbar(root, length=300, mode='determinate')
        self.barra_progreso.pack(pady=10)

        self.etiqueta_estado = tk.Label(root, text="", font=("Arial", 10))
        self.etiqueta_estado.pack()

    def scrapear_datos(self):
        carpeta = filedialog.askdirectory()
        if not carpeta:
            return
        self.etiqueta_estado.config(text="Scrapeando datos...")
        threading.Thread(target=self._tarea_scrapeo, args=(carpeta,), daemon=True).start()

    def _tarea_scrapeo(self, carpeta):
        ds.bulk_download(estaciones_scrapeables, carpeta)
        self.etiqueta_estado.config(text="Datos scrapeados correctamente")

    def seleccionar_carpeta(self):
        carpeta = filedialog.askdirectory()
        if not carpeta:
            return
        self.etiqueta_carpeta.config(text=f"Carpeta seleccionada:\n{carpeta}")
        self.etiqueta_estado.config(text="Iniciando limpieza...")
        self.boton_seleccionar.config(state='disabled')
        threading.Thread(target=self.ejecutar_pipeline, args=(carpeta,), daemon=True).start()

    def ejecutar_pipeline(self, carpeta):
        try:
            umbral = int(self.porcentaje.get())
            ld.procesar_lote_txt_a_csv(carpeta, umbral, self.generar_logs_calidad.get())
            self.etiqueta_estado.config(text="Generando datos sintéticos...")

            # Reemplazar con metodos de imputacion de datos faltantes
        
            gdsv1.generar_datos_sinteticos(
                directorio_base=carpeta,
                umbral_km=30,
                barra_progreso=self.barra_progreso,
                etiqueta_estado=self.etiqueta_estado
            )

            self.etiqueta_estado.config(text="Actualizando log de calidad...")
            ld.agg_datos_sinteticos_log_calidad(carpeta)

            self.etiqueta_estado.config(text="Generando concentrados...")
            ld.generar_concentrados_anuales(carpeta)
            ld.concatenar_concentrados_anuales(carpeta)
            ld.generar_concentrados_mensuales(carpeta)
            ld.generar_concentrados_diarios(carpeta)
            ld.concatenar_concentrados_mensuales(carpeta)

            self.etiqueta_estado.config(text="Proceso finalizado con éxito")
        except Exception as e:
            self.etiqueta_estado.config(text=f"Error: {e}")
            
        finally:
            self.barra_progreso['value'] = 100
            self.boton_seleccionar.config(state='normal')

if __name__ == "__main__":
    root = tk.Tk()
    app = AppController(root)
    root.mainloop()
