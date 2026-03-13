# gui/main_window.py
"""
Ventana principal de Onoruame - VERSIÓN CORREGIDA
Conecta directo a PostgreSQL via repositories.

Pestañas:
  1. Importar   — carga Excel, lanza generación
  2. Rutas      — tabla en tiempo real
  3. Repartidores — asignación de rutas
  4. Avances    — entregas registradas por el bot
"""

import os
import sys
import json
import threading
import webbrowser
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

import pandas as pd

# Core de Onoruame
from core.config import settings
from core.database import db
from core.repositories import (
    RutaRepo, RepartidorRepo, AvanceRepo, PersonaRepo, GeocacheRepo
)
from core.route_generator import RouteGenerator
from core.geocoder import Geocoder

# Generador de mapas (tu archivo existente)
from .file_generator import FileGenerator

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN VISUAL
# =============================================================================

COLORES = {
    'bg':        '#1e1e2e',      # Fondo principal
    'panel':     '#2a2a3e',      # Fondo de paneles
    'accent':    '#7c6af7',      # Púrpura (botones principales)
    'accent2':   '#4ecdc4',      # Turquesa (hover)
    'success':   '#4caf50',      # Verde
    'warning':   '#ff9800',      # Naranja
    'danger':    '#f44336',      # Rojo
    'text':      '#cdd6f4',      # Texto claro
    'text_dim':  '#7f849c',      # Texto tenue
    'border':    '#45475a',      # Bordes
}

ESTADO_COLORES = {
    'pendiente':    '#ff9800',   # Naranja
    'asignada':     '#2196f3',   # Azul
    'en_progreso':  '#9c27b0',   # Púrpura
    'completada':   '#4caf50',   # Verde
    'cancelada':    '#f44336',   # Rojo
}

ESTADO_ICONOS = {
    'pendiente':    '🟡',
    'asignada':     '🔵',
    'en_progreso':  '🟣',
    'completada':   '🟢',
    'cancelada':    '🔴',
}


# =============================================================================
# VENTANA PRINCIPAL
# =============================================================================

class MainWindow:
    """Ventana principal con 4 pestañas."""

    REFRESH_MS = 15_000  # Auto-refresh cada 15 segundos

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("🌵 Onoruame — Sistema de Rutas PJCDMX")
        self.root.geometry("1280x800")
        self.root.minsize(1024, 640)
        self.root.configure(bg=COLORES['bg'])

        # Estado de la aplicación
        self.archivo_excel: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self.generando: bool = False
        self._refresh_job: Optional[str] = None
        self._rep_ids: Dict[str, str] = {}  # Mapeo nombre → id repartidor

        # Aplicar estilos
        self._aplicar_estilos()
        
        # Construir UI
        self._construir_ui()
        
        # Verificar conexión a BD
        self._verificar_db()
        
        # Iniciar auto-refresh
        self._auto_refresh()

    # -------------------------------------------------------------------------
    # ESTILOS
    # -------------------------------------------------------------------------

    def _aplicar_estilos(self):
        """Configura los estilos de ttk"""
        style = ttk.Style()
        style.theme_use('clam')

        # Estilos base
        style.configure('.',
            background=COLORES['bg'],
            foreground=COLORES['text'],
            fieldbackground=COLORES['panel'],
            font=('Segoe UI', 10),
        )

        # Notebook (pestañas)
        style.configure('TNotebook',
            background=COLORES['bg'],
            tabmargins=[2, 5, 2, 0],
        )
        style.configure('TNotebook.Tab',
            background=COLORES['panel'],
            foreground=COLORES['text_dim'],
            padding=[16, 8],
            font=('Segoe UI', 10),
        )
        style.map('TNotebook.Tab',
            background=[('selected', COLORES['accent'])],
            foreground=[('selected', '#ffffff')],
        )

        # Frames
        style.configure('TFrame', background=COLORES['bg'])
        style.configure('TLabel', background=COLORES['bg'], foreground=COLORES['text'])
        
        # Botones
        style.configure('TButton',
            background=COLORES['accent'],
            foreground='#ffffff',
            padding=[12, 6],
            relief='flat',
        )
        style.map('TButton',
            background=[('active', COLORES['accent2']), ('disabled', COLORES['border'])],
        )
        style.configure('Danger.TButton', background=COLORES['danger'])
        style.configure('Success.TButton', background=COLORES['success'])
        style.configure('Warning.TButton', background=COLORES['warning'])

        # Treeview (tablas)
        style.configure('Treeview',
            background=COLORES['panel'],
            foreground=COLORES['text'],
            fieldbackground=COLORES['panel'],
            rowheight=28,
            borderwidth=0,
        )
        style.configure('Treeview.Heading',
            background=COLORES['bg'],
            foreground=COLORES['accent'],
            font=('Segoe UI', 9, 'bold'),
            relief='flat',
        )
        style.map('Treeview',
            background=[('selected', COLORES['accent'])],
            foreground=[('selected', '#ffffff')],
        )

        # Entradas
        style.configure('TEntry',
            fieldbackground=COLORES['panel'],
            foreground=COLORES['text'],
            insertcolor=COLORES['text'],
        )
        style.configure('TCombobox',
            fieldbackground=COLORES['panel'],
            foreground=COLORES['text'],
        )

        # Progressbar
        style.configure('TProgressbar',
            troughcolor=COLORES['panel'],
            background=COLORES['accent'],
        )

        # LabelFrame
        style.configure('TLabelframe',
            background=COLORES['bg'],
            foreground=COLORES['text_dim'],
        )
        style.configure('TLabelframe.Label',
            background=COLORES['bg'],
            foreground=COLORES['text_dim'],
            font=('Segoe UI', 9),
        )

    # -------------------------------------------------------------------------
    # CONSTRUCCIÓN DE LA UI
    # -------------------------------------------------------------------------

    def _construir_ui(self):
        """Construye todos los elementos de la interfaz"""
        
        # ----- HEADER -----
        header = tk.Frame(self.root, bg=COLORES['accent'], height=48)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(header,
            text="🌵 ONORUAME — Sistema de Rutas PJCDMX",
            bg=COLORES['accent'], fg='#ffffff',
            font=('Segoe UI', 14, 'bold'),
        ).pack(side=tk.LEFT, padx=20, pady=10)

        self.lbl_db = tk.Label(header,
            text="● DB",
            bg=COLORES['accent'], fg='#ffffff',
            font=('Segoe UI', 9),
        )
        self.lbl_db.pack(side=tk.RIGHT, padx=20)

        self.lbl_hora = tk.Label(header,
            text="",
            bg=COLORES['accent'], fg='#ffffff',
            font=('Segoe UI', 9),
        )
        self.lbl_hora.pack(side=tk.RIGHT, padx=10)
        self._tick_hora()

        # ----- NOTEBOOK (PESTAÑAS) -----
        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Crear las 4 pestañas
        self._tab_importar()
        self._tab_rutas()
        self._tab_repartidores()
        self._tab_avances()

        # ----- BARRA DE ESTADO -----
        self.statusbar = tk.Label(self.root,
            text="Listo",
            bg=COLORES['panel'], fg=COLORES['text_dim'],
            anchor=tk.W, padx=12, pady=4,
            font=('Segoe UI', 9),
        )
        self.statusbar.pack(fill=tk.X, side=tk.BOTTOM)

    # -------------------------------------------------------------------------
    # PESTAÑA 1: IMPORTAR EXCEL
    # -------------------------------------------------------------------------

    def _tab_importar(self):
        """Pestaña de importación de Excel y generación de rutas"""
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  📥 Importar  ')

        # Panel izquierdo: configuración
        left = ttk.LabelFrame(frame, text='Configuración', padding=15)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8), pady=0)
        left.pack_propagate(False)
        left.configure(width=320)

        # Archivo Excel
        ttk.Label(left, text='Archivo Excel:').pack(anchor=tk.W)
        file_row = ttk.Frame(left)
        file_row.pack(fill=tk.X, pady=(4, 12))

        self.lbl_archivo = ttk.Label(file_row,
            text='Sin seleccionar',
            foreground=COLORES['text_dim'],
            wraplength=200,
        )
        self.lbl_archivo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(file_row, text='📂', width=4,
                   command=self._seleccionar_excel).pack(side=tk.RIGHT)

        # Origen (coordenadas)
        ttk.Label(left, text='Coordenadas de origen:').pack(anchor=tk.W)
        self.entry_coords = ttk.Entry(left)
        self.entry_coords.insert(0, settings.ORIGEN_COORDS)
        self.entry_coords.pack(fill=tk.X, pady=(4, 12))

        # Origen (nombre)
        ttk.Label(left, text='Nombre del origen:').pack(anchor=tk.W)
        self.entry_origen = ttk.Entry(left)
        self.entry_origen.insert(0, settings.ORIGEN_NOMBRE)
        self.entry_origen.pack(fill=tk.X, pady=(4, 12))

        # Máx paradas por ruta
        ttk.Label(left, text='Máx. paradas por ruta:').pack(anchor=tk.W)
        self.spin_max = ttk.Spinbox(left, from_=3, to=15, width=6)
        self.spin_max.set(settings.MAX_EDIFICIOS_POR_RUTA)
        self.spin_max.pack(anchor=tk.W, pady=(4, 20))

        # API Key (visible/oculta)
        ttk.Label(left, text='Google API Key:').pack(anchor=tk.W)
        api_frame = ttk.Frame(left)
        api_frame.pack(fill=tk.X, pady=(4, 12))
        
        self.entry_api = ttk.Entry(api_frame, show='*')
        self.entry_api.insert(0, settings.GOOGLE_MAPS_API_KEY)
        self.entry_api.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.show_api = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text='👁', variable=self.show_api,
                       command=self._toggle_api_visibility).pack(side=tk.RIGHT, padx=(4,0))

        # Botón generar
        self.btn_generar = ttk.Button(left,
            text='▶  GENERAR RUTAS',
            command=self._lanzar_generacion,
            state='disabled',
        )
        self.btn_generar.pack(fill=tk.X, pady=(0, 8))

        # Botones auxiliares
        ttk.Button(left,
            text='🗄  Inicializar BD',
            command=self._init_db,
        ).pack(fill=tk.X, pady=(0, 8))

        ttk.Button(left,
            text='📊  Stats geocache',
            command=self._ver_geocache,
        ).pack(fill=tk.X)

        # Barra de progreso
        self.progress = ttk.Progressbar(left, mode='indeterminate')
        self.progress.pack(fill=tk.X, pady=(20, 4))
        
        self.lbl_progress = ttk.Label(left, text='', foreground=COLORES['text_dim'],
                                      font=('Segoe UI', 9))
        self.lbl_progress.pack(anchor=tk.W)

        # Panel derecho: log
        right = ttk.LabelFrame(frame, text='Log', padding=10)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.log_text = scrolledtext.ScrolledText(
            right,
            wrap=tk.WORD,
            bg=COLORES['panel'],
            fg=COLORES['text'],
            insertbackground=COLORES['text'],
            font=('Consolas', 9),
            relief='flat',
            borderwidth=0,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Tags de color para el log
        self.log_text.tag_config('ok',    foreground=COLORES['success'])
        self.log_text.tag_config('err',   foreground=COLORES['danger'])
        self.log_text.tag_config('warn',  foreground=COLORES['warning'])
        self.log_text.tag_config('info',  foreground=COLORES['accent2'])
        self.log_text.tag_config('dim',   foreground=COLORES['text_dim'])

    def _toggle_api_visibility(self):
        """Muestra/oculta la API key"""
        if self.show_api.get():
            self.entry_api.config(show='')
        else:
            self.entry_api.config(show='*')

    # -------------------------------------------------------------------------
    # PESTAÑA 2: RUTAS
    # -------------------------------------------------------------------------

    def _tab_rutas(self):
        """Pestaña de visualización y gestión de rutas"""
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  🗺 Rutas  ')

        # Barra superior
        top = ttk.Frame(frame)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(top, text='Filtrar por estado:').pack(side=tk.LEFT, padx=(0, 6))
        self.combo_estado_filtro = ttk.Combobox(top,
            values=['todos', 'pendiente', 'asignada', 'en_progreso', 'completada', 'cancelada'],
            state='readonly', width=14,
        )
        self.combo_estado_filtro.set('todos')
        self.combo_estado_filtro.pack(side=tk.LEFT)
        self.combo_estado_filtro.bind('<<ComboboxSelected>>', lambda _: self.cargar_rutas())

        ttk.Button(top, text='🔄 Actualizar',
                   command=self.cargar_rutas).pack(side=tk.LEFT, padx=8)

        self.lbl_total_rutas = ttk.Label(top, text='',
                                          foreground=COLORES['text_dim'])
        self.lbl_total_rutas.pack(side=tk.RIGHT, padx=8)

        # Tabla de rutas
        cols = ('id', 'zona', 'estado', 'paradas', 'personas',
                'dist_km', 'tiempo', 'repartidor', 'creado')
        self.tree_rutas = ttk.Treeview(frame, columns=cols, show='headings', height=14)

        anchors = {
            'id': 50, 'zona': 90, 'estado': 110, 'paradas': 70,
            'personas': 80, 'dist_km': 80, 'tiempo': 80,
            'repartidor': 160, 'creado': 140
        }
        cabeceras = {
            'id': 'ID', 'zona': 'Zona', 'estado': 'Estado',
            'paradas': 'Paradas', 'personas': 'Personas',
            'dist_km': 'Distancia', 'tiempo': 'Tiempo',
            'repartidor': 'Repartidor', 'creado': 'Creado'
        }

        for col in cols:
            self.tree_rutas.heading(col, text=cabeceras[col],
                                    command=lambda c=col: self._ordenar_rutas(c))
            self.tree_rutas.column(col, width=anchors[col], anchor=tk.CENTER)

        scroll_rutas = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                                      command=self.tree_rutas.yview)
        self.tree_rutas.configure(yscrollcommand=scroll_rutas.set)
        self.tree_rutas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_rutas.pack(side=tk.LEFT, fill=tk.Y)

        # Panel derecho: acciones
        acciones = ttk.LabelFrame(frame, text='Acciones', padding=12)
        acciones.pack(side=tk.LEFT, fill=tk.Y, padx=(8, 0))
        acciones.pack_propagate(False)
        acciones.configure(width=200)

        ttk.Button(acciones, text='🗺 Abrir mapa',
                   command=self._abrir_mapa_seleccionada).pack(fill=tk.X, pady=3)
        ttk.Button(acciones, text='🔗 Google Maps',
                   command=self._abrir_gmaps_seleccionada).pack(fill=tk.X, pady=3)
        ttk.Button(acciones, text='👁 Ver detalle',
                   command=self._ver_detalle_ruta).pack(fill=tk.X, pady=3)

        ttk.Separator(acciones, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)

        ttk.Label(acciones, text='Cambiar estado:',
                  foreground=COLORES['text_dim']).pack(anchor=tk.W)
        self.combo_nuevo_estado = ttk.Combobox(acciones,
            values=['pendiente', 'asignada', 'en_progreso', 'completada', 'cancelada'],
            state='readonly', width=16,
        )
        self.combo_nuevo_estado.pack(fill=tk.X, pady=4)
        ttk.Button(acciones, text='✅ Aplicar estado',
                   style='Success.TButton',
                   command=self._cambiar_estado_ruta).pack(fill=tk.X)

    # -------------------------------------------------------------------------
    # PESTAÑA 3: REPARTIDORES
    # -------------------------------------------------------------------------

    def _tab_repartidores(self):
        """Pestaña de gestión de repartidores"""
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  👤 Repartidores  ')

        left = ttk.Frame(frame)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Tabla de repartidores
        top = ttk.Frame(left)
        top.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(top, text='Repartidores registrados:').pack(side=tk.LEFT)
        ttk.Button(top, text='🔄', width=3,
                   command=self.cargar_repartidores).pack(side=tk.LEFT, padx=6)

        cols_rep = ('id', 'nombre', 'telefono', 'activo', 'rutas_asignadas')
        self.tree_reps = ttk.Treeview(left, columns=cols_rep,
                                       show='headings', height=10)
        
        col_widths = [60, 200, 120, 60, 120]
        col_texts = ['ID', 'Nombre', 'Teléfono', 'Activo', 'Rutas']
        
        for col, w, txt in zip(cols_rep, col_widths, col_texts):
            self.tree_reps.heading(col, text=txt)
            self.tree_reps.column(col, width=w, anchor=tk.CENTER)

        scroll_reps = ttk.Scrollbar(left, orient=tk.VERTICAL,
                                     command=self.tree_reps.yview)
        self.tree_reps.configure(yscrollcommand=scroll_reps.set)
        self.tree_reps.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_reps.pack(side=tk.LEFT, fill=tk.Y)

        # Panel derecho: acciones
        right = ttk.Frame(frame)
        right.pack(side=tk.LEFT, fill=tk.Y, padx=(12, 0))

        # Formulario de alta
        form = ttk.LabelFrame(right, text='Nuevo repartidor', padding=12)
        form.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(form, text='Nombre:').pack(anchor=tk.W)
        self.entry_rep_nombre = ttk.Entry(form, width=24)
        self.entry_rep_nombre.pack(fill=tk.X, pady=(2, 8))

        ttk.Label(form, text='Teléfono:').pack(anchor=tk.W)
        self.entry_rep_tel = ttk.Entry(form, width=24)
        self.entry_rep_tel.pack(fill=tk.X, pady=(2, 8))

        ttk.Label(form, text='Telegram ID:').pack(anchor=tk.W)
        self.entry_rep_telegram = ttk.Entry(form, width=24)
        self.entry_rep_telegram.pack(fill=tk.X, pady=(2, 8))

        ttk.Button(form, text='➕ Agregar repartidor',
                   style='Success.TButton',
                   command=self._agregar_repartidor).pack(fill=tk.X)

        # Asignación de ruta
        asignar = ttk.LabelFrame(right, text='Asignar ruta', padding=12)
        asignar.pack(fill=tk.X)

        ttk.Label(asignar, text='Ruta (ID):').pack(anchor=tk.W)
        self.entry_ruta_asignar = ttk.Entry(asignar, width=10)
        self.entry_ruta_asignar.pack(anchor=tk.W, pady=(2, 8))

        ttk.Label(asignar, text='Repartidor:').pack(anchor=tk.W)
        self.combo_rep_asignar = ttk.Combobox(asignar, state='readonly', width=22)
        self.combo_rep_asignar.pack(fill=tk.X, pady=(2, 8))

        ttk.Button(asignar, text='📋 Asignar ruta',
                   command=self._asignar_ruta).pack(fill=tk.X)

    # -------------------------------------------------------------------------
    # PESTAÑA 4: AVANCES
    # -------------------------------------------------------------------------

    def _tab_avances(self):
        """Pestaña de seguimiento de entregas"""
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  📦 Avances  ')

        # Barra superior
        top = ttk.Frame(frame)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(top, text='Mostrar:').pack(side=tk.LEFT)
        self.combo_avances_filtro = ttk.Combobox(top,
            values=['pendientes', 'procesados', 'todos'],
            state='readonly', width=12,
        )
        self.combo_avances_filtro.set('pendientes')
        self.combo_avances_filtro.pack(side=tk.LEFT, padx=6)
        self.combo_avances_filtro.bind('<<ComboboxSelected>>', lambda _: self.cargar_avances())

        ttk.Button(top, text='🔄 Actualizar',
                   command=self.cargar_avances).pack(side=tk.LEFT, padx=6)

        ttk.Button(top, text='✅ Marcar procesados',
                   style='Success.TButton',
                   command=self._marcar_procesados).pack(side=tk.RIGHT)

        self.lbl_total_avances = ttk.Label(top, text='',
                                            foreground=COLORES['text_dim'])
        self.lbl_total_avances.pack(side=tk.RIGHT, padx=12)

        # Tabla de avances
        cols_av = ('id', 'ruta', 'persona', 'repartidor', 'tipo', 'estado', 'foto', 'timestamp')
        self.tree_avances = ttk.Treeview(frame, columns=cols_av,
                                          show='headings', height=18)

        col_widths = [80, 60, 200, 150, 90, 90, 80, 140]
        col_texts = ['ID', 'Ruta', 'Persona', 'Repartidor', 'Tipo', 'Estado', 'Foto', 'Registrado']
        
        for col, w, txt in zip(cols_av, col_widths, col_texts):
            self.tree_avances.heading(col, text=txt)
            self.tree_avances.column(col, width=w, anchor=tk.CENTER)

        scroll_av = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                                   command=self.tree_avances.yview)
        self.tree_avances.configure(yscrollcommand=scroll_av.set)
        self.tree_avances.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_av.pack(side=tk.LEFT, fill=tk.Y)

    # =========================================================================
    # LÓGICA: IMPORTAR / GENERAR
    # =========================================================================

    def _seleccionar_excel(self):
        """Abre diálogo para seleccionar archivo Excel"""
        path = filedialog.askopenfilename(
            title='Seleccionar Excel',
            filetypes=[('Excel', '*.xlsx *.xls')],
        )
        if not path:
            return
            
        try:
            self.log(f'Cargando {os.path.basename(path)}…', 'info')
            from core.excel_processor import ExcelProcessor
            
            proc = ExcelProcessor(path)
            self.df = proc.procesar()
            self.archivo_excel = path
            
            self.lbl_archivo.config(
                text=os.path.basename(path),
                foreground=COLORES['success'],
            )
            self.btn_generar.config(state='normal')
            self.log(f'✅ {len(self.df)} registros cargados', 'ok')

            # Vista previa
            for i, row in self.df.head(4).iterrows():
                nombre = str(row.get('nombre', ''))[:28]
                direccion = str(row.get('direccion', ''))[:40]
                self.log(f"   {i+1}. {nombre}… → {direccion}…", 'dim')
                
        except Exception as e:
            self.log(f'❌ Error cargando Excel: {e}', 'err')
            messagebox.showerror('Error', str(e))

    def _lanzar_generacion(self):
        """Lanza el hilo de generación de rutas"""
        if self.generando or self.df is None:
            return

        # Validar API key
        api_key = self.entry_api.get().strip()
        if not api_key:
            messagebox.showwarning('API Key', 'Ingresa la API Key de Google Maps')
            return

        self.generando = True
        self.btn_generar.config(state='disabled')
        self.progress.start(12)
        self._set_progress('Iniciando geocodificación…')

        hilo = threading.Thread(target=self._generar_rutas, daemon=True)
        hilo.start()

    def _generar_rutas(self):
        """
        Ejecuta la generación de rutas en un hilo secundario.
        CORREGIDO: Ahora pasa api_key correctamente.
        """
        try:
            self.log('🚀 Iniciando generación de rutas…', 'info')

            # Obtener valores de la UI
            api_key = self.entry_api.get().strip()
            origen_coords = self.entry_coords.get().strip()
            origen_nombre = self.entry_origen.get().strip()
            max_paradas = int(self.spin_max.get())

            # Actualizar configuración
            settings.MAX_EDIFICIOS_POR_RUTA = max_paradas

            # Crear generador con API key (¡CORREGIDO!)
            generator = RouteGenerator(
                api_key=api_key,
                origen_coords=origen_coords,
                origen_nombre=origen_nombre
            )

            self._set_progress('Agrupando edificios…')
            edificios_por_zona = generator.agrupar_edificios(self.df)

            total_edificios = sum(len(v) for v in edificios_por_zona.values())
            self.log(f'   Edificios únicos: {total_edificios}', 'dim')
            self._set_progress(f'Creando rutas ({total_edificios} edificios)…')

            rutas = generator.crear_rutas(edificios_por_zona)
            self._set_progress('Guardando en PostgreSQL…')
            
            # Guardar en BD usando repositories
            from core.repositories import RutaRepo
            for ruta in rutas:
                RutaRepo.crear_desde_generador(ruta)

            # Generar mapas
            self._set_progress('Generando mapas…')
            os.makedirs('mapas_pro', exist_ok=True)

            try:
                fg = FileGenerator()
                for ruta in rutas:
                    fg.generar_mapa(ruta)
                self.log('🗺️ Mapas generados', 'ok')
            except Exception as e:
                self.log(f'⚠️ Error generando mapas: {e}', 'warn')

            # Estadísticas finales
            total_paradas = sum(r.total_edificios for r in rutas)
            total_personas = sum(r.total_personas for r in rutas)
            
            self.log(
                f'🎉 {len(rutas)} rutas | {total_paradas} paradas | {total_personas} personas',
                'ok',
            )
            
            # Refrescar tabla de rutas
            self.root.after(0, self.cargar_rutas)

        except Exception as e:
            import traceback
            self.log(f'❌ ERROR: {e}', 'err')
            self.log(traceback.format_exc(), 'err')

        finally:
            self.root.after(0, self._fin_generacion)

    def _fin_generacion(self):
        """Finaliza el proceso de generación"""
        self.generando = False
        self.btn_generar.config(state='normal')
        self.progress.stop()
        self._set_progress('Listo')

    # =========================================================================
    # LÓGICA: RUTAS
    # =========================================================================

    def cargar_rutas(self):
        """Carga las rutas desde la BD y las muestra en la tabla"""
        estado = self.combo_estado_filtro.get()
        try:
            if estado == 'todos':
                rutas = RutaRepo.list_all()
            else:
                rutas = RutaRepo.list_by_estado(estado)
        except Exception as e:
            self.log(f'❌ Error cargando rutas: {e}', 'err')
            return

        # Limpiar tabla
        for item in self.tree_rutas.get_children():
            self.tree_rutas.delete(item)

        # Insertar rutas
        for r in rutas:
            estado_r = r.get('estado', 'pendiente')
            icono = ESTADO_ICONOS.get(estado_r, '⚪')
            repartidor = r.get('repartidor_nombre') or '—'
            
            creado = ''
            if r.get('creado_en'):
                try:
                    creado = r['creado_en'].strftime('%d/%m %H:%M')
                except:
                    creado = str(r['creado_en'])[:16]

            self.tree_rutas.insert('', tk.END,
                iid=str(r['id']),
                values=(
                    r['id'],
                    r.get('zona', ''),
                    f"{icono} {estado_r}",
                    r.get('total_paradas', 0),
                    r.get('total_personas', 0),
                    f"{r.get('distancia_km', 0):.1f} km",
                    f"{r.get('tiempo_min', 0)} min",
                    repartidor,
                    creado,
                ),
                tags=(estado_r,),
            )

        # Aplicar colores por estado
        for est, color in ESTADO_COLORES.items():
            self.tree_rutas.tag_configure(est, foreground=color)

        self.lbl_total_rutas.config(text=f'{len(rutas)} rutas')
        self._status(f'Rutas cargadas: {len(rutas)}')

    def _ruta_seleccionada_id(self) -> Optional[int]:
        """Obtiene el ID de la ruta seleccionada"""
        sel = self.tree_rutas.selection()
        if not sel:
            messagebox.showwarning('Atención', 'Selecciona una ruta primero')
            return None
        return int(sel[0])

    def _abrir_mapa_seleccionada(self):
        """Abre el mapa HTML de la ruta seleccionada"""
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
            
        # Buscar archivo HTML generado
        if os.path.exists('mapas_pro'):
            for archivo in os.listdir('mapas_pro'):
                if archivo.startswith(f'Ruta_{ruta_id}_'):
                    webbrowser.open(f'file://{os.path.abspath(os.path.join("mapas_pro", archivo))}')
                    return
                    
        messagebox.showinfo('Info', f'No se encontró mapa para ruta {ruta_id}')

    def _abrir_gmaps_seleccionada(self):
        """Abre la URL de Google Maps de la ruta seleccionada"""
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
            
        ruta = RutaRepo.get(ruta_id)
        if ruta and ruta.get('google_maps_url'):
            webbrowser.open(ruta['google_maps_url'])
        else:
            messagebox.showinfo('Info', 'URL de Google Maps no disponible')

    def _ver_detalle_ruta(self):
        """Muestra ventana con detalle de la ruta seleccionada"""
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
            
        ruta = RutaRepo.get_full(ruta_id)
        if not ruta:
            return

        win = tk.Toplevel(self.root)
        win.title(f'Ruta {ruta_id} — {ruta.get("zona", "")}')
        win.geometry('700x500')
        win.configure(bg=COLORES['bg'])

        txt = scrolledtext.ScrolledText(win,
            bg=COLORES['panel'], fg=COLORES['text'],
            font=('Consolas', 9), relief='flat',
        )
        txt.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        txt.insert(tk.END, f"RUTA {ruta_id} — {ruta.get('zona')}\n")
        txt.insert(tk.END, f"{'='*50}\n")
        txt.insert(tk.END, f"Estado: {ruta.get('estado')}\n")
        txt.insert(tk.END, f"Distancia: {ruta.get('distancia_km', 0):.1f} km\n")
        txt.insert(tk.END, f"Tiempo: {ruta.get('tiempo_min', 0)} min\n")
        txt.insert(tk.END, f"{'='*50}\n\n")

        for i, parada in enumerate(ruta.get('paradas', []), 1):
            txt.insert(tk.END, f"📍 PARADA {i}\n")
            txt.insert(tk.END, f"   Dirección: {parada.get('direccion_original', '')[:80]}\n")
            
            personas = parada.get('personas', [])
            if isinstance(personas, str):
                import json
                personas = json.loads(personas)
                
            for p in personas:
                estado_p = '✅' if p.get('estado') == 'entregado' else '⏳'
                txt.insert(tk.END, f"   {estado_p} {p.get('nombre', '')}\n")
            txt.insert(tk.END, '\n')

        txt.config(state='disabled')

    def _cambiar_estado_ruta(self):
        """Cambia el estado de la ruta seleccionada"""
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
            
        nuevo = self.combo_nuevo_estado.get()
        if not nuevo:
            messagebox.showwarning('Atención', 'Selecciona un estado')
            return
            
        try:
            RutaRepo.cambiar_estado(ruta_id, nuevo)
            self.cargar_rutas()
            self._status(f'Ruta {ruta_id} → {nuevo}')
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _ordenar_rutas(self, col: str):
        """Ordena la tabla de rutas por columna"""
        items = [(self.tree_rutas.set(k, col), k)
                 for k in self.tree_rutas.get_children('')]
        items.sort()
        for i, (_, k) in enumerate(items):
            self.tree_rutas.move(k, '', i)

    # =========================================================================
    # LÓGICA: REPARTIDORES
    # =========================================================================

    def cargar_repartidores(self):
        """Carga la lista de repartidores desde la BD"""
        try:
            reps = RepartidorRepo.list_all()
        except Exception as e:
            self.log(f'❌ Error cargando repartidores: {e}', 'err')
            return

        # Limpiar tabla
        for item in self.tree_reps.get_children():
            self.tree_reps.delete(item)

        # Mapeo de nombres a IDs
        self._rep_ids = {}
        nombres = []

        for r in reps:
            activo = '✅' if r.get('activo', True) else '❌'
            
            self.tree_reps.insert('', tk.END, iid=str(r['id']), values=(
                str(r['id'])[:8] + '…',
                r['nombre'],
                r.get('telefono') or '—',
                activo,
                r.get('rutas_activas', 0),
            ))
            
            self._rep_ids[r['nombre']] = str(r['id'])
            nombres.append(r['nombre'])

        # Actualizar combobox
        self.combo_rep_asignar['values'] = nombres

    def _agregar_repartidor(self):
        """Agrega un nuevo repartidor a la BD"""
        nombre = self.entry_rep_nombre.get().strip()
        telefono = self.entry_rep_tel.get().strip()
        telegram_id = self.entry_rep_telegram.get().strip()

        if not nombre:
            messagebox.showwarning('Atención', 'El nombre es obligatorio')
            return

        try:
            RepartidorRepo.create(
                nombre=nombre,
                telefono=telefono or None,
                telegram_id=telegram_id or None
            )
            
            # Limpiar campos
            self.entry_rep_nombre.delete(0, tk.END)
            self.entry_rep_tel.delete(0, tk.END)
            self.entry_rep_telegram.delete(0, tk.END)
            
            # Recargar lista
            self.cargar_repartidores()
            self._status(f'Repartidor "{nombre}" creado')
            
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _asignar_ruta(self):
        """Asigna una ruta a un repartidor"""
        ruta_id_str = self.entry_ruta_asignar.get().strip()
        nombre_rep = self.combo_rep_asignar.get()

        if not ruta_id_str or not nombre_rep:
            messagebox.showwarning('Atención', 'Completa ID de ruta y repartidor')
            return

        try:
            ruta_id = int(ruta_id_str)
            rep_id = self._rep_ids.get(nombre_rep)
            
            if not rep_id:
                messagebox.showerror('Error', 'Repartidor no encontrado')
                return
                
            RutaRepo.asignar(ruta_id, rep_id)
            
            self.entry_ruta_asignar.delete(0, tk.END)
            self.cargar_rutas()
            self._status(f'Ruta {ruta_id} asignada a {nombre_rep}')
            
        except ValueError:
            messagebox.showerror('Error', 'ID de ruta debe ser un número')
        except Exception as e:
            messagebox.showerror('Error', str(e))

    # =========================================================================
    # LÓGICA: AVANCES
    # =========================================================================

    def cargar_avances(self):
        """Carga los avances/entregas desde la BD"""
        filtro = self.combo_avances_filtro.get()
        
        try:
            if filtro == 'pendientes':
                avances = AvanceRepo.pendientes()
            elif filtro == 'todos':
                avances = AvanceRepo.list_all(limit=200)
            else:  # procesados
                avances = AvanceRepo.procesados(limit=200)
        except Exception as e:
            self.log(f'❌ Error cargando avances: {e}', 'err')
            return

        # Limpiar tabla
        for item in self.tree_avances.get_children():
            self.tree_avances.delete(item)

        for av in avances:
            tiene_foto = '📷' if av.get('foto_path') else '—'
            
            ts = ''
            if av.get('creado_en'):
                try:
                    ts = av['creado_en'].strftime('%d/%m %H:%M')
                except:
                    ts = str(av['creado_en'])[:16]

            self.tree_avances.insert('', tk.END,
                iid=str(av['id']),
                values=(
                    str(av['id'])[:8] + '…',
                    av.get('ruta_id', ''),
                    av.get('persona_nombre') or '—',
                    av.get('repartidor_nombre') or '—',
                    av.get('tipo', 'entrega'),
                    av.get('estado', ''),
                    tiene_foto,
                    ts,
                ),
            )

        self.lbl_total_avances.config(text=f'{len(avances)} registros')

    def _marcar_procesados(self):
        """Marca los avances seleccionados como procesados"""
        sel = self.tree_avances.selection()
        if not sel:
            messagebox.showwarning('Atención', 'Selecciona avances primero')
            return
            
        for iid in sel:
            try:
                AvanceRepo.marcar_procesado(int(iid))
            except Exception as e:
                self.log(f'❌ Error marcando {iid}: {e}', 'err')
                
        self.cargar_avances()
        self._status(f'{len(sel)} avances marcados como procesados')

    # =========================================================================
    # UTILIDADES
    # =========================================================================

    def _verificar_db(self):
        """Verifica la conexión a la base de datos"""
        try:
            db.health_check()
            self.lbl_db.config(text='● DB conectada', fg=COLORES['success'])
            
            # Cargar datos iniciales
            self.cargar_rutas()
            self.cargar_repartidores()
            self.cargar_avances()
            
        except Exception as e:
            self.lbl_db.config(text='● DB sin conexión', fg=COLORES['danger'])
            self.log(f'❌ Sin conexión a PostgreSQL: {e}', 'err')

    def _init_db(self):
        """Inicializa el esquema de la base de datos"""
        try:
            db.init_schema()
            self.log('✅ Esquema de BD inicializado', 'ok')
            self._verificar_db()
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _ver_geocache(self):
        """Muestra estadísticas del caché de geocodificación"""
        try:
            stats = GeocacheRepo.stats()
            messagebox.showinfo('Geocoding Cache',
                f"✅ Exitosos: {stats.get('exitosos', 0)}\n"
                f"❌ Fallidos: {stats.get('fallidos', 0)}\n"
                f"📊 Total: {stats.get('total', 0)}"
            )
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _auto_refresh(self):
        """Refresca automáticamente las tablas cada cierto tiempo"""
        try:
            if db.health_check():
                self.cargar_rutas()
                self.cargar_avances()
        except:
            pass
            
        self._refresh_job = self.root.after(self.REFRESH_MS, self._auto_refresh)

    def _set_progress(self, msg: str):
        """Actualiza el mensaje de progreso (thread-safe)"""
        self.root.after(0, lambda: self.lbl_progress.config(text=msg))

    def _status(self, msg: str):
        """Actualiza la barra de estado"""
        ts = datetime.now().strftime('%H:%M:%S')
        self.statusbar.config(text=f'[{ts}]  {msg}')

    def _tick_hora(self):
        """Actualiza el reloj en el header"""
        self.lbl_hora.config(text=datetime.now().strftime('%H:%M:%S'))
        self.root.after(1000, self._tick_hora)

    def log(self, msg: str, tag: str = ''):
        """Agrega una línea al log (thread-safe)"""
        ts = datetime.now().strftime('%H:%M:%S')

        def _insert():
            self.log_text.insert(tk.END, f'[{ts}] ', 'dim')
            self.log_text.insert(tk.END, f'{msg}\n', tag or '')
            self.log_text.see(tk.END)

        if threading.current_thread() is threading.main_thread():
            _insert()
        else:
            self.root.after(0, _insert)


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Punto de entrada principal"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-8s  %(name)s  %(message)s',
    )
    
    root = tk.Tk()
    app = MainWindow(root)
    root.mainloop()


if __name__ == '__main__':
    main()
