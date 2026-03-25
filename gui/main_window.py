# gui/main_window.py
"""
Ventana principal de Onoruame.
Conecta directo a PostgreSQL via repositories (sin pasar por Flask API).

Pestañas:
  1. Importar   — carga Excel, lanza generación en hilo separado
  2. Rutas      — tabla en tiempo real con estado y acciones
  3. Repartidores — asignación de rutas
  4. Avances    — entregas registradas por el bot
"""

import os
import sys
import json
import threading
import webbrowser
import subprocess
import logging
from datetime import datetime
from typing import Optional, List, Dict

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

import pandas as pd

# Core
from core.config      import settings
from core.database    import db
from core.repositories import (
    RutaRepo, RepartidorRepo, AvanceRepo, PersonaRepo, GeocacheRepo
)
from core.route_generator import RouteGenerator

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Paleta y estilos
# ─────────────────────────────────────────────────────────────

COLORES = {
    'bg':        '#1e1e2e',
    'panel':     '#2a2a3e',
    'accent':    '#7c6af7',
    'accent2':   '#4ecdc4',
    'success':   '#4caf50',
    'warning':   '#ff9800',
    'danger':    '#f44336',
    'text':      '#cdd6f4',
    'text_dim':  '#7f849c',
    'border':    '#45475a',
}

ESTADO_COLORES = {
    'pendiente':    '#ff9800',
    'asignada':     '#2196f3',
    'en_progreso':  '#9c27b0',
    'completada':   '#4caf50',
    'cancelada':    '#f44336',
}

ESTADO_ICONOS = {
    'pendiente':    '🟡',
    'asignada':     '🔵',
    'en_progreso':  '🟣',
    'completada':   '🟢',
    'cancelada':    '🔴',
}


# ─────────────────────────────────────────────────────────────
# Ventana principal
# ─────────────────────────────────────────────────────────────

class MainWindow:
    """Ventana principal con 4 pestañas."""

    REFRESH_MS = 15_000      # auto-refresh cada 15 s

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Onoruame — Sistema de Rutas")
        self.root.geometry("1280x800")
        self.root.minsize(1024, 640)
        self.root.configure(bg=COLORES['bg'])

        # Estado
        self.archivo_excel: Optional[str] = None
        self.df:            Optional[pd.DataFrame] = None
        self.generando:     bool = False
        self._refresh_job:  Optional[str] = None

        self._aplicar_estilos()
        self._construir_ui()
        self._verificar_db()
        self._auto_refresh()

    # ──────────────────────────────────────────────────────────
    # Estilos ttk
    # ──────────────────────────────────────────────────────────

    def _aplicar_estilos(self):
        style = ttk.Style()
        style.theme_use('clam')

        style.configure('.',
            background=COLORES['bg'],
            foreground=COLORES['text'],
            fieldbackground=COLORES['panel'],
            font=('Segoe UI', 10),
        )
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
        style.configure('TFrame',   background=COLORES['bg'])
        style.configure('TLabel',   background=COLORES['bg'], foreground=COLORES['text'])
        style.configure('TButton',
            background=COLORES['accent'],
            foreground='#ffffff',
            padding=[12, 6],
            relief='flat',
        )
        style.map('TButton',
            background=[('active', COLORES['accent2']), ('disabled', COLORES['border'])],
        )
        style.configure('Danger.TButton',  background=COLORES['danger'])
        style.configure('Success.TButton', background=COLORES['success'])
        style.configure('Warning.TButton', background=COLORES['warning'])

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
        style.configure('TEntry',
            fieldbackground=COLORES['panel'],
            foreground=COLORES['text'],
            insertcolor=COLORES['text'],
        )
        style.configure('TCombobox',
            fieldbackground=COLORES['panel'],
            foreground=COLORES['text'],
        )
        style.configure('TProgressbar',
            troughcolor=COLORES['panel'],
            background=COLORES['accent'],
        )
        style.configure('TLabelframe',
            background=COLORES['bg'],
            foreground=COLORES['text_dim'],
        )
        style.configure('TLabelframe.Label',
            background=COLORES['bg'],
            foreground=COLORES['text_dim'],
            font=('Segoe UI', 9),
        )

    # ──────────────────────────────────────────────────────────
    # UI principal
    # ──────────────────────────────────────────────────────────

    def _construir_ui(self):
        # Header
        header = tk.Frame(self.root, bg=COLORES['accent'], height=48)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(header,
            text="⚡ ONORUAME",
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
            bg=COLORES['accent'], fg='#cccccc',
            font=('Segoe UI', 9),
        )
        self.lbl_hora.pack(side=tk.RIGHT, padx=10)
        self._tick_hora()

        # Notebook
        self.nb = ttk.Notebook(self.root)
        self.nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self._tab_importar()
        self._tab_rutas()
        self._tab_repartidores()
        self._tab_avances()

        # Barra de estado
        self.statusbar = tk.Label(self.root,
            text="Listo",
            bg=COLORES['panel'], fg=COLORES['text_dim'],
            anchor=tk.W, padx=12, pady=4,
            font=('Segoe UI', 9),
        )
        self.statusbar.pack(fill=tk.X, side=tk.BOTTOM)

    # ──────────────────────────────────────────────────────────
    # TAB 1: Importar Excel
    # ──────────────────────────────────────────────────────────

    def _tab_importar(self):
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  📥 Importar  ')

        # Panel izquierdo: configuración
        left = ttk.LabelFrame(frame, text='Configuración', padding=15)
        left.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8), pady=0)
        left.pack_propagate(False)
        left.configure(width=320)

        # Archivo
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

        # Origen
        ttk.Label(left, text='Coordenadas de origen:').pack(anchor=tk.W)
        self.entry_coords = ttk.Entry(left)
        self.entry_coords.insert(0, settings.ORIGEN_COORDS)
        self.entry_coords.pack(fill=tk.X, pady=(4, 12))

        ttk.Label(left, text='Nombre del origen:').pack(anchor=tk.W)
        self.entry_origen = ttk.Entry(left)
        self.entry_origen.insert(0, settings.ORIGEN_NOMBRE)
        self.entry_origen.pack(fill=tk.X, pady=(4, 12))

        ttk.Label(left, text='Máx. paradas por ruta:').pack(anchor=tk.W)
        self.spin_max = ttk.Spinbox(left, from_=3, to=15, width=6)
        self.spin_max.set(settings.MAX_EDIFICIOS_POR_RUTA)
        self.spin_max.pack(anchor=tk.W, pady=(4, 20))

        # Botones acción
        self.btn_generar = ttk.Button(left,
            text='▶  GENERAR RUTAS',
            command=self._lanzar_generacion,
            state='disabled',
        )
        self.btn_generar.pack(fill=tk.X, pady=(0, 8))

        ttk.Button(left,
            text='🗄  Inicializar BD',
            command=self._init_db,
        ).pack(fill=tk.X, pady=(0, 8))

        ttk.Button(left,
            text='📊  Stats geocache',
            command=self._ver_geocache,
        ).pack(fill=tk.X)

        # Progress
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

        # Tags de color en el log
        self.log_text.tag_config('ok',    foreground=COLORES['success'])
        self.log_text.tag_config('err',   foreground=COLORES['danger'])
        self.log_text.tag_config('warn',  foreground=COLORES['warning'])
        self.log_text.tag_config('info',  foreground=COLORES['accent2'])
        self.log_text.tag_config('dim',   foreground=COLORES['text_dim'])

    # ──────────────────────────────────────────────────────────
    # TAB 2: Rutas
    # ──────────────────────────────────────────────────────────

    def _tab_rutas(self):
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

        # Tabla rutas
        cols = ('id', 'zona', 'estado', 'paradas', 'personas',
                'dist_km', 'tiempo', 'repartidor', 'creado')
        self.tree_rutas = ttk.Treeview(frame, columns=cols, show='headings', height=14)

        anchors = {'id': 50, 'zona': 90, 'estado': 110, 'paradas': 70,
                   'personas': 80, 'dist_km': 80, 'tiempo': 80,
                   'repartidor': 160, 'creado': 140}
        cabeceras = {'id': 'ID', 'zona': 'Zona', 'estado': 'Estado',
                     'paradas': 'Paradas', 'personas': 'Personas',
                     'dist_km': 'Distancia', 'tiempo': 'Tiempo',
                     'repartidor': 'Repartidor', 'creado': 'Creado'}

        for col in cols:
            self.tree_rutas.heading(col, text=cabeceras[col],
                                    command=lambda c=col: self._ordenar_rutas(c))
            self.tree_rutas.column(col, width=anchors[col], anchor=tk.CENTER)

        sb_rutas = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                                  command=self.tree_rutas.yview)
        self.tree_rutas.configure(yscrollcommand=sb_rutas.set)
        self.tree_rutas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb_rutas.pack(side=tk.LEFT, fill=tk.Y)

        # Panel derecho: acciones de ruta
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

    # ──────────────────────────────────────────────────────────
    # TAB 3: Repartidores
    # ──────────────────────────────────────────────────────────

    def _tab_repartidores(self):
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  👤 Repartidores  ')

        left = ttk.Frame(frame)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Tabla repartidores
        top = ttk.Frame(left)
        top.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(top, text='Repartidores activos:').pack(side=tk.LEFT)
        ttk.Button(top, text='🔄', width=3,
                   command=self.cargar_repartidores).pack(side=tk.LEFT, padx=6)

        cols_rep = ('id', 'nombre', 'telefono', 'rutas_asignadas')
        self.tree_reps = ttk.Treeview(left, columns=cols_rep,
                                       show='headings', height=10)
        for col, w, txt in [('id', 60, 'ID'), ('nombre', 200, 'Nombre'),
                              ('telefono', 120, 'Teléfono'),
                              ('rutas_asignadas', 120, 'Rutas asignadas')]:
            self.tree_reps.heading(col, text=txt)
            self.tree_reps.column(col, width=w, anchor=tk.CENTER)

        sb_r = ttk.Scrollbar(left, orient=tk.VERTICAL,
                              command=self.tree_reps.yview)
        self.tree_reps.configure(yscrollcommand=sb_r.set)
        self.tree_reps.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb_r.pack(side=tk.LEFT, fill=tk.Y)

        # Panel derecho
        right = ttk.Frame(frame)
        right.pack(side=tk.LEFT, fill=tk.Y, padx=(12, 0))

        # Alta de repartidor
        form = ttk.LabelFrame(right, text='Nuevo repartidor', padding=12)
        form.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(form, text='Nombre:').pack(anchor=tk.W)
        self.entry_rep_nombre = ttk.Entry(form, width=24)
        self.entry_rep_nombre.pack(fill=tk.X, pady=(2, 8))

        ttk.Label(form, text='Teléfono:').pack(anchor=tk.W)
        self.entry_rep_tel = ttk.Entry(form, width=24)
        self.entry_rep_tel.pack(fill=tk.X, pady=(2, 8))

        ttk.Button(form, text='➕ Agregar',
                   style='Success.TButton',
                   command=self._agregar_repartidor).pack(fill=tk.X)

        # Asignar ruta
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

    # ──────────────────────────────────────────────────────────
    # TAB 4: Avances
    # ──────────────────────────────────────────────────────────

    def _tab_avances(self):
        frame = ttk.Frame(self.nb)
        self.nb.add(frame, text='  📦 Avances  ')

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

        ttk.Button(top, text='✅ Marcar procesados seleccionados',
                   style='Success.TButton',
                   command=self._marcar_procesados).pack(side=tk.RIGHT)

        self.lbl_total_avances = ttk.Label(top, text='',
                                            foreground=COLORES['text_dim'])
        self.lbl_total_avances.pack(side=tk.RIGHT, padx=12)

        # Tabla avances
        cols_av = ('id', 'ruta', 'persona', 'repartidor', 'tipo', 'estado', 'foto', 'timestamp')
        self.tree_avances = ttk.Treeview(frame, columns=cols_av,
                                          show='headings', height=18)

        for col, w, txt in [
            ('id',          80,  'ID'),
            ('ruta',        60,  'Ruta'),
            ('persona',     200, 'Persona'),
            ('repartidor',  150, 'Repartidor'),
            ('tipo',        90,  'Tipo'),
            ('estado',      90,  'Estado'),
            ('foto',        80,  'Foto'),
            ('timestamp',   140, 'Registrado'),
        ]:
            self.tree_avances.heading(col, text=txt)
            self.tree_avances.column(col, width=w, anchor=tk.CENTER)

        sb_av = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                               command=self.tree_avances.yview)
        self.tree_avances.configure(yscrollcommand=sb_av.set)
        self.tree_avances.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb_av.pack(side=tk.LEFT, fill=tk.Y)

    # ──────────────────────────────────────────────────────────
    # Lógica: Importar / Generar
    # ──────────────────────────────────────────────────────────

    def _seleccionar_excel(self):
        path = filedialog.askopenfilename(
            title='Seleccionar Excel',
            filetypes=[('Excel', '*.xlsx *.xls')],
        )
        if not path:
            return
        try:
            self.log(f'Cargando {os.path.basename(path)}…', 'info')
            from core.excel_processor import ExcelProcessor
            proc    = ExcelProcessor(path)
            self.df = proc.procesar()
            self.archivo_excel = path
            self.lbl_archivo.config(
                text=os.path.basename(path),
                foreground=COLORES['success'],
            )
            self.btn_generar.config(state='normal')
            self.log(f'✅ {len(self.df)} registros cargados', 'ok')

            # Vista previa
            for _, row in self.df.head(4).iterrows():
                self.log(
                    f"   {str(row.get('nombre',''))[:28]} → "
                    f"{str(row.get('direccion',''))[:40]}", 'dim'
                )
        except Exception as e:
            self.log(f'❌ Error cargando Excel: {e}', 'err')
            messagebox.showerror('Error', str(e))

    def _lanzar_generacion(self):
        if self.generando or self.df is None:
            return

        self.generando = True
        self.btn_generar.config(state='disabled')
        self.progress.start(12)
        self._set_progress('Geocodificando direcciones…')

        hilo = threading.Thread(target=self._generar_rutas, daemon=True)
        hilo.start()

    def _generar_rutas(self):
        """Corre en hilo secundario — NO tocar widgets directamente."""
        try:
            self.log('🚀 Iniciando generación de rutas…', 'info')

            # Sincronizar settings desde UI antes de instanciar el generador
            settings.MAX_EDIFICIOS_POR_RUTA = int(self.spin_max.get())
            #settings.GOOGLE_MAPS_API_KEY    = self.entry_api.get().strip()
            settings.ORIGEN_COORDS          = self.entry_coords.get().strip()
            settings.ORIGEN_NOMBRE          = self.entry_origen.get().strip()

            generator = RouteGenerator()

            self._set_progress('Agrupando edificios…')
            edificios_por_zona = generator.agrupar_edificios(self.df)

            total_ed = sum(len(v) for v in edificios_por_zona.values())
            self.log(f'   Edificios únicos: {total_ed}', 'dim')
            self._set_progress(f'Creando rutas ({total_ed} edificios)…')

            rutas = generator.crear_rutas(edificios_por_zona)
            self._set_progress('Guardando en PostgreSQL…')

            generator.persistir_en_db(rutas)

            # Generar mapas
            self._set_progress('Generando mapas…')
            os.makedirs('mapas_pro', exist_ok=True)

            try:
                fg = FileGenerator()
                fg.generar_todos(rutas)
            except Exception as e:
                self.log(f'⚠️ Error generando mapas: {e}', 'warn')

            total_p = sum(r.total_paradas  for r in rutas)
            total_pe = sum(r.total_personas for r in rutas)
            self.log(
                f'🎉 {len(rutas)} rutas | {total_p} paradas | {total_pe} personas',
                'ok',
            )
            self.root.after(0, self.cargar_rutas)

        except Exception as e:
            import traceback
            self.log(f'❌ ERROR: {e}', 'err')
            self.log(traceback.format_exc(), 'err')

        finally:
            self.root.after(0, self._fin_generacion)

    def _fin_generacion(self):
        self.generando = False
        self.btn_generar.config(state='normal')
        self.progress.stop()
        self._set_progress('Listo')

    # ──────────────────────────────────────────────────────────
    # Lógica: Rutas
    # ──────────────────────────────────────────────────────────

    def cargar_rutas(self):
        estado = self.combo_estado_filtro.get()
        try:
            rutas = RutaRepo.list_all(None if estado == 'todos' else estado)
        except Exception as e:
            self.log(f'❌ Error cargando rutas: {e}', 'err')
            return

        self.tree_rutas.delete(*self.tree_rutas.get_children())

        for r in rutas:
            estado_r   = r.get('estado', 'pendiente')
            icono      = ESTADO_ICONOS.get(estado_r, '⚪')
            repartidor = r.get('repartidor') or '—'
            creado     = ''
            if r.get('creado_en'):
                try:
                    creado = r['creado_en'].strftime('%d/%m %H:%M')
                except Exception:
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

        # Colorear filas por estado
        for est, color in ESTADO_COLORES.items():
            self.tree_rutas.tag_configure(est, foreground=color)

        self.lbl_total_rutas.config(text=f'{len(rutas)} rutas')
        self._status(f'Rutas cargadas: {len(rutas)}')

    def _ruta_seleccionada_id(self) -> Optional[int]:
        sel = self.tree_rutas.selection()
        if not sel:
            messagebox.showwarning('Atención', 'Selecciona una ruta primero')
            return None
        return int(sel[0])

    def _abrir_mapa_seleccionada(self):
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
        # Buscar archivo HTML generado
        for zona in ['CENTRO', 'SUR', 'ORIENTE', 'NORTE', 'PONIENTE', 'OTRAS']:
            path = f'mapas_pro/Ruta_{ruta_id}_{zona}.html'
            if os.path.exists(path):
                webbrowser.open(f'file://{os.path.abspath(path)}')
                return
        # Intentar sin zona
        candidatos = [f for f in os.listdir('mapas_pro')
                      if f.startswith(f'Ruta_{ruta_id}_')] if os.path.exists('mapas_pro') else []
        if candidatos:
            webbrowser.open(f'file://{os.path.abspath(os.path.join("mapas_pro", candidatos[0]))}')
        else:
            messagebox.showinfo('Info', f'No se encontró mapa para ruta {ruta_id}')

    def _abrir_gmaps_seleccionada(self):
        ruta_id = self._ruta_seleccionada_id()
        if ruta_id is None:
            return
        ruta = RutaRepo.get(ruta_id)
        if ruta and ruta.get('google_maps_url'):
            webbrowser.open(ruta['google_maps_url'])
        else:
            messagebox.showinfo('Info', 'URL de Google Maps no disponible')

    def _ver_detalle_ruta(self):
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

        txt.insert(tk.END, f"Ruta {ruta_id} — {ruta.get('zona')}\n")
        txt.insert(tk.END, f"Estado: {ruta.get('estado')}\n")
        txt.insert(tk.END, f"Distancia: {ruta.get('distancia_km', 0):.1f} km  |  "
                           f"Tiempo: {ruta.get('tiempo_min', 0)} min\n\n")

        for i, parada in enumerate(ruta.get('paradas', []), 1):
            txt.insert(tk.END,
                f"  Parada {i}: {parada.get('direccion_original', '')[:80]}\n")
            personas = parada.get('personas', [])
            if isinstance(personas, str):
                import json as _json
                personas = _json.loads(personas)
            for p in personas:
                est = '✅' if p.get('estado') == 'entregado' else '⏳'
                txt.insert(tk.END, f"    {est} {p.get('nombre', '')}\n")
            txt.insert(tk.END, '\n')

        txt.config(state='disabled')

    def _cambiar_estado_ruta(self):
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
        """Ordenar tabla por columna al hacer clic en encabezado."""
        items = [(self.tree_rutas.set(k, col), k)
                 for k in self.tree_rutas.get_children('')]
        items.sort()
        for i, (_, k) in enumerate(items):
            self.tree_rutas.move(k, '', i)

    # ──────────────────────────────────────────────────────────
    # Lógica: Repartidores
    # ──────────────────────────────────────────────────────────

    def cargar_repartidores(self):
        try:
            reps = RepartidorRepo.list_activos()
        except Exception as e:
            self.log(f'❌ Error cargando repartidores: {e}', 'err')
            return

        self.tree_reps.delete(*self.tree_reps.get_children())

        nombres = []
        for r in reps:
            self.tree_reps.insert('', tk.END, iid=str(r['id']), values=(
                str(r['id'])[:8] + '…',
                r['nombre'],
                r.get('telefono') or '—',
                '—',          # rutas asignadas (podría consultarse)
            ))
            nombres.append(r['nombre'])

        # Actualizar combo en tab Repartidores
        self.combo_rep_asignar['values'] = nombres
        self._rep_ids = {r['nombre']: str(r['id']) for r in reps}

    def _agregar_repartidor(self):
        nombre = self.entry_rep_nombre.get().strip()
        tel    = self.entry_rep_tel.get().strip()
        if not nombre:
            messagebox.showwarning('Atención', 'El nombre es obligatorio')
            return
        try:
            RepartidorRepo.create(nombre, tel)
            self.entry_rep_nombre.delete(0, tk.END)
            self.entry_rep_tel.delete(0, tk.END)
            self.cargar_repartidores()
            self._status(f'Repartidor "{nombre}" creado')
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _asignar_ruta(self):
        ruta_id_str  = self.entry_ruta_asignar.get().strip()
        nombre_rep   = self.combo_rep_asignar.get()

        if not ruta_id_str or not nombre_rep:
            messagebox.showwarning('Atención', 'Completa ruta ID y repartidor')
            return
        try:
            ruta_id   = int(ruta_id_str)
            rep_id    = getattr(self, '_rep_ids', {}).get(nombre_rep)
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

    # ──────────────────────────────────────────────────────────
    # Lógica: Avances
    # ──────────────────────────────────────────────────────────

    def cargar_avances(self):
        filtro = self.combo_avances_filtro.get()
        try:
            if filtro == 'pendientes':
                avances = AvanceRepo.pendientes()
            elif filtro == 'todos':
                # todos = pendientes + procesados via query directa
                with db.get_cursor() as cur:
                    cur.execute("""
                        SELECT a.*, pe.nombre, rep.nombre AS repartidor_nombre
                        FROM avances a
                        LEFT JOIN personas pe      ON pe.id = a.persona_id
                        LEFT JOIN repartidores rep ON rep.id = a.repartidor_id
                        ORDER BY a.creado_en DESC
                        LIMIT 200
                    """)
                    avances = [dict(r) for r in cur.fetchall()]
            else:
                with db.get_cursor() as cur:
                    cur.execute("""
                        SELECT a.*, pe.nombre, rep.nombre AS repartidor_nombre
                        FROM avances a
                        LEFT JOIN personas pe      ON pe.id = a.persona_id
                        LEFT JOIN repartidores rep ON rep.id = a.repartidor_id
                        WHERE a.estado = 'procesado'
                        ORDER BY a.creado_en DESC LIMIT 200
                    """)
                    avances = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            self.log(f'❌ Error cargando avances: {e}', 'err')
            return

        self.tree_avances.delete(*self.tree_avances.get_children())

        for av in avances:
            tiene_foto = '📷' if av.get('foto_path') else '—'
            ts = ''
            if av.get('creado_en'):
                try:
                    ts = av['creado_en'].strftime('%d/%m %H:%M')
                except Exception:
                    ts = str(av['creado_en'])[:16]

            self.tree_avances.insert('', tk.END,
                iid=str(av['id']),
                values=(
                    str(av['id'])[:8] + '…',
                    av.get('ruta_id', ''),
                    av.get('nombre') or '—',
                    av.get('repartidor_nombre') or '—',
                    av.get('tipo', 'entrega'),
                    av.get('estado', ''),
                    tiene_foto,
                    ts,
                ),
            )

        self.lbl_total_avances.config(text=f'{len(avances)} registros')

    def _marcar_procesados(self):
        sel = self.tree_avances.selection()
        if not sel:
            messagebox.showwarning('Atención', 'Selecciona avances primero')
            return
        for iid in sel:
            try:
                AvanceRepo.marcar_procesado(iid)
            except Exception as e:
                self.log(f'❌ Error marcando {iid}: {e}', 'err')
        self.cargar_avances()
        self._status(f'{len(sel)} avances marcados como procesados')

    # ──────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────

    def _verificar_db(self):
        ok = db.health_check()
        color = COLORES['success'] if ok else COLORES['danger']
        texto = '● DB conectada' if ok else '● DB sin conexión'
        self.lbl_db.config(text=texto, fg=color)
        if ok:
            self.cargar_rutas()
            self.cargar_repartidores()
            self.cargar_avances()
        else:
            self.log('❌ Sin conexión a PostgreSQL — verifica .env', 'err')

    def _init_db(self):
        try:
            db.init_schema()
            self.log('✅ Schema aplicado', 'ok')
            self._verificar_db()
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _ver_geocache(self):
        try:
            stats = GeocacheRepo.stats()
            messagebox.showinfo('Geocaching cache',
                f"Exitosos: {stats.get('exitosos', 0)}\n"
                f"Fallidos: {stats.get('fallidos', 0)}\n"
                f"Total:    {stats.get('total', 0)}"
            )
        except Exception as e:
            messagebox.showerror('Error', str(e))

    def _auto_refresh(self):
        """Refresca rutas y avances cada REFRESH_MS milisegundos."""
        try:
            if db.health_check():
                self.cargar_rutas()
                self.cargar_avances()
        except Exception:
            pass
        self._refresh_job = self.root.after(self.REFRESH_MS, self._auto_refresh)

    def _set_progress(self, msg: str):
        self.root.after(0, lambda: self.lbl_progress.config(text=msg))

    def _status(self, msg: str):
        ts = datetime.now().strftime('%H:%M:%S')
        self.statusbar.config(text=f'[{ts}]  {msg}')

    def _tick_hora(self):
        self.lbl_hora.config(text=datetime.now().strftime('%H:%M:%S'))
        self.root.after(1000, self._tick_hora)

    def log(self, msg: str, tag: str = ''):
        """Agrega línea al log de la pestaña Importar."""
        ts = datetime.now().strftime('%H:%M:%S')

        def _insert():
            self.log_text.insert(tk.END, f'[{ts}] ', 'dim')
            self.log_text.insert(tk.END, f'{msg}\n', tag or '')
            self.log_text.see(tk.END)

        if threading.current_thread() is threading.main_thread():
            _insert()
        else:
            self.root.after(0, _insert)


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-8s  %(name)s  %(message)s',
    )
    root = tk.Tk()
    app  = MainWindow(root)
    root.mainloop()


if __name__ == '__main__':
    main()
