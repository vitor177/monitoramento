import time
import os
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Lock, Thread
import re

class MyHandler(FileSystemEventHandler):
    def __init__(self, file_path, db_path):
        self.file_path = file_path
        self.db_path = db_path
        self.table_name = re.sub(r'[^a-zA-Z0-9_]', '_', os.path.basename(file_path).replace('.dat', ''))
        self.lock = Lock()
        
        # Inicializa a conexão com o banco de dados
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_table()
        
        # Processa os dados iniciais
        self.process_initial_data()
        
        # Obtém o tamanho do arquivo após processar os dados iniciais
        self.last_size = os.path.getsize(self.file_path) if os.path.exists(self.file_path) else 0
        
        super().__init__()

    def create_table(self):
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                TIMESTAMP TEXT,
                RECORD INTEGER,
                GHI1 INTEGER,
                GHI2 INTEGER,
                GHI3 INTEGER,
                GRI INTEGER,
                Cell_Isc REAL
            )
        ''')
        self.conn.commit()

    def process_initial_data(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                initial_data = file.read().strip().split('\n')
                if len(initial_data) > 4:
                    initial_data = initial_data[4:]  # Ignora as primeiras quatro linhas
                if initial_data:
                    print(f"Lendo dados iniciais de {self.file_path}:")
                    print(f"Nome da tabela:{self.table_name}")
                    for line in initial_data:
                        print(line)
                    self.save_to_db('\n'.join(initial_data))

    def on_modified(self, event):
        if event.src_path == self.file_path:
            self.process_new_data()

    def process_new_data(self):
        current_size = os.path.getsize(self.file_path)
        if current_size > self.last_size:
            with open(self.file_path, 'r') as file:
                file.seek(self.last_size)
                new_data = file.read().strip()
                if new_data:
                    print(f"Nova linha adicionada em {self.file_path}:")
                    print(f"Nome da tabela:{self.table_name}")
                    print(new_data)
                    self.save_to_db(new_data)
                self.last_size = current_size

    def save_to_db(self, data):
        rows = data.split('\n')
        with self.lock:
            for row in rows:
                values = row.split(',')
                # Preenche os valores faltantes com None
                while len(values) < 7:
                    values.append(None)
                self.cursor.execute(f'''
                    INSERT INTO {self.table_name} (TIMESTAMP, RECORD, GHI1, GHI2, GHI3, GRI, Cell_Isc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', values)
            self.conn.commit()

    def __del__(self):
        self.conn.close()

def start_observer(path, db_path):
    event_handler = MyHandler(file_path=path, db_path=db_path)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(path), recursive=False)
    observer.start()
    return observer, event_handler

def monitor_suspension(observers, handlers, check_interval=1, suspension_threshold=5):
    last_time = time.time()
    while True:
        time.sleep(check_interval)
        current_time = time.time()
        if current_time - last_time > suspension_threshold:
            print("Sistema foi suspenso e reativado. Processando alterações...")
            for handler in handlers:
                handler.process_new_data()
            for observer in observers:
                observer.stop()
            observers.clear()
            for handler in handlers:
                observer, _ = start_observer(handler.file_path, handler.db_path)
                observers.append(observer)
        last_time = current_time

if __name__ == "__main__":

    paths = [
        "Z:/SAO JOAO PIAUI -PI_GHI_seg_SJP_PI.dat",
        "Z:/ILHA SOLTEIRA-SP_GHI_seg_ILHA_SP.dat",
        "Z:/SOUSA-PB_GHI_seg_SOUSA_PB.dat"
        ]

    db_path = 'banco_de_dados_vitinho.db'

    for path in paths:
        if not os.path.exists(path):
            print(f"O arquivo especificado não existe: {path}")
            exit(1)

    observers = []
    handlers = []
    for path in paths:
        observer, handler = start_observer(path, db_path)
        observers.append(observer)
        handlers.append(handler)

    monitor_thread = Thread(target=monitor_suspension, args=(observers, handlers))
    monitor_thread.daemon = True
    monitor_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for observer in observers:
            observer.stop()

    for observer in observers:
        observer.join()
