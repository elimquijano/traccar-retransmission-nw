# Sistema de Retransmisión de Datos GPS

Un sistema robusto para recibir datos GPS desde Traccar y retransmitirlos a múltiples sistemas externos con diferentes formatos requeridos(solo Linux).

## Descripción General

Este proyecto implementa un sistema que:
1. Se conecta a un servidor Traccar para recibir datos GPS en tiempo real
2. Transforma los datos según los requisitos de cada sistema destino
3. Retransmite los datos a múltiples sistemas externos
4. Registra todas las operaciones y errores para auditoría

## Características Principales

- **Conexión WebSocket con Traccar**: Recepción en tiempo real de datos GPS
- **Múltiples Handlers de Retransmisión**: Adaptación de datos para diferentes sistemas
- **Procesamiento Asíncrono**: Manejo eficiente de múltiples retransmisiones concurrentes
- **Registro de Operaciones**: Logging detallado en base de datos y archivos
- **Respaldo Automático**: Manejo de fallos con respaldo a archivo
- **Reintentos Automáticos**: Para conexiones fallidas

## Requisitos

- Python 3.9+
- MySQL 5.7+
- Servidor Traccar configurado

## Instalación

1. Clonar el repositorio:

```bash
git clone https://github.com/elimquijano/traccar-retransmission-nw.git
cd tu-repositorio
```

2. Crear un entorno virtual:

```bash
python -m venv venv
source venv/bin/activate  # En Linux/Mac
# venv\Scripts\activate  # En Windows
```

3. Instalar las dependencias:

```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

## Configuración

1. Configurar el archivo `.env` con tus credenciales y ajustes

```bash
# Configuración de Traccar
TRACCAR_URL=https://tu-servidor-traccar.com
TRACCAR_EMAIL=tu@email.com
TRACCAR_PASSWORD=tu-contraseña

# Configuración de Base de Datos
DB_CONFIG_HOST=localhost
DB_CONFIG_USER=usuario
DB_CONFIG_PASSWORD=contraseña
DB_CONFIG_NAME=nombre_bd
```

2. Configurar los handlers de retransmisión en app/config.py según los sistemas destino.

## Estructura del Proyecto

```bash
.
├── app/
│   ├── config.py               # Configuraciones principales
│   ├── logger_setup.py          # Configuración del logging
│   ├── constants.py             # Constantes del sistema
│   ├── traccar_client.py        # Cliente para conexión con Traccar
│   ├── persistence/             # Módulos de persistencia
│   │   ├── db_config_loader.py   # Cargador de configuraciones
│   │   └── log_writer_db.py      # Escritor de logs a BD
│   ├── services/                # Servicios principales
│   │   ├── base_retransmission_handler.py  # Handler base
│   │   ├── retransmission_manager.py        # Gestor de retransmisiones
│   │   ├── seguridad_ciudadana_handler.py   # Handler específico
│   │   ├── pnp_handler.py
│   │   ├── comsatel_handler.py
│   │   ├── osinergmin_handler.py
│   │   └── sutran_handler.py
│   └── utils/                   # Utilidades
│       └── helpers.py            # Funciones auxiliares
├── main.py                     # Punto de entrada principal
├── requirements.txt             # Dependencias
└── README.md                    # Este archivo
```

## Uso

1. Iniciar el servicio:

```bash
python main.py
```

2. La aplicación:
    - Se conectará a Traccar
    - Cargará las configuraciones de retransmisión
    - Comenzará a recibir datos GPS
    - Retransmitirá los datos a los sistemas configurados

## Manejo de señales

1. La aplicación responde a:
    - SIGINT (Ctrl+C): Apagado ordenado
    - SIGTERM: Apagado ordenado

## Logging

1. Los logs se generan en:
    - Archivos diarios en el directorio logs/
    - Base de datos (si está configurada)
    - Archivo de respaldo para logs pendientes

## Contribución

1. Hacer fork del proyecto
2. Crear una rama para tu feature (git checkout -b feature/nueva-funcionalidad)
3. Commit tus cambios (git commit -am 'Añadir nueva funcionalidad')
4. Hacer push a la rama (git push origin feature/nueva-funcionalidad)
5. Abrir un Pull Request

## Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo LICENSE para más detalles.

## Copyright

© 2025 Elim Quijano. Todos los derechos reservados.