# 📰 Sistema Distribuido de Análisis de Noticias Financieras

Este proyecto implementa un sistema distribuido asincrónico para la recolección, estandarización, análisis y almacenamiento de noticias económicas **sin necesidad de Kafka**, utilizando archivos locales como sistema de mensajería.

---

## 🚀 ¿Cómo ejecutar el programa?

### ⚙️ Requisitos

- Python 3.9 o superior
- Acceso a Internet (para acceder a fuentes de noticias)
- Compatible con Windows, Linux o macOS

### 📦 Instalación de dependencias

```bash
pip install aiohttp feedparser beautifulsoup4
```

### ▶️ Iniciar servidores

#### 1. Servidor Local (Madrid, Londres o São Paulo)

Recolecta noticias y las guarda en la cola local (`message_queues/raw-articles/`):

```bash
python news_system.py madrid
python news_system.py london
python news_system.py saopaulo
```

#### 2. Servidor Central

Procesa las noticias recolectadas y publica los resultados en `message_queues/analyzed-articles/`:

```bash
python news_system.py central
```

---

## 🧠 ¿Qué hace este sistema?

Este sistema permite:

- Recolectar noticias de múltiples fuentes:
    - RSS (`feedparser`)
    - APIs REST (`aiohttp`)
    - Sitios web (`BeautifulSoup`)
- Unificar los datos en un modelo común: `StandardArticle`
- Guardarlos como archivos `.json` en un "topic" local simulado
- Procesarlos y enriquecerlos con análisis (por ejemplo, timestamp, fuente, etc.)

---

## 🧱 Arquitectura del sistema

```text
[ Fuentes de noticias ]
         ↓
[ Servidor Local ]
         ↓
[ message_queues/raw-articles/ ]
         ↓
[ Servidor Central ]
         ↓
[ message_queues/analyzed-articles/ ]
```

---

## 🗃 Estructura de Archivos

Los artículos se almacenan como archivos `.json` en el directorio `message_queues/`:

```
message_queues/
├── raw-articles/
│   └── 1715288820000_abcd-1234.json
├── analyzed-articles/
│   └── 1715288888888_defg-5678.json
├── consumer-central-server/
│   └── offsets/
│       └── processed_files.json
```

---

## 🧩 Componentes principales

- `LocalServer`: recolecta artículos de noticias y los publica localmente
- `CentralServer`: consume, analiza y vuelve a publicar los artículos
- `FileQueueMessageBroker`: reemplazo de Kafka usando archivos
- `FileQueueConsumer`: simulación de un consumidor Kafka
- `StandardArticle`: modelo de datos estandarizado para todos los artículos

---

## 📌 Notas

- No requiere servicios externos (Kafka, Redis, etc.)
- Basado en `asyncio` para alta eficiencia y concurrencia
- Ideal para entornos de desarrollo, pruebas y educación

---

## 📤 Futuras mejoras

- Análisis de sentimientos real con NLP
- Interfaz web para visualizar artículos procesados
- Persistencia en base de datos (MongoDB, PostgreSQL, etc.)
