import asyncio
import json
import time
import uuid
import logging
import random
import glob
from datetime import datetime
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any, Optional, Union
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
import aiohttp
import feedparser
import requests
from bs4 import BeautifulSoup
import os
from queue import Queue, Empty
import threading
import signal

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuración de fuentes para Madrid
MADRID_SOURCES = [
    {
        'id': 'el-economista-rss',
        'name': 'El Economista',
        'type': 'rss',
        'url': 'https://www.eleconomista.es/rss/rss-economia.php'
    },
    {
        'id': 'cinco-dias-rss',
        'name': 'Cinco Días',
        'type': 'rss',
        'url': 'https://cincodias.elpais.com/seccion/rss/mercados/'
    },
    {
        'id': 'expansion-web',
        'name': 'Expansión',
        'type': 'webpage',
        'url': 'https://www.expansion.com/mercados.html',
        'selector': 'article.article'
    }
]

# Configuración de fuentes para Londres
LONDON_SOURCES = [
    {
        'id': 'ft-api',
        'name': 'Financial Times',
        'type': 'api',
        'url': 'https://api.ft.com/content/search',
        'api_key': 'YOUR_FT_API_KEY'
    },
    {
        'id': 'bbc-business-rss',
        'name': 'BBC Business',
        'type': 'rss',
        'url': 'https://feeds.bbci.co.uk/news/business/rss.xml'
    },
    {
        'id': 'the-economist-web',
        'name': 'The Economist',
        'type': 'webpage',
        'url': 'https://www.economist.com/finance-and-economics',
        'selector': '.teaser'
    }
]

# Configuración de fuentes para São Paulo
SAO_PAULO_SOURCES = [
    {
        'id': 'valor-economico-rss',
        'name': 'Valor Econômico',
        'type': 'rss',
        'url': 'https://valor.globo.com/rss/valor'
    },
    {
        'id': 'infomoney-api',
        'name': 'InfoMoney',
        'type': 'api',
        'url': 'https://api.infomoney.com.br/news',
        'api_key': 'YOUR_INFOMONEY_API_KEY'
    },
    {
        'id': 'exame-web',
        'name': 'Exame',
        'type': 'webpage',
        'url': 'https://exame.com/economia/',
        'selector': '.article-card'
    }
]


# ===============================================================
# MODELO DE DATOS
# ===============================================================

@dataclass
class StandardArticle:
    """Formato estandarizado para todos los artículos"""
    id: str
    title: str
    content: str
    source: str
    published_date: str
    url: str
    location: str  # Ubicación del servidor que recolectó la noticia
    metadata: Dict[str, Any]
    timestamp_collected: float


class SourceType(Enum):
    """Tipos de fuentes de noticias"""
    RSS = "rss"
    API = "api"
    WEBPAGE = "webpage"


# ===============================================================
# RECOLECTORES DE FUENTES
# ===============================================================

class NewsSource(ABC):
    """Clase base abstracta para todas las fuentes de noticias"""

    def __init__(self, source_id: str, source_name: str, location: str):
        self.source_id = source_id
        self.source_name = source_name
        self.location = location
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{source_id}")

    @abstractmethod
    async def fetch_articles(self) -> List[Dict[str, Any]]:
        """Obtiene artículos de la fuente en su formato nativo"""
        pass

    @abstractmethod
    async def transform_to_standard(self, raw_articles: List[Dict[str, Any]]) -> List[StandardArticle]:
        """Transforma artículos del formato nativo al formato estándar"""
        pass

    async def get_standardized_articles(self) -> List[StandardArticle]:
        """Método principal para obtener y transformar artículos"""
        try:
            raw_articles = await self.fetch_articles()
            return await self.transform_to_standard(raw_articles)
        except Exception as e:
            self.logger.error(f"Error al obtener artículos de {self.source_name}: {str(e)}")
            return []


class RSSNewsSource(NewsSource):
    """Fuente de noticias basada en RSS"""

    def __init__(self, source_id: str, source_name: str, feed_url: str, location: str):
        super().__init__(source_id, source_name, location)
        self.feed_url = feed_url

    async def fetch_articles(self) -> List[Dict[str, Any]]:
        """Obtiene artículos del feed RSS"""
        try:
            self.logger.info(f"Obteniendo artículos RSS de {self.feed_url}")

            # Usamos un executor para la operación bloqueante de feedparser
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, self.feed_url)

            if not feed.entries:
                self.logger.warning(f"No se encontraron artículos en {self.feed_url}")
                return []

            return feed.entries
        except Exception as e:
            self.logger.error(f"Error al obtener feed RSS {self.feed_url}: {str(e)}")
            raise

    async def transform_to_standard(self, raw_articles: List[Dict[str, Any]]) -> List[StandardArticle]:
        """Transforma entradas RSS al formato estándar"""
        standardized = []

        for entry in raw_articles:
            try:
                # Extraer campos comunes de RSS
                article = StandardArticle(
                    id=str(uuid.uuid4()),
                    title=entry.get('title', 'Sin título'),
                    content=entry.get('summary', entry.get('description', 'Sin contenido')),
                    source=self.source_name,
                    published_date=entry.get('published', datetime.now().isoformat()),
                    url=entry.get('link', ''),
                    location=self.location,
                    metadata={
                        'categories': entry.get('tags', []),
                        'author': entry.get('author', 'Desconocido')
                    },
                    timestamp_collected=time.time()
                )
                standardized.append(article)
            except Exception as e:
                self.logger.error(f"Error al transformar artículo RSS: {str(e)}")

        return standardized


class APINewsSource(NewsSource):
    """Fuente de noticias basada en API REST"""

    def __init__(self, source_id: str, source_name: str, api_url: str,
                 api_key: Optional[str] = None, location: str = None):
        super().__init__(source_id, source_name, location)
        self.api_url = api_url
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}

    async def fetch_articles(self) -> List[Dict[str, Any]]:
        """Obtiene artículos de la API"""
        try:
            self.logger.info(f"Obteniendo artículos API de {self.api_url}")

            async with aiohttp.ClientSession() as session:
                async with session.get(self.api_url, headers=self.headers) as response:
                    response.raise_for_status()
                    data = await response.json()

                    # El formato puede variar según la API, asumimos una estructura común
                    articles = data.get('articles', data.get('items', data.get('results', [])))

                    if not articles:
                        self.logger.warning(f"No se encontraron artículos en la API {self.api_url}")

                    return articles
        except Exception as e:
            self.logger.error(f"Error al obtener artículos de API {self.api_url}: {str(e)}")
            raise

    async def transform_to_standard(self, raw_articles: List[Dict[str, Any]]) -> List[StandardArticle]:
        """Transforma respuestas de API al formato estándar"""
        standardized = []

        for article in raw_articles:
            try:
                # Adaptar según la estructura específica de la API
                standardized.append(StandardArticle(
                    id=article.get('id', str(uuid.uuid4())),
                    title=article.get('title', 'Sin título'),
                    content=article.get('content', article.get('description', 'Sin contenido')),
                    source=self.source_name,
                    published_date=article.get('publishedAt', article.get('date', datetime.now().isoformat())),
                    url=article.get('url', ''),
                    location=self.location,
                    metadata={
                        'author': article.get('author', 'Desconocido'),
                        'source_info': article.get('source', {})
                    },
                    timestamp_collected=time.time()
                ))
            except Exception as e:
                self.logger.error(f"Error al transformar artículo de API: {str(e)}")

        return standardized


class WebpageNewsSource(NewsSource):
    """Fuente de noticias basada en scraping de páginas web"""

    def __init__(self, source_id: str, source_name: str, webpage_url: str,
                 article_selector: str, location: str):
        super().__init__(source_id, source_name, location)
        self.webpage_url = webpage_url
        self.article_selector = article_selector

    async def fetch_articles(self) -> List[Dict[str, Any]]:
        """Obtiene artículos mediante scraping de la página web"""
        try:
            self.logger.info(f"Obteniendo artículos web de {self.webpage_url}")

            async with aiohttp.ClientSession() as session:
                async with session.get(self.webpage_url) as response:
                    response.raise_for_status()
                    html = await response.text()

                    # Procesamiento con BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    article_elements = soup.select(self.article_selector)

                    if not article_elements:
                        self.logger.warning(f"No se encontraron artículos en {self.webpage_url}")
                        return []

                    articles = []
                    for element in article_elements:
                        # Adaptación según la estructura específica del sitio
                        title_el = element.select_one('.title') or element.select_one('h2')
                        content_el = element.select_one('.content') or element.select_one('p')
                        link_el = element.select_one('a')

                        article = {
                            'title': title_el.text.strip() if title_el else 'Sin título',
                            'content': content_el.text.strip() if content_el else 'Sin contenido',
                            'url': link_el['href'] if link_el and link_el.has_attr('href') else '',
                            'date': element.select_one('.date').text if element.select_one('.date') else None
                        }
                        articles.append(article)

                    return articles
        except Exception as e:
            self.logger.error(f"Error al obtener artículos web de {self.webpage_url}: {str(e)}")
            raise

    async def transform_to_standard(self, raw_articles: List[Dict[str, Any]]) -> List[StandardArticle]:
        """Transforma artículos scrapeados al formato estándar"""
        standardized = []

        for article in raw_articles:
            try:
                # Normalizar la URL si es relativa
                url = article['url']
                if url and not url.startswith(('http://', 'https://')):
                    if url.startswith('/'):
                        base_url = '/'.join(self.webpage_url.split('/')[:3])  # http(s)://dominio.com
                        url = f"{base_url}{url}"
                    else:
                        url = f"{self.webpage_url.rstrip('/')}/{url}"

                standardized.append(StandardArticle(
                    id=str(uuid.uuid4()),
                    title=article.get('title', 'Sin título'),
                    content=article.get('content', 'Sin contenido'),
                    source=self.source_name,
                    published_date=article.get('date', datetime.now().isoformat()),
                    url=url,
                    location=self.location,
                    metadata={
                        'scraped': True,
                        'source_url': self.webpage_url
                    },
                    timestamp_collected=time.time()
                ))
            except Exception as e:
                self.logger.error(f"Error al transformar artículo web: {str(e)}")

        return standardized


# ===============================================================
# SISTEMA DE MENSAJERÍA BASADO EN ARCHIVOS (Reemplazo de Kafka)
# ===============================================================

class FileQueueMessageBroker:
    """Implementación de mensajería basada en archivos locales que reemplaza a Kafka"""

    def __init__(self, base_dir: str, client_id: str):
        self.base_dir = base_dir
        self.client_id = client_id
        self.logger = logging.getLogger(f"FileQueueBroker.{client_id}")

        # Crear directorio base si no existe
        os.makedirs(self.base_dir, exist_ok=True)

        # Cola de mensajes pendientes para envío asíncrono
        self.message_queue = Queue()

        # Worker thread para procesamiento asíncrono
        self.worker_thread = None
        self.running = False

        # Iniciar worker thread
        self.start_worker()

    def start_worker(self):
        """Inicia el thread de worker para procesamiento asíncrono"""
        self.running = True
        self.worker_thread = threading.Thread(target=self._process_queue)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        self.logger.info(f"Worker thread iniciado para {self.client_id}")

    def _process_queue(self):
        """Procesa la cola de mensajes en background"""
        while self.running:
            try:
                # Obtener mensaje de la cola con timeout
                message_data = self.message_queue.get(timeout=1.0)
                if message_data:
                    topic, key, value, callback = message_data
                    self._write_message_to_file(topic, key, value, callback)
                self.message_queue.task_done()
            except Empty:
                # No hay mensajes, esperar
                pass
            except Exception as e:
                self.logger.error(f"Error procesando cola de mensajes: {str(e)}")

    def _write_message_to_file(self, topic: str, key: str, value: str, callback: callable):
        """Escribe un mensaje en el archivo correspondiente al topic"""
        try:
            # Crear directorio para el topic si no existe
            topic_dir = os.path.join(self.base_dir, topic)
            os.makedirs(topic_dir, exist_ok=True)

            # Crear nombre de archivo único con timestamp y key
            timestamp = int(time.time() * 1000)
            filename = f"{timestamp}_{key}.json"
            filepath = os.path.join(topic_dir, filename)

            # Escribir mensaje en archivo
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(value)

            # Llamar al callback con éxito
            if callback:
                callback(None, MessageResult(topic, 0, 0, key))

        except Exception as e:
            self.logger.error(f"Error escribiendo mensaje en archivo {filepath}: {str(e)}")
            if callback:
                callback(e, None)

    def delivery_report(self, err, msg):
        """Callback para confirmar entrega de mensajes"""
        if err is not None:
            self.logger.error(f"Error al entregar mensaje: {err}")
        else:
            self.logger.debug(f"Mensaje entregado a {msg.topic} en archivo")

    def publish_article(self, article: StandardArticle, topic: str):
        """Publica un artículo en el topic especificado"""
        try:
            # Serializar el artículo a JSON
            article_json = json.dumps(asdict(article))

            # Encolar para procesamiento asíncrono
            self.message_queue.put((topic, article.id, article_json, self.delivery_report))

        except Exception as e:
            self.logger.error(f"Error al publicar artículo en {topic}: {str(e)}")

    def flush(self, timeout=10):
        """Asegura que todos los mensajes pendientes sean enviados"""
        try:
            # Esperar a que se procesen todos los mensajes en cola
            self.message_queue.join()
            self.logger.debug("Flush completado, todos los mensajes fueron procesados")
        except Exception as e:
            self.logger.error(f"Error en flush: {str(e)}")

    def close(self):
        """Cierra el broker y libera recursos"""
        self.running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        self.logger.info(f"FileQueueMessageBroker {self.client_id} cerrado")


class MessageResult:
    """Clase simuladora del resultado de mensajes de Kafka"""
    def __init__(self, topic: str, partition: int, offset: int, key: str):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key


class FileQueueConsumer:
    """Consumidor de mensajes basado en archivos que simula el comportamiento de Kafka Consumer"""

    def __init__(self, base_dir: str, group_id: str, topics: List[str]):
        self.base_dir = base_dir
        self.group_id = group_id
        self.topics = topics
        self.logger = logging.getLogger(f"FileQueueConsumer.{group_id}")

        # Directorio para el grupo de consumidores
        self.group_dir = os.path.join(self.base_dir, f"consumer-{group_id}")
        os.makedirs(self.group_dir, exist_ok=True)

        # Directorio para los offsets procesados
        self.offset_dir = os.path.join(self.group_dir, "offsets")
        os.makedirs(self.offset_dir, exist_ok=True)

        # Estado interno
        self.running = False
        self.processed_files = set()
        self._load_processed_files()

    def _load_processed_files(self):
        """Carga los archivos ya procesados desde el directorio de offsets"""
        try:
            offset_file = os.path.join(self.offset_dir, "processed_files.json")
            if os.path.exists(offset_file):
                with open(offset_file, 'r', encoding='utf-8') as f:
                    self.processed_files = set(json.load(f))
            self.logger.info(f"Cargados {len(self.processed_files)} archivos procesados anteriormente")
        except Exception as e:
            self.logger.error(f"Error cargando archivos procesados: {str(e)}")

    def _save_processed_files(self):
        """Guarda los archivos procesados en el directorio de offsets"""
        try:
            offset_file = os.path.join(self.offset_dir, "processed_files.json")
            with open(offset_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.processed_files), f)
        except Exception as e:
            self.logger.error(f"Error guardando archivos procesados: {str(e)}")

    def subscribe(self, topics: List[str]):
        """Suscribe el consumidor a los topics especificados"""
        self.topics = topics
        self.logger.info(f"Suscrito a topics: {', '.join(topics)}")

    def poll(self, timeout: float = 1.0) -> Optional[Any]:
        """Obtiene el siguiente mensaje no procesado"""
        if not self.running:
            return None

        start_time = time.time()

        while time.time() - start_time < timeout:
            # Buscar archivos nuevos en cada topic
            for topic in self.topics:
                topic_dir = os.path.join(self.base_dir, topic)

                # Verificar si existe el directorio del topic
                if not os.path.exists(topic_dir):
                    continue

                # Obtener todos los archivos de mensajes ordenados por timestamp
                files = sorted(glob.glob(os.path.join(topic_dir, "*.json")))

                for filepath in files:
                    filename = os.path.basename(filepath)

                    # Saltar archivos ya procesados
                    if filename in self.processed_files:
                        continue

                    try:
                        # Leer contenido del archivo
                        with open(filepath, 'r', encoding='utf-8') as f:
                            value = f.read()

                        # Extraer key del nombre del archivo
                        key = filename.split('_', 1)[1].rsplit('.', 1)[0]

                        # Crear un objeto de mensaje similar a Kafka
                        return MessageWrapper(topic, filename, key, value)

                    except Exception as e:
                        self.logger.error(f"Error leyendo mensaje de {filepath}: {str(e)}")

            # Si no se encontraron mensajes nuevos, esperar un poco
            time.sleep(0.1)

        # No se encontró ningún mensaje en el timeout
        return None

    def commit(self, msg):
        """Marca un mensaje como procesado"""
        if isinstance(msg, MessageWrapper):
            # Añadir a la lista de archivos procesados
            self.processed_files.add(msg.filename)
            # Guardar la lista actualizada
            self._save_processed_files()
            self.logger.debug(f"Commit de mensaje {msg.filename}")

    def close(self):
        """Cierra el consumidor y libera recursos"""
        self.running = False
        self._save_processed_files()
        self.logger.info("Consumidor cerrado")

    def error(self):
        """Devuelve None para simular que no hay error"""
        return None


class MessageWrapper:
    """Wrapper para simular los mensajes de Kafka"""
    def __init__(self, topic: str, filename: str, key: str, value: str):
        self._topic = topic
        self.filename = filename
        self._key = key
        self._value = value
        self._error = None

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error

    def set_error(self, error):
        self._error = error


class KafkaError:
    """Clase simuladora de errores de Kafka"""
    _PARTITION_EOF = "partition_eof"

    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


# ===============================================================
# MODIFICACIONES PARA EL SERVIDOR LOCAL
# ===============================================================

class LocalServer:
    """Servidor local para recolección de noticias en cada ubicación"""

    def __init__(self, location: str, queue_base_dir: str):
        self.location = location
        self.sources: List[NewsSource] = []
        self.logger = logging.getLogger(f"LocalServer.{location}")

        # Crear directorio base si no existe
        os.makedirs(queue_base_dir, exist_ok=True)

        # Inicializar broker de mensajes basado en archivos
        self.message_broker = FileQueueMessageBroker(
            base_dir=queue_base_dir,
            client_id=f"local-server-{location.lower()}"
        )

        # Topic para artículos recolectados
        self.articles_topic = "raw-articles"

        # Control de ejecución
        self.running = False
        self.fetch_interval = 60  # segundos entre recolecciones

    def configure_sources(self, sources_config: List[Dict]):
        """Configura las fuentes de noticias para este servidor"""
        for config in sources_config:
            source_id = config['id']
            source_name = config['name']
            source_type = config['type']

            try:
                if source_type == 'rss':
                    self.sources.append(RSSNewsSource(
                        source_id=source_id,
                        source_name=source_name,
                        feed_url=config['url'],
                        location=self.location
                    ))
                elif source_type == 'api':
                    self.sources.append(APINewsSource(
                        source_id=source_id,
                        source_name=source_name,
                        api_url=config['url'],
                        api_key=config.get('api_key'),
                        location=self.location
                    ))
                elif source_type == 'webpage':
                    self.sources.append(WebpageNewsSource(
                        source_id=source_id,
                        source_name=source_name,
                        webpage_url=config['url'],
                        article_selector=config['selector'],
                        location=self.location
                    ))
                else:
                    self.logger.warning(f"Tipo de fuente desconocido: {source_type} para {source_id}")
            except Exception as e:
                self.logger.error(f"Error configurando fuente {source_id}: {str(e)}")

        self.logger.info(f"Configuradas {len(self.sources)} fuentes para {self.location}")

    async def fetch_from_sources(self):
        """Obtiene artículos de todas las fuentes configuradas"""
        all_articles = []

        for source in self.sources:
            try:
                self.logger.info(f"Obteniendo artículos de {source.source_name}")
                articles = await source.get_standardized_articles()
                self.logger.info(f"Obtenidos {len(articles)} artículos de {source.source_name}")
                all_articles.extend(articles)
            except Exception as e:
                self.logger.error(f"Error obteniendo artículos de {source.source_name}: {str(e)}")

        return all_articles

    async def publish_articles(self, articles: List[StandardArticle]):
        """Publica artículos en el topic configurado"""
        if not articles:
            self.logger.info("No hay artículos para publicar")
            return

        self.logger.info(f"Publicando {len(articles)} artículos en topic {self.articles_topic}")

        for article in articles:
            try:
                self.message_broker.publish_article(article, self.articles_topic)
            except Exception as e:
                self.logger.error(f"Error publicando artículo {article.id}: {str(e)}")

        # Asegurar que todos los mensajes sean procesados
        self.message_broker.flush()
        self.logger.info(f"Publicados {len(articles)} artículos")

    async def run(self):
        """Ejecuta el servidor local en un bucle continuo"""
        self.running = True
        self.logger.info(f"Iniciando servidor local de {self.location}")

        # Configurar manejo de señales para cierre gracioso
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    signal.signal(sig, lambda s, f: asyncio.create_task(self.stop()))
            except NotImplementedError:
                logger.warning("Manejo de señales no soportado en esta plataforma (probablemente Windows)")

        try:
            # Bucle principal de recolección
            while self.running:
                start_time = time.time()

                try:
                    # Obtener artículos de todas las fuentes
                    articles = await self.fetch_from_sources()

                    # Publicar artículos
                    await self.publish_articles(articles)

                except Exception as e:
                    self.logger.error(f"Error en ciclo de recolección: {str(e)}")

                # Calcular tiempo para la próxima recolección
                elapsed = time.time() - start_time
                sleep_time = max(0, self.fetch_interval - elapsed)

                if sleep_time > 0:
                    self.logger.info(f"Esperando {sleep_time:.2f} segundos hasta próxima recolección")
                    await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            self.logger.info("Servidor cancelado")
        finally:
            await self.stop()

    async def stop(self):
        """Detiene el servidor local"""
        if not self.running:
            return

        self.running = False
        self.logger.info(f"Deteniendo servidor local de {self.location}")

        # Cerrar broker de mensajes
        self.message_broker.close()

        self.logger.info(f"Servidor local de {self.location} detenido")


# ===============================================================
# SERVIDOR CENTRAL (Frankfurt)
# ===============================================================

class CentralServer:
    """Servidor central para procesamiento y análisis de noticias"""

    def __init__(self, queue_base_dir: str):
        self.logger = logging.getLogger("CentralServer")
        self.queue_base_dir = queue_base_dir

        # Crear directorio base si no existe
        os.makedirs(queue_base_dir, exist_ok=True)

        # Inicializar consumidor basado en archivos
        self.consumer = FileQueueConsumer(
            base_dir=queue_base_dir,
            group_id='central-server',
            topics=[]
        )

        # Topic para consumir artículos
        self.articles_topic = "raw-articles"

        # Control de ejecución
        self.running = False

        # Inicializar productor para resultados procesados
        self.message_broker = FileQueueMessageBroker(
            base_dir=queue_base_dir,
            client_id="central-server-producer"
        )

        # Topic para resultados de análisis
        self.analysis_topic = "analyzed-articles"

    def process_article(self, article: StandardArticle) -> StandardArticle:
        """Procesa un artículo para análisis"""
        # Aquí iría la lógica de procesamiento y análisis
        # Por ahora solo registramos y devolvemos el mismo artículo
        self.logger.info(f"Procesando artículo: {article.title} de {article.source}")

        # Ejemplo simple de procesamiento: añadir timestamp de procesamiento
        article.metadata['processed_timestamp'] = time.time()
        article.metadata['processed_by'] = 'central-server'

        return article

    def store_article(self, article: StandardArticle):
        """Almacena un artículo procesado"""
        try:
            # Publicar en el topic de análisis
            self.message_broker.publish_article(article, self.analysis_topic)
            self.logger.debug(f"Artículo {article.id} almacenado en topic {self.analysis_topic}")
        except Exception as e:
            self.logger.error(f"Error almacenando artículo {article.id}: {str(e)}")

    def run(self):
        """Ejecuta el servidor central"""
        self.running = True
        self.logger.info("Iniciando servidor central")

        # Suscribirse al topic de artículos
        self.consumer.subscribe([self.articles_topic])
        self.consumer.running = True

        # Configurar manejo de señales para cierre gracioso
        signal.signal(signal.SIGINT, lambda s, f: self.stop())
        signal.signal(signal.SIGTERM, lambda s, f: self.stop())

        try:
            while self.running:
                # Consumir mensaje
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de partición, no es un error
                        continue
                    else:
                        self.logger.error(f"Error de queue: {msg.error()}")
                        continue

                try:
                    # Deserializar artículo
                    article_data = json.loads(msg.value())
                    article = StandardArticle(**article_data)

                    # Procesar y almacenar
                    processed_article = self.process_article(article)
                    self.store_article(processed_article)

                    # Commit manual del offset
                    self.consumer.commit(msg)

                except Exception as e:
                    self.logger.error(f"Error procesando mensaje: {str(e)}")

        except KeyboardInterrupt:
            self.logger.info("Interrupción recibida, deteniendo servidor central")
        finally:
            self.stop()

    def stop(self):
        """Detiene el servidor central"""
        self.running = False
        self.consumer.close()
        self.message_broker.close()
        self.logger.info("Servidor central detenido")

async def run_local_server(location: str, queue_base_dir: str, sources_config: List[Dict]):
    """Configura y ejecuta un servidor local"""
    server = LocalServer(location=location, queue_base_dir=queue_base_dir)
    server.configure_sources(sources_config)
    await server.run()

def run_central_server(queue_base_dir: str):
    server = CentralServer(queue_base_dir=queue_base_dir)
    server.run()


if __name__ == "__main__":
    import sys

    # Configuración del directorio base para las colas
    QUEUE_BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "message_queues")

    # Crear directorio base si no existe
    os.makedirs(QUEUE_BASE_DIR, exist_ok=True)

    if len(sys.argv) < 2:
        print("Uso: python news_system.py [location|central]")
        print("  location: madrid, london, saopaulo")
        print("  central: para ejecutar el servidor central")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "madrid":
        asyncio.run(run_local_server("Madrid", QUEUE_BASE_DIR, MADRID_SOURCES))
    elif mode == "london":
        asyncio.run(run_local_server("London", QUEUE_BASE_DIR, LONDON_SOURCES))
    elif mode == "saopaulo":
        asyncio.run(run_local_server("SaoPaulo", QUEUE_BASE_DIR, SAO_PAULO_SOURCES))
    elif mode == "central":
        run_central_server(QUEUE_BASE_DIR)
    else:
        print(f"Modo desconocido: {mode}")
        print("Opciones válidas: madrid, london, saopaulo, central")
        sys.exit(1)