# KC_BDA
Práctica KeepCoding
PMMG
# Práctica BDA KeepCoding

**Idea general**
Recomendador de restaurantes con un precio inferior a 35€/pax en función de la localización del Airbnb y recomendador de actividades turísticas/ocio disponibles según la fecha y opiniones.

**Nombre del producto**
Viajes BRB (bueno, rico y barato)

**Estrategias del DAaaS**
Sacar un reporte semanal de los 50 mejores sitios para alquilar vía Airbnb según filtros de zona, actividades y restaurantes

**Arquitectura**

El proyecto estará alojado en la nube de Google. El proyecto se compondrá de:

- Dataset de Airbnb: disponible [aquí](https://public.opendatasoft.com/explore/dataset/airbnb-listings/export/?disjunctive.host_verifications&disjunctive.amenities&disjunctive.features&q=Madrid&dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQ09VTlQiLCJ5QXhpcyI6Imhvc3RfbGlzdGluZ3NfY291bnQiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiJyYW5nZS1jdXN0b20ifV0sInhBeGlzIjoiY2l0eSIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6IiIsInNvcnQiOiIiLCJzZXJpZXNCcmVha2Rvd24iOiJyb29tX3R5cGUiLCJjb25maWciOnsiZGF0YXNldCI6ImFpcmJuYi1saXN0aW5ncyIsIm9wdGlvbnMiOnsiZGlzanVuY3RpdmUuaG9zdF92ZXJpZmljYXRpb25zIjp0cnVlLCJkaXNqdW5jdGl2ZS5hbWVuaXRpZXMiOnRydWUsImRpc2p1bmN0aXZlLmZlYXR1cmVzIjp0cnVlfX19XSwidGltZXNjYWxlIjoiIiwiZGlzcGxheUxlZ2VuZCI6dHJ1ZSwiYWxpZ25Nb250aCI6dHJ1ZX0%3D&location=16,41.38377,2.15774&basemap=jawg.streets](https://public.opendatasoft.com/explore/dataset/airbnb-listings/export/?disjunctive.host_verifications&disjunctive.amenities&disjunctive.features&q=Madrid&dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQ09VTlQiLCJ5QXhpcyI6Imhvc3RfbGlzdGluZ3NfY291bnQiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiJyYW5nZS1jdXN0b20ifV0sInhBeGlzIjoiY2l0eSIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6IiIsInNvcnQiOiIiLCJzZXJpZXNCcmVha2Rvd24iOiJyb29tX3R5cGUiLCJjb25maWciOnsiZGF0YXNldCI6ImFpcmJuYi1saXN0aW5ncyIsIm9wdGlvbnMiOnsiZGlzanVuY3RpdmUuaG9zdF92ZXJpZmljYXRpb25zIjp0cnVlLCJkaXNqdW5jdGl2ZS5hbWVuaXRpZXMiOnRydWUsImRpc2p1bmN0aXZlLmZlYXR1cmVzIjp0cnVlfX19XSwidGltZXNjYWxlIjoiIiwiZGlzcGxheUxlZ2VuZCI6dHJ1ZSwiYWxpZ25Nb250aCI6dHJ1ZX0%3D&location=16,41.38377,2.15774&basemap=jawg.streets))  utilizando ";" como delimitador. 
-  Crawler de Eltenedor obtenido con Scrapy  para sacar los restaurantes con un precio inferior a 35€ por persona.  Se ejecutará a mano a través de un Cloud Function cada 15 días y los resultados se almacenarán en Google Cloud Storage.
-  Crawler de Civitatis obtenido con Scrapy  para sacar las actividades disponibles en Madrid. Se ejecutará a mano a través de un Cloud Function cada 15 días y los resultados se almacenarán en Google Cloud Storage.
- Se crearán 3 tablas en HIVE cogiendo los datos del Google Cloud Storage se realizará una query con un JOIN que obtenga los restaurantes más cercanos a cada alojamiento de airbnb y los restaurantes con mejores puntuaciones, y se hará un SELECT con la tabla de actividades. El resultado de la query se subirá a Google Cloud Storage.


**Operating Model**

Se va a ejecutar cada 15 días el Cloud Function que guardará el resultado en un directorio del segmento llamado "pmmg_kc_bda". La estructura de las carpetas será "input/PRODUCTO/AÑO/MES/DIA/HORA/MINUTOcrawl.csv" pudiendo ser PRODUCTO igual a actividades o restaurantes. 

En el segmento siempre habrá un archivo llamado "airbnb-listings.csv".

Cada 15 días se levantará el CLUSTER de forma automática y se enviarán las tareas de:

- Creación de tabla de Eltenedor
- Creación de la tabla de Civitatis
- Creación de la tabla de Airbnb
- load data inpath 'gs://XXXX:input/actividades/...csv' into Civitatis
- load data inpath 'gs://XXXX:input/airbnb-listings.csv' into Airbnb
- load data inpath 'gs://XXXX:input/actividades into Tenedor
- SELECT  JOIN INTO DIRECTORY 'gs://output/resultados'

Idealmente se creará una web con el acceso a los resultados.

### [](#diagrama)Diagrama

[https://docs.google.com/drawings/d/10L6MIQcNVsGOTKsCOHSEqJlxrUDkhbTgT6mlWPSppt0/edit?usp=sharing)

### [](#crawler)Crawlers
- eltenedor
 ``` python
 
from google.cloud import storage
from scrapy.crawler import CrawlerProcess
from datetime import datetime
import scrapy
import json
import tempfile

ARCHIVO_TEMPORAL = tempfile.NamedTemporaryFile(delete=False, mode='w+t') 

def upload_file_to_bucket(bucket_name, blob_file, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(blob_file.name, content_type='text/csv')

class BlogSpider(scrapy.Spider):
    name = 'tenedor'
    start_urls = ['https://www.eltenedor.es/restaurante+madrid#sort=QUALITY_DESC&filters%5BPRICE%5D%5Bmin%5D=0&filters%5BPRICE%5D%5Bmax%5D=35&filters%5BRATE%5D%5Bmin%5D=0']

    count=0
    COUNT_MAX=100

    def parse(self, response):
        for restaurant in response.css('li.resultItem'):
            title_rest = restaurant.css('h3.resultItem-name a ::text').extract_first().strip().replace(',', '').replace('.', '')
            address = restaurant.css('div.resultItem-address ::text').extract_first()
            precio = restaurant.css('div.resultItem-averagePrice ::text').extract_first().strip().split(' ')[2].split('\xa0€')[0]
            puntos = restaurant.css('span.rating-ratingValue ::text').extract_first().replace(',','.')

            ARCHIVO_TEMPORAL.writelines(f"{title_rest};{address};{precio};{puntos}\n")

            for next_page in response.css('li.next a ::attr(href)'):
                yield response.follow(next_page, self.parse)

def lanza(request):
    now = datetime.now() 
    request_json = request.get_json()
    BUCKET_NAME = 'pmmg_kc_bda'
    DESTINATION_FILE_NAME = 'input/restaurante/' + now.strftime("%Y/%m/%d/%H/%M") +'crawl.csv'
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    })
    process.crawl(BlogSpider)
    process.start()
    ARCHIVO_TEMPORAL.seek(0)
    upload_file_to_bucket(BUCKET_NAME, ARCHIVO_TEMPORAL, DESTINATION_FILE_NAME)
    ARCHIVO_TEMPORAL.close()
    return "Amonoh!"
 ```
    
- Civitatis 

```python
from google.cloud import storage
from scrapy.crawler import CrawlerProcess
from datetime import datetime
import scrapy
import json
import tempfile

ARCHIVO_TEMPORAL = tempfile.NamedTemporaryFile(delete=False, mode='w+t') 

def upload_file_to_bucket(bucket_name, blob_file, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(blob_file.name, content_type='text/csv')

class BlogSpider(scrapy.Spider):
    name = 'civitatis'
    start_urls = ['https://www.civitatis.com/es/madrid/']

    count=0
    COUNT_MAX=100

    def parse(self, response):
         for actividades in response.css('div.o-search-list__item'):
            title_act = actividades.css('article a::attr(data-eventlabel)').get(default=None)
            duracion= actividades.css('span.a-feature ::text').get(default=None)
            puntos=actividades.css('span.m-rating--text ::text').get(default=None).replace(',', '.')

            ARCHIVO_TEMPORAL.writelines(f"{title_act};{duracion.strip()};{puntos}\n")

            for next_page in response.css('a.next-element ::attr(href)'):
                yield response.follow(next_page, self.parse)

def lanza(request):
    now = datetime.now() 
    request_json = request.get_json()
    BUCKET_NAME = 'pmmg_kc_bda'
    DESTINATION_FILE_NAME = 'input/actividades/' + now.strftime("%Y/%m/%d/%H/%M") +'crawl.csv'
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    })
    process.crawl(BlogSpider)
    process.start()
    ARCHIVO_TEMPORAL.seek(0)
    upload_file_to_bucket(BUCKET_NAME, ARCHIVO_TEMPORAL, DESTINATION_FILE_NAME)
    ARCHIVO_TEMPORAL.close()
    return "Actividades!"
``` 
