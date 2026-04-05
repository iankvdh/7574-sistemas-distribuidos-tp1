import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=True)

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en {host}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar la cola: {e}")


    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar conexión: {e}")
        
    def send(self, message):
        try: 
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2, # 2 es persistent, lo que hace que el mensaje se guarde en disco y no se pierda si RabbitMQ se reinicia
                )
            )
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar mensaje: {e}")



    
    def start_consuming(self, on_message_callback):
        self._channel.basic_qos(prefetch_count=1) # Asegura que solo se entregue un mensaje a la vez al consumidor para que si llegara a haber más consumidores la carga se reparta entre ellos.

        
        def callback(ch, method, properties, body):
            def ack():
                if ch.is_open:    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            def nack():
                if ch.is_open:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                if ch.is_open:
                    nack()
                print(f"Error en lógica de usuario: {e}")


        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=callback
            )

            self._channel.start_consuming()
            
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError("Conexión perdida durante el consumo")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno en el consumidor: {e}")


    def stop_consuming(self):
            try:
                if self._channel and self._channel.is_open:
                    self._channel.stop_consuming()
            except pika.exceptions.AMQPError:
                raise MessageMiddlewareDisconnectedError("Error al detener el consumo")



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._queue_name = None  # Solo se usará si somos consumidores

        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self._exchange_name, 
                exchange_type='direct', 
                durable=True
            )

            # Lógica de Binding para Consumidores
            if routing_keys:
                result = self._channel.queue_declare(queue='', exclusive=True)
                self._queue_name = result.method.queue

                for key in routing_keys:
                    self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name, routing_key=key)

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en {host}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar Exchange: {e}")     


    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error al cerrar conexión: {e}")
        
    def send(self, message):
        try:
            routing_key = self._routing_keys[0] if self._routing_keys else ''
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2, # 2 es persistent, lo que hace que el mensaje se guarde en disco y no se pierda si RabbitMQ se reinicia
                )
            )
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError("Conexión perdida con RabbitMQ")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno al enviar mensaje: {e}")


    def start_consuming(self, on_message_callback):
        self._channel.basic_qos(prefetch_count=1) # Asegura que solo se entregue un mensaje a la vez al consumidor para que si llegara a haber más consumidores la carga se reparta entre ellos.

        
        def callback(ch, method, properties, body):
            def ack():
                if ch.is_open:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            def nack():
                if ch.is_open:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                if ch.is_open:
                    nack()
                print(f"Error en lógica de usuario (Exchange): {e}")


        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=callback
            )

            self._channel.start_consuming()
            
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
            raise MessageMiddlewareDisconnectedError("Conexión perdida durante el consumo")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error interno en el consumidor: {e}")


    def stop_consuming(self):
            try:
                if self._channel and self._channel.is_open:
                    self._channel.stop_consuming()
            except pika.exceptions.AMQPError:
                raise MessageMiddlewareDisconnectedError("Error al detener el consumo")