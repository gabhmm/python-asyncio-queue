import asyncio
import logging
from message_payload import ActionType, MessagePayload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)
 
async def produtor( 
    nome: str,
    acoes: list[tuple[ActionType, str]],
    fila: asyncio.Queue[MessagePayload | None],
) -> None:
 
    logger.info(f"[Produtor {nome}] Iniciando geração de mensagens...")

    for action, content in acoes:

        payload: MessagePayload = MessagePayload.create(nome,action,content)

        logger.info(f"[Produtor {nome}] Enfileirando -> {action}: {content}")

        await asyncio.sleep(0.5)
        await fila.put(payload)
    logger.info(f"[Produtor {nome}] Finalizou a produção de mensagens.")
    

async def consumidor(
    worker_id: int,
    fila: asyncio.Queue[MessagePayload | None],
) -> None:

    while True:
        try:
            payload = await fila.get()

            if payload is None:
                logger.info(f"[Consumidor {worker_id}] Recebeu sinal de encerramento. Finalizando.")
                break

            logger.info(f"[Consumidor {worker_id}] Processando mensagem de [{payload.client_id}] | Ação: {payload.action} | Conteúdo: {payload.content}")        
            await asyncio.sleep(0.1)

        finally:
            fila.task_done()


async def main() -> None:

    fila: asyncio.Queue[MessagePayload | None] = asyncio.Queue(maxsize=10)

    acoes_sensor01: list[tuple[ActionType,str]] = [
        ("AUTH", "token_alpha_123"),
        ("QUERY", "SELECT temp FROM sala_01"),
        ("DISCONNECT", "logout"),
    ]

    acoes_sensor02: list[tuple[ActionType,str]] = [
        ("AUTH", "token_alpha_123"),
        ("QUERY", "SELECT temp FROM sala_01"),
        ("DISCONNECT", "logout"),  
    ]


    async with asyncio.TaskGroup() as tg:

        tg.create_task(consumidor(1, fila))
        tg.create_task(consumidor(2, fila))


        tg.create_task(produtor("Sensor-01",acoes_sensor01,fila))
        tg.create_task(produtor("Sensor-02",acoes_sensor02,fila))
        
        await fila.join()

        await fila.put(None)
        await fila.put(None)

if __name__ == "__main__":
    asyncio.run(main())
